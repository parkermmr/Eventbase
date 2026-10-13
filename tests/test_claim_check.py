import asyncio
import json

from eventbase import Courier, DirectoryStore, Store
from memory_bus import MemoryBus


class SlowStore(Store):
    """A store that records how many gets overlap."""

    def __init__(self) -> None:
        self.active = 0
        self.peak = 0
        self.order: list[str] = []

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def ping(self) -> bool:
        return True

    async def put(self, key: str, content: bytes, metadata: dict) -> dict:
        return {"key": key}

    async def get(self, claim: dict) -> tuple[bytes, dict]:
        self.active += 1
        self.peak = max(self.peak, self.active)
        await asyncio.sleep(0.02 if claim["key"] != "0" else 0.06)
        self.order.append(claim["key"])
        self.active -= 1
        return claim["key"].encode(), {}


def test_kafka_only_shape():
    async def scenario():
        bus = MemoryBus()
        courier = Courier(bus)
        await courier.publish(b"\x00\x01", {"kind": "raw"}, key="k")
        assert bus.records[0].value == b"\x00\x01"
        assert bus.records[0].metadata == {"kind": "raw"}
        [message] = await courier.poll()
        assert (message.id, message.content, message.metadata) == ("k", b"\x00\x01", {"kind": "raw"})
        await courier.acknowledge(message)
        assert bus.acknowledged == [0]

    asyncio.run(scenario())


def test_directory_claim_check(tmp_path):
    async def scenario():
        bus = MemoryBus()
        async with Courier(bus, DirectoryStore(tmp_path)) as courier:
            key = await courier.publish(b"payload", {"trace": "abc"})
            claim = json.loads(bus.records[0].value)
            assert claim == {"store": "directory", "key": key}
            assert bus.records[0].metadata == {"trace": "abc"}
            [message] = await courier.poll()
            assert message.content == b"payload"
            assert message.metadata["trace"] == "abc"
            assert message.metadata["size"] == 7
            await courier.acknowledge(message)
        assert (tmp_path / key).exists()

    asyncio.run(scenario())


def test_parallel_is_capped_and_ordered():
    async def scenario(sequential: bool):
        bus = MemoryBus()
        store = SlowStore()
        courier = Courier(bus, store, parallel=3)
        for index in range(8):
            await courier.publish(b"", key=str(index))
        messages = await courier.poll(limit=8, sequential=sequential)
        return [message.content for message in messages], store

    contents, store = asyncio.run(scenario(False))
    assert contents == [str(index).encode() for index in range(8)]
    assert store.peak == 3
    assert store.order[0] != "0"

    contents, store = asyncio.run(scenario(True))
    assert contents == [str(index).encode() for index in range(8)]
    assert store.peak == 1
    assert store.order == [str(index) for index in range(8)]
