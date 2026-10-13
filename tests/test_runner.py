import asyncio
import time

from eventbase import Courier, Runner
from memory_bus import MemoryBus


def run(runner: Runner) -> None:
    asyncio.run(asyncio.wait_for(runner.run(signals=False), 5))


async def fill(courier: Courier, count: int) -> None:
    await courier.connect()
    for index in range(count):
        await courier.publish(str(index).encode(), key=str(index))


def test_directory_runs_until_stopped(tmp_path):
    courier = Courier.directory(tmp_path / "inbox")
    seen = []

    async def handle(message):
        seen.append(message.id)
        if len(seen) == 3:
            runner.stop()

    runner = Runner(courier, handle, timeout=0.1)
    asyncio.run(fill(courier, 3))
    run(runner)
    assert sorted(seen) == ["0", "1", "2"]
    assert sorted(path.name for path in (tmp_path / "inbox" / ".done").iterdir()) == ["0", "1", "2"]


def test_failure_retries_in_order():
    bus = MemoryBus()
    courier = Courier(bus)
    seen = []

    async def handle(message):
        seen.append(message.id)
        if message.id == "1" and seen.count("1") < 3:
            raise RuntimeError("flaky")
        if seen.count("2"):
            runner.stop()

    runner = Runner(courier, handle, retries=3, pause=0.01)
    asyncio.run(fill(courier, 3))
    run(runner)
    assert seen == ["0", "1", "1", "1", "2"]
    assert bus.acknowledged == [0, 1, 2]


def test_poison_goes_to_dead_letter(tmp_path):
    bus = MemoryBus()
    courier = Courier(bus)
    dead = Courier.directory(tmp_path / "dead")

    async def handle(message):
        if message.id == "0":
            raise ValueError("bad payload")
        runner.stop()

    runner = Runner(courier, handle, retries=1, dead=dead, pause=0.01)
    asyncio.run(fill(courier, 2))
    run(runner)
    assert bus.acknowledged == [0, 1]
    assert (tmp_path / "dead" / "0").read_bytes() == b"0"
    assert runner.attempts == {}


def test_reconnects_after_bus_failure():
    bus = MemoryBus()
    bus.failures = 2
    courier = Courier(bus)

    async def handle(message):
        runner.stop()

    runner = Runner(courier, handle, pause=0.01)
    asyncio.run(fill(courier, 1))
    run(runner)
    assert bus.connects == 4
    assert bus.acknowledged == [0]


def test_concurrency_and_sync_handlers():
    bus = MemoryBus()
    courier = Courier(bus)
    active = []
    peak = []

    def handle(message):
        active.append(message.id)
        peak.append(len(active))
        time.sleep(0.05)
        active.remove(message.id)
        if message.id == "7":
            runner.stop()

    runner = Runner(courier, handle, limit=8, concurrency=4)
    asyncio.run(fill(courier, 8))
    run(runner)
    assert max(peak) == 4
    assert bus.acknowledged == list(range(8))


def test_outputs_go_to_sink_before_acknowledging(tmp_path):
    bus = MemoryBus()
    courier = Courier(bus)
    sink = Courier.directory(tmp_path / "out", layout="{tenant}/{key}")

    async def handle(message):
        if message.id == "0":
            return message.content + b"!"
        if message.id == "1":
            return None
        runner.stop()
        return [b"a", (b"b", {"tenant": "other"})]

    async def setup():
        await courier.connect()
        for index in range(3):
            await courier.publish(str(index).encode(), {"tenant": "acme"}, key=str(index))

    runner = Runner(courier, handle, sink=sink)
    asyncio.run(setup())
    run(runner)
    assert bus.acknowledged == [0, 1, 2]
    assert (tmp_path / "out" / "acme" / "0").read_bytes() == b"0!"
    assert (tmp_path / "out" / "acme" / "2-0").read_bytes() == b"a"
    assert (tmp_path / "out" / "other" / "2-1").read_bytes() == b"b"
    assert not (tmp_path / "out" / "acme" / "1").exists()


def test_sink_failure_reconnects_without_spending_retries():
    class FlakyBus(MemoryBus):
        async def publish(self, key, value, metadata):
            if self.failures:
                self.failures -= 1
                raise ConnectionError("sink went away")
            await super().publish(key, value, metadata)

    bus = MemoryBus()
    courier = Courier(bus)
    out = FlakyBus()
    out.failures = 1

    async def handle(message):
        if not out.failures:
            runner.stop()
        return message.content

    runner = Runner(courier, handle, sink=Courier(out), retries=0, pause=0.01)
    asyncio.run(fill(courier, 1))
    run(runner)
    assert bus.acknowledged == [0]
    assert [record.key for record in out.records] == ["0"]
    assert out.connects == 2
    assert runner.attempts == {}


def test_bad_output_is_a_handler_failure(tmp_path):
    bus = MemoryBus()
    courier = Courier(bus)
    dead = Courier.directory(tmp_path / "dead")

    async def handle(message):
        runner.stop()
        return "not bytes"

    runner = Runner(courier, handle, sink=Courier(MemoryBus()), retries=0, dead=dead)
    asyncio.run(fill(courier, 1))
    run(runner)
    assert bus.acknowledged == [0]
    assert (tmp_path / "dead" / "0").read_bytes() == b"0"
