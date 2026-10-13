import asyncio
import os

import pytest

from eventbase import Courier, DirectoryStore


def run(coroutine):
    return asyncio.run(coroutine)


def test_publish_poll_acknowledge(tmp_path):
    async def scenario():
        async with Courier.directory(tmp_path) as courier:
            assert await courier.ping(timeout=1)
            key = await courier.publish(b"hello", {"ignored": True}, key="a.txt")
            messages = await courier.poll(limit=5, timeout=0)
            assert [message.id for message in messages] == [key]
            message = messages[0]
            assert message.content == b"hello"
            assert message.metadata["size"] == 5
            assert message.metadata["type"] == "text/plain"
            assert "ignored" not in message.metadata
            assert await courier.poll(timeout=0) == []
            await courier.acknowledge(message)
        assert not (tmp_path / "a.txt").exists()
        assert (tmp_path / ".done" / "a.txt").read_bytes() == b"hello"

    run(scenario())


def test_picks_up_foreign_files_oldest_first(tmp_path):
    (tmp_path / "nested").mkdir()
    for index, name in enumerate(["second", "nested/first", "third"]):
        location = tmp_path / name
        location.write_bytes(name.encode())
        stamp = 1_000_000 + [1, 0, 2][index]
        os.utime(location, (stamp, stamp))
    (tmp_path / ".hidden").write_bytes(b"skip")

    async def scenario():
        async with Courier.directory(tmp_path) as courier:
            first = await courier.poll(limit=2, timeout=0)
            rest = await courier.poll(limit=2, timeout=0)
            return [message.id for message in first], [message.id for message in rest]

    assert run(scenario()) == (["nested/first", "second"], ["third"])


def test_prefixes_filter(tmp_path):
    for name in ["invoice_1.pdf", "receipt_1.pdf", "invoice_2.pdf"]:
        (tmp_path / name).write_bytes(b"x")

    async def scenario():
        async with Courier.directory(tmp_path, prefixes=["invoice_"]) as courier:
            return sorted(message.id for message in await courier.poll(timeout=0))

    assert run(scenario()) == ["invoice_1.pdf", "invoice_2.pdf"]


def test_poll_waits_for_a_file(tmp_path):
    async def scenario():
        async with Courier.directory(tmp_path, interval=0.05) as courier:
            async def later():
                await asyncio.sleep(0.1)
                await courier.publish(b"late", key="late")

            task = asyncio.create_task(later())
            messages = await courier.poll(timeout=2)
            await task
            return [message.id for message in messages]

    assert run(scenario()) == ["late"]


def test_unacknowledged_files_return_after_reconnect(tmp_path):
    (tmp_path / "a").write_bytes(b"x")

    async def scenario():
        courier = Courier.directory(tmp_path)
        await courier.connect()
        assert len(await courier.poll(timeout=0)) == 1
        await courier.disconnect()
        await courier.connect()
        assert len(await courier.poll(timeout=0)) == 1
        await courier.disconnect()

    run(scenario())


def test_claims_cannot_escape(tmp_path):
    store = DirectoryStore(tmp_path / "store")
    with pytest.raises(ValueError):
        run(store.get({"key": "../outside"}))
    with pytest.raises(ValueError):
        run(store.put("/etc/passwd", b"x", {}))
