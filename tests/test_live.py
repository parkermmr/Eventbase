"""
Round trips against a real broker and bucket. Set EVENTBASE_KAFKA to a
bootstrap address and EVENTBASE_S3 to an s3 endpoint (with the usual
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) to run them.
"""

import asyncio
import os
from uuid import uuid4

import pytest

from eventbase import Courier, DirectoryStore, Runner, S3Store

KAFKA = os.environ.get("EVENTBASE_KAFKA")
S3 = os.environ.get("EVENTBASE_S3")
BUCKET = os.environ.get("EVENTBASE_BUCKET", "eventbase")

kafka = pytest.mark.skipif(not KAFKA, reason="EVENTBASE_KAFKA not set")
s3 = pytest.mark.skipif(not (KAFKA and S3), reason="EVENTBASE_KAFKA and EVENTBASE_S3 not set")


async def drain(courier: Courier, count: int) -> list:
    """Poll until count messages arrive or ten seconds pass."""
    messages = []
    for _ in range(20):
        messages += await courier.poll(limit=count, timeout=0.5)
        if len(messages) >= count:
            break
    return messages


async def round_trip(store=None) -> list:
    """Publish three messages on a fresh topic, read and acknowledge them."""
    topic = f"eventbase-{uuid4().hex[:8]}"
    group = f"group-{uuid4().hex[:8]}"
    sent = {}
    async with Courier.kafka(topic, store=store, bootstrap=KAFKA, group=group) as courier:
        assert await courier.ping(timeout=5)
        for index in range(3):
            key = await courier.publish(f"body-{index}".encode(), {"index": index, "tags": ["x"]})
            sent[key] = index
        messages = await drain(courier, 3)
        for message in messages:
            await courier.acknowledge(message)
    async with Courier.kafka(topic, store=store, bootstrap=KAFKA, group=group) as again:
        assert await again.poll(timeout=2) == []
    assert {message.id for message in messages} == set(sent)
    for message in messages:
        assert message.content == f"body-{sent[message.id]}".encode()
        assert message.metadata["index"] == sent[message.id]
        assert message.metadata["tags"] == ["x"]
    return messages


@kafka
def test_kafka_only():
    asyncio.run(round_trip())


@kafka
def test_kafka_directory_claim_check(tmp_path):
    messages = asyncio.run(round_trip(DirectoryStore(tmp_path)))
    assert all("size" in message.metadata for message in messages)


@s3
def test_kafka_s3_claim_check():
    async def scenario():
        store = S3Store(BUCKET, prefix=uuid4().hex[:8], endpoint=S3, region="us-east-1")
        await store.connect()
        try:
            await store._client.create_bucket(Bucket=BUCKET)
        except Exception:
            pass
        await store.disconnect()
        return await round_trip(store)

    asyncio.run(scenario())


@kafka
def test_ping_fails_fast_without_broker():
    async def scenario():
        courier = Courier.kafka("none", bootstrap="127.0.0.1:1")
        return await courier.ping(timeout=1)

    assert asyncio.run(scenario()) is False


@kafka
def test_runner_retries_through_kafka_seek():
    async def scenario():
        topic = f"eventbase-{uuid4().hex[:8]}"
        async with Courier.kafka(topic, bootstrap=KAFKA) as producer:
            for index in range(3):
                await producer.publish(str(index).encode(), key=str(index))
        seen = []

        async def handle(message):
            seen.append(message.id)
            if message.id == "1" and seen.count("1") == 1:
                raise RuntimeError("flaky")
            if message.id == "2":
                runner.stop()

        courier = Courier.kafka(topic, bootstrap=KAFKA, group=f"group-{uuid4().hex[:8]}")
        runner = Runner(courier, handle, pause=0.1, timeout=0.5)
        await asyncio.wait_for(runner.run(signals=False), 30)
        return seen

    assert asyncio.run(scenario()) == ["0", "1", "1", "2"]
