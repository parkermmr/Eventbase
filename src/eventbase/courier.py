"""
Courier is the client for an event base: publish, poll, acknowledge.
"""

import asyncio
import json
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from eventbase.bus.bus import Bus
from eventbase.bus.directory_bus import DirectoryBus
from eventbase.bus.kafka_bus import KafkaBus
from eventbase.models.message import Message
from eventbase.models.record import Record
from eventbase.store.directory_store import DirectoryStore
from eventbase.store.store import Store


class Courier:
    """
    Pairs a bus, where events travel, with an optional store, where
    claim checked content parks. Messages are binary content with json
    metadata either way:

        Courier.kafka(topic)                              kafka only
        Courier.kafka(topic, store=S3Store(bucket))       kafka + s3 claim check
        Courier.kafka(topic, store=DirectoryStore(path))  kafka + directory claim check
        Courier.directory(path)                           directory only

    Metadata lives with the content wherever that can hold it: kafka
    headers with no store, s3 user metadata with s3. A file cannot
    hold metadata, so files report what the filesystem knows about
    them, and with kafka in front, published metadata rides the
    headers and merges over it.

    Fetching claimed content is the slow part, so poll redeems a batch
    concurrently, at most parallel at a time, unless told sequential.
    """

    def __init__(self, bus: Bus, store: Store | None = None, parallel: int = 8) -> None:
        if parallel < 1:
            raise ValueError("parallel must be at least 1")
        self.bus = bus
        self.store = store
        self.parallel = parallel
        self.gate = asyncio.Semaphore(parallel)

    @classmethod
    def kafka(
        cls,
        topic: str,
        store: Store | None = None,
        parallel: int = 8,
        **options,
    ) -> "Courier":
        """A kafka topic as the bus; options go to KafkaBus."""
        return cls(KafkaBus(topic, **options), store, parallel)

    @classmethod
    def directory(
        cls,
        path: str | Path,
        prefixes: Iterable[str] = (),
        parallel: int = 8,
        **options,
    ) -> "Courier":
        """A watched directory as both bus and store; options go to DirectoryBus."""
        bus = DirectoryBus(path, prefixes, **options)
        return cls(bus, DirectoryStore(path), parallel)

    def parts(self) -> list[Bus | Store]:
        """The bus, then the store when there is one."""
        return [self.bus] if self.store is None else [self.bus, self.store]

    async def connect(self) -> None:
        """Connect the bus and the store."""
        for part in self.parts():
            await part.connect()

    async def disconnect(self) -> None:
        """Disconnect the store and the bus, even if one fails."""
        try:
            if self.store is not None:
                await self.store.disconnect()
        finally:
            await self.bus.disconnect()

    async def ping(self, timeout: float = 5.0) -> bool:
        """True when the bus and the store both answer within timeout."""
        checks = asyncio.gather(*(part.ping() for part in self.parts()))
        try:
            answers = await asyncio.wait_for(checks, timeout)
        except Exception:
            return False
        return all(answers)

    async def publish(
        self,
        content: bytes,
        metadata: dict | None = None,
        key: str | None = None,
    ) -> str:
        """
        Send content with metadata and return its key, a fresh one
        unless given. With a store, the content parks there first and
        only its claim goes on the bus.
        """
        key = key or uuid4().hex
        metadata = metadata or {}
        if self.store is None:
            await self.bus.publish(key, content, metadata)
            return key
        claim = await self.store.put(key, content, metadata)
        headers = {} if self.store.keeps else metadata
        await self.bus.publish(key, json.dumps(claim).encode(), headers)
        return key

    async def redeem(self, record: Record) -> Message:
        """A record as a message, fetching its content from the store if claimed."""
        if self.store is None:
            return Message(record.key, record.value, record.metadata, record.receipt)
        claim = record.claim
        if claim is None:
            claim = json.loads(record.value)
        async with self.gate:
            content, stored = await self.store.get(claim)
        metadata = {**stored, **record.metadata}
        return Message(record.key, content, metadata, record.receipt)

    async def poll(
        self,
        limit: int = 10,
        sequential: bool = False,
        timeout: float = 1.0,
    ) -> list[Message]:
        """
        Up to limit messages, waiting at most timeout seconds for the
        first. Claimed content is fetched concurrently, at most parallel
        at a time, or one by one when sequential. Messages come back in
        bus order either way.
        """
        records = await self.bus.poll(limit, timeout)
        if sequential:
            return [await self.redeem(record) for record in records]
        return list(await asyncio.gather(*(self.redeem(record) for record in records)))

    async def acknowledge(self, message: Message) -> None:
        """Mark a polled message handled on the bus; stores are left alone."""
        await self.bus.acknowledge(message.receipt)

    async def release(self, messages: list[Message]) -> None:
        """Hand polled, unacknowledged messages back so the next poll redelivers them."""
        await self.bus.release([message.receipt for message in messages])

    async def __aenter__(self) -> "Courier":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.disconnect()
