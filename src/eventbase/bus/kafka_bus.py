"""
KafkaBus: a kafka topic as the bus.
"""

from typing import Any

from eventbase.bus.bus import Bus
from eventbase.metadata import decode, encode
from eventbase.models.record import Record


class KafkaBus(Bus):
    """
    Values carry the content, or a claim when the courier has a store,
    and metadata rides as headers, one per top level key. The consumer
    joins group on the first poll, so a courier that only publishes
    never joins a group. Offsets commit on acknowledge and nowhere
    else. Kafka commits a position, so acknowledging a record also
    acknowledges every earlier record of its partition.

    options go to both the aiokafka producer and consumer (security,
    sasl, ssl_context, client_id). Needs eventbase[kafka].
    """

    def __init__(
        self,
        topic: str,
        bootstrap: str = "localhost:9092",
        group: str = "eventbase",
        earliest: bool = True,
        **options,
    ) -> None:
        self.topic = topic
        self.bootstrap = bootstrap
        self.group = group
        self.earliest = earliest
        self.options = options
        self.committed: dict[int, int] = {}
        self._producer = None
        self._consumer = None

    async def connect(self) -> None:
        """Start the producer."""
        from aiokafka import AIOKafkaProducer

        if self._producer is None:
            producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap, **self.options)
            await producer.start()
            self._producer = producer

    async def consumer(self):
        """The group consumer, started on first use."""
        if self._consumer is None:
            from aiokafka import AIOKafkaConsumer

            consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap,
                group_id=self.group,
                enable_auto_commit=False,
                auto_offset_reset="earliest" if self.earliest else "latest",
                **self.options,
            )
            await consumer.start()
            self._consumer = consumer
        return self._consumer

    async def disconnect(self) -> None:
        """Stop the consumer, then the producer."""
        consumer, self._consumer = self._consumer, None
        if consumer is not None:
            await consumer.stop()
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
        self.committed.clear()

    async def ping(self) -> bool:
        """Fetch cluster metadata through the producer's connection."""
        if self._producer is None:
            return False
        await self._producer.client.fetch_all_metadata()
        return True

    async def publish(self, key: str, value: bytes, metadata: dict) -> None:
        """Send and wait for the broker to accept the record."""
        if self._producer is None:
            raise RuntimeError("connect before publishing")
        headers = [(name, text.encode()) for name, text in encode(metadata).items()]
        await self._producer.send_and_wait(
            self.topic, value, key=key.encode(), headers=headers
        )

    async def poll(self, limit: int, timeout: float) -> list[Record]:
        """One fetch of up to limit records, in offset order per partition."""
        consumer = await self.consumer()
        batches = await consumer.getmany(
            timeout_ms=int(timeout * 1000), max_records=limit
        )
        records = []
        for partition, items in batches.items():
            for item in items:
                key = f"{self.topic}-{partition.partition}-{item.offset}"
                if item.key:
                    key = item.key.decode()
                records.append(
                    Record(
                        key=key,
                        value=item.value or b"",
                        metadata=decode(item.headers or ()),
                        receipt=(partition, item.offset),
                    )
                )
        return records

    async def acknowledge(self, receipt: Any) -> None:
        """Commit the position after the record, unless already past it."""
        if self._consumer is None:
            raise RuntimeError("nothing polled to acknowledge")
        partition, offset = receipt
        position = offset + 1
        if position <= self.committed.get(partition.partition, 0):
            return
        await self._consumer.commit({partition: position})
        self.committed[partition.partition] = position

    async def release(self, receipts: list[Any]) -> None:
        """Seek each partition back to its earliest released offset."""
        if self._consumer is None:
            return
        earliest = {}
        for partition, offset in receipts:
            earliest[partition] = min(offset, earliest.get(partition, offset))
        for partition, offset in earliest.items():
            self._consumer.seek(partition, offset)
