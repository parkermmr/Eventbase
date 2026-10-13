"""
MemoryBus: an in process stand in for a kafka topic, for tests.
"""

from eventbase import Bus, Record


class MemoryBus(Bus):
    """
    Records live in a list and the receipt is the index. Like kafka,
    polling advances a cursor and release seeks it back.
    """

    def __init__(self) -> None:
        self.records: list[Record] = []
        self.cursor = 0
        self.acknowledged: list = []
        self.connects = 0
        self.failures = 0

    async def connect(self) -> None:
        self.connects += 1

    async def disconnect(self) -> None:
        pass

    async def ping(self) -> bool:
        return True

    async def publish(self, key: str, value: bytes, metadata: dict) -> None:
        receipt = len(self.records)
        self.records.append(Record(key, value, dict(metadata), receipt))

    async def poll(self, limit: int, timeout: float) -> list[Record]:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("broker went away")
        batch = self.records[self.cursor:self.cursor + limit]
        self.cursor += len(batch)
        return batch

    async def acknowledge(self, receipt) -> None:
        self.acknowledged.append(receipt)

    async def release(self, receipts: list) -> None:
        if receipts:
            self.cursor = min(self.cursor, min(receipts))
