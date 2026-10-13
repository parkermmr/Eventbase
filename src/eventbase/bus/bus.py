"""
Bus is the interface for where events travel.
"""

import abc
from typing import Any

from eventbase.models.record import Record


class Bus(abc.ABC):
    """
    A bus publishes values with flat metadata, hands back batches of
    records, and acknowledges them by receipt.
    """

    @abc.abstractmethod
    async def connect(self) -> None:
        """Open whatever the bus needs to publish."""

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Release everything connect and poll opened."""

    @abc.abstractmethod
    async def ping(self) -> bool:
        """True when the bus is reachable; may raise instead of False."""

    @abc.abstractmethod
    async def publish(self, key: str, value: bytes, metadata: dict) -> None:
        """Send one value under key, with metadata alongside."""

    @abc.abstractmethod
    async def poll(self, limit: int, timeout: float) -> list[Record]:
        """Up to limit records, waiting at most timeout seconds for any."""

    @abc.abstractmethod
    async def acknowledge(self, receipt: Any) -> None:
        """Mark the record behind receipt as handled."""

    @abc.abstractmethod
    async def release(self, receipts: list[Any]) -> None:
        """Hand unacknowledged records back so the next poll redelivers them."""
