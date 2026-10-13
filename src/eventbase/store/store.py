"""
Store is the interface for where claim checked content parks.
"""

import abc


class Store(abc.ABC):
    """
    put parks content and returns the json claim that travels on the
    bus; get redeems a claim back into content and metadata. keeps
    says whether the store holds metadata itself. When it cannot, the
    courier sends metadata along the bus instead.
    """

    keeps: bool = True

    @abc.abstractmethod
    async def connect(self) -> None:
        """Open whatever the store needs."""

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Release everything connect opened."""

    @abc.abstractmethod
    async def ping(self) -> bool:
        """True when the store is reachable; may raise instead of False."""

    @abc.abstractmethod
    async def put(self, key: str, content: bytes, metadata: dict) -> dict:
        """Park content under key and return its claim."""

    @abc.abstractmethod
    async def get(self, claim: dict) -> tuple[bytes, dict]:
        """The content and metadata a claim names."""
