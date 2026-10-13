"""
Message is one event as a client sees it.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Message:
    """
    Binary content, json metadata, and an id. receipt is the bus's own
    handle for acknowledging the message and is not meant to be read.
    """

    id: str
    content: bytes
    metadata: dict = field(default_factory=dict)
    receipt: Any = field(default=None, compare=False)

    def __repr__(self) -> str:
        return (
            f"Message(id={self.id!r}, content=<{len(self.content)} bytes>, "
            f"metadata={self.metadata!r})"
        )
