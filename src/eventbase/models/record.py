"""
Record is an event as it travelled on a bus, before any claim is redeemed.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Record:
    """
    What a bus hands the courier: the key, the value and flat metadata
    as they travelled, plus the receipt the bus needs to acknowledge
    it. A bus that is its own store, like a watched directory, names
    its content in claim instead of carrying it in value.
    """

    key: str
    value: bytes
    metadata: dict
    receipt: Any
    claim: dict | None = None
