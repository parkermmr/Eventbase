"""
Flat string encodings of json metadata, for kafka headers and s3 user
metadata. Every top level key becomes its own entry with a json value,
so other tools can read single keys and every value round trips.
"""

import json
from typing import Iterable


def encode(metadata: dict) -> dict[str, str]:
    """Each top level key, its value as compact json."""
    return {
        str(key): json.dumps(value, separators=(",", ":"), default=str)
        for key, value in metadata.items()
    }


def decode(pairs: Iterable[tuple[str, str | bytes | None]]) -> dict:
    """
    The inverse of encode. Values that are not json, as producers that
    never used eventbase tend to send, are kept as plain strings.
    """
    metadata = {}
    for key, value in pairs:
        if value is None:
            metadata[key] = None
            continue
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        try:
            metadata[key] = json.loads(value)
        except ValueError:
            metadata[key] = value
    return metadata
