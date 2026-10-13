"""
DirectoryStore: a directory as the claim check store.
"""

import asyncio
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from eventbase.layout import place
from eventbase.store.store import Store


def write(location: Path, content: bytes) -> None:
    """
    Write a file atomically: a hidden temporary beside it, renamed into
    place, so readers never see half of it.
    """
    location.parent.mkdir(parents=True, exist_ok=True)
    temporary = location.with_name(f".{location.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, location)
    finally:
        temporary.unlink(missing_ok=True)


def read(location: Path, key: str) -> tuple[bytes, dict]:
    """A file's bytes and what the filesystem knows about it."""
    content = location.read_bytes()
    status = location.stat()
    modified = datetime.fromtimestamp(status.st_mtime, timezone.utc)
    metadata = {
        "key": key,
        "name": location.name,
        "size": status.st_size,
        "modified": modified.isoformat(),
    }
    kind, _ = mimetypes.guess_type(location.name)
    if kind is not None:
        metadata["type"] = kind
    return content, metadata


class DirectoryStore(Store):
    """
    Content is a file under the directory and the claim is its path
    relative to it, so producers and consumers can mount the directory
    in different places. A file cannot hold metadata, so redeemed
    messages carry what the filesystem knows (key, name, size,
    modified, type) and keeps is False. Claims that resolve outside
    the directory are refused.

    layout names the file, as a format string over the published
    metadata's top level fields and key, the message key, which wins
    over a field of the same name: "{tenant}/{key}" files by tenant.
    Publishing without a field the layout names is refused, as is a
    layout that fills to a path outside the directory. With a
    directory bus on the same path, the filled path is the polled
    message's id.
    """

    keeps = False

    def __init__(self, path: str | Path, layout: str = "{key}") -> None:
        self.root = Path(path)
        self.layout = layout

    async def connect(self) -> None:
        """Create the directory if it is missing."""
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True)

    async def disconnect(self) -> None:
        """Nothing is held open."""

    async def ping(self) -> bool:
        """True when the directory exists and is readable and writable."""
        return self.root.is_dir() and os.access(self.root, os.R_OK | os.W_OK)

    def locate(self, key: str) -> Path:
        """The file a key names, refusing keys that escape the directory."""
        root = self.root.resolve()
        location = (root / key).resolve()
        if location == root or not location.is_relative_to(root):
            raise ValueError(f"key {key!r} is outside {root}")
        return location

    async def put(self, key: str, content: bytes, metadata: dict) -> dict:
        """Write the file where layout puts it; metadata cannot be kept here."""
        name = place(self.layout, key, metadata)
        await asyncio.to_thread(write, self.locate(name), content)
        return {"store": "directory", "key": name}

    async def get(self, claim: dict) -> tuple[bytes, dict]:
        """Read the file and stat it."""
        key = str(claim["key"])
        return await asyncio.to_thread(read, self.locate(key), key)
