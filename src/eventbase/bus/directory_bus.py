"""
DirectoryBus: a watched directory as the bus.
"""

import asyncio
import os
from pathlib import Path
from typing import Any, Iterable

from eventbase.bus.bus import Bus
from eventbase.models.record import Record


def scan(root: Path, done: Path) -> list[str]:
    """
    Every visible file under root as a path relative to it, oldest
    first. Hidden names are skipped, which covers writes still in
    flight, and so is the done folder.
    """
    found = []
    for folder, names, files in os.walk(root):
        names[:] = [
            name for name in names
            if not name.startswith(".") and Path(folder, name) != done
        ]
        for name in files:
            if name.startswith("."):
                continue
            location = Path(folder, name)
            try:
                modified = location.stat().st_mtime_ns
            except FileNotFoundError:
                continue
            found.append((modified, location.relative_to(root).as_posix()))
    found.sort()
    return [key for _, key in found]


def move(source: Path, target: Path) -> None:
    """Move a file, creating the target's folders."""
    target.parent.mkdir(parents=True, exist_ok=True)
    os.replace(source, target)


class DirectoryBus(Bus):
    """
    Every file in the directory is an event and the file is its own
    content, so records name it in a claim for a DirectoryStore on the
    same path to redeem, and publish has nothing to send once that
    store has written the file. Any file is picked up, whoever wrote
    it; prefixes, matched against the path relative to the directory
    ("invoice_" or "incoming/"), narrow that to the ones accepted.

    acknowledge moves a file into done, keeping its relative path.
    Files polled but not yet acknowledged are not handed out again by
    this bus until released or the bus reconnects. When nothing is waiting, poll rescans every interval
    seconds until its timeout.
    """

    def __init__(
        self,
        path: str | Path,
        prefixes: Iterable[str] = (),
        done: str = ".done",
        interval: float = 0.5,
    ) -> None:
        self.root = Path(path)
        self.prefixes = tuple(prefixes)
        self.done = self.root / done
        self.interval = interval
        self.pending: set[str] = set()

    async def connect(self) -> None:
        """Create the directory if it is missing."""
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True)

    async def disconnect(self) -> None:
        """Forget what was handed out; unacknowledged files stay put."""
        self.pending.clear()

    async def ping(self) -> bool:
        """True when the directory exists and is readable and writable."""
        return self.root.is_dir() and os.access(self.root, os.R_OK | os.W_OK)

    async def publish(self, key: str, value: bytes, metadata: dict) -> None:
        """The store already wrote the file, which is the event."""

    def accepts(self, key: str) -> bool:
        """Whether a file is new to this bus and matches the prefixes."""
        if key in self.pending:
            return False
        return not self.prefixes or key.startswith(self.prefixes)

    async def poll(self, limit: int, timeout: float) -> list[Record]:
        """Up to limit of the oldest accepted files."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            keys = await asyncio.to_thread(scan, self.root, self.done)
            chosen = [key for key in keys if self.accepts(key)][:limit]
            if chosen or loop.time() >= deadline:
                break
            await asyncio.sleep(min(self.interval, deadline - loop.time()))
        self.pending.update(chosen)
        return [
            Record(
                key=key,
                value=b"",
                metadata={},
                receipt=key,
                claim={"store": "directory", "key": key},
            )
            for key in chosen
        ]

    async def acknowledge(self, receipt: Any) -> None:
        """Move the file into the done folder."""
        await asyncio.to_thread(move, self.root / receipt, self.done / receipt)
        self.pending.discard(receipt)

    async def release(self, receipts: list[Any]) -> None:
        """Let the files be polled again."""
        self.pending.difference_update(receipts)
