from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class StoredMedia:
    path: str
    size_bytes: int
    sha256: str


class MediaStorage(Protocol):
    async def store(self, relative_path: str, content: bytes) -> StoredMedia: ...
    async def exists(self, relative_path: str) -> bool: ...


class LocalMediaStorage:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def _resolve(self, relative_path: str) -> Path:
        path = (self.root / relative_path).resolve()
        if self.root not in path.parents:
            raise ValueError("media path escapes storage root")
        return path

    async def store(self, relative_path: str, content: bytes) -> StoredMedia:
        path = self._resolve(relative_path)
        digest = hashlib.sha256(content).hexdigest()

        def write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            temporary = path.with_suffix(path.suffix + ".part")
            temporary.write_bytes(content)
            os.chmod(temporary, 0o640)
            temporary.replace(path)

        await asyncio.to_thread(write)
        return StoredMedia(
            path=str(path.relative_to(self.root)), size_bytes=len(content), sha256=digest
        )

    async def exists(self, relative_path: str) -> bool:
        path = self._resolve(relative_path)
        def usable() -> bool:
            try:
                return path.is_file() and path.stat().st_size > 0
            except OSError:
                return False
        return await asyncio.to_thread(usable)
