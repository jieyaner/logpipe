"""Checkpoint manager for persisting file tail positions across restarts."""

import json
import os
from typing import Dict, Optional


class CheckpointManager:
    """Persists and restores file read positions (offsets) keyed by inode."""

    def __init__(self, checkpoint_path: str) -> None:
        self._path = checkpoint_path
        self._data: Dict[str, int] = self._load()

    def _load(self) -> Dict[str, int]:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {}

    def get_offset(self, inode: int) -> Optional[int]:
        """Return the last saved offset for *inode*, or None if unknown."""
        return self._data.get(str(inode))

    def set_offset(self, inode: int, offset: int) -> None:
        """Update the in-memory offset for *inode*."""
        self._data[str(inode)] = offset

    def save(self) -> None:
        """Flush the current state to disk atomically."""
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh)
        os.replace(tmp, self._path)

    def remove(self, inode: int) -> None:
        """Drop a stale inode entry (e.g. after log rotation)."""
        self._data.pop(str(inode), None)

    def __repr__(self) -> str:  # pragma: no cover
        return f"CheckpointManager(path={self._path!r}, entries={len(self._data)})"
