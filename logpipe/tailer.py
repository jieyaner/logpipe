"""FileTailer — follows a log file and yields new lines, with optional checkpointing."""

import os
import time
from typing import Generator, Optional

from logpipe.checkpoint import CheckpointManager


class FileTailer:
    """Tail a single file, yielding new lines as they appear.

    Parameters
    ----------
    path:
        Absolute or relative path to the file being tailed.
    poll_interval:
        Seconds to sleep between read attempts when no new data is available.
    checkpoint_manager:
        Optional :class:`CheckpointManager` used to persist and restore the
        read offset so tailing can resume after a process restart.
    """

    def __init__(
        self,
        path: str,
        poll_interval: float = 0.5,
        checkpoint_manager: Optional[CheckpointManager] = None,
    ) -> None:
        self.path = path
        self.poll_interval = poll_interval
        self._checkpoint = checkpoint_manager

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_inode(self) -> Optional[int]:
        try:
            return os.stat(self.path).st_ino
        except FileNotFoundError:
            return None

    def _file_was_rotated(self, fh, current_inode: int) -> bool:
        """Return True when the open file handle no longer matches *current_inode*."""
        try:
            return os.fstat(fh.fileno()).st_ino != current_inode
        except OSError:
            return True

    def _resolve_start_offset(self, inode: int, file_size: int) -> int:
        """Return the byte offset at which reading should begin."""
        if self._checkpoint is not None:
            saved = self._checkpoint.get_offset(inode)
            if saved is not None:
                return min(saved, file_size)
        return 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tail(self, max_lines: Optional[int] = None) -> Generator[str, None, None]:
        """Yield lines from the tailed file indefinitely (or up to *max_lines*)."""
        yielded = 0
        inode: Optional[int] = None
        fh = None

        try:
            while max_lines is None or yielded < max_lines:
                current_inode = self._get_inode()

                if current_inode is None:
                    time.sleep(self.poll_interval)
                    continue

                if fh is None or self._file_was_rotated(fh, current_inode):
                    if fh is not None:
                        fh.close()
                        if inode is not None and self._checkpoint is not None:
                            self._checkpoint.remove(inode)
                    inode = current_inode
                    fh = open(self.path, "r", encoding="utf-8", errors="replace")
                    start = self._resolve_start_offset(inode, os.path.getsize(self.path))
                    fh.seek(start)

                line = fh.readline()
                if line:
                    if line.endswith("\n"):
                        line = line.rstrip("\n")
                    if self._checkpoint is not None and inode is not None:
                        self._checkpoint.set_offset(inode, fh.tell())
                    yield line
                    yielded += 1
                else:
                    if self._checkpoint is not None:
                        self._checkpoint.save()
                    time.sleep(self.poll_interval)
        finally:
            if fh is not None:
                fh.close()
