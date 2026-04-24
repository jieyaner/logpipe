"""File tailer module for logpipe.

Provides a FileTailer class that continuously reads new lines
from a log file, similar to `tail -f`.
"""

import os
import time
from typing import Generator, Optional


class FileTailer:
    """Tails a file and yields new lines as they are written."""

    def __init__(self, path: str, poll_interval: float = 0.5, encoding: str = "utf-8") -> None:
        """
        Args:
            path: Absolute or relative path to the file to tail.
            poll_interval: Seconds to wait between polls when no new data is available.
            encoding: File encoding.
        """
        self.path = path
        self.poll_interval = poll_interval
        self.encoding = encoding
        self._offset: int = 0
        self._inode: Optional[int] = None

    def _get_inode(self) -> Optional[int]:
        try:
            return os.stat(self.path).st_ino
        except FileNotFoundError:
            return None

    def _file_was_rotated(self, current_inode: Optional[int]) -> bool:
        return self._inode is not None and current_inode != self._inode

    def tail(self) -> Generator[str, None, None]:
        """Yield new lines from the file indefinitely.

        Handles log rotation by detecting inode changes.
        """
        while True:
            current_inode = self._get_inode()

            if current_inode is None:
                # File does not exist yet; wait and retry
                time.sleep(self.poll_interval)
                continue

            if self._file_was_rotated(current_inode):
                # File was rotated; reset offset to read from the beginning
                self._offset = 0

            self._inode = current_inode

            try:
                with open(self.path, "r", encoding=self.encoding) as fh:
                    fh.seek(self._offset)
                    while True:
                        line = fh.readline()
                        if not line:
                            self._offset = fh.tell()
                            break
                        self._offset = fh.tell()
                        yield line.rstrip("\n")
            except FileNotFoundError:
                self._offset = 0
                self._inode = None

            time.sleep(self.poll_interval)
