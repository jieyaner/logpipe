"""Pipeline: wires together a FileTailer, parser, and Router into a
runnable unit that can be ticked or run in a background thread."""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from logpipe.checkpoint import CheckpointManager
from logpipe.parser import BaseParser
from logpipe.router import Router
from logpipe.tailer import FileTailer

logger = logging.getLogger(__name__)


class Pipeline:
    """Ties a single log file to a parser and a router."""

    def __init__(
        self,
        path: str,
        parser: BaseParser,
        router: Router,
        checkpoint_manager: Optional[CheckpointManager] = None,
        poll_interval: float = 1.0,
    ) -> None:
        self._path = path
        self._parser = parser
        self._router = router
        self._checkpoint = checkpoint_manager
        self._poll_interval = poll_interval
        self._tailer = FileTailer(path, checkpoint_manager=checkpoint_manager)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tick(self) -> int:
        """Read all currently available lines, parse, and route them.
        Returns the number of records forwarded."""
        forwarded = 0
        for raw_line in self._tailer.readlines():
            try:
                record = self._parser.parse(raw_line)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Parse error in %s: %s", self._path, exc)
                continue
            self._router.route(record)
            forwarded += 1
        return forwarded

    def flush(self) -> None:
        """Ask all sinks reachable via the router to flush."""
        self._router.flush()

    def start(self) -> None:
        """Start a background polling thread."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Pipeline already running")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        logger.debug("Pipeline started for %s", self._path)
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                logger.error("Unexpected error in pipeline loop: %s", exc)
            self._stop_event.wait(timeout=self._poll_interval)
        self.flush()
        logger.debug("Pipeline stopped for %s", self._path)
