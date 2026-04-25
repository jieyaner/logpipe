"""BufferedSink — wraps another sink and batches writes until a size or
time threshold is reached before flushing downstream."""

import time
from logpipe.sinks import BaseSink


class BufferedSink(BaseSink):
    """Accumulates records in memory and flushes to *inner* when either
    ``max_size`` records have been buffered or ``max_age_seconds`` have
    elapsed since the last flush.

    Args:
        inner: The downstream :class:`~logpipe.sinks.BaseSink` to forward
            records to.
        max_size: Maximum number of records to buffer before an automatic
            flush (default 100).
        max_age_seconds: Maximum number of seconds to hold records before
            an automatic flush (default 5.0).
    """

    def __init__(self, inner: BaseSink, *, max_size: int = 100, max_age_seconds: float = 5.0) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be > 0")

        self._inner = inner
        self._max_size = max_size
        self._max_age = max_age_seconds
        self._buffer: list[dict] = []
        self._last_flush: float = time.monotonic()

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        """Buffer *record*; flush downstream if a threshold is exceeded."""
        self._buffer.append(record)
        if self._should_flush():
            self.flush()

    def flush(self) -> None:
        """Immediately forward all buffered records to the inner sink."""
        if not self._buffer:
            return
        for record in self._buffer:
            self._inner.write(record)
        self._buffer.clear()
        self._last_flush = time.monotonic()
        self._inner.flush()

    def close(self) -> None:
        """Flush any remaining records then close the inner sink."""
        self.flush()
        self._inner.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self._max_size:
            return True
        age = time.monotonic() - self._last_flush
        return age >= self._max_age

    @property
    def buffered_count(self) -> int:
        """Number of records currently held in the buffer."""
        return len(self._buffer)
