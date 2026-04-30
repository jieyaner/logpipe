"""Rate-limiting sink that drops records exceeding a per-second threshold."""

import time
from collections import deque
from logpipe.sinks import BaseSink


class RateLimitExceeded(Exception):
    """Raised when a record is dropped due to rate limiting."""


class RateLimitSink(BaseSink):
    """Forwards records to *inner* only when the rolling-window rate is within
    *max_per_second*.  Excess records are silently dropped (or optionally raise
    :class:`RateLimitExceeded` when *raise_on_drop* is ``True``).

    Uses a sliding-window counter keyed on wall-clock seconds so no background
    thread is required.
    """

    def __init__(self, inner: BaseSink, max_per_second: int, raise_on_drop: bool = False):
        if max_per_second <= 0:
            raise ValueError("max_per_second must be a positive integer")
        self._inner = inner
        self._max = max_per_second
        self._raise = raise_on_drop
        # Each entry is the timestamp (float) of an accepted record.
        self._window: deque = deque()

    def _evict_old(self, now: float) -> None:
        """Remove timestamps older than 1 second from the sliding window."""
        cutoff = now - 1.0
        while self._window and self._window[0] <= cutoff:
            self._window.popleft()

    def write(self, record: dict) -> None:
        now = time.monotonic()
        self._evict_old(now)
        if len(self._window) >= self._max:
            if self._raise:
                raise RateLimitExceeded(
                    f"Rate limit of {self._max} records/s exceeded"
                )
            return  # drop silently
        self._window.append(now)
        self._inner.write(record)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
