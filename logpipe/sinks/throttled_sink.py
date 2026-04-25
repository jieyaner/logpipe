"""A sink decorator that applies rate-limiting to an underlying sink."""

from logpipe.sinks import BaseSink
from logpipe.throttle import TokenBucketThrottle, RateLimitExceeded


class ThrottledSink(BaseSink):
    """Wraps another :class:`BaseSink` and enforces a token-bucket rate limit.

    Records that would exceed the rate limit are *dropped* (not buffered) and
    the ``dropped`` counter is incremented so callers can observe the loss.

    Parameters
    ----------
    sink:
        The underlying sink to forward records to.
    rate:
        Maximum number of records to forward per *period* seconds.
    period:
        Length of the rate-limiting window in seconds (default ``1.0``).
    """

    def __init__(self, sink: BaseSink, rate: int, period: float = 1.0) -> None:
        self._sink = sink
        self._throttle = TokenBucketThrottle(rate=rate, period=period)
        self.dropped: int = 0
        self.forwarded: int = 0

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._throttle.try_acquire():
            self._sink.write(record)
            self.forwarded += 1
        else:
            self.dropped += 1

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    @property
    def available_tokens(self) -> float:
        """Expose the current token count for observability / testing."""
        return self._throttle.available
