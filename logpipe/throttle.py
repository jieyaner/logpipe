"""Rate-limiting / throttle utilities for logpipe sinks."""

import time
from collections import deque
from typing import Optional


class RateLimitExceeded(Exception):
    """Raised when a sink exceeds its configured rate limit."""


class TokenBucketThrottle:
    """Token-bucket rate limiter.

    Allows up to *rate* events per *period* seconds.  Thread-safety is the
    caller's responsibility; this class is intentionally single-threaded.
    """

    def __init__(self, rate: int, period: float = 1.0) -> None:
        if rate <= 0:
            raise ValueError("rate must be a positive integer")
        if period <= 0:
            raise ValueError("period must be a positive number")
        self.rate = rate
        self.period = period
        self._tokens: float = float(rate)
        self._last_refill: float = time.monotonic()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self.rate),
            self._tokens + elapsed * (self.rate / self.period),
        )
        self._last_refill = now

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, tokens: int = 1) -> None:
        """Consume *tokens* from the bucket.

        Raises :class:`RateLimitExceeded` if there are not enough tokens
        available right now.
        """
        self._refill()
        if self._tokens < tokens:
            raise RateLimitExceeded(
                f"rate limit exceeded: need {tokens} token(s), "
                f"{self._tokens:.2f} available (limit {self.rate}/{self.period}s)"
            )
        self._tokens -= tokens

    def try_acquire(self, tokens: int = 1) -> bool:
        """Like :meth:`acquire` but returns *False* instead of raising."""
        try:
            self.acquire(tokens)
            return True
        except RateLimitExceeded:
            return False

    @property
    def available(self) -> float:
        """Current token count after a refill (read-only snapshot)."""
        self._refill()
        return self._tokens
