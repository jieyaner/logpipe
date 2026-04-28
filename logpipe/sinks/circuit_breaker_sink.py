"""CircuitBreakerSink — stops forwarding to *inner* after consecutive failures.

States
------
CLOSED  : normal operation; failures are counted.
OPEN    : inner is not called; records are dropped.
HALF    : one probe record is forwarded; success → CLOSED, failure → OPEN.
"""

import time
from logpipe.sinks import BaseSink


class CircuitOpenError(Exception):
    """Raised (optionally) when the circuit is open and raise_on_open=True."""


class CircuitBreakerSink(BaseSink):
    CLOSED = "closed"
    OPEN = "open"
    HALF = "half"

    def __init__(
        self,
        inner: BaseSink,
        failure_threshold: int = 3,
        recovery_timeout: float = 30.0,
        metrics=None,
        raise_on_open: bool = False,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._inner = inner
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._metrics = metrics
        self._raise_on_open = raise_on_open

        self._state = self.CLOSED
        self._failures = 0
        self._opened_at: float | None = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def state(self) -> str:
        return self._state

    # ------------------------------------------------------------------
    # Internal state machine
    # ------------------------------------------------------------------

    def _trip(self) -> None:
        self._state = self.OPEN
        self._opened_at = time.monotonic()
        if self._metrics:
            self._metrics.increment("circuit_breaker.tripped")

    def _reset(self) -> None:
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = None
        if self._metrics:
            self._metrics.increment("circuit_breaker.reset")

    def _maybe_probe(self) -> bool:
        """Return True if the circuit should transition to HALF for a probe."""
        if self._state == self.OPEN and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self._recovery_timeout:
                self._state = self.HALF
                return True
        return self._state == self.HALF

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._state == self.OPEN:
            if not self._maybe_probe():
                if self._raise_on_open:
                    raise CircuitOpenError("circuit is open")
                return

        try:
            self._inner.write(record)
        except Exception:
            self._failures += 1
            if self._failures >= self._threshold or self._state == self.HALF:
                self._trip()
            raise
        else:
            if self._state == self.HALF:
                self._reset()

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
