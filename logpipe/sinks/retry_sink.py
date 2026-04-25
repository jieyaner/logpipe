"""Retry wrapper sink that retries failed writes with exponential backoff."""

import time
import logging
from logpipe.sinks import BaseSink

logger = logging.getLogger(__name__)


class RetryExhausted(Exception):
    """Raised when all retry attempts have been exhausted."""


class RetrySink(BaseSink):
    """Wraps another sink and retries writes on failure with exponential backoff."""

    def __init__(self, inner: BaseSink, max_attempts: int = 3, base_delay: float = 0.5, max_delay: float = 10.0):
        """
        :param inner: The underlying sink to delegate writes to.
        :param max_attempts: Maximum number of total attempts (including the first).
        :param base_delay: Initial delay in seconds before the first retry.
        :param max_delay: Maximum delay in seconds between retries.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._inner = inner
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay

    def write(self, record: dict) -> None:
        delay = self._base_delay
        last_exc = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._inner.write(record)
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self._max_attempts:
                    logger.warning(
                        "RetrySink: write attempt %d/%d failed (%s); retrying in %.2fs",
                        attempt,
                        self._max_attempts,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, self._max_delay)
        raise RetryExhausted(
            f"write failed after {self._max_attempts} attempts"
        ) from last_exc

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
