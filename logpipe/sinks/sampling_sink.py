"""Sampling sink — forwards only a deterministic fraction of records."""

import hashlib
import threading

from logpipe.sinks import BaseSink


class SamplingSink(BaseSink):
    """Wrap another sink and forward only *rate* fraction of records.

    Sampling is deterministic when *key_field* is provided: the same record
    will always be either forwarded or dropped.  When no key field is given
    a simple thread-safe counter is used (round-robin style).

    Args:
        sink:       Downstream :class:`~logpipe.sinks.BaseSink`.
        rate:       Float in (0, 1].  E.g. ``0.1`` forwards ~10 % of records.
        key_field:  Optional record field used for deterministic hashing.
    """

    def __init__(self, sink: BaseSink, rate: float, key_field: str | None = None) -> None:
        if not (0 < rate <= 1.0):
            raise ValueError(f"rate must be in (0, 1], got {rate!r}")
        self._sink = sink
        self._rate = rate
        self._key_field = key_field
        self._counter = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_forward(self, record: dict) -> bool:
        if self._key_field is not None:
            raw = str(record.get(self._key_field, "")).encode()
            digest = int(hashlib.md5(raw).hexdigest(), 16)  # noqa: S324
            bucket = (digest % 1_000_000) / 1_000_000.0
            return bucket < self._rate
        # Counter-based (no key field)
        with self._lock:
            idx = self._counter
            self._counter += 1
        threshold = int(round(1.0 / self._rate))
        return (idx % threshold) == 0

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._should_forward(record):
            self._sink.write(record)

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()
