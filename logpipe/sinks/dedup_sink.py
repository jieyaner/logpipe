"""Deduplication sink — drops records whose key was seen recently."""

import collections
import threading
import time
from typing import Any

from logpipe.sinks import BaseSink

_SENTINEL = object()


class DedupSink(BaseSink):
    """Wrap a sink and suppress duplicate records within a rolling window.

    A record is considered a duplicate when the value at *key_field* matches
    a value seen within the last *ttl_seconds*.  Expired entries are evicted
    lazily on each :meth:`write` call.

    Args:
        sink:           Downstream :class:`~logpipe.sinks.BaseSink`.
        key_field:      Field name used to derive the dedup key.
        ttl_seconds:    How long (seconds) a seen key is remembered.
        max_cache:      Hard upper bound on cache entries (LRU eviction).
    """

    def __init__(
        self,
        sink: BaseSink,
        key_field: str,
        ttl_seconds: float = 60.0,
        max_cache: int = 10_000,
    ) -> None:
        self._sink = sink
        self._key_field = key_field
        self._ttl = ttl_seconds
        self._max_cache = max_cache
        # OrderedDict preserves insertion order for LRU eviction.
        self._seen: collections.OrderedDict[Any, float] = collections.OrderedDict()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_expired(self, now: float) -> None:
        """Remove all entries older than *ttl*."""
        cutoff = now - self._ttl
        while self._seen:
            _, ts = next(iter(self._seen.items()))
            if ts < cutoff:
                self._seen.popitem(last=False)
            else:
                break

    def _is_duplicate(self, key: Any, now: float) -> bool:
        if key not in self._seen:
            return False
        return (now - self._seen[key]) < self._ttl

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        key = record.get(self._key_field, _SENTINEL)
        now = time.monotonic()
        with self._lock:
            self._evict_expired(now)
            if self._is_duplicate(key, now):
                return
            # Enforce max_cache via LRU eviction.
            while len(self._seen) >= self._max_cache:
                self._seen.popitem(last=False)
            self._seen[key] = now
        self._sink.write(record)

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()
