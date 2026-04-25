"""RotatingSink — wraps a sink factory and recreates the sink after N records or T seconds."""

import time
from logpipe.sinks import BaseSink


class RotatingSink(BaseSink):
    """Forwards records to an inner sink, rotating (flush+close+recreate) it
    when either *max_records* have been written or *max_age_seconds* have
    elapsed since the last rotation.

    Args:
        factory:          Callable[[], BaseSink] — creates a fresh inner sink.
        max_records:      Rotate after this many records (0 = disabled).
        max_age_seconds:  Rotate after this many seconds (0 = disabled).
        clock:            Optional callable returning current time (default: time.monotonic).
    """

    def __init__(self, factory, *, max_records=0, max_age_seconds=0, clock=None):
        if max_records == 0 and max_age_seconds == 0:
            raise ValueError("At least one of max_records or max_age_seconds must be set")
        self._factory = factory
        self._max_records = max_records
        self._max_age = max_age_seconds
        self._clock = clock or time.monotonic
        self._sink = self._factory()
        self._records_written = 0
        self._created_at = self._clock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_rotate(self):
        if self._max_records and self._records_written >= self._max_records:
            return True
        if self._max_age and (self._clock() - self._created_at) >= self._max_age:
            return True
        return False

    def _rotate(self):
        self._sink.flush()
        self._sink.close()
        self._sink = self._factory()
        self._records_written = 0
        self._created_at = self._clock()

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record):
        if self._should_rotate():
            self._rotate()
        self._sink.write(record)
        self._records_written += 1

    def flush(self):
        self._sink.flush()

    def close(self):
        self._sink.flush()
        self._sink.close()
