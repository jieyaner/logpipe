"""Time-windowed aggregation sink.

Buffers records within a fixed time window and flushes the window
summary (count + optional numeric field stats) downstream when the
window closes.
"""

import time
from logpipe.sinks import BaseSink


class WindowSink(BaseSink):
    """Collect records in tumbling time windows and emit a summary record.

    Args:
        downstream: sink that receives the summary record on flush.
        window_seconds: width of each tumbling window in seconds.
        value_field: optional numeric field to compute sum/min/max over.
        clock: callable returning current time (injectable for testing).
    """

    def __init__(self, downstream, window_seconds=60, value_field=None, clock=None):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._downstream = downstream
        self._window_seconds = window_seconds
        self._value_field = value_field
        self._clock = clock or time.time
        self._reset()

    def _reset(self):
        self._window_start = self._clock()
        self._count = 0
        self._total = 0.0
        self._min = None
        self._max = None

    def _window_expired(self):
        return (self._clock() - self._window_start) >= self._window_seconds

    def _build_summary(self):
        summary = {
            "window_start": self._window_start,
            "window_end": self._window_start + self._window_seconds,
            "count": self._count,
        }
        if self._value_field is not None:
            summary["sum"] = self._total
            summary["min"] = self._min
            summary["max"] = self._max
            if self._count:
                summary["avg"] = self._total / self._count
        return summary

    def write(self, record):
        if self._window_expired():
            self.flush()
        self._count += 1
        if self._value_field is not None:
            try:
                val = float(record[self._value_field])
            except (KeyError, TypeError, ValueError):
                val = 0.0
            self._total += val
            self._min = val if self._min is None else min(self._min, val)
            self._max = val if self._max is None else max(self._max, val)

    def flush(self):
        if self._count > 0:
            self._downstream.write(self._build_summary())
        self._downstream.flush()
        self._reset()

    def close(self):
        self.flush()
        self._downstream.close()
