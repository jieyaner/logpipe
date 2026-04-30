"""Tests for RateLimitSink."""

import time
import pytest
from logpipe.sinks.rate_limit_sink import RateLimitSink, RateLimitExceeded


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flushed = False
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def _make_sink(max_per_second=3, raise_on_drop=False):
    inner = _CaptureSink()
    sink = RateLimitSink(inner, max_per_second=max_per_second, raise_on_drop=raise_on_drop)
    return sink, inner


_RECORD = {"level": "info", "msg": "hello"}


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestRateLimitSinkConstruction:
    def test_zero_limit_raises(self):
        with pytest.raises(ValueError):
            RateLimitSink(_CaptureSink(), max_per_second=0)

    def test_negative_limit_raises(self):
        with pytest.raises(ValueError):
            RateLimitSink(_CaptureSink(), max_per_second=-1)


# ---------------------------------------------------------------------------
# Rate limiting behaviour
# ---------------------------------------------------------------------------

class TestRateLimitSinkBehaviour:
    def test_records_within_limit_are_forwarded(self):
        sink, inner = _make_sink(max_per_second=5)
        for _ in range(5):
            sink.write(_RECORD)
        assert len(inner.records) == 5

    def test_excess_records_are_dropped_silently(self):
        sink, inner = _make_sink(max_per_second=2)
        for _ in range(5):
            sink.write(_RECORD)
        assert len(inner.records) == 2

    def test_excess_records_raise_when_configured(self):
        sink, inner = _make_sink(max_per_second=2, raise_on_drop=True)
        sink.write(_RECORD)
        sink.write(_RECORD)
        with pytest.raises(RateLimitExceeded):
            sink.write(_RECORD)

    def test_window_resets_after_one_second(self):
        """After 1 s the sliding window should allow new records through."""
        sink, inner = _make_sink(max_per_second=2)
        # Fill the window.
        sink.write(_RECORD)
        sink.write(_RECORD)
        assert len(inner.records) == 2
        # Manually age the timestamps so they fall outside the 1-second window.
        aged = [t - 1.1 for t in sink._window]
        sink._window.clear()
        sink._window.extend(aged)
        # Should now accept new records.
        sink.write(_RECORD)
        assert len(inner.records) == 3

    def test_flush_delegates(self):
        sink, inner = _make_sink()
        sink.flush()
        assert inner.flushed

    def test_close_delegates(self):
        sink, inner = _make_sink()
        sink.close()
        assert inner.closed

    def test_record_content_preserved(self):
        sink, inner = _make_sink(max_per_second=10)
        rec = {"level": "error", "msg": "boom", "code": 500}
        sink.write(rec)
        assert inner.records[0] == rec
