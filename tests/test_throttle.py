"""Tests for TokenBucketThrottle and ThrottledSink."""

import time
import pytest

from logpipe.throttle import TokenBucketThrottle, RateLimitExceeded
from logpipe.sinks.throttled_sink import ThrottledSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flushed = 0
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# TokenBucketThrottle
# ---------------------------------------------------------------------------

class TestTokenBucketThrottle:
    def test_invalid_rate_raises(self):
        with pytest.raises(ValueError):
            TokenBucketThrottle(rate=0)

    def test_invalid_period_raises(self):
        with pytest.raises(ValueError):
            TokenBucketThrottle(rate=5, period=0)

    def test_acquire_within_limit_succeeds(self):
        t = TokenBucketThrottle(rate=5, period=1.0)
        for _ in range(5):
            t.acquire()  # should not raise

    def test_acquire_beyond_limit_raises(self):
        t = TokenBucketThrottle(rate=3, period=1.0)
        for _ in range(3):
            t.acquire()
        with pytest.raises(RateLimitExceeded):
            t.acquire()

    def test_try_acquire_returns_false_when_exhausted(self):
        t = TokenBucketThrottle(rate=2, period=1.0)
        assert t.try_acquire() is True
        assert t.try_acquire() is True
        assert t.try_acquire() is False

    def test_tokens_refill_over_time(self):
        t = TokenBucketThrottle(rate=10, period=0.1)
        for _ in range(10):
            t.acquire()
        time.sleep(0.12)
        # After ~1.2 periods at least 10 tokens should be back
        assert t.available >= 9.0

    def test_available_does_not_exceed_rate(self):
        t = TokenBucketThrottle(rate=5, period=1.0)
        time.sleep(0.05)
        assert t.available <= 5.0


# ---------------------------------------------------------------------------
# ThrottledSink
# ---------------------------------------------------------------------------

class TestThrottledSink:
    def _make(self, rate=3, period=1.0):
        inner = _CaptureSink()
        sink = ThrottledSink(inner, rate=rate, period=period)
        return sink, inner

    def test_forwards_records_within_limit(self):
        sink, inner = self._make(rate=3)
        for i in range(3):
            sink.write({"n": i})
        assert len(inner.records) == 3
        assert sink.forwarded == 3
        assert sink.dropped == 0

    def test_drops_records_beyond_limit(self):
        sink, inner = self._make(rate=2)
        for i in range(5):
            sink.write({"n": i})
        assert len(inner.records) == 2
        assert sink.dropped == 3

    def test_flush_delegates(self):
        sink, inner = self._make()
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self):
        sink, inner = self._make()
        sink.close()
        assert inner.closed is True
