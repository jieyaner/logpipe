"""Tests for DeadlineSink."""

import time
import threading
import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.deadline_sink import DeadlineSink
from logpipe.metrics import MetricsCollector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self, delay: float = 0.0):
        self.received: list[dict] = []
        self.flushed = 0
        self.closed = False
        self._delay = delay

    def write(self, record: dict) -> None:
        if self._delay:
            time.sleep(self._delay)
        self.received.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True


class _ErrorSink(BaseSink):
    def write(self, record):
        raise RuntimeError("boom")

    def flush(self): pass
    def close(self): pass


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestDeadlineSinkConstruction:
    def test_negative_timeout_raises(self):
        inner = _CaptureSink()
        with pytest.raises(ValueError):
            DeadlineSink(inner, timeout_seconds=-1)

    def test_zero_timeout_raises(self):
        inner = _CaptureSink()
        with pytest.raises(ValueError):
            DeadlineSink(inner, timeout_seconds=0)


# ---------------------------------------------------------------------------
# Normal forwarding
# ---------------------------------------------------------------------------

class TestDeadlineSinkForwarding:
    def test_fast_write_is_forwarded(self):
        inner = _CaptureSink(delay=0.0)
        sink = DeadlineSink(inner, timeout_seconds=1.0)
        sink.write({"msg": "hello"})
        assert inner.received == [{"msg": "hello"}]

    def test_flush_delegates(self):
        inner = _CaptureSink()
        sink = DeadlineSink(inner, timeout_seconds=1.0)
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self):
        inner = _CaptureSink()
        sink = DeadlineSink(inner, timeout_seconds=1.0)
        sink.close()
        assert inner.closed is True

    def test_inner_exception_is_re_raised(self):
        sink = DeadlineSink(_ErrorSink(), timeout_seconds=1.0)
        with pytest.raises(RuntimeError, match="boom"):
            sink.write({"x": 1})


# ---------------------------------------------------------------------------
# Timeout / deadline exceeded
# ---------------------------------------------------------------------------

class TestDeadlineSinkTimeout:
    def test_slow_write_is_dropped(self):
        inner = _CaptureSink(delay=0.5)
        sink = DeadlineSink(inner, timeout_seconds=0.05)
        sink.write({"msg": "slow"})
        # Record must NOT have been forwarded within the deadline window.
        assert inner.received == []

    def test_timeout_increments_metric(self):
        mc = MetricsCollector()
        inner = _CaptureSink(delay=0.5)
        sink = DeadlineSink(inner, timeout_seconds=0.05, metrics=mc)
        sink.write({"msg": "slow"})
        assert mc.get_counter("deadline_sink.timeout") == 1

    def test_no_metric_collected_on_fast_write(self):
        mc = MetricsCollector()
        inner = _CaptureSink(delay=0.0)
        sink = DeadlineSink(inner, timeout_seconds=1.0, metrics=mc)
        sink.write({"msg": "fast"})
        assert mc.get_counter("deadline_sink.timeout") == 0

    def test_multiple_timeouts_accumulate(self):
        mc = MetricsCollector()
        inner = _CaptureSink(delay=0.5)
        sink = DeadlineSink(inner, timeout_seconds=0.05, metrics=mc)
        for _ in range(3):
            sink.write({"n": _})
        assert mc.get_counter("deadline_sink.timeout") == 3
