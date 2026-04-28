"""Tests for CircuitBreakerSink."""

import time
import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.circuit_breaker_sink import CircuitBreakerSink, CircuitOpenError
from logpipe.metrics import MetricsCollector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self, fail_times: int = 0):
        self.received: list[dict] = []
        self.flushed = 0
        self.closed = False
        self._fail_times = fail_times
        self._call_count = 0

    def write(self, record: dict) -> None:
        self._call_count += 1
        if self._call_count <= self._fail_times:
            raise IOError("write failed")
        self.received.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestCircuitBreakerConstruction:
    def test_invalid_threshold_raises(self):
        with pytest.raises(ValueError):
            CircuitBreakerSink(_CaptureSink(), failure_threshold=0)


# ---------------------------------------------------------------------------
# CLOSED state — normal operation
# ---------------------------------------------------------------------------

class TestCircuitBreakerClosed:
    def test_records_forwarded_when_closed(self):
        inner = _CaptureSink()
        cb = CircuitBreakerSink(inner, failure_threshold=3)
        cb.write({"a": 1})
        assert inner.received == [{"a": 1}]
        assert cb.state == CircuitBreakerSink.CLOSED

    def test_failure_below_threshold_stays_closed(self):
        inner = _CaptureSink(fail_times=2)
        cb = CircuitBreakerSink(inner, failure_threshold=3)
        for _ in range(2):
            with pytest.raises(IOError):
                cb.write({"n": _})
        assert cb.state == CircuitBreakerSink.CLOSED


# ---------------------------------------------------------------------------
# OPEN state — circuit trips
# ---------------------------------------------------------------------------

class TestCircuitBreakerOpen:
    def test_trips_after_threshold(self):
        inner = _CaptureSink(fail_times=3)
        cb = CircuitBreakerSink(inner, failure_threshold=3)
        for _ in range(3):
            with pytest.raises(IOError):
                cb.write({"n": _})
        assert cb.state == CircuitBreakerSink.OPEN

    def test_open_circuit_drops_records_silently(self):
        inner = _CaptureSink(fail_times=3)
        cb = CircuitBreakerSink(inner, failure_threshold=3)
        for _ in range(3):
            with pytest.raises(IOError):
                cb.write({"n": _})
        # Circuit is now open; subsequent writes should be silently dropped.
        cb.write({"dropped": True})
        assert {"dropped": True} not in inner.received

    def test_raise_on_open_flag(self):
        inner = _CaptureSink(fail_times=3)
        cb = CircuitBreakerSink(inner, failure_threshold=3, raise_on_open=True)
        for _ in range(3):
            with pytest.raises(IOError):
                cb.write({"n": _})
        with pytest.raises(CircuitOpenError):
            cb.write({"x": 1})

    def test_metrics_tripped(self):
        mc = MetricsCollector()
        inner = _CaptureSink(fail_times=2)
        cb = CircuitBreakerSink(inner, failure_threshold=2, metrics=mc)
        for _ in range(2):
            with pytest.raises(IOError):
                cb.write({"n": _})
        assert mc.get_counter("circuit_breaker.tripped") == 1


# ---------------------------------------------------------------------------
# HALF / recovery
# ---------------------------------------------------------------------------

class TestCircuitBreakerRecovery:
    def test_resets_after_successful_probe(self):
        inner = _CaptureSink(fail_times=3)
        cb = CircuitBreakerSink(inner, failure_threshold=3, recovery_timeout=0.0)
        for _ in range(3):
            with pytest.raises(IOError):
                cb.write({"n": _})
        # Recovery timeout is 0 — next write should probe and succeed.
        cb.write({"probe": True})
        assert cb.state == CircuitBreakerSink.CLOSED

    def test_metrics_reset(self):
        mc = MetricsCollector()
        inner = _CaptureSink(fail_times=2)
        cb = CircuitBreakerSink(
            inner, failure_threshold=2, recovery_timeout=0.0, metrics=mc
        )
        for _ in range(2):
            with pytest.raises(IOError):
                cb.write({"n": _})
        cb.write({"probe": True})
        assert mc.get_counter("circuit_breaker.reset") == 1

    def test_flush_and_close_delegate(self):
        inner = _CaptureSink()
        cb = CircuitBreakerSink(inner)
        cb.flush()
        cb.close()
        assert inner.flushed == 1
        assert inner.closed is True
