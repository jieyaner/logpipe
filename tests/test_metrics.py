"""Unit tests for logpipe.metrics."""

import threading
import time

import pytest

from logpipe.metrics import MetricsCollector


@pytest.fixture()
def mc() -> MetricsCollector:
    return MetricsCollector()


class TestCounters:
    def test_starts_at_zero(self, mc):
        assert mc.get_counter("anything") == 0

    def test_increment_by_one(self, mc):
        mc.increment("hits")
        assert mc.get_counter("hits") == 1

    def test_increment_by_custom_value(self, mc):
        mc.increment("bytes", 512)
        assert mc.get_counter("bytes") == 512

    def test_accumulates(self, mc):
        for _ in range(5):
            mc.increment("calls")
        assert mc.get_counter("calls") == 5


class TestGauges:
    def test_returns_none_for_unknown(self, mc):
        assert mc.get_gauge("lag") is None

    def test_set_and_retrieve(self, mc):
        mc.set_gauge("queue_depth", 42.0)
        assert mc.get_gauge("queue_depth") == 42.0

    def test_overwrite(self, mc):
        mc.set_gauge("lag", 1.0)
        mc.set_gauge("lag", 9.9)
        assert mc.get_gauge("lag") == 9.9


class TestSnapshot:
    def test_snapshot_contains_expected_keys(self, mc):
        snap = mc.snapshot()
        assert set(snap.keys()) == {"uptime_seconds", "counters", "gauges"}

    def test_snapshot_is_copy(self, mc):
        mc.increment("x")
        snap = mc.snapshot()
        mc.increment("x")
        assert snap["counters"]["x"] == 1  # snapshot not mutated

    def test_uptime_is_positive(self, mc):
        time.sleep(0.01)
        assert mc.snapshot()["uptime_seconds"] > 0


class TestReset:
    def test_reset_clears_counters(self, mc):
        mc.increment("a", 10)
        mc.reset()
        assert mc.get_counter("a") == 0

    def test_reset_clears_gauges(self, mc):
        mc.set_gauge("g", 7.0)
        mc.reset()
        assert mc.get_gauge("g") is None


class TestThreadSafety:
    def test_concurrent_increments(self, mc):
        threads = [threading.Thread(target=lambda: mc.increment("t", 1)) for _ in range(200)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert mc.get_counter("t") == 200
