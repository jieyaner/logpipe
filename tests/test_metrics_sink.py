"""Unit tests for MetricsSink wrapper."""

import pytest

from logpipe.metrics import MetricsCollector
from logpipe.sinks import BaseSink
from logpipe.sinks.metrics_sink import MetricsSink


class _CaptureSink(BaseSink):
    def __init__(self):
        self.records = []
        self.flush_count = 0
        self.close_count = 0

    def write(self, record: dict) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flush_count += 1

    def close(self) -> None:
        self.close_count += 1


@pytest.fixture()
def mc():
    return MetricsCollector()


@pytest.fixture()
def inner():
    return _CaptureSink()


@pytest.fixture()
def sink(inner, mc):
    return MetricsSink(inner, metrics=mc)


class TestMetricsSinkDelegation:
    def test_write_delegates_to_inner(self, sink, inner):
        sink.write({"msg": "hello"})
        assert inner.records == [{"msg": "hello"}]

    def test_flush_delegates_to_inner(self, sink, inner):
        sink.flush()
        assert inner.flush_count == 1

    def test_close_delegates_to_inner(self, sink, inner):
        sink.close()
        assert inner.close_count == 1


class TestMetricsSinkCounters:
    def test_write_increments_records_written(self, sink, mc):
        sink.write({"a": 1})
        sink.write({"b": 2})
        assert mc.get_counter("sink.records_written") == 2

    def test_flush_increments_flush_calls(self, sink, mc):
        sink.flush()
        sink.flush()
        assert mc.get_counter("sink.flush_calls") == 2

    def test_close_increments_close_calls(self, sink, mc):
        sink.close()
        assert mc.get_counter("sink.close_calls") == 1

    def test_uses_default_metrics_when_none_given(self, inner):
        from logpipe.metrics import default_metrics
        default_metrics.reset()
        s = MetricsSink(inner)
        s.write({"x": 0})
        assert default_metrics.get_counter("sink.records_written") >= 1
