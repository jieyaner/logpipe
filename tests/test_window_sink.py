"""Tests for WindowSink."""

import pytest
from logpipe.sinks.window_sink import WindowSink


class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flush_count = 0
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flush_count += 1

    def close(self):
        self.closed = True


@pytest.fixture
def _now(monkeypatch):
    """Mutable clock fixture — call `_now.advance(seconds)` to move time."""
    class _Clock:
        def __init__(self):
            self._t = 1_000.0

        def __call__(self):
            return self._t

        def advance(self, seconds):
            self._t += seconds

    return _Clock()


def _make_sink(downstream, window_seconds=10, value_field=None, clock=None):
    return WindowSink(
        downstream,
        window_seconds=window_seconds,
        value_field=value_field,
        clock=clock,
    )


class TestWindowSinkBasic:
    def test_no_flush_within_window(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        for _ in range(5):
            sink.write({"msg": "hello"})
        assert cap.records == []

    def test_flush_emits_summary_after_window_expires(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        sink.write({"msg": "a"})
        sink.write({"msg": "b"})
        _now.advance(11)
        sink.write({"msg": "c"})  # triggers flush of previous window
        assert len(cap.records) == 1
        assert cap.records[0]["count"] == 2

    def test_explicit_flush_emits_summary(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        sink.write({"msg": "x"})
        sink.flush()
        assert cap.records[0]["count"] == 1

    def test_empty_window_not_emitted(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        sink.flush()
        assert cap.records == []

    def test_close_flushes_remaining(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        sink.write({"msg": "z"})
        sink.close()
        assert cap.records[0]["count"] == 1
        assert cap.closed is True


class TestWindowSinkValueField:
    def test_sum_min_max_avg(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, value_field="latency", clock=_now)
        sink.write({"latency": 10})
        sink.write({"latency": 20})
        sink.write({"latency": 30})
        sink.flush()
        rec = cap.records[0]
        assert rec["sum"] == 60.0
        assert rec["min"] == 10.0
        assert rec["max"] == 30.0
        assert rec["avg"] == 20.0

    def test_missing_field_treated_as_zero(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, value_field="latency", clock=_now)
        sink.write({"other": 5})
        sink.flush()
        assert cap.records[0]["sum"] == 0.0

    def test_window_timestamps_correct(self, _now):
        cap = _CaptureSink()
        sink = _make_sink(cap, window_seconds=10, clock=_now)
        start = _now()
        sink.write({})
        sink.flush()
        rec = cap.records[0]
        assert rec["window_start"] == start
        assert rec["window_end"] == start + 10


class TestWindowSinkValidation:
    def test_invalid_window_seconds_raises(self):
        cap = _CaptureSink()
        with pytest.raises(ValueError, match="window_seconds must be positive"):
            WindowSink(cap, window_seconds=0)

    def test_negative_window_seconds_raises(self):
        cap = _CaptureSink()
        with pytest.raises(ValueError):
            WindowSink(cap, window_seconds=-5)
