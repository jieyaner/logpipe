"""Tests for logpipe.sinks.rollup_sink."""

import time
from typing import Any, Dict, List

import pytest

from logpipe.sinks.rollup_sink import RollupError, RollupSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.flushed = 0
        self.closed = False

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True


def _make_sink(fields=None, window_seconds=60.0):
    cap = _CaptureSink()
    sink = RollupSink(cap, fields=fields or ["duration", "bytes"], window_seconds=window_seconds)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestRollupSinkConstruction:
    def test_empty_fields_raises(self):
        with pytest.raises(RollupError, match="field"):
            RollupSink(_CaptureSink(), fields=[])

    def test_non_positive_window_raises(self):
        with pytest.raises(RollupError, match="positive"):
            RollupSink(_CaptureSink(), fields=["x"], window_seconds=0)

    def test_negative_window_raises(self):
        with pytest.raises(RollupError, match="positive"):
            RollupSink(_CaptureSink(), fields=["x"], window_seconds=-5)


# ---------------------------------------------------------------------------
# Accumulation
# ---------------------------------------------------------------------------

class TestRollupSinkAccumulation:
    def test_no_emit_before_window_closes(self):
        sink, cap = _make_sink(window_seconds=9999)
        sink.write({"duration": 1, "bytes": 100})
        sink.write({"duration": 2, "bytes": 200})
        assert cap.records == []

    def test_flush_emits_summary(self):
        sink, cap = _make_sink(fields=["duration"], window_seconds=9999)
        sink.write({"duration": 10})
        sink.write({"duration": 20})
        sink.flush()
        assert len(cap.records) == 1
        r = cap.records[0]
        assert r["duration.count"] == 2
        assert r["duration.sum"] == pytest.approx(30.0)
        assert r["duration.min"] == pytest.approx(10.0)
        assert r["duration.max"] == pytest.approx(20.0)

    def test_missing_field_skipped(self):
        sink, cap = _make_sink(fields=["duration"], window_seconds=9999)
        sink.write({"other": 99})
        sink.flush()
        assert cap.records[0]["duration.count"] == 0

    def test_non_numeric_field_skipped(self):
        sink, cap = _make_sink(fields=["duration"], window_seconds=9999)
        sink.write({"duration": "fast"})
        sink.flush()
        assert cap.records[0]["duration.count"] == 0

    def test_summary_has_timestamp_field(self):
        sink, cap = _make_sink(fields=["x"], window_seconds=9999)
        sink.write({"x": 1})
        sink.flush()
        assert "timestamp" in cap.records[0]

    def test_custom_timestamp_field(self):
        cap = _CaptureSink()
        sink = RollupSink(cap, fields=["x"], window_seconds=9999, timestamp_field="ts")
        sink.write({"x": 5})
        sink.flush()
        assert "ts" in cap.records[0]

    def test_window_expiry_triggers_emit_on_write(self, monkeypatch):
        sink, cap = _make_sink(fields=["x"], window_seconds=1)
        sink.write({"x": 7})
        # Advance monotonic clock past the window
        original = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: original() + 5)
        sink.write({"x": 3})
        # First window summary should have been emitted
        assert len(cap.records) == 1
        assert cap.records[0]["x.sum"] == pytest.approx(7.0)

    def test_flush_resets_state(self):
        sink, cap = _make_sink(fields=["x"], window_seconds=9999)
        sink.write({"x": 5})
        sink.flush()
        sink.flush()  # second flush on empty window
        # Only one summary (second flush sees count==0, no emit)
        assert len(cap.records) == 1

    def test_close_flushes_and_closes_downstream(self):
        sink, cap = _make_sink(fields=["x"], window_seconds=9999)
        sink.write({"x": 42})
        sink.close()
        assert len(cap.records) == 1
        assert cap.closed is True

    def test_multiple_fields(self):
        sink, cap = _make_sink(fields=["a", "b"], window_seconds=9999)
        sink.write({"a": 1, "b": 10})
        sink.write({"a": 3, "b": 20})
        sink.flush()
        r = cap.records[0]
        assert r["a.sum"] == pytest.approx(4.0)
        assert r["b.sum"] == pytest.approx(30.0)
        assert r["a.min"] == pytest.approx(1.0)
        assert r["b.max"] == pytest.approx(20.0)
