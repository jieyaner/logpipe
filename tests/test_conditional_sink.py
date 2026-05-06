"""Tests for logpipe.sinks.conditional_sink.ConditionalSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks.conditional_sink import ConditionalError, ConditionalSink


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


def _make(condition: str):
    inner = _CaptureSink()
    sink = ConditionalSink(condition, inner=inner)
    return sink, inner


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConditionalSinkConstruction:
    def test_empty_condition_raises(self):
        with pytest.raises(ConditionalError, match="non-empty"):
            ConditionalSink("", inner=_CaptureSink())

    def test_whitespace_only_raises(self):
        with pytest.raises(ConditionalError, match="non-empty"):
            ConditionalSink("   ", inner=_CaptureSink())

    def test_syntax_error_raises(self):
        with pytest.raises(ConditionalError, match="invalid condition"):
            ConditionalSink("if if if", inner=_CaptureSink())

    def test_valid_condition_accepted(self):
        sink, _ = _make("r.get('level') == 'error'")
        assert sink is not None


# ---------------------------------------------------------------------------
# Filtering behaviour
# ---------------------------------------------------------------------------

class TestConditionalSinkFiltering:
    def test_matching_record_forwarded(self):
        sink, inner = _make("r.get('level') == 'error'")
        sink.write({"level": "error", "msg": "boom"})
        assert len(inner.records) == 1

    def test_non_matching_record_dropped(self):
        sink, inner = _make("r.get('level') == 'error'")
        sink.write({"level": "info", "msg": "ok"})
        assert len(inner.records) == 0

    def test_multiple_records_filtered_correctly(self):
        sink, inner = _make("r.get('status', 0) >= 500")
        sink.write({"status": 200})
        sink.write({"status": 500})
        sink.write({"status": 503})
        sink.write({"status": 404})
        assert len(inner.records) == 2
        assert all(r["status"] >= 500 for r in inner.records)

    def test_expression_runtime_error_drops_record(self):
        # Division by zero should not crash the sink
        sink, inner = _make("1 / r['x'] > 0")
        sink.write({"x": 0})
        assert len(inner.records) == 0

    def test_missing_key_drops_record_gracefully(self):
        sink, inner = _make("r['missing_key'] == 'value'")
        sink.write({"other": "data"})
        assert len(inner.records) == 0

    def test_falsy_value_drops_record(self):
        sink, inner = _make("r.get('active')")
        sink.write({"active": False})
        assert len(inner.records) == 0

    def test_truthy_value_forwards_record(self):
        sink, inner = _make("r.get('active')")
        sink.write({"active": True})
        assert len(inner.records) == 1


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestConditionalSinkLifecycle:
    def test_flush_delegates_to_inner(self):
        sink, inner = _make("True")
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates_to_inner(self):
        sink, inner = _make("True")
        sink.close()
        assert inner.closed
