"""Tests for logpipe.sinks.transform_sink.TransformSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.transform_sink import TransformSink
from logpipe.transform import TransformError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self):
        self.records: List[Dict[str, Any]] = []
        self.flushed = 0
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed = True


def _make_sink(rules):
    cap = _CaptureSink()
    sink = TransformSink(cap, rules)
    return sink, cap


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

class TestTransformSink:
    def test_transforms_are_applied(self):
        sink, cap = _make_sink([{"field": "level", "op": "uppercase"}])
        sink.write({"level": "info", "msg": "ok"})
        assert cap.records[0]["level"] == "INFO"

    def test_unaffected_fields_pass_through(self):
        sink, cap = _make_sink([{"field": "level", "op": "uppercase"}])
        sink.write({"level": "info", "msg": "hello"})
        assert cap.records[0]["msg"] == "hello"

    def test_multiple_rules_applied_in_order(self):
        rules = [
            {"field": "count", "op": "to_int"},
            {"field": "label", "op": "strip"},
        ]
        sink, cap = _make_sink(rules)
        sink.write({"count": "7", "label": "  web  "})
        assert cap.records[0] == {"count": 7, "label": "web"}

    def test_flush_delegates(self):
        sink, cap = _make_sink([])
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink([])
        sink.close()
        assert cap.closed is True

    def test_invalid_op_raises_at_construction(self):
        with pytest.raises(TransformError):
            _make_sink([{"field": "x", "op": "bad_op"}])

    def test_bad_coercion_raises_transform_error(self):
        sink, _ = _make_sink([{"field": "n", "op": "to_int"}])
        with pytest.raises(TransformError):
            sink.write({"n": "not-an-int"})
