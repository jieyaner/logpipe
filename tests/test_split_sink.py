"""Tests for logpipe.sinks.split_sink.SplitSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks.split_sink import SplitError, SplitSink


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


def _make_split(field="level", fallback=None):
    sinks = {
        "info": _CaptureSink(),
        "error": _CaptureSink(),
    }
    split = SplitSink(field=field, routes=dict(sinks), fallback=fallback)
    return split, sinks


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestSplitSinkConstruction:
    def test_empty_field_raises(self):
        with pytest.raises(SplitError, match="field"):
            SplitSink(field="", routes={"a": _CaptureSink()})

    def test_empty_routes_raises(self):
        with pytest.raises(SplitError, match="routes"):
            SplitSink(field="level", routes={})

    def test_valid_construction(self):
        split, _ = _make_split()
        assert split is not None


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

class TestSplitSinkRouting:
    def test_routes_to_correct_sink(self):
        split, sinks = _make_split()
        split.write({"level": "info", "msg": "hello"})
        assert len(sinks["info"].records) == 1
        assert len(sinks["error"].records) == 0

    def test_routes_error_to_error_sink(self):
        split, sinks = _make_split()
        split.write({"level": "error", "msg": "boom"})
        assert len(sinks["error"].records) == 1
        assert len(sinks["info"].records) == 0

    def test_unmatched_dropped_without_fallback(self):
        split, sinks = _make_split()
        split.write({"level": "debug", "msg": "verbose"})
        assert all(len(s.records) == 0 for s in sinks.values())

    def test_unmatched_goes_to_fallback(self):
        fallback = _CaptureSink()
        split, sinks = _make_split(fallback=fallback)
        split.write({"level": "warn", "msg": "careful"})
        assert len(fallback.records) == 1
        assert all(len(s.records) == 0 for s in sinks.values())

    def test_missing_field_goes_to_fallback(self):
        fallback = _CaptureSink()
        split, _ = _make_split(fallback=fallback)
        split.write({"msg": "no level here"})
        assert len(fallback.records) == 1

    def test_missing_field_dropped_without_fallback(self):
        split, sinks = _make_split()
        split.write({"msg": "no level here"})
        assert all(len(s.records) == 0 for s in sinks.values())

    def test_nested_field_routing(self):
        info_sink = _CaptureSink()
        split = SplitSink(
            field="meta.severity",
            routes={"high": info_sink},
        )
        split.write({"meta": {"severity": "high"}, "msg": "alert"})
        assert len(info_sink.records) == 1

    def test_nested_field_missing_intermediate_key(self):
        info_sink = _CaptureSink()
        fallback = _CaptureSink()
        split = SplitSink(
            field="meta.severity",
            routes={"high": info_sink},
            fallback=fallback,
        )
        split.write({"msg": "no meta"})
        assert len(fallback.records) == 1


# ---------------------------------------------------------------------------
# flush / close propagation
# ---------------------------------------------------------------------------

class TestSplitSinkLifecycle:
    def test_flush_propagates_to_all_routes(self):
        split, sinks = _make_split()
        split.flush()
        assert all(s.flushed == 1 for s in sinks.values())

    def test_flush_propagates_to_fallback(self):
        fallback = _CaptureSink()
        split, _ = _make_split(fallback=fallback)
        split.flush()
        assert fallback.flushed == 1

    def test_close_propagates_to_all_routes(self):
        split, sinks = _make_split()
        split.close()
        assert all(s.closed for s in sinks.values())

    def test_close_propagates_to_fallback(self):
        fallback = _CaptureSink()
        split, _ = _make_split(fallback=fallback)
        split.close()
        assert fallback.closed
