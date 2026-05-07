"""Tests for PrioritySink and build_priority_sink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.priority_sink import PriorityError, PrioritySink
from logpipe.sinks.priority_sink_builder import build_priority_sink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.flushed = 0
        self.closed = 0

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed += 1


def _make_sinks(n: int = 3):
    return [_CaptureSink() for _ in range(n)]


# ---------------------------------------------------------------------------
# Construction errors
# ---------------------------------------------------------------------------

class TestPrioritySinkConstruction:
    def test_empty_field_raises(self):
        sink = _CaptureSink()
        with pytest.raises(PriorityError, match="field"):
            PrioritySink(field="", levels=[(1, sink)])

    def test_empty_levels_raises(self):
        with pytest.raises(PriorityError, match="level"):
            PrioritySink(field="level", levels=[])


# ---------------------------------------------------------------------------
# Numeric routing
# ---------------------------------------------------------------------------

class TestNumericPriority:
    def _make(self):
        high, med, low = _make_sinks()
        # levels are checked highest-first
        sink = PrioritySink(
            field="priority",
            levels=[(80, high), (50, med), (10, low)],
        )
        return sink, high, med, low

    def test_routes_to_highest_matching_level(self):
        sink, high, med, low = self._make()
        sink.write({"priority": 90})
        assert len(high.records) == 1
        assert len(med.records) == 0

    def test_routes_to_middle_level(self):
        sink, high, med, low = self._make()
        sink.write({"priority": 60})
        assert len(med.records) == 1
        assert len(high.records) == 0

    def test_routes_to_lowest_level(self):
        sink, high, med, low = self._make()
        sink.write({"priority": 15})
        assert len(low.records) == 1

    def test_drops_when_no_level_matches_and_no_default(self):
        sink, high, med, low = self._make()
        sink.write({"priority": 5})
        assert all(len(s.records) == 0 for s in (high, med, low))

    def test_routes_to_default_when_no_level_matches(self):
        default = _CaptureSink()
        high, med, low = _make_sinks()
        sink = PrioritySink(
            field="priority",
            levels=[(80, high), (50, med), (10, low)],
            default=default,
        )
        sink.write({"priority": 2})
        assert len(default.records) == 1


# ---------------------------------------------------------------------------
# String routing
# ---------------------------------------------------------------------------

class TestStringPriority:
    def _make(self):
        crit, err, warn, default = _make_sinks(4)
        sink = PrioritySink(
            field="level",
            levels=[("critical", crit), ("error", err), ("warning", warn)],
            default=default,
        )
        return sink, crit, err, warn, default

    def test_exact_match_critical(self):
        sink, crit, err, warn, default = self._make()
        sink.write({"level": "critical"})
        assert len(crit.records) == 1

    def test_exact_match_warning(self):
        sink, crit, err, warn, default = self._make()
        sink.write({"level": "warning"})
        assert len(warn.records) == 1

    def test_unknown_string_goes_to_default(self):
        sink, crit, err, warn, default = self._make()
        sink.write({"level": "debug"})
        assert len(default.records) == 1


# ---------------------------------------------------------------------------
# Nested field
# ---------------------------------------------------------------------------

class TestNestedField:
    def test_dot_path_resolved(self):
        high = _CaptureSink()
        sink = PrioritySink(field="meta.priority", levels=[(50, high)])
        sink.write({"meta": {"priority": 75}})
        assert len(high.records) == 1

    def test_missing_nested_field_goes_to_default(self):
        default = _CaptureSink()
        high = _CaptureSink()
        sink = PrioritySink(field="meta.priority", levels=[(50, high)], default=default)
        sink.write({"other": "value"})
        assert len(default.records) == 1


# ---------------------------------------------------------------------------
# flush / close propagation
# ---------------------------------------------------------------------------

class TestFlushClose:
    def test_flush_calls_all_unique_sinks(self):
        a, b, default = _make_sinks(3)
        sink = PrioritySink(field="p", levels=[(10, a), (5, b)], default=default)
        sink.flush()
        assert a.flushed == 1 and b.flushed == 1 and default.flushed == 1

    def test_shared_sink_flushed_once(self):
        shared = _CaptureSink()
        sink = PrioritySink(field="p", levels=[(10, shared), (5, shared)])
        sink.flush()
        assert shared.flushed == 1

    def test_close_propagates(self):
        a, b = _make_sinks(2)
        sink = PrioritySink(field="p", levels=[(10, a), (5, b)])
        sink.close()
        assert a.closed == 1 and b.closed == 1


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

class TestBuildPrioritySink:
    def test_builds_from_config(self):
        high, low = _make_sinks(2)
        result = build_priority_sink(
            field="priority",
            levels=[
                {"threshold": 80, "sink": high},
                {"threshold": 40, "sink": low},
            ],
        )
        result.write({"priority": 85})
        assert len(high.records) == 1

    def test_coerces_string_threshold_to_int(self):
        high = _CaptureSink()
        result = build_priority_sink(
            field="priority",
            levels=[{"threshold": "80", "sink": high}],
        )
        result.write({"priority": 90})
        assert len(high.records) == 1

    def test_empty_levels_raises(self):
        with pytest.raises(PriorityError):
            build_priority_sink(field="p", levels=[])

    def test_missing_threshold_raises(self):
        sink = _CaptureSink()
        with pytest.raises(PriorityError, match="threshold"):
            build_priority_sink(field="p", levels=[{"sink": sink}])

    def test_missing_sink_raises(self):
        with pytest.raises(PriorityError, match="sink"):
            build_priority_sink(field="p", levels=[{"threshold": 10}])

    def test_invalid_sink_type_raises(self):
        with pytest.raises(PriorityError, match="BaseSink"):
            build_priority_sink(
                field="p",
                levels=[{"threshold": 10, "sink": "not-a-sink"}],
            )
