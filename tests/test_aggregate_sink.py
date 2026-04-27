"""Tests for AggregateSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.aggregate_sink import AggregateSink, AggregationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.flush_calls = 0
        self.closed = False

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flush_calls += 1

    def close(self) -> None:
        self.closed = True


def _make_sink(**kwargs) -> tuple[AggregateSink, _CaptureSink]:
    cap = _CaptureSink()
    agg = AggregateSink(cap, **kwargs)
    return agg, cap


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

class TestAggregateSinkConstruction:
    def test_unknown_op_raises(self):
        with pytest.raises(AggregationError, match="Unsupported"):
            AggregateSink(_CaptureSink(), ops=["mean"])

    def test_sum_without_field_raises(self):
        with pytest.raises(AggregationError, match="'field' is required"):
            AggregateSink(_CaptureSink(), ops=["sum"])

    def test_count_without_field_is_valid(self):
        sink, _ = _make_sink(ops=["count"])
        assert sink is not None


# ---------------------------------------------------------------------------
# Count aggregation
# ---------------------------------------------------------------------------

class TestCountAggregation:
    def test_count_empty_flush_emits_nothing(self):
        sink, cap = _make_sink(ops=["count"])
        sink.flush()
        assert cap.records == []

    def test_count_single_group(self):
        sink, cap = _make_sink(ops=["count"])
        for _ in range(5):
            sink.write({"msg": "hello"})
        sink.flush()
        assert len(cap.records) == 1
        assert cap.records[0]["count"] == 5

    def test_buckets_cleared_after_flush(self):
        sink, cap = _make_sink(ops=["count"])
        sink.write({"x": 1})
        sink.flush()
        sink.flush()  # second flush should emit nothing
        assert len(cap.records) == 1


# ---------------------------------------------------------------------------
# Numeric aggregations
# ---------------------------------------------------------------------------

class TestNumericAggregations:
    def test_sum(self):
        sink, cap = _make_sink(field="latency", ops=["sum"])
        for v in [10, 20, 30]:
            sink.write({"latency": v})
        sink.flush()
        assert cap.records[0]["sum"] == 60.0

    def test_min_max(self):
        sink, cap = _make_sink(field="latency", ops=["min", "max"])
        for v in [3, 1, 4, 1, 5]:
            sink.write({"latency": v})
        sink.flush()
        rec = cap.records[0]
        assert rec["min"] == 1.0
        assert rec["max"] == 5.0

    def test_missing_field_values_skipped(self):
        sink, cap = _make_sink(field="latency", ops=["count", "sum"])
        sink.write({"latency": 10})
        sink.write({"other": "x"})  # no latency field
        sink.flush()
        assert cap.records[0]["count"] == 2
        assert cap.records[0]["sum"] == 10.0


# ---------------------------------------------------------------------------
# Group-by
# ---------------------------------------------------------------------------

class TestGroupBy:
    def test_two_groups_emit_two_records(self):
        sink, cap = _make_sink(field="val", ops=["count", "sum"], group_by="env")
        sink.write({"env": "prod", "val": 5})
        sink.write({"env": "dev", "val": 3})
        sink.write({"env": "prod", "val": 7})
        sink.flush()
        assert len(cap.records) == 2
        by_env = {r["env"]: r for r in cap.records}
        assert by_env["prod"]["count"] == 2
        assert by_env["prod"]["sum"] == 12.0
        assert by_env["dev"]["count"] == 1

    def test_group_key_present_in_summary(self):
        sink, cap = _make_sink(ops=["count"], group_by="region")
        sink.write({"region": "us-east"})
        sink.flush()
        assert cap.records[0]["region"] == "us-east"


# ---------------------------------------------------------------------------
# Close behaviour
# ---------------------------------------------------------------------------

class TestClose:
    def test_close_flushes_and_closes_downstream(self):
        sink, cap = _make_sink(ops=["count"])
        sink.write({"a": 1})
        sink.close()
        assert len(cap.records) == 1
        assert cap.closed
