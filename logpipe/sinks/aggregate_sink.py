"""AggregateSink — buffers records and emits aggregated summaries downstream.

Supported aggregations per numeric field:
  - count   : number of records seen
  - sum     : total of field values
  - min/max : extremes of field values
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from logpipe.sinks import BaseSink


class AggregationError(Exception):
    """Raised when an unsupported aggregation operation is requested."""


_SUPPORTED_OPS = frozenset({"count", "sum", "min", "max"})


class AggregateSink(BaseSink):
    """Accumulates records in a window and forwards a single summary record.

    Parameters
    ----------
    downstream:
        Sink that receives the aggregated summary record on flush.
    field:
        Name of the numeric field to aggregate.  Required for sum/min/max;
        ignored (but still validated) for count-only usage.
    ops:
        List of aggregation operations to apply (default: ["count"]).
    group_by:
        Optional field name whose value is used as a grouping key.  When set,
        one summary record is emitted per distinct value.
    """

    def __init__(
        self,
        downstream: BaseSink,
        field: Optional[str] = None,
        ops: Optional[List[str]] = None,
        group_by: Optional[str] = None,
    ) -> None:
        ops = ops or ["count"]
        unknown = sorted(set(ops) - _SUPPORTED_OPS)
        if unknown:
            raise AggregationError(f"Unsupported aggregation ops: {unknown}")
        if any(op in ops for op in ("sum", "min", "max")) and field is None:
            raise AggregationError("'field' is required for sum/min/max aggregations")

        self._downstream = downstream
        self._field = field
        self._ops = list(ops)
        self._group_by = group_by
        # {group_key -> {"count": int, "sum": float, "min": float, "max": float}}
        self._buckets: Dict[Any, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    def write(self, record: Dict[str, Any]) -> None:
        key = record.get(self._group_by) if self._group_by else "__all__"
        bucket = self._buckets.setdefault(key, {})

        bucket["count"] = bucket.get("count", 0) + 1

        if self._field is not None:
            raw = record.get(self._field)
            if raw is not None:
                value = float(raw)
                bucket["sum"] = bucket.get("sum", 0.0) + value
                bucket["min"] = min(bucket["min"], value) if "min" in bucket else value
                bucket["max"] = max(bucket["max"], value) if "max" in bucket else value

    def flush(self) -> None:
        for key, bucket in self._buckets.items():
            summary: Dict[str, Any] = {}
            if self._group_by:
                summary[self._group_by] = key
            for op in self._ops:
                if op in bucket:
                    summary[op] = bucket[op]
            self._downstream.write(summary)
        self._buckets.clear()
        self._downstream.flush()

    def close(self) -> None:
        self.flush()
        self._downstream.close()
