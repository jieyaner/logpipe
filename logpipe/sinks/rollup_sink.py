"""RollupSink – periodically emits a rolled-up summary record.

Instead of forwarding every individual record, the sink accumulates
numeric field values over a configurable time window and emits a single
summary record (with sum, count, min, max) when the window closes.
"""

import time
from typing import Any, Dict, List, Optional

from logpipe.sinks import BaseSink


class RollupError(Exception):
    """Raised when RollupSink is mis-configured."""


class RollupSink(BaseSink):
    """Accumulate numeric fields and emit a summary record each window.

    Parameters
    ----------
    sink:
        Downstream sink that receives the rolled-up summary records.
    fields:
        Numeric field names to aggregate.  At least one required.
    window_seconds:
        Length of each aggregation window in seconds (default: 60).
    timestamp_field:
        Name of the field used to carry the window-end timestamp in the
        emitted summary record (default: ``"timestamp"``).
    """

    def __init__(
        self,
        sink: BaseSink,
        fields: List[str],
        window_seconds: float = 60.0,
        timestamp_field: str = "timestamp",
    ) -> None:
        if not fields:
            raise RollupError("At least one field name must be specified.")
        if window_seconds <= 0:
            raise RollupError("window_seconds must be positive.")

        self._sink = sink
        self._fields = list(fields)
        self._window_seconds = window_seconds
        self._timestamp_field = timestamp_field
        self._reset()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reset(self) -> None:
        self._window_start: float = time.monotonic()
        self._counts: Dict[str, int] = {f: 0 for f in self._fields}
        self._sums: Dict[str, float] = {f: 0.0 for f in self._fields}
        self._mins: Dict[str, Optional[float]] = {f: None for f in self._fields}
        self._maxs: Dict[str, Optional[float]] = {f: None for f in self._fields}

    def _window_expired(self) -> bool:
        return (time.monotonic() - self._window_start) >= self._window_seconds

    def _emit_summary(self) -> None:
        if all(c == 0 for c in self._counts.values()):
            self._reset()
            return
        record: Dict[str, Any] = {self._timestamp_field: time.time()}
        for f in self._fields:
            record[f"{f}.count"] = self._counts[f]
            record[f"{f}.sum"] = self._sums[f]
            record[f"{f}.min"] = self._mins[f]
            record[f"{f}.max"] = self._maxs[f]
        self._sink.write(record)
        self._reset()

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        if self._window_expired():
            self._emit_summary()

        for f in self._fields:
            raw = record.get(f)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            self._counts[f] += 1
            self._sums[f] += value
            self._mins[f] = value if self._mins[f] is None else min(self._mins[f], value)
            self._maxs[f] = value if self._maxs[f] is None else max(self._maxs[f], value)

    def flush(self) -> None:
        self._emit_summary()
        self._sink.flush()

    def close(self) -> None:
        self.flush()
        self._sink.close()
