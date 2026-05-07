"""PrioritySink — routes records to different sinks based on a priority field.

Records are matched against priority levels (highest first).  If no level
matches the configured thresholds the record falls through to the default
sink (if provided).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from logpipe.sinks import BaseSink


class PriorityError(Exception):
    """Raised for misconfigured priority rules."""


class PrioritySink(BaseSink):
    """Route records to a sink chosen by a numeric or string priority field.

    Parameters
    ----------
    field:
        Dot-separated path to the priority field inside each record.
    levels:
        Ordered list of ``(threshold, sink)`` pairs.  For *numeric* fields the
        record is forwarded to the first sink whose threshold is <= the field
        value.  For *string* fields an exact match is performed.
    default:
        Sink that receives records that match no level.  When ``None`` such
        records are silently dropped.
    """

    def __init__(
        self,
        field: str,
        levels: List[Tuple[Any, BaseSink]],
        default: Optional[BaseSink] = None,
    ) -> None:
        if not field:
            raise PriorityError("field must be a non-empty string")
        if not levels:
            raise PriorityError("at least one priority level is required")
        self._field = field.split(".")
        self._levels = levels
        self._default = default

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_field(self, record: Dict[str, Any]) -> Any:
        value = record
        for part in self._field:
            if not isinstance(value, dict):
                return None
            value = value.get(part)
        return value

    def _pick_sink(self, value: Any) -> Optional[BaseSink]:
        if value is None:
            return self._default
        for threshold, sink in self._levels:
            if isinstance(threshold, (int, float)) and isinstance(value, (int, float)):
                if value >= threshold:
                    return sink
            else:
                if value == threshold:
                    return sink
        return self._default

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        value = self._get_field(record)
        sink = self._pick_sink(value)
        if sink is not None:
            sink.write(record)

    def flush(self) -> None:
        seen: set = set()
        for _, sink in self._levels:
            if id(sink) not in seen:
                sink.flush()
                seen.add(id(sink))
        if self._default is not None and id(self._default) not in seen:
            self._default.flush()

    def close(self) -> None:
        seen: set = set()
        for _, sink in self._levels:
            if id(sink) not in seen:
                sink.close()
                seen.add(id(sink))
        if self._default is not None and id(self._default) not in seen:
            self._default.close()
