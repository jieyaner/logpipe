"""Builder helper for PrioritySink — wires up levels from a plain dict config.

Expected config shape::

    {
      "field": "level",
      "levels": [
        {"threshold": "critical", "sink": <BaseSink>},
        {"threshold": "error",    "sink": <BaseSink>}
      ],
      "default": <BaseSink>          # optional
    }

Numeric thresholds are automatically coerced from strings so that YAML /
JSON configs work without extra ceremony.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from logpipe.sinks import BaseSink
from logpipe.sinks.priority_sink import PriorityError, PrioritySink


def _coerce(value: Any) -> Any:
    """Try to parse *value* as int then float; return original on failure."""
    for cast in (int, float):
        try:
            return cast(value)  # type: ignore[call-overload]
        except (TypeError, ValueError):
            pass
    return value


def build_priority_sink(
    field: str,
    levels: List[Dict[str, Any]],
    default: Optional[BaseSink] = None,
) -> PrioritySink:
    """Construct a :class:`PrioritySink` from a declarative config.

    Parameters
    ----------
    field:
        Dot-path of the priority field.
    levels:
        List of dicts, each with ``threshold`` and ``sink`` keys.
    default:
        Fallback sink for unmatched records.

    Raises
    ------
    PriorityError
        When *levels* is empty or a level entry is malformed.
    """
    if not levels:
        raise PriorityError("levels list must not be empty")

    parsed: List[Tuple[Any, BaseSink]] = []
    for i, entry in enumerate(levels):
        if "threshold" not in entry:
            raise PriorityError(f"level[{i}] is missing 'threshold'")
        if "sink" not in entry:
            raise PriorityError(f"level[{i}] is missing 'sink'")
        threshold = _coerce(entry["threshold"])
        sink = entry["sink"]
        if not isinstance(sink, BaseSink):
            raise PriorityError(
                f"level[{i}]['sink'] must be a BaseSink instance, got {type(sink)!r}"
            )
        parsed.append((threshold, sink))

    return PrioritySink(field=field, levels=parsed, default=default)
