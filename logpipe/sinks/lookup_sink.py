"""LookupSink — enriches records by joining a field value against a
static key/value lookup table supplied at construction time.

If the looked-up key is found the result is stored under *dest_field*
(default: the source field name suffixed with ``_lookup``).  Missing
keys are handled according to *on_miss*:
  - ``"skip"``   – forward the record unchanged  (default)
  - ``"drop"``   – discard the record entirely
  - ``"error"``  – raise ``LookupError``
"""

from __future__ import annotations

from typing import Any, Dict, Literal

from logpipe.sinks import BaseSink


class LookupError(Exception):  # noqa: A001 – intentional shadow in this module
    pass


class LookupSink(BaseSink):
    """Enrich records via a static lookup table."""

    _MISS_POLICIES = frozenset({"skip", "drop", "error"})

    def __init__(
        self,
        downstream: BaseSink,
        *,
        src_field: str,
        table: Dict[str, Any],
        dest_field: str | None = None,
        on_miss: Literal["skip", "drop", "error"] = "skip",
    ) -> None:
        if on_miss not in self._MISS_POLICIES:
            raise ValueError(
                f"on_miss must be one of {sorted(self._MISS_POLICIES)}, got {on_miss!r}"
            )
        self._downstream = downstream
        self._src_field = src_field
        self._table = dict(table)
        self._dest_field = dest_field if dest_field is not None else f"{src_field}_lookup"
        self._on_miss = on_miss

    # ------------------------------------------------------------------
    def write(self, record: Dict[str, Any]) -> None:
        key = record.get(self._src_field)
        if key in self._table:
            enriched = dict(record)
            enriched[self._dest_field] = self._table[key]
            self._downstream.write(enriched)
            return

        # key not found
        if self._on_miss == "drop":
            return
        if self._on_miss == "error":
            raise LookupError(
                f"No entry for key {key!r} in lookup table (field={self._src_field!r})"
            )
        # "skip" — forward unchanged
        self._downstream.write(record)

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
