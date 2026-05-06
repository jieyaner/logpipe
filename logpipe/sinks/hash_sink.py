"""HashSink – deterministically routes records to one of N downstream sinks
based on a stable hash of a chosen field value.

Useful for sharding high-volume streams across parallel workers or buckets
without duplicating records.
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

from logpipe.sinks import BaseSink


class HashError(Exception):
    """Raised when the hash sink is misconfigured or the field is missing."""


class HashSink(BaseSink):
    """Route each record to a downstream sink chosen by hashing *field*.

    Parameters
    ----------
    field:
        Dot-separated path to the record field whose value is hashed.
    sinks:
        Ordered list of downstream :class:`BaseSink` instances.  The record
        is forwarded to ``sinks[hash(value) % len(sinks)]``.
    missing:
        What to do when *field* is absent.  ``"error"`` raises
        :class:`HashError`; ``"drop"`` silently discards the record;
        ``"first"`` always sends to ``sinks[0]``.  Default: ``"error"``.
    algorithm:
        Hash algorithm passed to :func:`hashlib.new`.  Default: ``"md5"``.
    """

    _MISSING_MODES = {"error", "drop", "first"}

    def __init__(
        self,
        field: str,
        sinks: List[BaseSink],
        *,
        missing: str = "error",
        algorithm: str = "md5",
    ) -> None:
        if not sinks:
            raise HashError("HashSink requires at least one downstream sink")
        if missing not in self._MISSING_MODES:
            raise HashError(
                f"unknown missing mode {missing!r}; choose from {self._MISSING_MODES}"
            )
        self._field_parts: List[str] = field.split(".")
        self._sinks = list(sinks)
        self._missing = missing
        self._algorithm = algorithm

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _get_field(self, record: Dict) -> Optional[str]:
        node = record
        for part in self._field_parts:
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return str(node)

    def _pick_sink(self, value: str) -> BaseSink:
        digest = hashlib.new(self._algorithm, value.encode(), usedforsecurity=False)
        index = int(digest.hexdigest(), 16) % len(self._sinks)
        return self._sinks[index]

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict) -> None:
        value = self._get_field(record)
        if value is None:
            if self._missing == "error":
                field = ".".join(self._field_parts)
                raise HashError(f"field {field!r} not found in record")
            if self._missing == "drop":
                return
            # "first"
            self._sinks[0].write(record)
            return
        self._pick_sink(value).write(record)

    def flush(self) -> None:
        for sink in self._sinks:
            sink.flush()

    def close(self) -> None:
        for sink in self._sinks:
            sink.close()
