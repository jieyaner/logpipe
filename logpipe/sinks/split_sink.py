"""SplitSink — routes each record to one of N downstream sinks based on
a field value, with an optional fallback sink for unmatched records."""

from __future__ import annotations

from typing import Any, Dict, Optional

from logpipe.sinks import BaseSink


class SplitError(Exception):
    """Raised when the SplitSink is misconfigured."""


class SplitSink(BaseSink):
    """Route records to different sinks based on the value of *field*.

    Parameters
    ----------
    field:
        Dot-separated path to the record field used for routing.
    routes:
        Mapping of field-value (str) -> BaseSink.
    fallback:
        Sink that receives records whose field value is not in *routes*.
        If *None*, unmatched records are silently dropped.
    """

    def __init__(
        self,
        field: str,
        routes: Dict[str, BaseSink],
        fallback: Optional[BaseSink] = None,
    ) -> None:
        if not field:
            raise SplitError("field must be a non-empty string")
        if not routes:
            raise SplitError("routes must contain at least one entry")
        self._field = field.split(".")
        self._routes = routes
        self._fallback = fallback

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_field(self, record: Dict[str, Any]) -> Optional[str]:
        node: Any = record
        for key in self._field:
            if not isinstance(node, dict):
                return None
            node = node.get(key)
        return str(node) if node is not None else None

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        value = self._get_field(record)
        sink = self._routes.get(value) if value is not None else None
        if sink is not None:
            sink.write(record)
        elif self._fallback is not None:
            self._fallback.write(record)

    def flush(self) -> None:
        for sink in self._routes.values():
            sink.flush()
        if self._fallback is not None:
            self._fallback.flush()

    def close(self) -> None:
        for sink in self._routes.values():
            sink.close()
        if self._fallback is not None:
            self._fallback.close()
