"""Sink that redacts sensitive fields before forwarding records."""

import re
from typing import Any, Dict, List, Optional, Pattern

from logpipe.sinks import BaseSink

_MASK = "***"


class RedactSink(BaseSink):
    """Wraps another sink and redacts specified fields or regex patterns.

    Args:
        sink: Downstream sink to forward redacted records to.
        fields: List of top-level field names whose values will be masked.
        patterns: List of regex pattern strings; any field whose *value* (as a
                  string) matches will be fully masked.
        mask: Replacement string, defaults to '***'.
    """

    def __init__(
        self,
        sink: BaseSink,
        fields: Optional[List[str]] = None,
        patterns: Optional[List[str]] = None,
        mask: str = _MASK,
    ) -> None:
        self._sink = sink
        self._fields: List[str] = fields or []
        self._patterns: List[Pattern] = [
            re.compile(p) for p in (patterns or [])
        ]
        self._mask = mask

    def _redact(self, record: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(record)
        for key, value in out.items():
            if key in self._fields:
                out[key] = self._mask
                continue
            if self._patterns:
                str_value = str(value)
                for pat in self._patterns:
                    if pat.search(str_value):
                        out[key] = self._mask
                        break
        return out

    def write(self, record: Dict[str, Any]) -> None:
        self._sink.write(self._redact(record))

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"RedactSink(fields={self._fields!r}, "
            f"patterns={[p.pattern for p in self._patterns]!r})"
        )
