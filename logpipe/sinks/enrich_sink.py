"""EnrichSink — adds static or dynamic fields to every record before forwarding."""

from __future__ import annotations

from typing import Any, Callable, Dict, Union

from logpipe.sinks import BaseSink


FieldSpec = Union[Any, Callable[[Dict[str, Any]], Any]]


class EnrichSink(BaseSink):
    """Wraps another sink and injects extra fields into each record.

    Each entry in *fields* may be a plain value or a one-argument callable
    that receives the record dict and returns the value to inject.

    Example::

        EnrichSink(
            downstream,
            fields={
                "env": "production",
                "host": lambda r: socket.gethostname(),
            },
        )
    """

    def __init__(self, sink: BaseSink, fields: Dict[str, FieldSpec]) -> None:
        if not fields:
            raise ValueError("EnrichSink requires at least one field")
        self._sink = sink
        self._fields: Dict[str, FieldSpec] = dict(fields)

    def _enrich(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)
        for key, value in self._fields.items():
            enriched[key] = value(record) if callable(value) else value
        return enriched

    def write(self, record: Dict[str, Any]) -> None:
        self._sink.write(self._enrich(record))

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"EnrichSink(fields={list(self._fields)}, sink={self._sink!r})"
