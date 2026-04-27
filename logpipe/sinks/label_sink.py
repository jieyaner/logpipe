"""LabelSink — stamps every record with a fixed set of metadata labels.

This is a thin convenience wrapper around EnrichSink for the common case
where all enrichment values are static strings (e.g. pipeline name, source
tag, datacenter).  Labels are always plain values; use EnrichSink directly
if you need callable enrichment.
"""

from __future__ import annotations

from typing import Dict

from logpipe.sinks import BaseSink
from logpipe.sinks.enrich_sink import EnrichSink


class LabelSink(BaseSink):
    """Attaches static string labels to every forwarded record.

    Example::

        LabelSink(
            downstream,
            labels={"pipeline": "nginx-access", "dc": "eu-west-1"},
        )
    """

    def __init__(self, sink: BaseSink, labels: Dict[str, str]) -> None:
        if not labels:
            raise ValueError("LabelSink requires at least one label")
        # Delegate all logic to EnrichSink; labels are just static fields.
        self._inner = EnrichSink(sink, fields=dict(labels))

    def write(self, record: dict) -> None:
        self._inner.write(record)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"LabelSink(inner={self._inner!r})"
