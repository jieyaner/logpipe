"""Sink decorator that transforms records before forwarding."""

from __future__ import annotations

from typing import Any, Dict, List

from logpipe.sinks import BaseSink
from logpipe.transform import FieldTransformer


class TransformSink(BaseSink):
    """Applies field transformations to each record, then forwards downstream."""

    def __init__(self, downstream: BaseSink, rules: List[Dict[str, Any]]) -> None:
        """
        Parameters
        ----------
        downstream:
            The sink that receives transformed records.
        rules:
            Transformation rule list accepted by :class:`~logpipe.transform.FieldTransformer`.
        """
        self._downstream = downstream
        self._transformer = FieldTransformer(rules)

    def write(self, record: Dict[str, Any]) -> None:
        transformed = self._transformer.apply(record)
        self._downstream.write(transformed)

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
