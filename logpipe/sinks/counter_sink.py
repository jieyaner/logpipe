"""CounterSink — counts records passing through and optionally injects the
running total into each record as a metadata field."""

from __future__ import annotations

from typing import Callable, Optional

from logpipe.sinks import BaseSink


class CounterSink(BaseSink):
    """Wraps an inner sink and tracks how many records have been written.

    Parameters
    ----------
    inner:
        Downstream :class:`~logpipe.sinks.BaseSink`.
    field:
        When not *None*, the running total is injected into every record
        under this field name before forwarding.
    predicate:
        Optional callable ``(record) -> bool``.  Only records for which it
        returns *True* increment the counter (all records are still
        forwarded regardless).
    """

    def __init__(
        self,
        inner: BaseSink,
        *,
        field: Optional[str] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
    ) -> None:
        self._inner = inner
        self._field = field
        self._predicate = predicate
        self._count: int = 0

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._predicate is None or self._predicate(record):
            self._count += 1
        if self._field is not None:
            record = {**record, self._field: self._count}
        self._inner.write(record)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        """Number of records that have been counted so far."""
        return self._count

    def reset(self) -> None:
        """Reset the counter to zero."""
        self._count = 0
