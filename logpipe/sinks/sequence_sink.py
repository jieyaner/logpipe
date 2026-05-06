"""SequenceSink — stamps every record with a monotonically increasing sequence number."""

from logpipe.sinks import BaseSink


class SequenceSink(BaseSink):
    """Wraps an inner sink and injects a ``_seq`` field into every record.

    Parameters
    ----------
    inner:
        Downstream :class:`~logpipe.sinks.BaseSink`.
    field:
        Name of the field to inject (default ``"_seq"``).
    start:
        Initial sequence value (default ``1``).
    step:
        Increment per record (default ``1``).
    overwrite:
        When *True* (default) an existing field with the same name is
        replaced; when *False* the record is forwarded unchanged if the
        field already exists.
    """

    def __init__(
        self,
        inner: BaseSink,
        *,
        field: str = "_seq",
        start: int = 1,
        step: int = 1,
        overwrite: bool = True,
    ) -> None:
        if step < 1:
            raise ValueError("step must be >= 1")
        self._inner = inner
        self._field = field
        self._step = step
        self._overwrite = overwrite
        self._counter = start

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._field in record and not self._overwrite:
            self._inner.write(record)
            return
        stamped = {**record, self._field: self._counter}
        self._counter += self._step
        self._inner.write(stamped)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    @property
    def current(self) -> int:
        """Next sequence value that will be assigned."""
        return self._counter

    def reset(self, value: int = 1) -> None:
        """Reset the counter to *value*."""
        self._counter = value
