"""TruncateSink — truncates string field values that exceed a maximum length."""

from logpipe.sinks import BaseSink


class TruncateSink(BaseSink):
    """Wraps a downstream sink and truncates specified string fields to a
    maximum byte/character length before forwarding each record.

    Args:
        sink: Downstream :class:`~logpipe.sinks.BaseSink` to forward records to.
        fields: Mapping of ``{field_name: max_length}`` pairs.  Fields that are
            absent or not strings are left untouched.
        suffix: String appended to a truncated value to signal truncation.
            Defaults to ``"..."``.
    """

    def __init__(self, sink: BaseSink, fields: dict, suffix: str = "...") -> None:
        if not fields:
            raise ValueError("fields must be a non-empty mapping")
        for name, limit in fields.items():
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError(
                    f"max length for field '{name}' must be a positive integer, got {limit!r}"
                )
        self._sink = sink
        self._fields: dict = dict(fields)
        self._suffix: str = suffix

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _truncate(self, record: dict) -> dict:
        """Return a shallow copy of *record* with truncated field values."""
        out = dict(record)
        for field, limit in self._fields.items():
            value = out.get(field)
            if isinstance(value, str) and len(value) > limit:
                cut = limit - len(self._suffix)
                if cut < 0:
                    cut = 0
                out[field] = value[:cut] + self._suffix
        return out

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        self._sink.write(self._truncate(record))

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()
