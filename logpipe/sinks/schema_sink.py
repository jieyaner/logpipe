"""Schema-validating sink wrapper.

Drops or raises on records that do not conform to a required field schema.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from logpipe.sinks import BaseSink


class SchemaValidationError(Exception):
    """Raised when a record fails schema validation."""


class SchemaSink(BaseSink):
    """Wraps a downstream sink and validates records against a field schema.

    Args:
        sink: Downstream :class:`BaseSink` to forward valid records to.
        required_fields: Mapping of field name -> expected type (or ``None``
            to accept any type).  Records missing a required field, or whose
            field value is not an instance of the declared type, are handled
            according to *on_error*.
        on_error: ``"drop"`` (default) silently discards invalid records;
            ``"raise"`` raises :class:`SchemaValidationError`.
    """

    def __init__(
        self,
        sink: BaseSink,
        required_fields: Dict[str, Optional[type]],
        on_error: str = "drop",
    ) -> None:
        if on_error not in ("drop", "raise"):
            raise ValueError("on_error must be 'drop' or 'raise'")
        self._sink = sink
        self._required_fields = required_fields
        self._on_error = on_error
        self._dropped = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate(self, record: Dict[str, Any]) -> Optional[str]:
        """Return an error message if *record* is invalid, else ``None``."""
        for field, expected_type in self._required_fields.items():
            if field not in record:
                return f"missing required field '{field}'"
            if expected_type is not None and not isinstance(record[field], expected_type):
                actual = type(record[field]).__name__
                return (
                    f"field '{field}' expected {expected_type.__name__}, got {actual}"
                )
        return None

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        error = self._validate(record)
        if error:
            self._dropped += 1
            if self._on_error == "raise":
                raise SchemaValidationError(error)
            return
        self._sink.write(record)

    def flush(self) -> None:
        self._sink.flush()

    def close(self) -> None:
        self._sink.close()

    @property
    def dropped(self) -> int:
        """Number of records dropped due to schema violations."""
        return self._dropped
