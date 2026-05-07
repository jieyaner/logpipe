"""TimestampSink — injects or normalises a timestamp field on every record.

Behaviour
---------
* If the target field is absent the current UTC time is written (ISO-8601).
* If *overwrite* is True the field is always replaced with the current time.
* The output format is configurable; it defaults to ``%Y-%m-%dT%H:%M:%S.%fZ``.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict

from logpipe.sinks import BaseSink

_DEFAULT_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


class TimestampSink(BaseSink):
    """Wrap *downstream* and stamp each record with a UTC timestamp."""

    def __init__(
        self,
        downstream: BaseSink,
        *,
        field: str = "@timestamp",
        fmt: str = _DEFAULT_FMT,
        overwrite: bool = False,
    ) -> None:
        if not field:
            raise ValueError("field must be a non-empty string")
        self._downstream = downstream
        self._field = field
        self._fmt = fmt
        self._overwrite = overwrite

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return datetime.datetime.utcnow().strftime(self._fmt)

    def _stamp(self, record: Dict[str, Any]) -> Dict[str, Any]:
        if self._overwrite or self._field not in record:
            return {**record, self._field: self._now()}
        return record

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        self._downstream.write(self._stamp(record))

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
