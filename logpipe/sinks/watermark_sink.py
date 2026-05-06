"""WatermarkSink — tracks low/high watermarks for a numeric field across all
records seen, and optionally injects the current watermarks into each record."""

from __future__ import annotations

from typing import Optional

from logpipe.sinks import BaseSink


class WatermarkError(Exception):
    """Raised when a watermark-related configuration problem is detected."""


class WatermarkSink(BaseSink):
    """Tracks the minimum and maximum value of *source_field* across records.

    Parameters
    ----------
    inner:
        Downstream :class:`~logpipe.sinks.BaseSink`.
    source_field:
        Numeric field to track.
    low_field / high_field:
        When not *None*, the current low / high watermark is injected into
        each record under that field name before forwarding.
    skip_missing:
        When *True* (default) records that do not contain *source_field* are
        forwarded without updating watermarks.  When *False* a
        :class:`WatermarkError` is raised.
    """

    def __init__(
        self,
        inner: BaseSink,
        source_field: str,
        *,
        low_field: Optional[str] = None,
        high_field: Optional[str] = None,
        skip_missing: bool = True,
    ) -> None:
        self._inner = inner
        self._source_field = source_field
        self._low_field = low_field
        self._high_field = high_field
        self._skip_missing = skip_missing
        self._low: Optional[float] = None
        self._high: Optional[float] = None

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        if self._source_field not in record:
            if not self._skip_missing:
                raise WatermarkError(
                    f"Field '{self._source_field}' missing from record"
                )
            self._inner.write(record)
            return

        value = float(record[self._source_field])
        if self._low is None or value < self._low:
            self._low = value
        if self._high is None or value > self._high:
            self._high = value

        if self._low_field is not None or self._high_field is not None:
            record = dict(record)
            if self._low_field is not None:
                record[self._low_field] = self._low
            if self._high_field is not None:
                record[self._high_field] = self._high

        self._inner.write(record)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    @property
    def low(self) -> Optional[float]:
        """Current low watermark, or *None* if no records have been tracked."""
        return self._low

    @property
    def high(self) -> Optional[float]:
        """Current high watermark, or *None* if no records have been tracked."""
        return self._high

    def reset(self) -> None:
        """Clear both watermarks."""
        self._low = None
        self._high = None
