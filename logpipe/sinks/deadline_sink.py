"""DeadlineSink — drops records that exceed a per-record write budget.

If the downstream sink's write() call takes longer than `timeout_seconds`,
the record is dropped and the overrun is counted via an optional
MetricsCollector.
"""

import threading
from logpipe.sinks import BaseSink


class DeadlineSink(BaseSink):
    """Forward records to *inner* only if write completes within the deadline."""

    def __init__(self, inner: BaseSink, timeout_seconds: float, metrics=None):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._inner = inner
        self._timeout = timeout_seconds
        self._metrics = metrics

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        result = {"exc": None}

        def _do_write():
            try:
                self._inner.write(record)
            except Exception as exc:  # noqa: BLE001
                result["exc"] = exc

        t = threading.Thread(target=_do_write, daemon=True)
        t.start()
        t.join(self._timeout)

        if t.is_alive():
            # Write is still in progress — count the drop and move on.
            if self._metrics is not None:
                self._metrics.increment("deadline_sink.timeout")
            return

        if result["exc"] is not None:
            raise result["exc"]

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
