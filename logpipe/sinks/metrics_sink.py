"""A sink that records write/flush activity into the metrics collector."""

from logpipe.metrics import MetricsCollector, default_metrics
from logpipe.sinks import BaseSink


class MetricsSink(BaseSink):
    """Wraps another sink and tracks records written and bytes flushed.

    Parameters
    ----------
    inner:
        The real sink that will receive records.
    metrics:
        Collector to update.  Defaults to the module-level ``default_metrics``.
    """

    def __init__(self, inner: BaseSink, metrics: MetricsCollector | None = None) -> None:
        self._inner = inner
        self._metrics = metrics or default_metrics

    def write(self, record: dict) -> None:
        self._inner.write(record)
        self._metrics.increment("sink.records_written")

    def flush(self) -> None:
        self._inner.flush()
        self._metrics.increment("sink.flush_calls")

    def close(self) -> None:
        self._inner.close()
        self._metrics.increment("sink.close_calls")
