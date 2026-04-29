"""BatchSink — accumulates records and flushes downstream in fixed-size batches."""

from logpipe.sinks import BaseSink


class BatchSink(BaseSink):
    """Buffers up to *batch_size* records, then forwards them all at once.

    Parameters
    ----------
    downstream:
        The sink that receives each record when a batch is flushed.
    batch_size:
        Number of records to accumulate before an automatic flush.
        Must be >= 1.
    """

    def __init__(self, downstream: BaseSink, batch_size: int = 100) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        self._downstream = downstream
        self._batch_size = batch_size
        self._buffer: list[dict] = []

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        """Append *record* to the internal buffer; flush if batch is full."""
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            self._flush_buffer()

    def flush(self) -> None:
        """Force-flush any buffered records downstream, then propagate flush."""
        self._flush_buffer()
        self._downstream.flush()

    def close(self) -> None:
        """Flush remaining records and close the downstream sink."""
        self._flush_buffer()
        self._downstream.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flush_buffer(self) -> None:
        """Write all buffered records downstream and clear the buffer."""
        for record in self._buffer:
            self._downstream.write(record)
        self._buffer.clear()

    # Expose read-only view of current buffer depth for metrics / testing.
    @property
    def pending(self) -> int:
        """Number of records currently waiting in the buffer."""
        return len(self._buffer)
