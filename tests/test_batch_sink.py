"""Tests for logpipe.sinks.batch_sink.BatchSink."""

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.batch_sink import BatchSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self):
        self.received: list[dict] = []
        self.flush_calls: int = 0
        self.close_calls: int = 0

    def write(self, record: dict) -> None:
        self.received.append(record)

    def flush(self) -> None:
        self.flush_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _make_sink(batch_size: int = 3) -> tuple[BatchSink, _CaptureSink]:
    downstream = _CaptureSink()
    sink = BatchSink(downstream, batch_size=batch_size)
    return sink, downstream


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestBatchSinkConstruction:
    def test_invalid_batch_size_raises(self):
        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            BatchSink(_CaptureSink(), batch_size=0)

    def test_valid_batch_size_accepted(self):
        sink, _ = _make_sink(batch_size=1)
        assert sink._batch_size == 1


# ---------------------------------------------------------------------------
# Buffering behaviour
# ---------------------------------------------------------------------------

class TestBatchSinkBuffering:
    def test_records_buffered_until_batch_full(self):
        sink, downstream = _make_sink(batch_size=3)
        sink.write({"n": 1})
        sink.write({"n": 2})
        assert downstream.received == []
        assert sink.pending == 2

    def test_flush_triggered_at_batch_size(self):
        sink, downstream = _make_sink(batch_size=3)
        for i in range(3):
            sink.write({"n": i})
        assert len(downstream.received) == 3
        assert sink.pending == 0

    def test_overflow_starts_new_batch(self):
        sink, downstream = _make_sink(batch_size=2)
        for i in range(5):
            sink.write({"n": i})
        # Two full batches flushed automatically (4 records), 1 still pending
        assert len(downstream.received) == 4
        assert sink.pending == 1

    def test_records_forwarded_in_order(self):
        sink, downstream = _make_sink(batch_size=3)
        records = [{"n": i} for i in range(3)]
        for r in records:
            sink.write(r)
        assert downstream.received == records


# ---------------------------------------------------------------------------
# Explicit flush / close
# ---------------------------------------------------------------------------

class TestBatchSinkFlushClose:
    def test_explicit_flush_drains_partial_batch(self):
        sink, downstream = _make_sink(batch_size=10)
        sink.write({"x": 1})
        sink.write({"x": 2})
        sink.flush()
        assert len(downstream.received) == 2
        assert sink.pending == 0

    def test_explicit_flush_propagates_to_downstream(self):
        sink, downstream = _make_sink(batch_size=10)
        sink.flush()
        assert downstream.flush_calls == 1

    def test_close_drains_buffer_and_closes_downstream(self):
        sink, downstream = _make_sink(batch_size=10)
        sink.write({"y": 99})
        sink.close()
        assert len(downstream.received) == 1
        assert downstream.close_calls == 1

    def test_flush_on_empty_buffer_is_safe(self):
        sink, downstream = _make_sink(batch_size=5)
        sink.flush()  # should not raise
        assert downstream.flush_calls == 1
