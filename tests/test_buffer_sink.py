"""Tests for logpipe.sinks.buffer_sink.BufferedSink."""

import time
import pytest
from logpipe.sinks.buffer_sink import BufferedSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self):
        self.records: list[dict] = []
        self.flush_count: int = 0
        self.close_count: int = 0

    def write(self, record: dict) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flush_count += 1

    def close(self) -> None:
        self.close_count += 1


def _make_sink(**kwargs) -> tuple["BufferedSink", "_CaptureSink"]:
    inner = _CaptureSink()
    sink = BufferedSink(inner, **kwargs)
    return sink, inner


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

class TestBufferedSinkInit:
    def test_invalid_max_size_raises(self):
        with pytest.raises(ValueError, match="max_size"):
            BufferedSink(_CaptureSink(), max_size=0)

    def test_invalid_max_age_raises(self):
        with pytest.raises(ValueError, match="max_age_seconds"):
            BufferedSink(_CaptureSink(), max_age_seconds=0)


# ---------------------------------------------------------------------------
# Buffering behaviour
# ---------------------------------------------------------------------------

class TestBufferedSinkBatching:
    def test_records_held_until_max_size(self):
        sink, inner = _make_sink(max_size=3)
        sink.write({"n": 1})
        sink.write({"n": 2})
        assert inner.records == [], "should not have flushed yet"
        assert sink.buffered_count == 2

    def test_flushes_at_max_size(self):
        sink, inner = _make_sink(max_size=3)
        for i in range(3):
            sink.write({"n": i})
        assert len(inner.records) == 3
        assert sink.buffered_count == 0

    def test_explicit_flush_forwards_records(self):
        sink, inner = _make_sink(max_size=10)
        sink.write({"msg": "hello"})
        sink.flush()
        assert inner.records == [{"msg": "hello"}]
        assert inner.flush_count == 1

    def test_flush_clears_buffer(self):
        sink, inner = _make_sink(max_size=10)
        sink.write({"x": 1})
        sink.flush()
        assert sink.buffered_count == 0

    def test_flush_on_empty_buffer_is_noop(self):
        sink, inner = _make_sink(max_size=5)
        sink.flush()  # should not raise
        assert inner.flush_count == 0

    def test_close_flushes_remaining_records(self):
        sink, inner = _make_sink(max_size=100)
        sink.write({"final": True})
        sink.close()
        assert inner.records == [{"final": True}]
        assert inner.close_count == 1

    def test_age_based_flush(self):
        sink, inner = _make_sink(max_size=100, max_age_seconds=0.05)
        sink.write({"early": True})
        time.sleep(0.1)
        # next write should trigger age-based flush
        sink.write({"late": True})
        assert len(inner.records) == 2

    def test_multiple_batches_accumulate_correctly(self):
        sink, inner = _make_sink(max_size=2)
        for i in range(6):
            sink.write({"i": i})
        assert len(inner.records) == 6
        assert inner.flush_count == 3
