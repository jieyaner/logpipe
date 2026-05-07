"""Tests for logpipe.sinks.join_sink.JoinSink."""

import time
import pytest

from logpipe.sinks.join_sink import JoinError, JoinSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flushed = 0
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed = True


def _make_sink(channels=("a", "b"), key="id", ttl=60.0):
    cap = _CaptureSink()
    sink = JoinSink(cap, key=key, channels=list(channels), ttl=ttl)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestJoinSinkConstruction:
    def test_empty_channels_raises(self):
        with pytest.raises(JoinError, match="channels must not be empty"):
            JoinSink(_CaptureSink(), key="id", channels=[])

    def test_duplicate_channels_raises(self):
        with pytest.raises(JoinError, match="channels must be unique"):
            JoinSink(_CaptureSink(), key="id", channels=["a", "a"])


# ---------------------------------------------------------------------------
# Merging behaviour
# ---------------------------------------------------------------------------

class TestJoinSinkMerge:
    def test_no_emit_until_all_channels_present(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "a", "id": "1", "x": 1})
        assert cap.records == []

    def test_emits_when_both_channels_arrive(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "a", "id": "1", "x": 1})
        sink.write({"_channel": "b", "id": "1", "y": 2})
        assert len(cap.records) == 1
        merged = cap.records[0]
        assert merged["x"] == 1
        assert merged["y"] == 2
        assert merged["id"] == "1"

    def test_channel_field_stripped_from_merged_record(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "a", "id": "1"})
        sink.write({"_channel": "b", "id": "1"})
        assert "_channel" not in cap.records[0]

    def test_different_keys_tracked_independently(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "a", "id": "1", "v": "a1"})
        sink.write({"_channel": "a", "id": "2", "v": "a2"})
        sink.write({"_channel": "b", "id": "1", "v": "b1"})
        assert len(cap.records) == 1
        sink.write({"_channel": "b", "id": "2", "v": "b2"})
        assert len(cap.records) == 2

    def test_unknown_channel_is_dropped(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "z", "id": "1"})
        assert cap.records == []

    def test_missing_key_field_is_dropped(self):
        sink, cap = _make_sink()
        sink.write({"_channel": "a", "other": "x"})
        assert cap.records == []

    def test_nested_key(self):
        sink, cap = _make_sink(key="meta.id")
        sink.write({"_channel": "a", "meta": {"id": "9"}, "p": 1})
        sink.write({"_channel": "b", "meta": {"id": "9"}, "q": 2})
        assert cap.records[0]["p"] == 1
        assert cap.records[0]["q"] == 2

    def test_bucket_evicted_after_ttl(self):
        sink, cap = _make_sink(ttl=0.05)
        sink.write({"_channel": "a", "id": "1", "x": 1})
        time.sleep(0.1)
        # Trigger eviction via a new write
        sink.write({"_channel": "a", "id": "2"})
        # Now complete key "1" – bucket was evicted, no merge
        sink.write({"_channel": "b", "id": "1", "y": 2})
        assert cap.records == []

    def test_flush_and_close_delegated(self):
        sink, cap = _make_sink()
        sink.flush()
        sink.close()
        assert cap.flushed == 1
        assert cap.closed is True
