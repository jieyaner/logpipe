"""Tests for :class:`~logpipe.sinks.dedup_sink.DedupSink`."""

import time

import pytest

from logpipe.sinks.dedup_sink import DedupSink


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


def _make_sink(ttl=60.0, max_cache=1000, key_field="id"):
    cap = _CaptureSink()
    sink = DedupSink(cap, key_field=key_field, ttl_seconds=ttl, max_cache=max_cache)
    return sink, cap


# ---------------------------------------------------------------------------
# Basic deduplication
# ---------------------------------------------------------------------------

class TestDedupBasic:
    def test_first_write_forwarded(self):
        sink, cap = _make_sink()
        sink.write({"id": "a", "msg": "hello"})
        assert len(cap.records) == 1

    def test_duplicate_within_ttl_dropped(self):
        sink, cap = _make_sink(ttl=60.0)
        sink.write({"id": "a"})
        sink.write({"id": "a"})
        assert len(cap.records) == 1

    def test_different_keys_both_forwarded(self):
        sink, cap = _make_sink()
        sink.write({"id": "a"})
        sink.write({"id": "b"})
        assert len(cap.records) == 2

    def test_missing_key_field_treated_as_sentinel(self):
        """Records without the key field share the same sentinel slot."""
        sink, cap = _make_sink()
        sink.write({"msg": "no id"})
        sink.write({"msg": "also no id"})
        # Second write has same key (sentinel) → duplicate.
        assert len(cap.records) == 1


# ---------------------------------------------------------------------------
# TTL expiry
# ---------------------------------------------------------------------------

class TestDedupTTL:
    def test_record_forwarded_after_ttl_expires(self):
        sink, cap = _make_sink(ttl=0.05)
        sink.write({"id": "x"})
        time.sleep(0.1)
        sink.write({"id": "x"})
        assert len(cap.records) == 2

    def test_record_dropped_before_ttl_expires(self):
        sink, cap = _make_sink(ttl=5.0)
        sink.write({"id": "y"})
        sink.write({"id": "y"})
        assert len(cap.records) == 1


# ---------------------------------------------------------------------------
# Cache eviction
# ---------------------------------------------------------------------------

class TestDedupMaxCache:
    def test_max_cache_evicts_oldest(self):
        sink, cap = _make_sink(ttl=9999, max_cache=3)
        for i in range(4):
            sink.write({"id": str(i)})
        # After 4 writes with max_cache=3 the oldest key ("0") was evicted.
        assert len(cap.records) == 4
        # Writing "0" again should pass (it was evicted).
        sink.write({"id": "0"})
        assert len(cap.records) == 5


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestDedupDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed is True
