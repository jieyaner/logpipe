"""Tests for HashSink."""

from __future__ import annotations

import pytest

from logpipe.sinks.hash_sink import HashError, HashSink


# ---------------------------------------------------------------------------
# helpers
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


def _make_sinks(n=3):
    return [_CaptureSink() for _ in range(n)]


# ---------------------------------------------------------------------------
# construction guards
# ---------------------------------------------------------------------------

class TestHashSinkConstruction:
    def test_empty_sinks_raises(self):
        with pytest.raises(HashError, match="at least one"):
            HashSink("host", sinks=[])

    def test_unknown_missing_mode_raises(self):
        sinks = _make_sinks(2)
        with pytest.raises(HashError, match="unknown missing mode"):
            HashSink("host", sinks=sinks, missing="skip")

    def test_single_sink_accepted(self):
        sink = _CaptureSink()
        hs = HashSink("host", sinks=[sink])
        assert hs is not None


# ---------------------------------------------------------------------------
# routing behaviour
# ---------------------------------------------------------------------------

class TestHashSinkRouting:
    def test_same_value_always_routes_to_same_sink(self):
        sinks = _make_sinks(4)
        hs = HashSink("host", sinks=sinks)
        record = {"host": "web-01", "msg": "ok"}
        for _ in range(10):
            hs.write(record)
        counts = [len(s.records) for s in sinks]
        assert sum(counts) == 10
        # all writes went to the same bucket
        assert max(counts) == 10

    def test_different_values_can_spread_across_sinks(self):
        sinks = _make_sinks(4)
        hs = HashSink("host", sinks=sinks)
        hosts = [f"host-{i}" for i in range(20)]
        for h in hosts:
            hs.write({"host": h})
        used = sum(1 for s in sinks if s.records)
        assert used > 1  # spread across multiple buckets

    def test_nested_field_routing(self):
        sinks = _make_sinks(2)
        hs = HashSink("meta.region", sinks=sinks)
        hs.write({"meta": {"region": "us-east-1"}, "msg": "a"})
        hs.write({"meta": {"region": "us-east-1"}, "msg": "b"})
        counts = [len(s.records) for s in sinks]
        assert sum(counts) == 2
        assert max(counts) == 2  # same region → same sink

    def test_single_sink_receives_all(self):
        sink = _CaptureSink()
        hs = HashSink("id", sinks=[sink])
        for i in range(5):
            hs.write({"id": str(i)})
        assert len(sink.records) == 5


# ---------------------------------------------------------------------------
# missing field modes
# ---------------------------------------------------------------------------

class TestHashSinkMissingField:
    def test_missing_error_mode_raises(self):
        sinks = _make_sinks(2)
        hs = HashSink("host", sinks=sinks, missing="error")
        with pytest.raises(HashError, match="host"):
            hs.write({"msg": "no host here"})

    def test_missing_drop_mode_discards(self):
        sinks = _make_sinks(2)
        hs = HashSink("host", sinks=sinks, missing="drop")
        hs.write({"msg": "no host here"})
        assert all(len(s.records) == 0 for s in sinks)

    def test_missing_first_mode_sends_to_first_sink(self):
        sinks = _make_sinks(3)
        hs = HashSink("host", sinks=sinks, missing="first")
        hs.write({"msg": "no host here"})
        assert len(sinks[0].records) == 1
        assert len(sinks[1].records) == 0
        assert len(sinks[2].records) == 0


# ---------------------------------------------------------------------------
# flush / close delegation
# ---------------------------------------------------------------------------

class TestHashSinkLifecycle:
    def test_flush_propagates_to_all_sinks(self):
        sinks = _make_sinks(3)
        hs = HashSink("k", sinks=sinks)
        hs.flush()
        assert all(s.flushed == 1 for s in sinks)

    def test_close_propagates_to_all_sinks(self):
        sinks = _make_sinks(3)
        hs = HashSink("k", sinks=sinks)
        hs.close()
        assert all(s.closed for s in sinks)

    def test_alternative_algorithm_accepted(self):
        sinks = _make_sinks(2)
        hs = HashSink("id", sinks=sinks, algorithm="sha256")
        hs.write({"id": "abc"})
        assert sum(len(s.records) for s in sinks) == 1
