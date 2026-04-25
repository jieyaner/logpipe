"""Tests for :class:`~logpipe.sinks.sampling_sink.SamplingSink`."""

import pytest

from logpipe.sinks.sampling_sink import SamplingSink


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


def _make_sink(rate, key_field=None):
    cap = _CaptureSink()
    sink = SamplingSink(cap, rate=rate, key_field=key_field)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestSamplingSinkConstruction:
    def test_rate_zero_raises(self):
        with pytest.raises(ValueError, match="rate"):
            SamplingSink(_CaptureSink(), rate=0.0)

    def test_rate_above_one_raises(self):
        with pytest.raises(ValueError, match="rate"):
            SamplingSink(_CaptureSink(), rate=1.1)

    def test_rate_one_is_accepted(self):
        sink, _ = _make_sink(1.0)
        assert sink._rate == 1.0


# ---------------------------------------------------------------------------
# Counter-based sampling (no key field)
# ---------------------------------------------------------------------------

class TestCounterSampling:
    def test_rate_one_forwards_all(self):
        sink, cap = _make_sink(1.0)
        for i in range(10):
            sink.write({"n": i})
        assert len(cap.records) == 10

    def test_rate_half_forwards_every_other(self):
        sink, cap = _make_sink(0.5)
        for i in range(10):
            sink.write({"n": i})
        assert len(cap.records) == 5

    def test_rate_tenth_forwards_one_in_ten(self):
        sink, cap = _make_sink(0.1)
        for i in range(100):
            sink.write({"n": i})
        assert len(cap.records) == 10


# ---------------------------------------------------------------------------
# Hash-based (deterministic) sampling
# ---------------------------------------------------------------------------

class TestHashSampling:
    def test_same_key_always_same_decision(self):
        sink, cap = _make_sink(0.5, key_field="id")
        record = {"id": "abc-123", "msg": "hello"}
        # Write the same record many times — outcome must be consistent.
        decisions = set()
        for _ in range(5):
            before = len(cap.records)
            sink.write(record)
            decisions.add(len(cap.records) > before)
        assert len(decisions) == 1, "deterministic sampling must be stable"

    def test_missing_key_field_uses_empty_string(self):
        """Should not raise even when the key field is absent."""
        sink, cap = _make_sink(0.5, key_field="id")
        sink.write({"msg": "no id here"})
        # No assertion on count — just must not raise.


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink(1.0)
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink(1.0)
        sink.close()
        assert cap.closed is True
