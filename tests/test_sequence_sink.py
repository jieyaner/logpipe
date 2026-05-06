"""Tests for SequenceSink."""

import pytest
from logpipe.sinks.sequence_sink import SequenceSink


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


def _make_sink(**kwargs):
    cap = _CaptureSink()
    sink = SequenceSink(cap, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestSequenceSinkConstruction:
    def test_invalid_step_raises(self):
        with pytest.raises(ValueError, match="step must be"):
            SequenceSink(_CaptureSink(), step=0)

    def test_default_counter_starts_at_one(self):
        sink, _ = _make_sink()
        assert sink.current == 1

    def test_custom_start(self):
        sink, _ = _make_sink(start=100)
        assert sink.current == 100


# ---------------------------------------------------------------------------
# Sequencing behaviour
# ---------------------------------------------------------------------------

class TestSequenceSinkWrites:
    def test_injects_seq_field(self):
        sink, cap = _make_sink()
        sink.write({"msg": "hello"})
        assert cap.records[0]["_seq"] == 1

    def test_counter_increments_per_record(self):
        sink, cap = _make_sink()
        for _ in range(3):
            sink.write({"x": 1})
        seqs = [r["_seq"] for r in cap.records]
        assert seqs == [1, 2, 3]

    def test_custom_step(self):
        sink, cap = _make_sink(step=10)
        sink.write({})
        sink.write({})
        assert [r["_seq"] for r in cap.records] == [1, 11]

    def test_custom_field_name(self):
        sink, cap = _make_sink(field="seq_no")
        sink.write({"a": 1})
        assert "seq_no" in cap.records[0]
        assert "_seq" not in cap.records[0]

    def test_overwrite_true_replaces_existing(self):
        sink, cap = _make_sink(overwrite=True)
        sink.write({"_seq": 999, "msg": "x"})
        assert cap.records[0]["_seq"] == 1

    def test_overwrite_false_skips_stamping(self):
        sink, cap = _make_sink(overwrite=False)
        sink.write({"_seq": 42, "msg": "x"})
        assert cap.records[0]["_seq"] == 42
        # counter must NOT have advanced
        assert sink.current == 1

    def test_original_record_not_mutated(self):
        sink, _ = _make_sink()
        original = {"msg": "hi"}
        sink.write(original)
        assert "_seq" not in original


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestSequenceSinkReset:
    def test_reset_restarts_from_given_value(self):
        sink, cap = _make_sink()
        sink.write({})
        sink.write({})
        sink.reset(1)
        sink.write({})
        assert cap.records[-1]["_seq"] == 1

    def test_reset_default_is_one(self):
        sink, _ = _make_sink(start=50)
        sink.reset()
        assert sink.current == 1


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestSequenceSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed
