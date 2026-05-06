"""Tests for CounterSink."""

from logpipe.sinks.counter_sink import CounterSink


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
    sink = CounterSink(cap, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# Basic counting
# ---------------------------------------------------------------------------

class TestCounterSinkCounting:
    def test_starts_at_zero(self):
        sink, _ = _make_sink()
        assert sink.count == 0

    def test_increments_on_write(self):
        sink, _ = _make_sink()
        sink.write({"a": 1})
        assert sink.count == 1

    def test_counts_multiple_records(self):
        sink, _ = _make_sink()
        for _ in range(5):
            sink.write({})
        assert sink.count == 5

    def test_all_records_forwarded(self):
        sink, cap = _make_sink()
        sink.write({"x": 1})
        sink.write({"x": 2})
        assert len(cap.records) == 2


# ---------------------------------------------------------------------------
# Field injection
# ---------------------------------------------------------------------------

class TestCounterSinkFieldInjection:
    def test_no_field_by_default(self):
        sink, cap = _make_sink()
        sink.write({"msg": "hi"})
        assert "_count" not in cap.records[0]

    def test_injects_count_field(self):
        sink, cap = _make_sink(field="_count")
        sink.write({"msg": "a"})
        assert cap.records[0]["_count"] == 1

    def test_field_reflects_running_total(self):
        sink, cap = _make_sink(field="n")
        for _ in range(3):
            sink.write({})
        assert [r["n"] for r in cap.records] == [1, 2, 3]

    def test_original_record_not_mutated(self):
        sink, _ = _make_sink(field="n")
        rec = {"msg": "hi"}
        sink.write(rec)
        assert "n" not in rec


# ---------------------------------------------------------------------------
# Predicate filtering
# ---------------------------------------------------------------------------

class TestCounterSinkPredicate:
    def test_predicate_filters_count(self):
        sink, cap = _make_sink(predicate=lambda r: r.get("level") == "error")
        sink.write({"level": "info"})
        sink.write({"level": "error"})
        sink.write({"level": "error"})
        assert sink.count == 2
        assert len(cap.records) == 3  # all forwarded

    def test_predicate_false_for_all_keeps_zero(self):
        sink, _ = _make_sink(predicate=lambda r: False)
        sink.write({})
        assert sink.count == 0


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestCounterSinkReset:
    def test_reset_zeroes_counter(self):
        sink, _ = _make_sink()
        sink.write({})
        sink.write({})
        sink.reset()
        assert sink.count == 0

    def test_counting_continues_after_reset(self):
        sink, _ = _make_sink()
        sink.write({})
        sink.reset()
        sink.write({})
        assert sink.count == 1


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestCounterSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed
