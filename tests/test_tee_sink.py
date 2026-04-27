"""Tests for logpipe.sinks.tee_sink.TeeSink."""

import pytest
from logpipe.sinks.tee_sink import TeeSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self, raise_on=None):
        self.written = []
        self.flushed = 0
        self.closed = 0
        self._raise_on = raise_on or set()

    def write(self, record):
        if "write" in self._raise_on:
            raise RuntimeError("tap write failed")
        self.written.append(record)

    def flush(self):
        if "flush" in self._raise_on:
            raise RuntimeError("tap flush failed")
        self.flushed += 1

    def close(self):
        if "close" in self._raise_on:
            raise RuntimeError("tap close failed")
        self.closed += 1


def _make_tee(raise_on=None, silent_tap=True):
    primary = _CaptureSink()
    tap = _CaptureSink(raise_on=raise_on)
    sink = TeeSink(primary, tap, silent_tap=silent_tap)
    return sink, primary, tap


# ---------------------------------------------------------------------------
# Tests — normal operation
# ---------------------------------------------------------------------------

class TestTeeSinkNormal:
    def test_write_reaches_both_sinks(self):
        sink, primary, tap = _make_tee()
        rec = {"msg": "hello"}
        sink.write(rec)
        assert primary.written == [rec]
        assert tap.written == [rec]

    def test_flush_reaches_both_sinks(self):
        sink, primary, tap = _make_tee()
        sink.flush()
        assert primary.flushed == 1
        assert tap.flushed == 1

    def test_close_reaches_both_sinks(self):
        sink, primary, tap = _make_tee()
        sink.close()
        assert primary.closed == 1
        assert tap.closed == 1

    def test_multiple_records_preserved_in_order(self):
        sink, primary, tap = _make_tee()
        records = [{"i": i} for i in range(5)]
        for r in records:
            sink.write(r)
        assert primary.written == records
        assert tap.written == records


# ---------------------------------------------------------------------------
# Tests — silent tap (default)
# ---------------------------------------------------------------------------

class TestTeeSinkSilentTap:
    def test_tap_write_error_is_suppressed(self):
        sink, primary, tap = _make_tee(raise_on={"write"})
        sink.write({"msg": "ok"})  # must not raise
        assert primary.written == [{"msg": "ok"}]

    def test_tap_flush_error_is_suppressed(self):
        sink, primary, tap = _make_tee(raise_on={"flush"})
        sink.flush()  # must not raise
        assert primary.flushed == 1

    def test_tap_close_error_is_suppressed(self):
        sink, primary, tap = _make_tee(raise_on={"close"})
        sink.close()  # must not raise
        assert primary.closed == 1


# ---------------------------------------------------------------------------
# Tests — non-silent tap
# ---------------------------------------------------------------------------

class TestTeeSinkNoisyTap:
    def test_tap_write_error_propagates(self):
        sink, primary, tap = _make_tee(raise_on={"write"}, silent_tap=False)
        with pytest.raises(RuntimeError, match="tap write failed"):
            sink.write({"msg": "boom"})

    def test_tap_flush_error_propagates(self):
        sink, primary, tap = _make_tee(raise_on={"flush"}, silent_tap=False)
        with pytest.raises(RuntimeError, match="tap flush failed"):
            sink.flush()
