"""Tests for FanoutSink."""

import pytest

from logpipe.sinks.fanout_sink import FanoutError, FanoutSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self, raise_on=None):
        self.records = []
        self.flushed = 0
        self.closed = 0
        self._raise_on = raise_on or set()

    def write(self, record):
        if "write" in self._raise_on:
            raise RuntimeError("write boom")
        self.records.append(record)

    def flush(self):
        if "flush" in self._raise_on:
            raise RuntimeError("flush boom")
        self.flushed += 1

    def close(self):
        if "close" in self._raise_on:
            raise RuntimeError("close boom")
        self.closed += 1


_RECORD = {"level": "INFO", "msg": "hello"}


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestFanoutSinkConstruction:
    def test_requires_at_least_one_sink(self):
        with pytest.raises(ValueError, match="at least one"):
            FanoutSink([])

    def test_accepts_single_sink(self):
        s = _CaptureSink()
        fan = FanoutSink([s])
        assert fan is not None


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------

class TestFanoutSinkWrite:
    def test_delivers_to_all_sinks(self):
        a, b = _CaptureSink(), _CaptureSink()
        FanoutSink([a, b]).write(_RECORD)
        assert a.records == [_RECORD]
        assert b.records == [_RECORD]

    def test_continues_after_first_failure(self):
        bad = _CaptureSink(raise_on={"write"})
        good = _CaptureSink()
        with pytest.raises(FanoutError):
            FanoutSink([bad, good]).write(_RECORD)
        assert good.records == [_RECORD]

    def test_error_lists_all_failures(self):
        bad1 = _CaptureSink(raise_on={"write"})
        bad2 = _CaptureSink(raise_on={"write"})
        with pytest.raises(FanoutError) as exc_info:
            FanoutSink([bad1, bad2]).write(_RECORD)
        assert len(exc_info.value.errors) == 2


# ---------------------------------------------------------------------------
# flush / close
# ---------------------------------------------------------------------------

class TestFanoutSinkFlushClose:
    def test_flush_calls_all_sinks(self):
        a, b = _CaptureSink(), _CaptureSink()
        FanoutSink([a, b]).flush()
        assert a.flushed == 1
        assert b.flushed == 1

    def test_flush_collects_errors(self):
        bad = _CaptureSink(raise_on={"flush"})
        good = _CaptureSink()
        with pytest.raises(FanoutError):
            FanoutSink([bad, good]).flush()
        assert good.flushed == 1

    def test_close_calls_all_sinks(self):
        a, b = _CaptureSink(), _CaptureSink()
        FanoutSink([a, b]).close()
        assert a.closed == 1
        assert b.closed == 1

    def test_close_collects_errors(self):
        bad = _CaptureSink(raise_on={"close"})
        good = _CaptureSink()
        with pytest.raises(FanoutError):
            FanoutSink([bad, good]).close()
        assert good.closed == 1
