"""Tests for RetrySink."""

import pytest
from logpipe.sinks import BaseSink
from logpipe.sinks.retry_sink import RetrySink, RetryExhausted


class _CaptureSink(BaseSink):
    def __init__(self, fail_times: int = 0):
        self.received: list[dict] = []
        self.flushed = 0
        self.closed = 0
        self._fail_times = fail_times
        self._call_count = 0

    def write(self, record: dict) -> None:
        self._call_count += 1
        if self._call_count <= self._fail_times:
            raise IOError(f"simulated failure #{self._call_count}")
        self.received.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed += 1


RECORD = {"msg": "hello"}


def _make_sink(inner, max_attempts=3, base_delay=0.0):
    return RetrySink(inner, max_attempts=max_attempts, base_delay=base_delay)


class TestRetrySinkSuccess:
    def test_writes_on_first_attempt(self):
        inner = _CaptureSink(fail_times=0)
        sink = _make_sink(inner)
        sink.write(RECORD)
        assert inner.received == [RECORD]

    def test_retries_and_succeeds(self):
        inner = _CaptureSink(fail_times=2)
        sink = _make_sink(inner, max_attempts=3)
        sink.write(RECORD)
        assert inner.received == [RECORD]
        assert inner._call_count == 3

    def test_single_attempt_no_retry(self):
        inner = _CaptureSink(fail_times=0)
        sink = _make_sink(inner, max_attempts=1)
        sink.write(RECORD)
        assert len(inner.received) == 1


class TestRetrySinkExhausted:
    def test_raises_retry_exhausted_after_all_failures(self):
        inner = _CaptureSink(fail_times=10)
        sink = _make_sink(inner, max_attempts=3)
        with pytest.raises(RetryExhausted):
            sink.write(RECORD)
        assert inner._call_count == 3
        assert inner.received == []

    def test_single_attempt_raises_immediately(self):
        inner = _CaptureSink(fail_times=10)
        sink = _make_sink(inner, max_attempts=1)
        with pytest.raises(RetryExhausted):
            sink.write(RECORD)
        assert inner._call_count == 1


class TestRetrySinkDelegation:
    def test_flush_delegates(self):
        inner = _CaptureSink()
        sink = _make_sink(inner)
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self):
        inner = _CaptureSink()
        sink = _make_sink(inner)
        sink.close()
        assert inner.closed == 1


class TestRetrySinkValidation:
    def test_max_attempts_zero_raises(self):
        with pytest.raises(ValueError):
            RetrySink(_CaptureSink(), max_attempts=0)

    def test_max_attempts_negative_raises(self):
        with pytest.raises(ValueError):
            RetrySink(_CaptureSink(), max_attempts=-1)
