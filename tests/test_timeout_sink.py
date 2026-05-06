"""Tests for TimeoutSink."""
from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.timeout_sink import TimeoutSink, WriteTimedOut


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.flushed = 0
        self.closed = 0

    def write(self, record: dict[str, Any]) -> None:
        self.received.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed += 1


class _SlowSink(BaseSink):
    """Blocks for *delay_s* seconds on every write."""

    def __init__(self, delay_s: float) -> None:
        self._delay_s = delay_s
        self._stop = threading.Event()

    def write(self, record: dict[str, Any]) -> None:  # noqa: ARG002
        time.sleep(self._delay_s)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._stop.set()


class _ErrorSink(BaseSink):
    def write(self, record: dict[str, Any]) -> None:  # noqa: ARG002
        raise RuntimeError("boom")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestTimeoutSinkConstruction:
    def test_zero_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="greater than zero"):
            TimeoutSink(_CaptureSink(), timeout_s=0)

    def test_negative_timeout_raises(self) -> None:
        with pytest.raises(ValueError):
            TimeoutSink(_CaptureSink(), timeout_s=-1)


# ---------------------------------------------------------------------------
# Normal path
# ---------------------------------------------------------------------------

class TestTimeoutSinkNormalPath:
    def test_fast_write_forwarded(self) -> None:
        inner = _CaptureSink()
        sink = TimeoutSink(inner, timeout_s=2.0)
        sink.write({"level": "info", "msg": "hello"})
        assert inner.received == [{"level": "info", "msg": "hello"}]

    def test_flush_delegates(self) -> None:
        inner = _CaptureSink()
        sink = TimeoutSink(inner, timeout_s=2.0)
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self) -> None:
        inner = _CaptureSink()
        sink = TimeoutSink(inner, timeout_s=2.0)
        sink.close()
        assert inner.closed == 1

    def test_downstream_exception_propagates(self) -> None:
        sink = TimeoutSink(_ErrorSink(), timeout_s=2.0)
        with pytest.raises(RuntimeError, match="boom"):
            sink.write({"x": 1})


# ---------------------------------------------------------------------------
# Timeout behaviour
# ---------------------------------------------------------------------------

class TestTimeoutSinkTimeout:
    def test_raises_when_raise_on_timeout_true(self) -> None:
        slow = _SlowSink(delay_s=5.0)
        sink = TimeoutSink(slow, timeout_s=0.05, raise_on_timeout=True)
        with pytest.raises(WriteTimedOut):
            sink.write({"x": 1})

    def test_drops_silently_when_raise_on_timeout_false(self) -> None:
        slow = _SlowSink(delay_s=5.0)
        sink = TimeoutSink(slow, timeout_s=0.05, raise_on_timeout=False)
        # Should not raise; record is dropped.
        sink.write({"x": 1})

    def test_error_message_contains_timeout_value(self) -> None:
        slow = _SlowSink(delay_s=5.0)
        sink = TimeoutSink(slow, timeout_s=0.05)
        with pytest.raises(WriteTimedOut, match="0.05s"):
            sink.write({"x": 1})
