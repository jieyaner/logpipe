"""Tests for logpipe.sinks.header_sink.HeaderSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.header_sink import HeaderSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.flushed = 0
        self.closed = False

    def write(self, record: Dict[str, Any]) -> None:
        self.received.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True


def _make_sink(headers, **kwargs) -> tuple[HeaderSink, _CaptureSink]:
    inner = _CaptureSink()
    sink = HeaderSink(inner=inner, headers=headers, **kwargs)
    return sink, inner


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestHeaderSinkConstruction:
    def test_empty_headers_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            HeaderSink(inner=_CaptureSink(), headers={})

    def test_valid_construction(self):
        sink, _ = _make_sink({"env": "prod"})
        assert sink is not None


# ---------------------------------------------------------------------------
# Static headers
# ---------------------------------------------------------------------------

class TestStaticHeaders:
    def test_header_injected(self):
        sink, inner = _make_sink({"env": "prod"})
        sink.write({"msg": "hello"})
        assert inner.received[0]["env"] == "prod"
        assert inner.received[0]["msg"] == "hello"

    def test_existing_key_preserved_by_default(self):
        sink, inner = _make_sink({"env": "prod"})
        sink.write({"env": "staging", "msg": "hi"})
        # record value wins when overwrite=False
        assert inner.received[0]["env"] == "staging"

    def test_overwrite_true_header_wins(self):
        sink, inner = _make_sink({"env": "prod"}, overwrite=True)
        sink.write({"env": "staging", "msg": "hi"})
        assert inner.received[0]["env"] == "prod"

    def test_multiple_headers_all_injected(self):
        sink, inner = _make_sink({"env": "prod", "region": "us-east-1"})
        sink.write({"msg": "x"})
        rec = inner.received[0]
        assert rec["env"] == "prod"
        assert rec["region"] == "us-east-1"


# ---------------------------------------------------------------------------
# Dynamic (callable) headers
# ---------------------------------------------------------------------------

class TestDynamicHeaders:
    def test_callable_evaluated_per_record(self):
        counter = {"n": 0}

        def seq():
            counter["n"] += 1
            return counter["n"]

        sink, inner = _make_sink({"seq": seq})
        sink.write({"msg": "a"})
        sink.write({"msg": "b"})
        assert inner.received[0]["seq"] == 1
        assert inner.received[1]["seq"] == 2

    def test_callable_not_stored_as_callable(self):
        sink, inner = _make_sink({"ts": lambda: 42.0})
        sink.write({"msg": "z"})
        assert inner.received[0]["ts"] == 42.0


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestDelegation:
    def test_flush_delegates(self):
        sink, inner = _make_sink({"env": "prod"})
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self):
        sink, inner = _make_sink({"env": "prod"})
        sink.close()
        assert inner.closed
