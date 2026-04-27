"""Tests for EnrichSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks.enrich_sink import EnrichSink


class _CaptureSink:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.flushed = 0
        self.closed = False

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True


def _make_sink(fields):
    cap = _CaptureSink()
    return EnrichSink(cap, fields=fields), cap


class TestEnrichSinkStaticFields:
    def test_adds_static_string_field(self):
        sink, cap = _make_sink({"env": "production"})
        sink.write({"msg": "hello"})
        assert cap.records[0]["env"] == "production"

    def test_original_fields_preserved(self):
        sink, cap = _make_sink({"env": "staging"})
        sink.write({"level": "info", "msg": "ok"})
        rec = cap.records[0]
        assert rec["level"] == "info"
        assert rec["msg"] == "ok"

    def test_adds_multiple_static_fields(self):
        sink, cap = _make_sink({"env": "prod", "region": "us-east-1"})
        sink.write({"msg": "hi"})
        rec = cap.records[0]
        assert rec["env"] == "prod"
        assert rec["region"] == "us-east-1"

    def test_static_field_overwrites_existing(self):
        sink, cap = _make_sink({"env": "prod"})
        sink.write({"env": "dev", "msg": "x"})
        assert cap.records[0]["env"] == "prod"


class TestEnrichSinkCallableFields:
    def test_callable_receives_original_record(self):
        received = []
        sink, cap = _make_sink({"copy": lambda r: received.append(dict(r)) or r.get("msg")})
        sink.write({"msg": "test"})
        assert received[0] == {"msg": "test"}

    def test_callable_return_value_used(self):
        sink, cap = _make_sink({"upper_msg": lambda r: r.get("msg", "").upper()})
        sink.write({"msg": "hello"})
        assert cap.records[0]["upper_msg"] == "HELLO"

    def test_mixed_static_and_callable(self):
        sink, cap = _make_sink({
            "env": "prod",
            "msg_len": lambda r: len(r.get("msg", "")),
        })
        sink.write({"msg": "hi"})
        rec = cap.records[0]
        assert rec["env"] == "prod"
        assert rec["msg_len"] == 2


class TestEnrichSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink({"x": 1})
        sink.flush()
        sink.flush()
        assert cap.flushed == 2

    def test_close_delegates(self):
        sink, cap = _make_sink({"x": 1})
        sink.close()
        assert cap.closed is True

    def test_original_record_not_mutated(self):
        sink, cap = _make_sink({"env": "prod"})
        original = {"msg": "hello"}
        sink.write(original)
        assert "env" not in original


class TestEnrichSinkConstruction:
    def test_empty_fields_raises(self):
        cap = _CaptureSink()
        with pytest.raises(ValueError, match="at least one field"):
            EnrichSink(cap, fields={})
