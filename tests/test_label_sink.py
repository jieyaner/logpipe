"""Tests for LabelSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks.label_sink import LabelSink


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


def _make_sink(labels):
    cap = _CaptureSink()
    return LabelSink(cap, labels=labels), cap


class TestLabelSinkBasic:
    def test_label_appears_in_record(self):
        sink, cap = _make_sink({"pipeline": "nginx"})
        sink.write({"msg": "GET /"})
        assert cap.records[0]["pipeline"] == "nginx"

    def test_multiple_labels_all_present(self):
        sink, cap = _make_sink({"pipeline": "nginx", "dc": "eu-west-1"})
        sink.write({"msg": "ok"})
        rec = cap.records[0]
        assert rec["pipeline"] == "nginx"
        assert rec["dc"] == "eu-west-1"

    def test_original_fields_not_lost(self):
        sink, cap = _make_sink({"env": "prod"})
        sink.write({"level": "warn", "msg": "disk low"})
        rec = cap.records[0]
        assert rec["level"] == "warn"
        assert rec["msg"] == "disk low"

    def test_label_overwrites_existing_key(self):
        sink, cap = _make_sink({"env": "prod"})
        sink.write({"env": "dev"})
        assert cap.records[0]["env"] == "prod"

    def test_source_record_not_mutated(self):
        sink, cap = _make_sink({"env": "prod"})
        original = {"msg": "hi"}
        sink.write(original)
        assert "env" not in original


class TestLabelSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink({"x": "1"})
        sink.flush()
        sink.flush()
        assert cap.flushed == 2

    def test_close_delegates(self):
        sink, cap = _make_sink({"x": "1"})
        sink.close()
        assert cap.closed is True

    def test_multiple_writes_all_labelled(self):
        sink, cap = _make_sink({"src": "app"})
        for i in range(5):
            sink.write({"i": i})
        assert all(r["src"] == "app" for r in cap.records)
        assert len(cap.records) == 5


class TestLabelSinkConstruction:
    def test_empty_labels_raises(self):
        cap = _CaptureSink()
        with pytest.raises(ValueError, match="at least one label"):
            LabelSink(cap, labels={})
