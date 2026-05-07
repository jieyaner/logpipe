"""Tests for TimestampSink."""

from __future__ import annotations

import datetime
from typing import Any, Dict, List

import pytest

from logpipe.sinks.timestamp_sink import TimestampSink, _DEFAULT_FMT


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

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


def _make_sink(**kwargs) -> tuple[TimestampSink, _CaptureSink]:
    cap = _CaptureSink()
    sink = TimestampSink(cap, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------

class TestTimestampSinkConstruction:
    def test_empty_field_raises(self):
        cap = _CaptureSink()
        with pytest.raises(ValueError, match="field"):
            TimestampSink(cap, field="")

    def test_default_field_name(self):
        sink, _ = _make_sink()
        assert sink._field == "@timestamp"

    def test_custom_field_name(self):
        sink, _ = _make_sink(field="ts")
        assert sink._field == "ts"


# ---------------------------------------------------------------------------
# stamping logic
# ---------------------------------------------------------------------------

class TestTimestampSinkStamp:
    def test_adds_timestamp_when_absent(self):
        sink, cap = _make_sink()
        sink.write({"level": "info", "msg": "hello"})
        assert "@timestamp" in cap.records[0]

    def test_preserves_existing_timestamp_by_default(self):
        sink, cap = _make_sink()
        existing = "2000-01-01T00:00:00.000000Z"
        sink.write({"@timestamp": existing, "msg": "hi"})
        assert cap.records[0]["@timestamp"] == existing

    def test_overwrite_replaces_existing_timestamp(self):
        sink, cap = _make_sink(overwrite=True)
        old_ts = "2000-01-01T00:00:00.000000Z"
        sink.write({"@timestamp": old_ts, "msg": "hi"})
        assert cap.records[0]["@timestamp"] != old_ts

    def test_original_record_not_mutated(self):
        sink, _ = _make_sink()
        original: Dict[str, Any] = {"msg": "unchanged"}
        sink.write(original)
        assert "@timestamp" not in original

    def test_custom_format_applied(self):
        fmt = "%Y/%m/%d"
        sink, cap = _make_sink(fmt=fmt)
        sink.write({"msg": "x"})
        ts = cap.records[0]["@timestamp"]
        # must parse without error
        datetime.datetime.strptime(ts, fmt)

    def test_default_format_is_iso8601(self):
        sink, cap = _make_sink()
        sink.write({"msg": "x"})
        ts = cap.records[0]["@timestamp"]
        datetime.datetime.strptime(ts, _DEFAULT_FMT)

    def test_custom_field_written(self):
        sink, cap = _make_sink(field="event_time")
        sink.write({"msg": "x"})
        assert "event_time" in cap.records[0]


# ---------------------------------------------------------------------------
# delegation
# ---------------------------------------------------------------------------

class TestTimestampSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed

    def test_multiple_writes_all_forwarded(self):
        sink, cap = _make_sink()
        for i in range(5):
            sink.write({"i": i})
        assert len(cap.records) == 5
