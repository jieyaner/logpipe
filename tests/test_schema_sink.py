"""Tests for logpipe.sinks.schema_sink."""

from __future__ import annotations

import pytest

from logpipe.sinks.schema_sink import SchemaSink, SchemaValidationError


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


def _make_sink(required_fields, on_error="drop"):
    inner = _CaptureSink()
    sink = SchemaSink(inner, required_fields, on_error=on_error)
    return sink, inner


_SCHEMA = {"level": str, "message": str, "ts": float}

_VALID = {"level": "info", "message": "hello", "ts": 1234567890.0}


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestSchemaSinkConstruction:
    def test_invalid_on_error_raises(self):
        with pytest.raises(ValueError, match="on_error"):
            SchemaSink(_CaptureSink(), {}, on_error="ignore")

    def test_valid_on_error_drop(self):
        sink, _ = _make_sink({}, on_error="drop")
        assert sink is not None

    def test_valid_on_error_raise(self):
        sink, _ = _make_sink({}, on_error="raise")
        assert sink is not None


# ---------------------------------------------------------------------------
# Validation — drop mode
# ---------------------------------------------------------------------------


class TestSchemaSinkDrop:
    def test_valid_record_forwarded(self):
        sink, inner = _make_sink(_SCHEMA)
        sink.write(_VALID.copy())
        assert len(inner.records) == 1

    def test_missing_field_drops_record(self):
        sink, inner = _make_sink(_SCHEMA)
        sink.write({"level": "info", "message": "oops"})  # missing ts
        assert inner.records == []
        assert sink.dropped == 1

    def test_wrong_type_drops_record(self):
        sink, inner = _make_sink(_SCHEMA)
        sink.write({"level": "info", "message": "hi", "ts": "not-a-float"})
        assert inner.records == []
        assert sink.dropped == 1

    def test_none_type_accepts_any_value(self):
        sink, inner = _make_sink({"data": None})
        sink.write({"data": 42})
        sink.write({"data": "string"})
        assert len(inner.records) == 2

    def test_dropped_counter_accumulates(self):
        sink, _ = _make_sink(_SCHEMA)
        for _ in range(5):
            sink.write({"level": "info"})  # missing message + ts
        assert sink.dropped == 5


# ---------------------------------------------------------------------------
# Validation — raise mode
# ---------------------------------------------------------------------------


class TestSchemaSinkRaise:
    def test_missing_field_raises(self):
        sink, _ = _make_sink(_SCHEMA, on_error="raise")
        with pytest.raises(SchemaValidationError, match="missing required field"):
            sink.write({"level": "warn"})

    def test_wrong_type_raises(self):
        sink, _ = _make_sink(_SCHEMA, on_error="raise")
        with pytest.raises(SchemaValidationError, match="expected float"):
            sink.write({"level": "info", "message": "x", "ts": "bad"})


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------


class TestSchemaSinkDelegation:
    def test_flush_delegates(self):
        sink, inner = _make_sink({})
        sink.flush()
        assert inner.flushed == 1

    def test_close_delegates(self):
        sink, inner = _make_sink({})
        sink.close()
        assert inner.closed
