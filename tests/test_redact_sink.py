"""Tests for RedactSink."""

import pytest
from logpipe.sinks.redact_sink import RedactSink


class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flushed = 0
        self.closed = False

    def write(self, record):
        self.records.append(dict(record))

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed = True


def _make_sink(**kwargs):
    cap = _CaptureSink()
    sink = RedactSink(cap, **kwargs)
    return sink, cap


class TestRedactFields:
    def test_named_field_is_masked(self):
        sink, cap = _make_sink(fields=["password"])
        sink.write({"user": "alice", "password": "s3cr3t"})
        assert cap.records[0]["password"] == "***"
        assert cap.records[0]["user"] == "alice"

    def test_multiple_fields_masked(self):
        sink, cap = _make_sink(fields=["token", "secret"])
        sink.write({"token": "abc", "secret": "xyz", "level": "info"})
        rec = cap.records[0]
        assert rec["token"] == "***"
        assert rec["secret"] == "***"
        assert rec["level"] == "info"

    def test_unknown_field_passes_through(self):
        sink, cap = _make_sink(fields=["password"])
        sink.write({"msg": "hello"})
        assert cap.records[0]["msg"] == "hello"

    def test_original_record_not_mutated(self):
        sink, cap = _make_sink(fields=["password"])
        original = {"password": "secret", "user": "bob"}
        sink.write(original)
        assert original["password"] == "secret"


class TestRedactPatterns:
    def test_value_matching_pattern_is_masked(self):
        sink, cap = _make_sink(patterns=[r"\d{4}-\d{4}-\d{4}-\d{4}"])
        sink.write({"cc": "1234-5678-9012-3456", "user": "alice"})
        assert cap.records[0]["cc"] == "***"
        assert cap.records[0]["user"] == "alice"

    def test_non_matching_value_passes_through(self):
        sink, cap = _make_sink(patterns=[r"\d{4}-\d{4}-\d{4}-\d{4}"])
        sink.write({"msg": "hello world"})
        assert cap.records[0]["msg"] == "hello world"

    def test_field_and_pattern_combined(self):
        sink, cap = _make_sink(
            fields=["password"],
            patterns=[r"\d{3}-\d{2}-\d{4}"],
        )
        sink.write({"password": "abc", "ssn": "123-45-6789", "ok": "yes"})
        rec = cap.records[0]
        assert rec["password"] == "***"
        assert rec["ssn"] == "***"
        assert rec["ok"] == "yes"


class TestRedactCustomMask:
    def test_custom_mask_string(self):
        sink, cap = _make_sink(fields=["api_key"], mask="[REDACTED]")
        sink.write({"api_key": "sk-abc123"})
        assert cap.records[0]["api_key"] == "[REDACTED]"


class TestRedactDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed is True

    def test_no_fields_passes_record_unchanged(self):
        sink, cap = _make_sink()
        sink.write({"a": 1, "b": 2})
        assert cap.records[0] == {"a": 1, "b": 2}
