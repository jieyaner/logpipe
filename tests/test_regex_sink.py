"""Tests for RegexSink."""

import pytest
from logpipe.sinks.regex_sink import RegexSink, RegexError


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


def _make_sink(field="level", pattern=r"ERROR|WARN", **kwargs):
    cap = _CaptureSink()
    sink = RegexSink(cap, field, pattern, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction errors
# ---------------------------------------------------------------------------

class TestRegexSinkConstruction:
    def test_bad_pattern_raises(self):
        with pytest.raises(RegexError, match="Invalid regex"):
            _make_sink(pattern="[unclosed")

    def test_bad_on_missing_raises(self):
        with pytest.raises(RegexError, match="on_missing"):
            _make_sink(on_missing="explode")

    def test_valid_construction(self):
        sink, _ = _make_sink()
        assert sink is not None


# ---------------------------------------------------------------------------
# Matching behaviour
# ---------------------------------------------------------------------------

class TestRegexSinkMatching:
    def test_matching_record_forwarded(self):
        sink, cap = _make_sink()
        sink.write({"level": "ERROR", "msg": "boom"})
        assert len(cap.records) == 1

    def test_non_matching_record_dropped(self):
        sink, cap = _make_sink()
        sink.write({"level": "INFO", "msg": "ok"})
        assert cap.records == []

    def test_partial_match_is_sufficient(self):
        sink, cap = _make_sink(pattern=r"\d{3}")
        sink.write({"level": "code 404 not found"})
        assert len(cap.records) == 1

    def test_invert_forwards_non_matching(self):
        sink, cap = _make_sink(invert=True)
        sink.write({"level": "DEBUG"})
        assert len(cap.records) == 1

    def test_invert_drops_matching(self):
        sink, cap = _make_sink(invert=True)
        sink.write({"level": "ERROR"})
        assert cap.records == []


# ---------------------------------------------------------------------------
# Missing field
# ---------------------------------------------------------------------------

class TestRegexSinkMissingField:
    def test_missing_field_dropped_by_default(self):
        sink, cap = _make_sink()
        sink.write({"msg": "no level here"})
        assert cap.records == []

    def test_missing_field_forwarded_when_configured(self):
        sink, cap = _make_sink(on_missing="forward")
        sink.write({"msg": "no level here"})
        assert len(cap.records) == 1

    def test_nested_field_resolved(self):
        cap = _CaptureSink()
        sink = RegexSink(cap, "meta.severity", r"CRIT")
        sink.write({"meta": {"severity": "CRITICAL"}, "msg": "x"})
        assert len(cap.records) == 1

    def test_nested_field_missing_drops(self):
        cap = _CaptureSink()
        sink = RegexSink(cap, "meta.severity", r"CRIT")
        sink.write({"meta": {}, "msg": "x"})
        assert cap.records == []


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestRegexSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed
