"""Tests for TruncateSink."""

import pytest

from logpipe.sinks.truncate_sink import TruncateSink


# ---------------------------------------------------------------------------
# Minimal in-memory capture sink
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


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------

class TestTruncateSinkConstruction:
    def test_empty_fields_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            TruncateSink(_CaptureSink(), fields={})

    def test_non_positive_limit_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            TruncateSink(_CaptureSink(), fields={"msg": 0})

    def test_negative_limit_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            TruncateSink(_CaptureSink(), fields={"msg": -5})

    def test_valid_construction(self):
        sink = TruncateSink(_CaptureSink(), fields={"msg": 10})
        assert sink is not None


# ---------------------------------------------------------------------------
# Truncation behaviour
# ---------------------------------------------------------------------------

class TestTruncateSinkBehaviour:
    def _make(self, fields, suffix="..."):
        cap = _CaptureSink()
        return TruncateSink(cap, fields=fields, suffix=suffix), cap

    def test_short_value_passes_through_unchanged(self):
        sink, cap = self._make({"msg": 20})
        sink.write({"msg": "hello", "level": "info"})
        assert cap.records[0]["msg"] == "hello"

    def test_exact_limit_passes_through_unchanged(self):
        sink, cap = self._make({"msg": 5})
        sink.write({"msg": "hello"})
        assert cap.records[0]["msg"] == "hello"

    def test_long_value_is_truncated_with_suffix(self):
        sink, cap = self._make({"msg": 10})
        sink.write({"msg": "a" * 20})
        result = cap.records[0]["msg"]
        assert result == "aaaaaaa..."
        assert len(result) == 10

    def test_custom_suffix(self):
        sink, cap = self._make({"msg": 8}, suffix="!")
        sink.write({"msg": "toolongstring"})
        result = cap.records[0]["msg"]
        assert result.endswith("!")
        assert len(result) == 8

    def test_non_string_field_left_untouched(self):
        sink, cap = self._make({"count": 3})
        sink.write({"count": 12345})
        assert cap.records[0]["count"] == 12345

    def test_absent_field_left_untouched(self):
        sink, cap = self._make({"missing": 5})
        sink.write({"msg": "hello"})
        assert "missing" not in cap.records[0]
        assert cap.records[0]["msg"] == "hello"

    def test_multiple_fields_truncated_independently(self):
        sink, cap = self._make({"a": 5, "b": 4})
        sink.write({"a": "aaaaaaaaa", "b": "bbbbbbbb", "c": "untouched"})
        rec = cap.records[0]
        assert len(rec["a"]) == 5
        assert len(rec["b"]) == 4
        assert rec["c"] == "untouched"

    def test_original_record_not_mutated(self):
        sink, cap = self._make({"msg": 5})
        original = {"msg": "toolongvalue"}
        sink.write(original)
        assert original["msg"] == "toolongvalue"

    def test_flush_delegates(self):
        sink, cap = self._make({"msg": 10})
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = self._make({"msg": 10})
        sink.close()
        assert cap.closed is True
