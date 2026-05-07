"""Tests for NormalizeSink."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.normalize_sink import NormalizeError, NormalizeSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
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


def _make_sink(rules):
    cap = _CaptureSink()
    sink = NormalizeSink(cap, rules)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestNormalizeSinkConstruction:
    def test_unknown_op_raises(self):
        with pytest.raises(NormalizeError, match="Unknown normalization op"):
            NormalizeSink(_CaptureSink(), [("field", "explode")])

    def test_known_ops_accepted(self):
        ops = ["to_str", "to_int", "to_float", "to_bool", "lower", "upper", "strip"]
        for op in ops:
            NormalizeSink(_CaptureSink(), [("x", op)])  # should not raise


# ---------------------------------------------------------------------------
# Type coercions
# ---------------------------------------------------------------------------

class TestTypeCoercions:
    def test_to_str(self):
        sink, cap = _make_sink([("code", "to_str")])
        sink.write({"code": 404})
        assert cap.records[0]["code"] == "404"

    def test_to_int_from_string(self):
        sink, cap = _make_sink([("count", "to_int")])
        sink.write({"count": "42"})
        assert cap.records[0]["count"] == 42

    def test_to_int_truncates_float(self):
        sink, cap = _make_sink([("n", "to_int")])
        sink.write({"n": 3.9})
        assert cap.records[0]["n"] == 3

    def test_to_float(self):
        sink, cap = _make_sink([("val", "to_float")])
        sink.write({"val": "1.5"})
        assert cap.records[0]["val"] == pytest.approx(1.5)

    def test_to_bool_truthy_strings(self):
        sink, cap = _make_sink([("flag", "to_bool")])
        for truthy in ("1", "true", "yes", "on", "True", "YES"):
            sink.write({"flag": truthy})
        assert all(r["flag"] is True for r in cap.records)

    def test_to_bool_falsy_strings(self):
        sink, cap = _make_sink([("flag", "to_bool")])
        for falsy in ("0", "false", "no", "off"):
            sink.write({"flag": falsy})
        assert all(r["flag"] is False for r in cap.records)

    def test_to_bool_passthrough_bool(self):
        sink, cap = _make_sink([("flag", "to_bool")])
        sink.write({"flag": True})
        assert cap.records[0]["flag"] is True

    def test_to_bool_unknown_value_raises(self):
        sink, cap = _make_sink([("flag", "to_bool")])
        with pytest.raises(NormalizeError):
            sink.write({"flag": "maybe"})

    def test_to_int_invalid_raises(self):
        sink, cap = _make_sink([("n", "to_int")])
        with pytest.raises(NormalizeError):
            sink.write({"n": "not-a-number"})


# ---------------------------------------------------------------------------
# String transforms
# ---------------------------------------------------------------------------

class TestStringTransforms:
    def test_lower(self):
        sink, cap = _make_sink([("level", "lower")])
        sink.write({"level": "ERROR"})
        assert cap.records[0]["level"] == "error"

    def test_upper(self):
        sink, cap = _make_sink([("level", "upper")])
        sink.write({"level": "warn"})
        assert cap.records[0]["level"] == "WARN"

    def test_strip(self):
        sink, cap = _make_sink([("msg", "strip")])
        sink.write({"msg": "  hello  "})
        assert cap.records[0]["msg"] == "hello"


# ---------------------------------------------------------------------------
# Misc behaviour
# ---------------------------------------------------------------------------

class TestMiscBehaviour:
    def test_missing_field_skipped(self):
        sink, cap = _make_sink([("missing", "to_int")])
        sink.write({"other": "value"})
        assert cap.records[0] == {"other": "value"}

    def test_unrelated_fields_preserved(self):
        sink, cap = _make_sink([("n", "to_int")])
        sink.write({"n": "7", "keep": "me"})
        assert cap.records[0]["keep"] == "me"

    def test_multiple_rules_applied_in_order(self):
        sink, cap = _make_sink([("s", "strip"), ("s", "upper")])
        sink.write({"s": "  hello  "})
        assert cap.records[0]["s"] == "HELLO"

    def test_flush_delegated(self):
        sink, cap = _make_sink([])
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegated(self):
        sink, cap = _make_sink([])
        sink.close()
        assert cap.closed
