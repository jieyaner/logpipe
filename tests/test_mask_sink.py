"""Tests for logpipe.sinks.mask_sink."""

from __future__ import annotations

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.mask_sink import MaskError, MaskSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _CaptureSink(BaseSink):
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


def _make_sink(**kwargs):
    cap = _CaptureSink()
    sink = MaskSink(cap, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction errors
# ---------------------------------------------------------------------------


class TestMaskSinkConstruction:
    def test_empty_fields_raises(self):
        cap = _CaptureSink()
        with pytest.raises(MaskError, match="fields must not be empty"):
            MaskSink(cap, fields=[])

    def test_multi_char_mask_char_raises(self):
        cap = _CaptureSink()
        with pytest.raises(MaskError, match="mask_char must be exactly one character"):
            MaskSink(cap, fields=["token"], mask_char="XX")

    def test_negative_show_first_raises(self):
        cap = _CaptureSink()
        with pytest.raises(MaskError):
            MaskSink(cap, fields=["token"], show_first=-1)


# ---------------------------------------------------------------------------
# Masking behaviour
# ---------------------------------------------------------------------------


class TestMaskSinkMasking:
    def test_full_mask_by_default_show_last_4(self):
        sink, cap = _make_sink(fields=["card"], show_first=0, show_last=4, min_mask=3)
        sink.write({"card": "4111111111111234"})
        assert cap.records[0]["card"] == "************1234"

    def test_show_first_and_last(self):
        sink, cap = _make_sink(fields=["token"], show_first=2, show_last=2, min_mask=1)
        sink.write({"token": "ABCDEFGH"})
        result = cap.records[0]["token"]
        assert result.startswith("AB")
        assert result.endswith("GH")
        assert "*" in result

    def test_min_mask_respected_for_short_values(self):
        sink, cap = _make_sink(fields=["pin"], show_first=0, show_last=0, min_mask=5)
        sink.write({"pin": "42"})
        assert cap.records[0]["pin"] == "*****"

    def test_missing_field_is_a_noop(self):
        sink, cap = _make_sink(fields=["secret"])
        original = {"level": "info", "msg": "hello"}
        sink.write(original)
        assert cap.records[0] == original

    def test_non_string_field_is_left_unchanged(self):
        sink, cap = _make_sink(fields=["count"])
        sink.write({"count": 42})
        assert cap.records[0]["count"] == 42

    def test_nested_field_masking(self):
        sink, cap = _make_sink(fields=["user.password"], show_first=0, show_last=0, min_mask=4)
        sink.write({"user": {"name": "alice", "password": "s3cr3t"}})
        assert cap.records[0]["user"]["password"] == "****"
        assert cap.records[0]["user"]["name"] == "alice"

    def test_original_record_not_mutated(self):
        sink, cap = _make_sink(fields=["token"])
        rec = {"token": "abc123"}
        sink.write(rec)
        assert rec["token"] == "abc123"

    def test_multiple_fields_masked(self):
        sink, cap = _make_sink(fields=["email", "phone"], show_first=0, show_last=2, min_mask=3)
        sink.write({"email": "user@example.com", "phone": "555-1234"})
        assert cap.records[0]["email"].endswith("om")
        assert cap.records[0]["phone"].endswith("34")

    def test_custom_mask_char(self):
        sink, cap = _make_sink(fields=["ssn"], mask_char="#", show_first=0, show_last=4, min_mask=3)
        sink.write({"ssn": "123-45-6789"})
        assert "#" in cap.records[0]["ssn"]
        assert "*" not in cap.records[0]["ssn"]


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------


class TestMaskSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink(fields=["x"])
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink(fields=["x"])
        sink.close()
        assert cap.closed
