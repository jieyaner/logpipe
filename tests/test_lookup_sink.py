"""Tests for LookupSink."""

import pytest

from logpipe.sinks import BaseSink
from logpipe.sinks.lookup_sink import LookupError, LookupSink


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


_TABLE = {"us": "United States", "de": "Germany", "jp": "Japan"}


def _make_sink(**kwargs):
    cap = _CaptureSink()
    sink = LookupSink(cap, src_field="country", table=_TABLE, **kwargs)
    return sink, cap


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestLookupSinkConstruction:
    def test_invalid_on_miss_raises(self):
        with pytest.raises(ValueError, match="on_miss"):
            LookupSink(_CaptureSink(), src_field="x", table={}, on_miss="ignore")

    def test_default_dest_field(self):
        sink, _ = _make_sink()
        assert sink._dest_field == "country_lookup"

    def test_custom_dest_field(self):
        sink, _ = _make_sink(dest_field="country_name")
        assert sink._dest_field == "country_name"


# ---------------------------------------------------------------------------
# Hit behaviour
# ---------------------------------------------------------------------------

class TestLookupSinkHit:
    def test_adds_dest_field_on_hit(self):
        sink, cap = _make_sink()
        sink.write({"country": "us", "value": 1})
        assert cap.records == [{"country": "us", "value": 1, "country_lookup": "United States"}]

    def test_does_not_mutate_original_record(self):
        sink, cap = _make_sink()
        rec = {"country": "de"}
        sink.write(rec)
        assert "country_lookup" not in rec

    def test_custom_dest_field_used_on_hit(self):
        sink, cap = _make_sink(dest_field="name")
        sink.write({"country": "jp"})
        assert cap.records[0]["name"] == "Japan"


# ---------------------------------------------------------------------------
# Miss behaviour
# ---------------------------------------------------------------------------

class TestLookupSinkMissSkip:
    def test_skip_forwards_record_unchanged(self):
        sink, cap = _make_sink(on_miss="skip")
        rec = {"country": "xx", "v": 9}
        sink.write(rec)
        assert cap.records == [{"country": "xx", "v": 9}]

    def test_skip_is_default(self):
        sink, cap = _make_sink()
        sink.write({"country": "zz"})
        assert len(cap.records) == 1

    def test_missing_field_treated_as_miss(self):
        sink, cap = _make_sink(on_miss="skip")
        sink.write({"other": "field"})
        assert len(cap.records) == 1


class TestLookupSinkMissDrop:
    def test_drop_discards_record(self):
        sink, cap = _make_sink(on_miss="drop")
        sink.write({"country": "xx"})
        assert cap.records == []

    def test_drop_still_forwards_hits(self):
        sink, cap = _make_sink(on_miss="drop")
        sink.write({"country": "us"})
        assert len(cap.records) == 1


class TestLookupSinkMissError:
    def test_error_raises_on_miss(self):
        sink, cap = _make_sink(on_miss="error")
        with pytest.raises(LookupError, match="xx"):
            sink.write({"country": "xx"})

    def test_error_does_not_raise_on_hit(self):
        sink, cap = _make_sink(on_miss="error")
        sink.write({"country": "de"})  # should not raise
        assert len(cap.records) == 1


# ---------------------------------------------------------------------------
# Delegation
# ---------------------------------------------------------------------------

class TestLookupSinkDelegation:
    def test_flush_delegates(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1

    def test_close_delegates(self):
        sink, cap = _make_sink()
        sink.close()
        assert cap.closed
