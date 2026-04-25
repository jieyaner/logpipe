"""Tests for logpipe.filter and logpipe.sinks.filtered_sink."""

import pytest

from logpipe.filter import FieldFilter, FilterChain, FilterError
from logpipe.sinks import BaseSink
from logpipe.sinks.filtered_sink import FilteredSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink(BaseSink):
    def __init__(self):
        self.records = []
        self.flushed = False
        self.closed = False

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def _rec(**kwargs):
    return {"level": "info", "msg": "hello", **kwargs}


# ---------------------------------------------------------------------------
# FieldFilter — exact
# ---------------------------------------------------------------------------

class TestFieldFilterExact:
    def test_matching_value_kept(self):
        f = FieldFilter("level", "error")
        assert f.keep(_rec(level="error")) is True

    def test_non_matching_value_dropped(self):
        f = FieldFilter("level", "error")
        assert f.keep(_rec(level="info")) is False

    def test_missing_field_dropped(self):
        f = FieldFilter("service", "api")
        assert f.keep(_rec()) is False

    def test_invert_flips_result(self):
        f = FieldFilter("level", "debug", invert=True)
        assert f.keep(_rec(level="info")) is True
        assert f.keep(_rec(level="debug")) is False

    def test_missing_field_with_invert_kept(self):
        f = FieldFilter("service", "api", invert=True)
        assert f.keep(_rec()) is True


# ---------------------------------------------------------------------------
# FieldFilter — glob
# ---------------------------------------------------------------------------

class TestFieldFilterGlob:
    def test_glob_wildcard_matches(self):
        f = FieldFilter("msg", "conn*", match_type="glob")
        assert f.keep(_rec(msg="connection refused")) is True

    def test_glob_no_match(self):
        f = FieldFilter("msg", "conn*", match_type="glob")
        assert f.keep(_rec(msg="timeout")) is False


# ---------------------------------------------------------------------------
# FieldFilter — regex
# ---------------------------------------------------------------------------

class TestFieldFilterRegex:
    def test_regex_match(self):
        f = FieldFilter("msg", r"\d{3}", match_type="regex")
        assert f.keep(_rec(msg="error 404 not found")) is True

    def test_regex_no_match(self):
        f = FieldFilter("msg", r"\d{3}", match_type="regex")
        assert f.keep(_rec(msg="all good")) is False


# ---------------------------------------------------------------------------
# FieldFilter — nested field path
# ---------------------------------------------------------------------------

class TestFieldFilterNestedPath:
    def test_nested_key_resolved(self):
        f = FieldFilter("meta.env", "prod")
        record = {"meta": {"env": "prod"}}
        assert f.keep(record) is True

    def test_partial_path_missing(self):
        f = FieldFilter("meta.env", "prod")
        assert f.keep({"meta": None}) is False


# ---------------------------------------------------------------------------
# FieldFilter — invalid match_type
# ---------------------------------------------------------------------------

def test_invalid_match_type_raises():
    with pytest.raises(FilterError):
        FieldFilter("level", "error", match_type="fuzzy")


# ---------------------------------------------------------------------------
# FilterChain
# ---------------------------------------------------------------------------

class TestFilterChain:
    def test_empty_chain_keeps_all(self):
        chain = FilterChain()
        assert chain.keep(_rec()) is True

    def test_all_filters_must_pass(self):
        chain = FilterChain([
            FieldFilter("level", "error"),
            FieldFilter("service", "api"),
        ])
        assert chain.keep(_rec(level="error", service="api")) is True
        assert chain.keep(_rec(level="error", service="worker")) is False

    def test_add_appends_filter(self):
        chain = FilterChain()
        chain.add(FieldFilter("level", "warn"))
        assert len(chain) == 1


# ---------------------------------------------------------------------------
# FilteredSink
# ---------------------------------------------------------------------------

class TestFilteredSink:
    def _make(self, **filter_kwargs):
        inner = _CaptureSink()
        chain = FilterChain([FieldFilter(**filter_kwargs)])
        sink = FilteredSink(inner, chain)
        return sink, inner

    def test_passing_record_forwarded(self):
        sink, inner = self._make(field="level", pattern="error")
        sink.write(_rec(level="error"))
        assert len(inner.records) == 1
        assert sink.passed == 1
        assert sink.dropped == 0

    def test_failing_record_dropped(self):
        sink, inner = self._make(field="level", pattern="error")
        sink.write(_rec(level="info"))
        assert inner.records == []
        assert sink.dropped == 1
        assert sink.passed == 0

    def test_flush_delegates(self):
        sink, inner = self._make(field="level", pattern="error")
        sink.flush()
        assert inner.flushed is True

    def test_close_delegates(self):
        sink, inner = self._make(field="level", pattern="error")
        sink.close()
        assert inner.closed is True
