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
# FieldFilter — invalid match_type
# ---------------------------------------------------------------------------

class TestFieldFilterInvalidMatchType:
    def test_invalid_match_type_raises(self):
        """Constructing a FieldFilter with an unsupported match_type should
        raise FilterError immediately rather than failing silently at keep().
        """
        with pytest.raises(FilterError, match="match_type"):
            FieldFilter("level", "error", match_type="fuzzy")


# ---------------------------------------------------------------------------
# FieldFilter — nested field path
# ---------------------------------------------------------------------------

class TestFieldFilterNestedPath:
    def test