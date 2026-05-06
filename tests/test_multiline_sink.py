"""Tests for MultilineSink."""

import pytest
from logpipe.sinks.multiline_sink import MultilineSink


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


def _make_sink(**kwargs):
    cap = _CaptureSink()
    sink = MultilineSink(cap, start_pattern=r"^\d{4}-", **kwargs)
    return sink, cap


def _rec(msg, **extra):
    return {"message": msg, **extra}


# ---------------------------------------------------------------------------
# Basic coalescing
# ---------------------------------------------------------------------------

class TestMultilineSinkCoalesce:
    def test_single_start_line_emitted_on_flush(self):
        sink, cap = _make_sink()
        sink.write(_rec("2024-01-01 INFO hello"))
        assert cap.records == []
        sink.flush()
        assert len(cap.records) == 1
        assert cap.records[0]["message"] == "2024-01-01 INFO hello"

    def test_continuation_lines_merged(self):
        sink, cap = _make_sink()
        sink.write(_rec("2024-01-01 ERROR boom"))
        sink.write(_rec("  at foo.py:10"))
        sink.write(_rec("  at bar.py:20"))
        sink.flush()
        assert len(cap.records) == 1
        assert cap.records[0]["message"] == "2024-01-01 ERROR boom\n  at foo.py:10\n  at bar.py:20"

    def test_new_start_flushes_previous(self):
        sink, cap = _make_sink()
        sink.write(_rec("2024-01-01 INFO first"))
        sink.write(_rec("  continuation"))
        sink.write(_rec("2024-01-01 INFO second"))
        # first event should already be emitted
        assert len(cap.records) == 1
        assert "first" in cap.records[0]["message"]
        sink.flush()
        assert len(cap.records) == 2
        assert "second" in cap.records[1]["message"]

    def test_close_flushes_buffer(self):
        sink, cap = _make_sink()
        sink.write(_rec("2024-01-01 WARN closing"))
        sink.close()
        assert len(cap.records) == 1
        assert cap.closed is True


# ---------------------------------------------------------------------------
# max_lines cap
# ---------------------------------------------------------------------------

class TestMultilineSinkMaxLines:
    def test_max_lines_triggers_flush(self):
        sink, cap = _make_sink(max_lines=3)
        sink.write(_rec("2024-01-01 INFO start"))
        sink.write(_rec("line 2"))
        sink.write(_rec("line 3"))  # third line hits the cap
        assert len(cap.records) == 1
        assert cap.records[0]["message"].count("\n") == 2

    def test_zero_max_lines_means_unlimited(self):
        sink, cap = _make_sink(max_lines=0)
        sink.write(_rec("2024-01-01 INFO start"))
        for i in range(50):
            sink.write(_rec(f"  line {i}"))
        assert cap.records == []  # nothing emitted yet
        sink.flush()
        assert len(cap.records) == 1


# ---------------------------------------------------------------------------
# Extra fields preserved
# ---------------------------------------------------------------------------

class TestMultilineSinkFields:
    def test_extra_fields_preserved_from_first_line(self):
        sink, cap = _make_sink()
        sink.write({"message": "2024-01-01 INFO start", "host": "web-1", "level": "INFO"})
        sink.write({"message": "  traceback", "host": "web-1"})
        sink.flush()
        assert cap.records[0]["host"] == "web-1"
        assert cap.records[0]["level"] == "INFO"

    def test_orphan_continuation_becomes_own_event(self):
        """A continuation line with no preceding start line is emitted standalone."""
        sink, cap = _make_sink()
        sink.write(_rec("  orphan line"))
        sink.flush()
        assert len(cap.records) == 1
        assert cap.records[0]["message"] == "  orphan line"

    def test_flush_delegates_to_inner(self):
        sink, cap = _make_sink()
        sink.flush()
        assert cap.flushed == 1
