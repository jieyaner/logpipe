"""Tests for RotatingSink."""

import pytest
from logpipe.sinks.rotating_sink import RotatingSink


class _CaptureSink:
    def __init__(self):
        self.records = []
        self.flushed = 0
        self.closed = 0

    def write(self, record):
        self.records.append(record)

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed += 1


def _make_rotating(max_records=0, max_age_seconds=0, clock=None):
    sinks = []

    def factory():
        s = _CaptureSink()
        sinks.append(s)
        return s

    kwargs = {}
    if clock:
        kwargs["clock"] = clock
    sink = RotatingSink(factory, max_records=max_records,
                        max_age_seconds=max_age_seconds, **kwargs)
    return sink, sinks


class TestRotatingSinkConstruction:
    def test_requires_at_least_one_limit(self):
        with pytest.raises(ValueError):
            RotatingSink(lambda: _CaptureSink(), max_records=0, max_age_seconds=0)

    def test_creates_initial_sink_on_init(self):
        _, sinks = _make_rotating(max_records=5)
        assert len(sinks) == 1


class TestRotateOnRecordCount:
    def test_no_rotation_before_limit(self):
        sink, sinks = _make_rotating(max_records=3)
        for i in range(3):
            sink.write({"i": i})
        assert len(sinks) == 1
        assert len(sinks[0].records) == 3

    def test_rotation_at_limit(self):
        sink, sinks = _make_rotating(max_records=3)
        for i in range(4):
            sink.write({"i": i})
        # rotation happens *before* writing the 4th record
        assert len(sinks) == 2
        assert len(sinks[0].records) == 3
        assert len(sinks[1].records) == 1

    def test_old_sink_flushed_and_closed_on_rotation(self):
        sink, sinks = _make_rotating(max_records=2)
        for i in range(3):
            sink.write({"i": i})
        assert sinks[0].flushed == 1
        assert sinks[0].closed == 1

    def test_multiple_rotations(self):
        sink, sinks = _make_rotating(max_records=2)
        for i in range(6):
            sink.write({"i": i})
        assert len(sinks) == 3


class TestRotateOnAge:
    def test_rotation_triggered_by_age(self):
        now = [0.0]
        sink, sinks = _make_rotating(max_age_seconds=10, clock=lambda: now[0])
        sink.write({"a": 1})
        now[0] = 10.0          # exactly at boundary → should rotate
        sink.write({"b": 2})
        assert len(sinks) == 2

    def test_no_rotation_before_age(self):
        now = [0.0]
        sink, sinks = _make_rotating(max_age_seconds=10, clock=lambda: now[0])
        sink.write({"a": 1})
        now[0] = 9.9
        sink.write({"b": 2})
        assert len(sinks) == 1


class TestFlushAndClose:
    def test_flush_delegates_to_inner_sink(self):
        sink, sinks = _make_rotating(max_records=10)
        sink.flush()
        assert sinks[-1].flushed == 1

    def test_close_flushes_and_closes_inner_sink(self):
        sink, sinks = _make_rotating(max_records=10)
        sink.write({"x": 1})
        sink.close()
        assert sinks[-1].flushed >= 1
        assert sinks[-1].closed == 1
