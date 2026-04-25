"""Tests for logpipe.pipeline.Pipeline."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List

import pytest

from logpipe.parser import JSONParser
from logpipe.pipeline import Pipeline
from logpipe.router import Route, Router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureSink:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []
        self.flushed = False

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flushed = True

    def close(self) -> None:
        pass


def _make_pipeline(tmp_path: Path, content: str = "") -> tuple[Pipeline, _CaptureSink, Path]:
    log_file = tmp_path / "app.log"
    log_file.write_text(content)
    sink = _CaptureSink()
    router = Router(routes=[Route(sink=sink)])
    pipeline = Pipeline(str(log_file), parser=JSONParser(), router=router)
    return pipeline, sink, log_file


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPipelineTick:
    def test_parses_and_routes_json_lines(self, tmp_path):
        content = '{"level": "info", "msg": "hello"}\n{"level": "error", "msg": "boom"}\n'
        pipeline, sink, _ = _make_pipeline(tmp_path, content)
        forwarded = pipeline.tick()
        assert forwarded == 2
        assert sink.records[0]["msg"] == "hello"
        assert sink.records[1]["level"] == "error"

    def test_skips_unparseable_lines(self, tmp_path):
        content = 'not json at all\n{"ok": true}\n'
        pipeline, sink, _ = _make_pipeline(tmp_path, content)
        forwarded = pipeline.tick()
        assert forwarded == 1
        assert sink.records[0]["ok"] is True

    def test_empty_file_returns_zero(self, tmp_path):
        pipeline, sink, _ = _make_pipeline(tmp_path, "")
        assert pipeline.tick() == 0
        assert sink.records == []

    def test_incremental_ticks_do_not_reread(self, tmp_path):
        pipeline, sink, log_file = _make_pipeline(tmp_path, '{"n": 1}\n')
        pipeline.tick()
        # Append a second line
        with log_file.open("a") as fh:
            fh.write('{"n": 2}\n')
        pipeline.tick()
        assert len(sink.records) == 2
        assert sink.records[1]["n"] == 2


class TestPipelineFlush:
    def test_flush_propagates_to_sink(self, tmp_path):
        pipeline, sink, _ = _make_pipeline(tmp_path)
        pipeline.flush()
        assert sink.flushed is True


class TestPipelineThreading:
    def test_start_stop_does_not_raise(self, tmp_path):
        pipeline, _, log_file = _make_pipeline(tmp_path)
        pipeline.start()
        time.sleep(0.05)
        pipeline.stop(timeout=2.0)

    def test_double_start_raises(self, tmp_path):
        pipeline, _, _ = _make_pipeline(tmp_path)
        pipeline.start()
        try:
            with pytest.raises(RuntimeError, match="already running"):
                pipeline.start()
        finally:
            pipeline.stop(timeout=2.0)
