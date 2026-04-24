"""Tests for logpipe.tailer.FileTailer."""

import os
import tempfile
import threading
import time

import pytest

from logpipe.tailer import FileTailer


def collect_lines(tailer: FileTailer, count: int, timeout: float = 3.0):
    """Helper: collect *count* lines from tailer.tail() within *timeout* seconds."""
    results = []

    def _run():
        for line in tailer.tail():
            results.append(line)
            if len(results) >= count:
                return

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    return results


class TestFileTailer:
    def test_reads_existing_content(self, tmp_path):
        log_file = tmp_path / "app.log"
        log_file.write_text("line1\nline2\nline3\n")

        tailer = FileTailer(str(log_file), poll_interval=0.05)
        lines = collect_lines(tailer, 3)

        assert lines == ["line1", "line2", "line3"]

    def test_reads_appended_lines(self, tmp_path):
        log_file = tmp_path / "app.log"
        log_file.write_text("")

        tailer = FileTailer(str(log_file), poll_interval=0.05)

        def _append():
            time.sleep(0.1)
            with open(str(log_file), "a") as fh:
                fh.write("hello\nworld\n")

        threading.Thread(target=_append, daemon=True).start()
        lines = collect_lines(tailer, 2)

        assert lines == ["hello", "world"]

    def test_waits_for_file_creation(self, tmp_path):
        log_file = tmp_path / "late.log"
        tailer = FileTailer(str(log_file), poll_interval=0.05)

        def _create():
            time.sleep(0.15)
            log_file.write_text("created\n")

        threading.Thread(target=_create, daemon=True).start()
        lines = collect_lines(tailer, 1)

        assert lines == ["created"]

    def test_detects_log_rotation(self, tmp_path):
        log_file = tmp_path / "rotating.log"
        log_file.write_text("before_rotation\n")

        tailer = FileTailer(str(log_file), poll_interval=0.05)
        # Prime the tailer so offset is at end of first content
        first = collect_lines(tailer, 1)
        assert first == ["before_rotation"]

        # Simulate rotation: replace file
        log_file.unlink()
        log_file.write_text("after_rotation\n")

        lines = collect_lines(tailer, 1)
        assert lines == ["after_rotation"]
