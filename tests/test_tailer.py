"""Tests for logpipe.tailer.FileTailer (including checkpoint integration)."""

import os
import threading
import time

import pytest

from logpipe.checkpoint import CheckpointManager
from logpipe.tailer import FileTailer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def collect_lines(tailer: FileTailer, n: int, timeout: float = 3.0):
    """Run *tailer.tail* in a thread and collect *n* lines."""
    results = []
    exc_holder = []

    def _run():
        try:
            for line in tailer.tail(max_lines=n):
                results.append(line)
        except Exception as exc:  # pragma: no cover
            exc_holder.append(exc)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if exc_holder:
        raise exc_holder[0]
    return results


# ---------------------------------------------------------------------------
# Basic tailing tests
# ---------------------------------------------------------------------------

class TestFileTailer:
    def test_reads_existing_content(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_text("line1\nline2\nline3\n")
        tailer = FileTailer(str(f), poll_interval=0.05)
        lines = collect_lines(tailer, 3)
        assert lines == ["line1", "line2", "line3"]

    def test_reads_appended_lines(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_text("alpha\n")
        tailer = FileTailer(str(f), poll_interval=0.05)

        def _append():
            time.sleep(0.15)
            with open(str(f), "a") as fh:
                fh.write("beta\ngamma\n")

        threading.Thread(target=_append, daemon=True).start()
        lines = collect_lines(tailer, 3)
        assert lines == ["alpha", "beta", "gamma"]

    def test_waits_for_file_to_appear(self, tmp_path):
        f = tmp_path / "late.log"
        tailer = FileTailer(str(f), poll_interval=0.05)

        def _create():
            time.sleep(0.15)
            f.write_text("hello\n")

        threading.Thread(target=_create, daemon=True).start()
        lines = collect_lines(tailer, 1)
        assert lines == ["hello"]


# ---------------------------------------------------------------------------
# Checkpoint integration tests
# ---------------------------------------------------------------------------

class TestFileTailerCheckpoint:
    def test_resumes_from_checkpoint(self, tmp_path):
        f = tmp_path / "app.log"
        cp_path = str(tmp_path / "cp.json")
        f.write_text("one\ntwo\nthree\n")

        # First pass — read first two lines and save checkpoint
        mgr1 = CheckpointManager(cp_path)
        tailer1 = FileTailer(str(f), poll_interval=0.05, checkpoint_manager=mgr1)
        lines1 = collect_lines(tailer1, 2)
        assert lines1 == ["one", "two"]
        mgr1.save()

        # Second pass — should resume and only see the third line
        mgr2 = CheckpointManager(cp_path)
        tailer2 = FileTailer(str(f), poll_interval=0.05, checkpoint_manager=mgr2)
        lines2 = collect_lines(tailer2, 1)
        assert lines2 == ["three"]

    def test_checkpoint_saved_on_idle(self, tmp_path):
        f = tmp_path / "app.log"
        cp_path = str(tmp_path / "cp.json")
        f.write_text("x\n")

        mgr = CheckpointManager(cp_path)
        tailer = FileTailer(str(f), poll_interval=0.05, checkpoint_manager=mgr)
        collect_lines(tailer, 1)
        # After tailing, the checkpoint file should exist on disk
        assert os.path.exists(cp_path)
