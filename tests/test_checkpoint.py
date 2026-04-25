"""Tests for logpipe.checkpoint.CheckpointManager."""

import json
import os
import pytest

from logpipe.checkpoint import CheckpointManager


@pytest.fixture()
def cp_path(tmp_path):
    return str(tmp_path / "checkpoint.json")


class TestCheckpointManagerLoad:
    def test_returns_none_for_unknown_inode(self, cp_path):
        mgr = CheckpointManager(cp_path)
        assert mgr.get_offset(12345) is None

    def test_missing_file_yields_empty_state(self, cp_path):
        mgr = CheckpointManager(cp_path)
        assert mgr._data == {}

    def test_corrupted_file_yields_empty_state(self, cp_path):
        with open(cp_path, "w") as fh:
            fh.write("not-json")
        mgr = CheckpointManager(cp_path)
        assert mgr._data == {}

    def test_loads_existing_checkpoint(self, cp_path):
        with open(cp_path, "w") as fh:
            json.dump({"7": 1024}, fh)
        mgr = CheckpointManager(cp_path)
        assert mgr.get_offset(7) == 1024


class TestCheckpointManagerPersistence:
    def test_set_and_get_offset(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(42, 512)
        assert mgr.get_offset(42) == 512

    def test_save_writes_json_file(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(1, 100)
        mgr.save()
        with open(cp_path) as fh:
            data = json.load(fh)
        assert data == {"1": 100}

    def test_save_is_atomic_no_tmp_leftover(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(1, 0)
        mgr.save()
        assert not os.path.exists(cp_path + ".tmp")

    def test_reload_after_save(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(99, 4096)
        mgr.save()
        mgr2 = CheckpointManager(cp_path)
        assert mgr2.get_offset(99) == 4096

    def test_overwrite_existing_offset(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(3, 10)
        mgr.set_offset(3, 20)
        assert mgr.get_offset(3) == 20


class TestCheckpointManagerRemove:
    def test_remove_drops_inode(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(5, 256)
        mgr.remove(5)
        assert mgr.get_offset(5) is None

    def test_remove_nonexistent_is_safe(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.remove(999)  # should not raise

    def test_remove_persists_after_save(self, cp_path):
        mgr = CheckpointManager(cp_path)
        mgr.set_offset(8, 128)
        mgr.save()
        mgr.remove(8)
        mgr.save()
        mgr2 = CheckpointManager(cp_path)
        assert mgr2.get_offset(8) is None
