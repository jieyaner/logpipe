"""Tests for ElasticsearchSink."""

from __future__ import annotations

import json
from io import BytesIO
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from logpipe.sinks.es_sink import ElasticsearchSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sink(batch_size: int = 100) -> ElasticsearchSink:
    return ElasticsearchSink(
        host="http://localhost:9200",
        index="logs-test",
        batch_size=batch_size,
    )


def _parse_bulk_payload(payload: bytes) -> List[Dict[str, Any]]:
    """Return only the source documents from an NDJSON bulk payload."""
    lines = payload.decode("utf-8").strip().split("\n")
    docs = []
    for i, line in enumerate(lines):
        if i % 2 == 1:  # every second line is a source doc
            docs.append(json.loads(line))
    return docs


def _fake_urlopen(request, timeout=None):
    response = MagicMock()
    response.read.return_value = json.dumps({"errors": False, "items": []}).encode()
    response.__enter__ = lambda s: s
    response.__exit__ = MagicMock(return_value=False)
    return response


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestElasticsearchSinkBulkPayload:
    def test_payload_is_valid_ndjson(self):
        sink = _make_sink()
        records = [{"level": "INFO", "msg": "hello"}, {"level": "ERROR", "msg": "oops"}]
        payload = sink._build_bulk_payload(records)
        docs = _parse_bulk_payload(payload)
        assert docs == records

    def test_action_lines_contain_index_name(self):
        sink = _make_sink()
        payload = sink._build_bulk_payload([{"x": 1}])
        action = json.loads(payload.decode().split("\n")[0])
        assert action["index"]["_index"] == "logs-test"

    def test_empty_buffer_produces_empty_payload(self):
        sink = _make_sink()
        payload = sink._build_bulk_payload([])
        assert payload.strip() == b""


class TestElasticsearchSinkWrite:
    @patch("logpipe.sinks.es_sink.urllib.request.urlopen", side_effect=_fake_urlopen)
    def test_flush_sends_all_buffered_records(self, mock_urlopen):
        sink = _make_sink(batch_size=100)
        for i in range(3):
            sink.write({"i": i})
        sink.flush()
        assert mock_urlopen.called
        sent_payload = mock_urlopen.call_args[0][0].data
        docs = _parse_bulk_payload(sent_payload)
        assert docs == [{"i": 0}, {"i": 1}, {"i": 2}]

    @patch("logpipe.sinks.es_sink.urllib.request.urlopen", side_effect=_fake_urlopen)
    def test_auto_flush_on_batch_size(self, mock_urlopen):
        sink = _make_sink(batch_size=2)
        sink.write({"n": 1})
        assert not mock_urlopen.called
        sink.write({"n": 2})  # triggers auto-flush
        assert mock_urlopen.called
        assert sink._buffer == []

    @patch("logpipe.sinks.es_sink.urllib.request.urlopen", side_effect=_fake_urlopen)
    def test_flush_clears_buffer(self, mock_urlopen):
        sink = _make_sink()
        sink.write({"a": 1})
        sink.flush()
        assert sink._buffer == []

    @patch("logpipe.sinks.es_sink.urllib.request.urlopen", side_effect=_fake_urlopen)
    def test_flush_empty_buffer_is_noop(self, mock_urlopen):
        sink = _make_sink()
        sink.flush()
        assert not mock_urlopen.called

    @patch("logpipe.sinks.es_sink.urllib.request.urlopen", side_effect=_fake_urlopen)
    def test_close_flushes_remaining(self, mock_urlopen):
        sink = _make_sink(batch_size=100)
        sink.write({"msg": "last"})
        sink.close()
        assert mock_urlopen.called
