"""Tests for logpipe.sinks.s3_sink.S3Sink."""

import gzip
import io
import json
import time
import unittest
from unittest.mock import MagicMock, patch

from logpipe.sinks.s3_sink import S3Sink


def _make_sink(**kwargs) -> tuple:
    """Return (sink, mock_s3_client)."""
    client = MagicMock()
    sink = S3Sink(bucket="test-bucket", s3_client=client, **kwargs)
    return sink, client


def _decode_upload(call_kwargs: dict) -> list:
    """Decompress and parse NDJSON from a put_object call."""
    body = call_kwargs["Body"]
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
        lines = gz.read().decode().strip().splitlines()
    return [json.loads(l) for l in lines]


class TestS3SinkSerialize(unittest.TestCase):
    def test_serialize_produces_valid_ndjson_gz(self):
        records = [{"msg": "hello"}, {"msg": "world"}]
        data = S3Sink._serialize(records)
        decoded = _decode_upload({"Body": data})
        self.assertEqual(decoded, records)

    def test_serialize_empty(self):
        data = S3Sink._serialize([])
        decoded = _decode_upload({"Body": data})
        self.assertEqual(decoded, [])


class TestS3SinkFlush(unittest.TestCase):
    def test_flush_uploads_records(self):
        sink, client = _make_sink()
        sink._buffer = [{"level": "INFO", "msg": "test"}]
        sink.flush()
        client.put_object.assert_called_once()
        call_kwargs = client.put_object.call_args.kwargs
        self.assertEqual(call_kwargs["Bucket"], "test-bucket")
        self.assertIn(".ndjson.gz", call_kwargs["Key"])
        records = _decode_upload(call_kwargs)
        self.assertEqual(records[0]["msg"], "test")

    def test_flush_clears_buffer(self):
        sink, _ = _make_sink()
        sink._buffer = [{"x": 1}]
        sink.flush()
        self.assertEqual(sink._buffer, [])

    def test_flush_noop_when_empty(self):
        sink, client = _make_sink()
        sink.flush()
        client.put_object.assert_not_called()


class TestS3SinkWrite(unittest.TestCase):
    def test_write_flushes_at_max_buffer_size(self):
        sink, client = _make_sink(max_buffer_size=3, flush_interval=9999)
        for i in range(3):
            sink.write({"i": i})
        client.put_object.assert_called_once()

    def test_write_flushes_after_interval(self):
        sink, client = _make_sink(max_buffer_size=9999, flush_interval=0.0)
        sink.write({"msg": "now"})
        client.put_object.assert_called_once()

    def test_write_buffers_below_threshold(self):
        sink, client = _make_sink(max_buffer_size=100, flush_interval=9999)
        sink.write({"msg": "a"})
        sink.write({"msg": "b"})
        client.put_object.assert_not_called()
        self.assertEqual(len(sink._buffer), 2)
