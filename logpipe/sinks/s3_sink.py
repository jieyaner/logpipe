"""S3 sink: buffers log records and flushes to an S3 bucket."""

import gzip
import io
import json
import logging
import time
from typing import List

log = logging.getLogger(__name__)


class S3Sink:
    """Accumulates parsed log records and uploads them to S3 as gzipped NDJSON."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "logs/",
        flush_interval: float = 60.0,
        max_buffer_size: int = 1000,
        s3_client=None,
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.flush_interval = flush_interval
        self.max_buffer_size = max_buffer_size
        self._client = s3_client
        self._buffer: List[dict] = []
        self._last_flush: float = time.monotonic()

    @property
    def _s3(self):
        if self._client is None:
            import boto3  # lazy import
            self._client = boto3.client("s3")
        return self._client

    def write(self, record: dict) -> None:
        """Accept a single parsed record; flush when thresholds are exceeded."""
        self._buffer.append(record)
        elapsed = time.monotonic() - self._last_flush
        if len(self._buffer) >= self.max_buffer_size or elapsed >= self.flush_interval:
            self.flush()

    def flush(self) -> None:
        """Upload buffered records to S3 and clear the buffer."""
        if not self._buffer:
            return
        key = self._build_key()
        data = self._serialize(self._buffer)
        try:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentEncoding="gzip")
            log.info("s3_sink: uploaded %d records to s3://%s/%s", len(self._buffer), self.bucket, key)
        except Exception as exc:  # pragma: no cover
            log.error("s3_sink: upload failed: %s", exc)
            raise
        finally:
            self._buffer.clear()
            self._last_flush = time.monotonic()

    def _build_key(self) -> str:
        ts = time.strftime("%Y/%m/%d/%H%M%S", time.gmtime())
        return f"{self.prefix}{ts}.ndjson.gz"

    @staticmethod
    def _serialize(records: List[dict]) -> bytes:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for record in records:
                gz.write((json.dumps(record, default=str) + "\n").encode())
        return buf.getvalue()
