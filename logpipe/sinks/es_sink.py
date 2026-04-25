"""Elasticsearch sink — bulk-indexes structured log records."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 200


class ElasticsearchSink:
    """Buffers records and bulk-indexes them into an Elasticsearch index."""

    def __init__(
        self,
        host: str,
        index: str,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        timeout: float = 10.0,
    ) -> None:
        self._host = host.rstrip("/")
        self._index = index
        self._batch_size = batch_size
        self._timeout = timeout
        self._buffer: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        """Append *record* to the internal buffer, flushing when full."""
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """Send all buffered records to Elasticsearch via the Bulk API."""
        if not self._buffer:
            return

        payload = self._build_bulk_payload(self._buffer)
        self._bulk_request(payload)
        logger.debug("Flushed %d records to index '%s'", len(self._buffer), self._index)
        self._buffer.clear()

    def close(self) -> None:
        """Flush remaining records and release resources."""
        self.flush()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_bulk_payload(self, records: List[Dict[str, Any]]) -> bytes:
        lines: List[str] = []
        action = json.dumps({"index": {"_index": self._index}})
        for record in records:
            lines.append(action)
            lines.append(json.dumps(record, default=str))
        return ("\n".join(lines) + "\n").encode("utf-8")

    def _bulk_request(self, payload: bytes) -> None:
        url = f"{self._host}/_bulk"
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read())
                if body.get("errors"):
                    logger.warning("Elasticsearch bulk response contained errors: %s", body)
        except urllib.error.URLError as exc:
            logger.error("Failed to send records to Elasticsearch: %s", exc)
            raise
