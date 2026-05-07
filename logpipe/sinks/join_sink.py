"""JoinSink – correlates records from multiple named streams by a shared key.

Records arriving on different logical channels are held in per-key buckets
until *all* expected channels have contributed a record (or the TTL expires).
When the set is complete the merged record is forwarded to the downstream sink.
"""

import time
from typing import Dict, List, Optional

from logpipe.sinks import BaseSink


class JoinError(Exception):
    """Raised for configuration problems."""


class JoinSink(BaseSink):
    """Merge records from *channels* on a shared *key* field.

    Parameters
    ----------
    downstream:
        Sink that receives the merged record.
    key:
        Dot-separated field path used to correlate records across channels.
    channels:
        Ordered list of channel names that must all be present before the
        merged record is emitted.  Each record must carry a ``_channel``
        meta-field identifying which channel it belongs to.
    ttl:
        Seconds to keep an incomplete bucket before discarding it.
    """

    def __init__(
        self,
        downstream: BaseSink,
        key: str,
        channels: List[str],
        ttl: float = 60.0,
    ) -> None:
        if not channels:
            raise JoinError("channels must not be empty")
        if len(channels) != len(set(channels)):
            raise JoinError("channels must be unique")
        self._downstream = downstream
        self._key = key
        self._channels = list(channels)
        self._ttl = ttl
        # bucket: key_value -> {channel: record, "_ts": float}
        self._buckets: Dict[str, dict] = {}

    # ------------------------------------------------------------------
    def _get_key(self, record: dict) -> Optional[str]:
        value = record
        for part in self._key.split("."):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
        return str(value) if value is not None else None

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [k for k, b in self._buckets.items() if now - b["_ts"] > self._ttl]
        for k in expired:
            del self._buckets[k]

    def write(self, record: dict) -> None:
        self._evict_expired()
        channel = record.get("_channel")
        if channel not in self._channels:
            return  # unknown channel – drop silently
        key_val = self._get_key(record)
        if key_val is None:
            return  # key missing – drop silently

        bucket = self._buckets.setdefault(key_val, {"_ts": time.monotonic()})
        bucket[channel] = record

        if all(ch in bucket for ch in self._channels):
            merged: dict = {}
            for ch in self._channels:
                merged.update({k: v for k, v in bucket[ch].items() if k != "_channel"})
            del self._buckets[key_val]
            self._downstream.write(merged)

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
