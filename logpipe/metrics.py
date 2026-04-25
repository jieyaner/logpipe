"""Lightweight in-process metrics collector for logpipe."""

import threading
import time
from collections import defaultdict
from typing import Dict, Optional


class MetricsCollector:
    """Thread-safe counter/gauge store with optional snapshot support."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._started_at: float = time.monotonic()

    def increment(self, name: str, value: int = 1) -> None:
        """Increment a named counter by *value*."""
        with self._lock:
            self._counters[name] += value

    def set_gauge(self, name: str, value: float) -> None:
        """Set a named gauge to an absolute *value*."""
        with self._lock:
            self._gauges[name] = value

    def get_counter(self, name: str) -> int:
        with self._lock:
            return self._counters[name]

    def get_gauge(self, name: str) -> Optional[float]:
        with self._lock:
            return self._gauges.get(name)

    def snapshot(self) -> dict:
        """Return a point-in-time copy of all metrics."""
        with self._lock:
            return {
                "uptime_seconds": round(time.monotonic() - self._started_at, 3),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }

    def reset(self) -> None:
        """Clear all metrics (useful between tests)."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._started_at = time.monotonic()


# Module-level default instance shared across the process.
default_metrics = MetricsCollector()
