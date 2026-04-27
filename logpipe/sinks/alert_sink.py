"""Alert sink — forwards records to a webhook URL when a field threshold is crossed.

This sink inspects a numeric field in each record and fires an HTTP POST
to a configured webhook URL whenever the value exceeds (or falls below) a
configured threshold.  Repeated alerts are suppressed for a configurable
cooldown period so downstream systems are not flooded.

Example configuration (via builder):

    {
        "type": "alert",
        "field": "error_rate",
        "operator": "gt",
        "threshold": 0.05,
        "webhook_url": "https://hooks.example.com/alert",
        "cooldown_seconds": 60,
        "sink": { ... }   # optional pass-through sink
    }
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from logpipe.sinks import BaseSink

# Supported comparison operators.
_OPERATORS = {
    "gt": lambda v, t: v > t,
    "gte": lambda v, t: v >= t,
    "lt": lambda v, t: v < t,
    "lte": lambda v, t: v <= t,
    "eq": lambda v, t: v == t,
}


class AlertError(Exception):
    """Raised when the alert sink is mis-configured."""


class AlertSink(BaseSink):
    """Fires a webhook when a numeric field in a record crosses a threshold.

    Parameters
    ----------
    field:
        Dot-separated path to the numeric field to inspect (e.g. ``"metrics.error_rate"``).
    operator:
        One of ``gt``, ``gte``, ``lt``, ``lte``, ``eq``.
    threshold:
        Numeric value to compare against.
    webhook_url:
        HTTP(S) URL that receives a POST with a JSON body on each alert.
    cooldown_seconds:
        Minimum seconds between successive alerts for the same field/threshold
        combination.  Defaults to 60.
    sink:
        Optional downstream sink that receives *every* record regardless of
        whether an alert was fired.
    timeout:
        HTTP request timeout in seconds.  Defaults to 5.
    """

    def __init__(
        self,
        field: str,
        operator: str,
        threshold: float,
        webhook_url: str,
        cooldown_seconds: float = 60.0,
        sink: Optional[BaseSink] = None,
        timeout: float = 5.0,
    ) -> None:
        if operator not in _OPERATORS:
            raise AlertError(
                f"Unknown operator {operator!r}. Choose from: {sorted(_OPERATORS)}"
            )
        if not webhook_url:
            raise AlertError("webhook_url must not be empty.")

        self._field = field
        self._op_name = operator
        self._op = _OPERATORS[operator]
        self._threshold = threshold
        self._webhook_url = webhook_url
        self._cooldown = cooldown_seconds
        self._sink = sink
        self._timeout = timeout
        self._last_alert_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_field(self, record: Dict[str, Any]) -> Optional[float]:
        """Traverse a dot-separated field path and return the value, or None."""
        parts = self._field.split(".")
        node: Any = record
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        try:
            return float(node)
        except (TypeError, ValueError):
            return None

    def _in_cooldown(self) -> bool:
        """Return True if we are still within the suppression window."""
        if self._last_alert_at is None:
            return False
        return (time.monotonic() - self._last_alert_at) < self._cooldown

    def _fire(self, record: Dict[str, Any], value: float) -> None:
        """POST an alert payload to the configured webhook URL."""
        payload = {
            "alert": {
                "field": self._field,
                "operator": self._op_name,
                "threshold": self._threshold,
                "observed_value": value,
            },
            "record": record,
        }
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            self._webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout):
                pass
        except urllib.error.URLError as exc:
            # Log but do not crash the pipeline on delivery failure.
            import warnings
            warnings.warn(f"AlertSink: webhook delivery failed — {exc}")

        self._last_alert_at = time.monotonic()

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        """Evaluate the threshold condition and fire an alert if warranted."""
        value = self._get_field(record)
        if value is not None and self._op(value, self._threshold) and not self._in_cooldown():
            self._fire(record, value)

        if self._sink is not None:
            self._sink.write(record)

    def flush(self) -> None:
        if self._sink is not None:
            self._sink.flush()

    def close(self) -> None:
        if self._sink is not None:
            self._sink.close()
