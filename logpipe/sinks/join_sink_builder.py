"""Builder helpers for JoinSink – wires configuration dicts into instances.

Expected config shape::

    {
        "type": "join",
        "key": "request_id",
        "channels": ["ingress", "egress"],
        "ttl": 30.0,
        "downstream": { ... }   # nested sink config
    }
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from logpipe.sinks.join_sink import JoinSink

# Type alias for the recursive sink-builder used by logpipe.builder
_SinkBuilder = Callable[[Dict[str, Any]], Any]


def build_join_sink(cfg: Dict[str, Any], build_sink: _SinkBuilder) -> JoinSink:
    """Construct a :class:`JoinSink` from a configuration mapping.

    Parameters
    ----------
    cfg:
        Sink configuration dict.  Required keys: ``key``, ``channels``,
        ``downstream``.  Optional: ``ttl`` (default ``60.0``).
    build_sink:
        Callable that recursively builds a sink from a nested config dict.
        Typically ``logpipe.builder._build_sink``.

    Returns
    -------
    JoinSink
    """
    missing = [k for k in ("key", "channels", "downstream") if k not in cfg]
    if missing:
        raise ValueError(f"JoinSink config missing required keys: {missing}")

    key: str = cfg["key"]
    channels = list(cfg["channels"])
    ttl: float = float(cfg.get("ttl", 60.0))
    downstream = build_sink(cfg["downstream"])

    return JoinSink(downstream, key=key, channels=channels, ttl=ttl)
