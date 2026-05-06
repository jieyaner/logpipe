"""Build a :class:`~logpipe.pipeline.Pipeline` from a plain-dict config.

This module is intentionally kept dependency-light: it only imports from the
standard library and from other logpipe sub-packages.
"""

from __future__ import annotations

from typing import Any, Dict, List

from logpipe.parser import JSONParser, RegexParser, PlainParser
from logpipe.pipeline import Pipeline
from logpipe.checkpoint import CheckpointManager
from logpipe.tailer import FileTailer
from logpipe.router import Router, Route
from logpipe.sinks import BaseSink
from logpipe.sinks.s3_sink import S3Sink
from logpipe.sinks.es_sink import ElasticsearchSink
from logpipe.sinks.fanout_sink import FanoutSink
from logpipe.sinks.buffer_sink import BufferedSink
from logpipe.sinks.hash_sink import HashSink


# ---------------------------------------------------------------------------
# internal helpers
# ---------------------------------------------------------------------------

def _build_parser(cfg: Dict[str, Any]):
    kind = cfg.get("type", "json")
    if kind == "json":
        return JSONParser(
            timestamp_field=cfg.get("timestamp_field", "timestamp"),
            level_field=cfg.get("level_field", "level"),
        )
    if kind == "regex":
        return RegexParser(
            pattern=cfg["pattern"],
            timestamp_field=cfg.get("timestamp_field", "timestamp"),
            level_field=cfg.get("level_field", "level"),
        )
    if kind == "plain":
        return PlainParser()
    raise ValueError(f"unknown parser type: {kind!r}")


def _build_single_sink(cfg: Dict[str, Any]) -> BaseSink:
    kind = cfg.get("type")
    if kind == "s3":
        return S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            region=cfg.get("region", "us-east-1"),
            max_bytes=cfg.get("max_bytes", 10 * 1024 * 1024),
        )
    if kind == "elasticsearch":
        return ElasticsearchSink(
            url=cfg["url"],
            index=cfg["index"],
            batch_size=cfg.get("batch_size", 500),
        )
    if kind == "fanout":
        children = [_build_single_sink(c) for c in cfg["sinks"]]
        return FanoutSink(children)
    if kind == "buffer":
        child = _build_single_sink(cfg["sink"])
        return BufferedSink(child, max_size=cfg.get("max_size", 1000))
    if kind == "hash":
        children = [_build_single_sink(c) for c in cfg["sinks"]]
        return HashSink(
            field=cfg["field"],
            sinks=children,
            missing=cfg.get("missing", "error"),
            algorithm=cfg.get("algorithm", "md5"),
        )
    raise ValueError(f"unknown sink type: {kind!r}")


def _build_sink(cfg: Dict[str, Any]) -> BaseSink:
    """Accept either a single sink config dict or a list (→ FanoutSink)."""
    if isinstance(cfg, list):
        return FanoutSink([_build_single_sink(c) for c in cfg])
    return _build_single_sink(cfg)


# ---------------------------------------------------------------------------
# public entry-point
# ---------------------------------------------------------------------------

def build_pipeline(config: Dict[str, Any]) -> Pipeline:
    """Construct a :class:`Pipeline` from *config*.

    Minimal config skeleton::

        {
          "files": ["/var/log/app.log"],
          "parser": {"type": "json"},
          "sink": {"type": "s3", "bucket": "my-bucket"},
          "checkpoint_path": "/var/lib/logpipe/checkpoints.json"
        }
    """
    checkpoint_path = config.get("checkpoint_path", "/tmp/logpipe_checkpoints.json")
    checkpoints = CheckpointManager(checkpoint_path)

    parser = _build_parser(config.get("parser", {}))
    sink = _build_sink(config["sink"])

    routes_cfg: List[Dict] = config.get("routes", [])
    if routes_cfg:
        routes = [Route(r["match"], _build_sink(r["sink"])) for r in routes_cfg]
        router = Router(routes, default_sink=sink)
    else:
        router = None

    tailers = [
        FileTailer(path, checkpoints)
        for path in config.get("files", [])
    ]

    return Pipeline(
        tailers=tailers,
        parser=parser,
        sink=router or sink,
        checkpoints=checkpoints,
        flush_interval=config.get("flush_interval", 5),
    )
