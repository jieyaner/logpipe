"""Convenience factory that constructs a Pipeline from a plain dict config.

Example config::

    {
        "path": "/var/log/app.log",
        "parser": "json",
        "poll_interval": 2.0,
        "sinks": [
            {"type": "s3", "bucket": "my-bucket", "prefix": "logs/"},
        ]
    }
"""

from __future__ import annotations

from typing import Any, Dict, List

from logpipe.checkpoint import CheckpointManager
from logpipe.parser import JSONParser, BaseParser
from logpipe.pipeline import Pipeline
from logpipe.router import Route, Router
from logpipe.sinks import BaseSink


# ---------------------------------------------------------------------------
# Sink registry
# ---------------------------------------------------------------------------

def _build_sink(cfg: Dict[str, Any]) -> BaseSink:
    sink_type = cfg.get("type", "").lower()
    if sink_type == "s3":
        from logpipe.sinks.s3_sink import S3Sink
        return S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            region=cfg.get("region", "us-east-1"),
            batch_size=cfg.get("batch_size", 500),
        )
    if sink_type in ("elasticsearch", "es"):
        from logpipe.sinks.es_sink import ElasticsearchSink
        return ElasticsearchSink(
            host=cfg["host"],
            index=cfg.get("index", "logs"),
            batch_size=cfg.get("batch_size", 200),
        )
    raise ValueError(f"Unknown sink type: {sink_type!r}")


# ---------------------------------------------------------------------------
# Parser registry
# ---------------------------------------------------------------------------

def _build_parser(name: str) -> BaseParser:
    name = name.lower()
    if name == "json":
        return JSONParser()
    raise ValueError(f"Unknown parser: {name!r}")


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def build_pipeline(
    cfg: Dict[str, Any],
    checkpoint_dir: str | None = None,
) -> Pipeline:
    """Build a :class:`Pipeline` from a configuration dictionary."""
    path: str = cfg["path"]
    parser = _build_parser(cfg.get("parser", "json"))
    poll_interval: float = float(cfg.get("poll_interval", 1.0))

    sinks: List[BaseSink] = [_build_sink(s) for s in cfg.get("sinks", [])]
    if not sinks:
        raise ValueError("At least one sink must be configured")

    routes = [Route(sink=sink) for sink in sinks]
    router = Router(routes=routes)

    checkpoint_manager: CheckpointManager | None = None
    if checkpoint_dir:
        checkpoint_manager = CheckpointManager(checkpoint_dir)

    return Pipeline(
        path=path,
        parser=parser,
        router=router,
        checkpoint_manager=checkpoint_manager,
        poll_interval=poll_interval,
    )
