"""Builder – constructs a Pipeline from a plain-dict configuration.

Extended to support the ``rollup`` sink type.
"""

from __future__ import annotations

from typing import Any, Dict, List

from logpipe.parser import JSONParser, BaseParser
from logpipe.pipeline import Pipeline
from logpipe.router import Route, Router
from logpipe.sinks import BaseSink
from logpipe.sinks.s3_sink import S3Sink
from logpipe.sinks.es_sink import ElasticsearchSink
from logpipe.sinks.buffer_sink import BufferedSink
from logpipe.sinks.fanout_sink import FanoutSink
from logpipe.sinks.filtered_sink import FilteredSink
from logpipe.sinks.retry_sink import RetrySink
from logpipe.sinks.rollup_sink import RollupSink


def _build_parser(cfg: Dict[str, Any]) -> BaseParser:
    fmt = cfg.get("format", "json")
    if fmt == "json":
        return JSONParser(
            timestamp_field=cfg.get("timestamp_field", "timestamp"),
            source_field=cfg.get("source_field", "source"),
        )
    raise ValueError(f"Unknown parser format: {fmt!r}")


def _build_single_sink(cfg: Dict[str, Any]) -> BaseSink:
    kind = cfg.get("type", "")

    if kind == "s3":
        return S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            region=cfg.get("region", "us-east-1"),
            max_bytes=cfg.get("max_bytes", 10 * 1024 * 1024),
        )

    if kind == "elasticsearch":
        return ElasticsearchSink(
            host=cfg["host"],
            index=cfg["index"],
            batch_size=cfg.get("batch_size", 500),
        )

    if kind == "rollup":
        inner = _build_single_sink(cfg["sink"])
        return RollupSink(
            sink=inner,
            fields=cfg["fields"],
            window_seconds=cfg.get("window_seconds", 60.0),
            timestamp_field=cfg.get("timestamp_field", "timestamp"),
        )

    if kind == "buffer":
        inner = _build_single_sink(cfg["sink"])
        return BufferedSink(
            sink=inner,
            max_size=cfg.get("max_size", 1000),
        )

    if kind == "retry":
        inner = _build_single_sink(cfg["sink"])
        return RetrySink(
            sink=inner,
            max_attempts=cfg.get("max_attempts", 3),
            delay=cfg.get("delay", 1.0),
        )

    raise ValueError(f"Unknown sink type: {kind!r}")


def _build_sink(cfg: Any) -> BaseSink:
    if isinstance(cfg, list):
        return FanoutSink([_build_single_sink(c) for c in cfg])
    return _build_single_sink(cfg)


def build_pipeline(cfg: Dict[str, Any]) -> Pipeline:
    """Construct a :class:`~logpipe.pipeline.Pipeline` from *cfg*."""
    parser = _build_parser(cfg.get("parser", {}))

    routes: List[Route] = []
    for route_cfg in cfg.get("routes", []):
        sink = _build_sink(route_cfg["sink"])

        if "filter" in route_cfg:
            sink = FilteredSink(sink=sink, rules=route_cfg["filter"])

        routes.append(
            Route(
                pattern=route_cfg.get("match", "*"),
                sink=sink,
            )
        )

    router = Router(routes)
    return Pipeline(
        paths=cfg.get("paths", []),
        parser=parser,
        router=router,
        poll_interval=cfg.get("poll_interval", 0.5),
        checkpoint_path=cfg.get("checkpoint_path"),
    )
