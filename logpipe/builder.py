"""Factory helpers that construct pipeline components from config dicts."""

from __future__ import annotations

import importlib
from typing import Any

from logpipe.pipeline import Pipeline
from logpipe.router import Route, Router
from logpipe.checkpoint import CheckpointManager
from logpipe.tailer import FileTailer


# ---------------------------------------------------------------------------
# Sink builders
# ---------------------------------------------------------------------------

def _build_sink(cfg: dict[str, Any]):
    kind = cfg["type"]

    if kind == "s3":
        from logpipe.sinks.s3_sink import S3Sink
        return S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            region=cfg.get("region", "us-east-1"),
            max_bytes=cfg.get("max_bytes", 10 * 1024 * 1024),
        )

    if kind == "elasticsearch":
        from logpipe.sinks.es_sink import ElasticsearchSink
        return ElasticsearchSink(
            host=cfg["host"],
            index=cfg["index"],
            timeout=cfg.get("timeout", 10),
        )

    if kind == "fanout":
        from logpipe.sinks.fanout_sink import FanoutSink
        children = [_build_sink(c) for c in cfg["sinks"]]
        return FanoutSink(children)

    if kind == "tee":
        from logpipe.sinks.tee_sink import TeeSink
        primary = _build_sink(cfg["primary"])
        tap = _build_sink(cfg["tap"])
        return TeeSink(primary, tap, silent_tap=cfg.get("silent_tap", True))

    if kind == "buffer":
        from logpipe.sinks.buffer_sink import BufferedSink
        inner = _build_sink(cfg["sink"])
        return BufferedSink(inner, max_size=cfg.get("max_size", 100))

    if kind == "retry":
        from logpipe.sinks.retry_sink import RetrySink
        inner = _build_sink(cfg["sink"])
        return RetrySink(inner, max_attempts=cfg.get("max_attempts", 3))

    if kind == "filtered":
        from logpipe.sinks.filtered_sink import FilteredSink
        from logpipe.filter import FieldFilter
        inner = _build_sink(cfg["sink"])
        flt = FieldFilter(cfg["field"], cfg["op"], cfg.get("value"))
        return FilteredSink(inner, flt)

    if kind == "transform":
        from logpipe.sinks.transform_sink import TransformSink
        from logpipe.transform import FieldTransformer
        inner = _build_sink(cfg["sink"])
        transformer = FieldTransformer(cfg["rules"])
        return TransformSink(inner, transformer)

    if kind == "sampling":
        from logpipe.sinks.sampling_sink import SamplingSink
        inner = _build_sink(cfg["sink"])
        return SamplingSink(inner, rate=cfg.get("rate", 1.0))

    if kind == "dedup":
        from logpipe.sinks.dedup_sink import DedupSink
        inner = _build_sink(cfg["sink"])
        return DedupSink(inner, ttl=cfg.get("ttl", 60), fields=cfg.get("fields"))

    if kind == "schema":
        from logpipe.sinks.schema_sink import SchemaSink
        inner = _build_sink(cfg["sink"])
        return SchemaSink(inner, schema=cfg["schema"])

    if kind == "metrics":
        from logpipe.sinks.metrics_sink import MetricsSink
        from logpipe.metrics import MetricsCollector
        inner = _build_sink(cfg["sink"])
        mc = MetricsCollector()
        return MetricsSink(inner, mc)

    if kind == "throttled":
        from logpipe.sinks.throttled_sink import ThrottledSink
        inner = _build_sink(cfg["sink"])
        return ThrottledSink(inner, rate=cfg["rate"], capacity=cfg.get("capacity"))

    if kind == "rotating":
        from logpipe.sinks.rotating_sink import RotatingSink
        inner_factory = lambda: _build_sink(cfg["sink"])  # noqa: E731
        return RotatingSink(inner_factory, max_records=cfg.get("max_records", 1000))

    raise ValueError(f"Unknown sink type: {kind!r}")


# ---------------------------------------------------------------------------
# Parser builders
# ---------------------------------------------------------------------------

def _build_parser(cfg: dict[str, Any]):
    kind = cfg.get("type", "json")

    if kind == "json":
        from logpipe.parser import JSONParser
        return JSONParser(
            timestamp_field=cfg.get("timestamp_field", "timestamp"),
            level_field=cfg.get("level_field", "level"),
        )

    if kind == "logfmt":
        from logpipe.parser import LogfmtParser  # type: ignore[attr-defined]
        return LogfmtParser()

    if kind == "regex":
        from logpipe.parser import RegexParser  # type: ignore[attr-defined]
        return RegexParser(pattern=cfg["pattern"], fields=cfg["fields"])

    raise ValueError(f"Unknown parser type: {kind!r}")


# ---------------------------------------------------------------------------
# Top-level pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(cfg: dict[str, Any]) -> Pipeline:
    """Construct a :class:`Pipeline` from a configuration mapping."""
    checkpoint_path = cfg.get("checkpoint_path", "/tmp/logpipe_checkpoint.json")
    checkpoint = CheckpointManager(checkpoint_path)

    parser = _build_parser(cfg.get("parser", {}))
    sink = _build_sink(cfg["sink"])

    routes = [
        Route(
            field=r["field"],
            op=r["op"],
            value=r.get("value"),
            sink=_build_sink(r["sink"]),
        )
        for r in cfg.get("routes", [])
    ]
    router = Router(routes=routes, default_sink=sink)

    tailers = [
        FileTailer(path=src["path"], checkpoint=checkpoint)
        for src in cfg["sources"]
    ]

    return Pipeline(
        tailers=tailers,
        parser=parser,
        router=router,
        checkpoint=checkpoint,
        flush_interval=cfg.get("flush_interval", 5),
    )
