"""Pipeline builder — constructs a :class:`~logpipe.pipeline.Pipeline` from a
plain-Python configuration dictionary.

Supported sink types (``type`` key):
  s3, elasticsearch, metrics, throttled, filtered, retry, fanout, buffer,
  transform, sampling, dedup, rotating, **schema**
"""

from __future__ import annotations

from typing import Any, Dict

from logpipe.parser import JSONParser, RegexParser, PlainParser
from logpipe.pipeline import Pipeline
from logpipe.router import Router, Route
from logpipe.tailer import FileTailer
from logpipe.checkpoint import CheckpointManager


def _build_sink(cfg: Dict[str, Any]):
    """Recursively construct a sink from *cfg*."""
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
            batch_size=cfg.get("batch_size", 500),
        )

    if kind == "metrics":
        from logpipe.metrics import MetricsCollector
        from logpipe.sinks.metrics_sink import MetricsSink
        return MetricsSink(MetricsCollector(), _build_sink(cfg["sink"]))

    if kind == "throttled":
        from logpipe.sinks.throttled_sink import ThrottledSink
        return ThrottledSink(
            _build_sink(cfg["sink"]),
            rate=cfg["rate"],
            capacity=cfg.get("capacity", cfg["rate"]),
        )

    if kind == "filtered":
        from logpipe.filter import FieldFilter
        from logpipe.sinks.filtered_sink import FilteredSink
        flt = FieldFilter(cfg["field"], cfg["op"], cfg.get("value"))
        return FilteredSink(_build_sink(cfg["sink"]), flt)

    if kind == "retry":
        from logpipe.sinks.retry_sink import RetrySink
        return RetrySink(_build_sink(cfg["sink"]), max_attempts=cfg.get("max_attempts", 3))

    if kind == "fanout":
        return __import__("logpipe.sinks.fanout_sink", fromlist=["FanoutSink"]).FanoutSink(
            [_build_sink(s) for s in cfg["sinks"]]
        )

    if kind == "buffer":
        from logpipe.sinks.buffer_sink import BufferedSink
        return BufferedSink(_build_sink(cfg["sink"]), max_size=cfg.get("max_size", 100))

    if kind == "transform":
        from logpipe.transform import FieldTransformer
        from logpipe.sinks.transform_sink import TransformSink
        return TransformSink(_build_sink(cfg["sink"]), FieldTransformer(cfg["ops"]))

    if kind == "sampling":
        from logpipe.sinks.sampling_sink import SamplingSink
        return SamplingSink(_build_sink(cfg["sink"]), rate=cfg["rate"])

    if kind == "dedup":
        from logpipe.sinks.dedup_sink import DedupSink
        return DedupSink(_build_sink(cfg["sink"]), fields=cfg["fields"], ttl=cfg.get("ttl", 60))

    if kind == "rotating":
        from logpipe.sinks.rotating_sink import RotatingSink
        return RotatingSink(_build_sink(cfg["sink"]), max_records=cfg.get("max_records", 1000))

    if kind == "schema":
        from logpipe.sinks.schema_sink import SchemaSink
        raw_fields = cfg.get("required_fields", {})
        _type_map = {"str": str, "int": int, "float": float, "bool": bool, "any": None}
        required_fields = {
            k: _type_map.get(v, None) if isinstance(v, str) else v
            for k, v in raw_fields.items()
        }
        return SchemaSink(
            _build_sink(cfg["sink"]),
            required_fields=required_fields,
            on_error=cfg.get("on_error", "drop"),
        )

    raise ValueError(f"Unknown sink type: {kind!r}")


def _build_parser(cfg: Dict[str, Any]):
    kind = cfg.get("type", "json")
    if kind == "json":
        return JSONParser(field_map=cfg.get("field_map", {}), drop_raw=cfg.get("drop_raw", False))
    if kind == "regex":
        return RegexParser(pattern=cfg["pattern"], field_map=cfg.get("field_map", {}))
    if kind == "plain":
        return PlainParser(message_field=cfg.get("message_field", "message"))
    raise ValueError(f"Unknown parser type: {kind!r}")


def build_pipeline(cfg: Dict[str, Any]) -> Pipeline:
    """Build and return a :class:`~logpipe.pipeline.Pipeline` from *cfg*."""
    checkpoint_path = cfg.get("checkpoint_path", "/tmp/logpipe_checkpoints.json")
    checkpoint = CheckpointManager(checkpoint_path)

    tailers = [
        FileTailer(src["path"], checkpoint)
        for src in cfg.get("sources", [])
    ]

    routes = []
    for route_cfg in cfg.get("routes", []):
        parser = _build_parser(route_cfg.get("parser", {}))
        sink = _build_sink(route_cfg["sink"])
        match = route_cfg.get("match", {})
        routes.append(Route(parser=parser, sink=sink, match=match))

    router = Router(routes)
    return Pipeline(tailers=tailers, router=router, checkpoint=checkpoint)
