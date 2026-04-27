"""Builder that constructs a Pipeline from a configuration dictionary."""

from __future__ import annotations

from typing import Any, Dict

from logpipe.pipeline import Pipeline
from logpipe.tailer import FileTailer
from logpipe.checkpoint import CheckpointManager
from logpipe.router import Router, Route
from logpipe.parser import JSONParser, RegexParser, PlainParser
from logpipe.sinks import BaseSink
from logpipe.sinks.s3_sink import S3Sink
from logpipe.sinks.es_sink import ElasticsearchSink
from logpipe.sinks.buffer_sink import BufferedSink
from logpipe.sinks.retry_sink import RetrySink
from logpipe.sinks.fanout_sink import FanoutSink
from logpipe.sinks.filtered_sink import FilteredSink
from logpipe.sinks.transform_sink import TransformSink
from logpipe.sinks.sampling_sink import SamplingSink
from logpipe.sinks.dedup_sink import DedupSink
from logpipe.sinks.redact_sink import RedactSink
from logpipe.filter import FieldFilter
from logpipe.transform import FieldTransformer


def _build_sink(cfg: Dict[str, Any]) -> BaseSink:
    kind = cfg["type"]

    if kind == "s3":
        sink: BaseSink = S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            region=cfg.get("region", "us-east-1"),
            max_bytes=cfg.get("max_bytes", 10 * 1024 * 1024),
        )
    elif kind == "elasticsearch":
        sink = ElasticsearchSink(
            host=cfg["host"],
            index=cfg["index"],
            batch_size=cfg.get("batch_size", 500),
        )
    else:
        raise ValueError(f"Unknown sink type: {kind!r}")

    if cfg.get("redact"):
        rcfg = cfg["redact"]
        sink = RedactSink(
            sink,
            fields=rcfg.get("fields", []),
            patterns=rcfg.get("patterns", []),
            mask=rcfg.get("mask", "***"),
        )

    if cfg.get("deduplicate"):
        dcfg = cfg["deduplicate"]
        sink = DedupSink(
            sink,
            key_field=dcfg["key_field"],
            ttl=dcfg.get("ttl", 60),
        )

    if cfg.get("sample_rate") is not None:
        sink = SamplingSink(sink, rate=cfg["sample_rate"])

    if cfg.get("transforms"):
        transformer = FieldTransformer(cfg["transforms"])
        sink = TransformSink(sink, transformer)

    if cfg.get("filter"):
        flt = FieldFilter(**cfg["filter"])
        sink = FilteredSink(sink, flt)

    if cfg.get("retry"):
        rcfg = cfg["retry"]
        sink = RetrySink(
            sink,
            max_attempts=rcfg.get("max_attempts", 3),
            delay=rcfg.get("delay", 1.0),
        )

    if cfg.get("buffer"):
        bcfg = cfg["buffer"]
        sink = BufferedSink(
            sink,
            max_size=bcfg.get("max_size", 100),
            max_age=bcfg.get("max_age", 5.0),
        )

    return sink


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
        )
    if kind == "plain":
        return PlainParser()
    raise ValueError(f"Unknown parser type: {kind!r}")


def build_pipeline(cfg: Dict[str, Any]) -> Pipeline:
    """Construct a Pipeline from a configuration dict."""
    checkpoint_path = cfg.get("checkpoint_path", ".logpipe_checkpoints.json")
    checkpoint = CheckpointManager(checkpoint_path)

    tailers = [
        FileTailer(path=src["path"], checkpoint=checkpoint)
        for src in cfg["sources"]
    ]

    parsers = {
        src["path"]: _build_parser(src.get("parser", {}))
        for src in cfg["sources"]
    }

    sinks = [_build_sink(s) for s in cfg["sinks"]]
    sink = FanoutSink(sinks) if len(sinks) > 1 else sinks[0]

    routes = [
        Route(
            field=r["field"],
            value=r["value"],
            sink=_build_sink(r["sink"]),
        )
        for r in cfg.get("routes", [])
    ]
    router = Router(routes=routes, default_sink=sink) if routes else None

    return Pipeline(
        tailers=tailers,
        parsers=parsers,
        sink=router or sink,
        checkpoint=checkpoint,
        flush_interval=cfg.get("flush_interval", 5.0),
    )
