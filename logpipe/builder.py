"""Builder — construct a Pipeline from a plain-dict configuration."""

from logpipe.pipeline import Pipeline
from logpipe.tailer import FileTailer
from logpipe.checkpoint import CheckpointManager
from logpipe.router import Router, Route
from logpipe.parser import JSONParser, RegexParser, PlainParser
from logpipe.sinks import BaseSink
from logpipe.sinks.s3_sink import S3Sink
from logpipe.sinks.es_sink import ElasticsearchSink
from logpipe.sinks.buffer_sink import BufferedSink
from logpipe.sinks.fanout_sink import FanoutSink
from logpipe.sinks.retry_sink import RetrySink
from logpipe.sinks.filtered_sink import FilteredSink
from logpipe.sinks.transform_sink import TransformSink
from logpipe.sinks.sampling_sink import SamplingSink
from logpipe.sinks.dedup_sink import DedupSink
from logpipe.sinks.redact_sink import RedactSink
from logpipe.sinks.label_sink import LabelSink
from logpipe.sinks.batch_sink import BatchSink
from logpipe.sinks.truncate_sink import TruncateSink
from logpipe.sinks.window_sink import WindowSink


def _build_sink(cfg):
    kind = cfg.get("type", "")
    inner_cfg = cfg.get("sink")
    inner = _build_sink(inner_cfg) if inner_cfg else None

    if kind == "s3":
        return S3Sink(
            bucket=cfg["bucket"],
            prefix=cfg.get("prefix", ""),
            max_bytes=cfg.get("max_bytes", 10 * 1024 * 1024),
        )
    if kind == "elasticsearch":
        return ElasticsearchSink(
            url=cfg["url"],
            index=cfg["index"],
            batch_size=cfg.get("batch_size", 500),
        )
    if kind == "buffer":
        return BufferedSink(inner, max_size=cfg.get("max_size", 1000))
    if kind == "fanout":
        children = [_build_sink(c) for c in cfg.get("sinks", [])]
        return FanoutSink(children)
    if kind == "retry":
        return RetrySink(inner, max_attempts=cfg.get("max_attempts", 3))
    if kind == "filter":
        return FilteredSink(
            inner,
            field=cfg["field"],
            op=cfg["op"],
            value=cfg["value"],
        )
    if kind == "transform":
        return TransformSink(inner, transforms=cfg.get("transforms", []))
    if kind == "sample":
        return SamplingSink(inner, rate=cfg.get("rate", 1.0))
    if kind == "dedup":
        return DedupSink(inner, fields=cfg.get("fields", []), ttl=cfg.get("ttl", 60))
    if kind == "redact":
        return RedactSink(inner, fields=cfg.get("fields", []))
    if kind == "label":
        return LabelSink(inner, labels=cfg.get("labels", {}))
    if kind == "batch":
        return BatchSink(inner, batch_size=cfg.get("batch_size", 100))
    if kind == "truncate":
        return TruncateSink(
            inner,
            field=cfg["field"],
            max_length=cfg["max_length"],
        )
    if kind == "window":
        return WindowSink(
            inner,
            window_seconds=cfg.get("window_seconds", 60),
            value_field=cfg.get("value_field"),
        )
    raise ValueError(f"Unknown sink type: {kind!r}")


def _build_parser(cfg):
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


def build_pipeline(cfg):
    """Construct a :class:`Pipeline` from a configuration dictionary."""
    checkpoint_path = cfg.get("checkpoint", "/tmp/logpipe_checkpoint.json")
    checkpoint = CheckpointManager(checkpoint_path)

    tailers = [
        FileTailer(path=src["path"], checkpoint=checkpoint)
        for src in cfg.get("sources", [])
    ]

    routes = [
        Route(
            sink=_build_sink(r["sink"]),
            match=r.get("match"),
        )
        for r in cfg.get("routes", [])
    ]
    router = Router(routes)

    parser_cfg = cfg.get("parser", {"type": "json"})
    parser = _build_parser(parser_cfg)

    return Pipeline(
        tailers=tailers,
        parser=parser,
        router=router,
        checkpoint=checkpoint,
        poll_interval=cfg.get("poll_interval", 1.0),
    )
