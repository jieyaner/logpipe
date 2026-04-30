"""Base sink interface and sink registry for logpipe."""

from __future__ import annotations

from typing import Dict, Type

_REGISTRY: Dict[str, Type["BaseSink"]] = {}


class BaseSink:
    """Abstract base class for all logpipe sinks."""

    def write(self, record: dict) -> None:  # pragma: no cover
        raise NotImplementedError

    def flush(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover
        raise NotImplementedError


def register(name: str, cls: Type[BaseSink]) -> None:
    """Register a sink class under *name* for use in builder configuration."""
    _REGISTRY[name] = cls


def get_sink_class(name: str) -> Type[BaseSink]:
    """Return the sink class registered under *name*.

    Raises :class:`KeyError` if *name* is unknown.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"No sink registered under name '{name}'")


# Register built-in sinks so they are discoverable via builder.
def _register_builtins() -> None:
    from logpipe.sinks.s3_sink import S3Sink
    from logpipe.sinks.es_sink import ElasticsearchSink
    from logpipe.sinks.rate_limit_sink import RateLimitSink

    register("s3", S3Sink)
    register("elasticsearch", ElasticsearchSink)
    register("rate_limit", RateLimitSink)


_register_builtins()
