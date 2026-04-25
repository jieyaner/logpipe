"""Base sink interface and sink registry for logpipe."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional


class BaseSink:
    """Abstract base class for all sinks."""

    def write(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, Callable[..., BaseSink]] = {}


def register(name: str, factory: Callable[..., BaseSink]) -> None:
    """Register a sink factory under *name*."""
    _REGISTRY[name] = factory


def get(name: str) -> Optional[Callable[..., BaseSink]]:
    """Return the factory registered under *name*, or ``None``."""
    return _REGISTRY.get(name)
