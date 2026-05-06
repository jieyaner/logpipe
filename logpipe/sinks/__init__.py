"""Base sink interface and sink registry for logpipe."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type


class BaseSink(ABC):
    """Abstract base class that every sink must implement."""

    @abstractmethod
    def write(self, record: Dict[str, Any]) -> None:
        """Accept a single parsed log record."""

    @abstractmethod
    def flush(self) -> None:
        """Flush any internally buffered records to the underlying target."""

    @abstractmethod
    def close(self) -> None:
        """Flush and release any resources held by this sink."""


# ---------------------------------------------------------------------------
# Sink registry
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, Type[BaseSink]] = {}


def register(name: str, cls: Type[BaseSink]) -> None:
    """Register *cls* under *name* so builder.py can look it up by string."""
    if not issubclass(cls, BaseSink):
        raise TypeError(f"{cls!r} must be a subclass of BaseSink")
    _REGISTRY[name] = cls


def lookup(name: str) -> Type[BaseSink]:
    """Return the sink class registered under *name*.

    Raises
    ------
    KeyError
        If no sink has been registered with that name.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"No sink registered under {name!r}") from None


def registered_names() -> list:
    """Return a sorted list of all registered sink names."""
    return sorted(_REGISTRY.keys())
