"""Sink base class and registry helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Type


class BaseSink(ABC):
    """All sinks must implement this interface."""

    @abstractmethod
    def write(self, record: dict) -> None:
        """Accept a single parsed log record."""

    def flush(self) -> None:  # noqa: B027
        """Flush any buffered records to the backing store."""

    def close(self) -> None:  # noqa: B027
        """Release resources held by the sink."""


_REGISTRY: Dict[str, Type[BaseSink]] = {}


def register(name: str, cls: Type[BaseSink]) -> None:
    _REGISTRY[name] = cls


def get(name: str) -> Type[BaseSink]:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"Unknown sink type: {name!r}") from None
