"""A sink wrapper that drops records not passing a FilterChain."""

from typing import Any, Dict

from logpipe.filter import FilterChain
from logpipe.sinks import BaseSink


class FilteredSink(BaseSink):
    """Wraps another sink and only forwards records accepted by *chain*."""

    def __init__(self, inner: BaseSink, chain: FilterChain) -> None:
        self._inner = inner
        self._chain = chain
        self._dropped = 0
        self._passed = 0

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        if self._chain.keep(record):
            self._passed += 1
            self._inner.write(record)
        else:
            self._dropped += 1

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def passed(self) -> int:
        return self._passed

    @property
    def dropped(self) -> int:
        return self._dropped
