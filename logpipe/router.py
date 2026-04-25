"""Route parsed log records to one or more sinks based on configurable rules."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


Record = Dict[str, Any]
Predicate = Callable[[Record], bool]


class Route:
    """Associates a predicate with a list of sink names."""

    def __init__(self, predicate: Optional[Predicate], sink_names: List[str]) -> None:
        self.predicate: Predicate = predicate if predicate is not None else lambda _: True
        self.sink_names = list(sink_names)

    def matches(self, record: Record) -> bool:
        try:
            return bool(self.predicate(record))
        except Exception:
            return False


class Router:
    """Dispatch records to sinks according to an ordered list of routes.

    Sinks are looked up by name from the *sinks* mapping supplied at
    construction time.  A record is forwarded to every route whose
    predicate matches; if no route matches the record is silently
    dropped unless a *default_sink* name is provided.
    """

    def __init__(
        self,
        sinks: Dict[str, Any],
        routes: Optional[List[Route]] = None,
        default_sink: Optional[str] = None,
    ) -> None:
        self._sinks = sinks
        self._routes: List[Route] = list(routes) if routes else []
        self._default_sink = default_sink

    def add_route(self, predicate: Optional[Predicate], sink_names: List[str]) -> None:
        """Append a new route at the lowest priority."""
        self._routes.append(Route(predicate, sink_names))

    def dispatch(self, record: Record) -> int:
        """Send *record* to matching sinks.  Returns the number of sinks written."""
        written = 0
        matched = False

        for route in self._routes:
            if route.matches(record):
                matched = True
                for name in route.sink_names:
                    sink = self._sinks.get(name)
                    if sink is not None:
                        sink.write(record)
                        written += 1

        if not matched and self._default_sink:
            sink = self._sinks.get(self._default_sink)
            if sink is not None:
                sink.write(record)
                written += 1

        return written
