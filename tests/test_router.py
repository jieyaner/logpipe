"""Tests for logpipe.router."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from logpipe.router import Route, Router


Record = Dict[str, Any]


class _CaptureSink:
    """In-memory sink that records every write call."""

    def __init__(self) -> None:
        self.records: List[Record] = []

    def write(self, record: Record) -> None:
        self.records.append(record)


def _make_router(*sink_names: str, default: str | None = None) -> tuple[Router, dict]:
    sinks = {name: _CaptureSink() for name in sink_names}
    router = Router(sinks, default_sink=default)
    return router, sinks


# ---------------------------------------------------------------------------
# Route.matches
# ---------------------------------------------------------------------------

class TestRouteMatches:
    def test_none_predicate_always_matches(self):
        route = Route(None, ["s3"])
        assert route.matches({"level": "info"}) is True

    def test_true_predicate_matches(self):
        route = Route(lambda r: r.get("level") == "error", ["s3"])
        assert route.matches({"level": "error"}) is True

    def test_false_predicate_does_not_match(self):
        route = Route(lambda r: r.get("level") == "error", ["s3"])
        assert route.matches({"level": "info"}) is False

    def test_raising_predicate_returns_false(self):
        route = Route(lambda r: 1 / 0, ["s3"])  # type: ignore[arg-type]
        assert route.matches({}) is False


# ---------------------------------------------------------------------------
# Router.dispatch
# ---------------------------------------------------------------------------

class TestRouterDispatch:
    def test_dispatches_to_matching_sink(self):
        router, sinks = _make_router("s3", "es")
        router.add_route(lambda r: r.get("level") == "error", ["s3"])
        router.dispatch({"level": "error", "msg": "boom"})
        assert len(sinks["s3"].records) == 1
        assert len(sinks["es"].records) == 0

    def test_dispatches_to_multiple_sinks(self):
        router, sinks = _make_router("s3", "es")
        router.add_route(None, ["s3", "es"])
        router.dispatch({"msg": "hello"})
        assert len(sinks["s3"].records) == 1
        assert len(sinks["es"].records) == 1

    def test_no_match_uses_default_sink(self):
        router, sinks = _make_router("s3", "es", default="es")
        router.add_route(lambda r: False, ["s3"])
        router.dispatch({"msg": "unmatched"})
        assert len(sinks["s3"].records) == 0
        assert len(sinks["es"].records) == 1

    def test_no_match_no_default_drops_record(self):
        router, sinks = _make_router("s3")
        router.add_route(lambda r: False, ["s3"])
        written = router.dispatch({"msg": "dropped"})
        assert written == 0
        assert len(sinks["s3"].records) == 0

    def test_unknown_sink_name_is_skipped(self):
        router, sinks = _make_router("s3")
        router.add_route(None, ["nonexistent"])
        written = router.dispatch({"msg": "x"})
        assert written == 0

    def test_returns_count_of_sinks_written(self):
        router, sinks = _make_router("s3", "es")
        router.add_route(None, ["s3", "es"])
        assert router.dispatch({"msg": "hi"}) == 2
