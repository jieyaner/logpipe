"""RegexSink — forward records only when a field matches (or does not match) a regex."""

import re
from logpipe.sinks import BaseSink


class RegexError(Exception):
    """Raised for configuration problems in RegexSink."""


class RegexSink(BaseSink):
    """Filters records by testing a compiled regex against a named field.

    Parameters
    ----------
    downstream:
        Sink that receives records passing the filter.
    field:
        Dot-separated path to the field whose string value is tested.
    pattern:
        Regular-expression string compiled with ``re.search``.
    invert:
        When *True*, forward records that do *not* match (default False).
    on_missing:
        ``"drop"`` (default) silently drops records lacking the field;
        ``"forward"`` passes them through regardless of the pattern.
    """

    def __init__(self, downstream, field, pattern, *, invert=False, on_missing="drop"):
        if on_missing not in ("drop", "forward"):
            raise RegexError(f"on_missing must be 'drop' or 'forward', got {on_missing!r}")
        try:
            self._regex = re.compile(pattern)
        except re.error as exc:
            raise RegexError(f"Invalid regex pattern {pattern!r}: {exc}") from exc

        self._downstream = downstream
        self._field = field
        self._parts = field.split(".")
        self._invert = invert
        self._on_missing = on_missing

    # ------------------------------------------------------------------
    def _get_field(self, record):
        """Return (found, value) for the configured field path."""
        node = record
        for part in self._parts:
            if not isinstance(node, dict) or part not in node:
                return False, None
            node = node[part]
        return True, node

    def write(self, record):
        found, value = self._get_field(record)
        if not found:
            if self._on_missing == "forward":
                self._downstream.write(record)
            return

        matched = bool(self._regex.search(str(value)))
        should_forward = matched ^ self._invert  # XOR: invert flips the decision
        if should_forward:
            self._downstream.write(record)

    def flush(self):
        self._downstream.flush()

    def close(self):
        self._downstream.close()
