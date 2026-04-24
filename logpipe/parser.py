"""Log line parsers for logpipe.

Supports JSON structured logs and plain-text logs with optional regex-based
field extraction. Each parser returns a normalized dict ready for forwarding
to a sink.
"""

import json
import re
import time
from typing import Any, Dict, Optional


class ParseError(Exception):
    """Raised when a log line cannot be parsed by the selected parser."""


class BaseParser:
    """Abstract base class for log parsers."""

    def parse(self, line: str, source: str = "") -> Dict[str, Any]:
        """Parse a raw log line and return a structured record.

        Args:
            line: Raw log line (trailing newline will be stripped).
            source: Optional label identifying the originating file/stream.

        Returns:
            A dict containing at minimum ``message``, ``source``, and
            ``timestamp`` keys.

        Raises:
            ParseError: If the line cannot be parsed.
        """
        raise NotImplementedError

    @staticmethod
    def _base_record(message: str, source: str) -> Dict[str, Any]:
        """Build the minimal record envelope shared by all parsers."""
        return {
            "message": message,
            "source": source,
            "timestamp": time.time(),
        }


class JSONParser(BaseParser):
    """Parser for newline-delimited JSON log lines.

    If the line is valid JSON the decoded object is returned enriched with
    ``source`` and ``timestamp`` (only added when absent from the original
    payload so we never overwrite application-supplied values).
    """

    def parse(self, line: str, source: str = "") -> Dict[str, Any]:
        line = line.rstrip("\n")
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ParseError(f"Invalid JSON: {exc}") from exc

        if not isinstance(record, dict):
            raise ParseError("JSON line did not decode to an object")

        record.setdefault("source", source)
        record.setdefault("timestamp", time.time())
        # Ensure a 'message' key exists even for structured payloads that omit it
        record.setdefault("message", line)
        return record


class RegexParser(BaseParser):
    """Parser that extracts named groups from a log line via a regex pattern.

    Any named capture groups become top-level keys in the returned record.
    The full raw line is always stored under ``message``.

    Example::

        pattern = r'(?P<level>\\w+) (?P<msg>.+)'
        parser = RegexParser(pattern)
        record = parser.parse('ERROR something went wrong')
        # -> {'level': 'ERROR', 'msg': 'something went wrong', 'message': ..., ...}
    """

    def __init__(self, pattern: str, flags: int = 0) -> None:
        self._regex = re.compile(pattern, flags)

    def parse(self, line: str, source: str = "") -> Dict[str, Any]:
        line = line.rstrip("\n")
        match = self._regex.search(line)
        if match is None:
            raise ParseError(f"Line did not match pattern: {self._regex.pattern!r}")

        record = self._base_record(line, source)
        record.update(match.groupdict())
        return record


class PlainTextParser(BaseParser):
    """Fallback parser that treats the entire line as an unstructured message."""

    def parse(self, line: str, source: str = "") -> Dict[str, Any]:
        return self._base_record(line.rstrip("\n"), source)


# ---------------------------------------------------------------------------
# Factory helper
# ---------------------------------------------------------------------------

_PARSERS: Dict[str, type] = {
    "json": JSONParser,
    "plain": PlainTextParser,
}


def get_parser(name: str, **kwargs: Any) -> BaseParser:
    """Instantiate a parser by name.

    Args:
        name: One of ``'json'``, ``'plain'``, or ``'regex'``.
        **kwargs: Forwarded to the parser constructor (e.g. ``pattern`` for
            :class:`RegexParser`).

    Raises:
        ValueError: If *name* is not a recognised parser type.
    """
    if name == "regex":
        pattern: Optional[str] = kwargs.get("pattern")
        if not pattern:
            raise ValueError("RegexParser requires a 'pattern' keyword argument")
        return RegexParser(pattern, flags=kwargs.get("flags", 0))

    if name not in _PARSERS:
        raise ValueError(
            f"Unknown parser {name!r}. Available: {sorted(_PARSERS) + ['regex']}"
        )
    return _PARSERS[name]()
