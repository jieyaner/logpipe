"""HeaderSink — injects static or dynamic metadata fields into every record."""

from __future__ import annotations

from typing import Any, Callable, Dict, Union

from logpipe.sinks import BaseSink

# A header value can be a plain scalar or a zero-argument callable that is
# evaluated fresh for every record (useful for timestamps, counters, etc.).
_HeaderValue = Union[Any, Callable[[], Any]]


class HeaderSink(BaseSink):
    """Wraps *inner* and stamps every record with the supplied header fields.

    Fields whose value is a callable are invoked once per record so that
    dynamic values (e.g. ``time.time``) are captured at write-time.

    If *overwrite* is ``False`` (default) existing keys in the record are
    preserved; set it to ``True`` to let headers win.

    Example::

        sink = HeaderSink(
            inner=es_sink,
            headers={"env": "prod", "host": socket.gethostname},
        )
    """

    def __init__(
        self,
        inner: BaseSink,
        headers: Dict[str, _HeaderValue],
        *,
        overwrite: bool = False,
    ) -> None:
        if not headers:
            raise ValueError("headers must contain at least one entry")
        self._inner = inner
        self._headers: Dict[str, _HeaderValue] = dict(headers)
        self._overwrite = overwrite

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve(self) -> Dict[str, Any]:
        """Evaluate any callable header values and return a plain dict."""
        return {
            k: (v() if callable(v) else v)
            for k, v in self._headers.items()
        }

    def _stamp(self, record: Dict[str, Any]) -> Dict[str, Any]:
        resolved = self._resolve()
        if self._overwrite:
            return {**record, **resolved}
        return {**resolved, **record}

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        self._inner.write(self._stamp(record))

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
