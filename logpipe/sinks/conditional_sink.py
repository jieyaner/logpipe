"""ConditionalSink — forwards records to *inner* only when a Python
expression evaluated against the record returns truthy.

The expression is a plain Python expression string.  The record dict is
available as ``r`` inside the expression, e.g.::

    ConditionalSink("r.get('level') == 'error'", inner=my_sink)
"""

from __future__ import annotations

from typing import Any, Dict

from logpipe.sinks import BaseSink


class ConditionalError(Exception):
    """Raised when the condition expression cannot be compiled."""


class ConditionalSink(BaseSink):
    """Forward records to *inner* only when *condition* is truthy.

    Parameters
    ----------
    condition:
        A Python expression string.  The record is bound to the name ``r``.
    inner:
        Downstream sink that receives matching records.
    """

    def __init__(self, condition: str, inner: BaseSink) -> None:
        if not condition or not condition.strip():
            raise ConditionalError("condition must be a non-empty expression")
        try:
            self._code = compile(condition.strip(), "<condition>", "eval")
        except SyntaxError as exc:
            raise ConditionalError(f"invalid condition expression: {exc}") from exc
        self._inner = inner

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        try:
            result = eval(self._code, {"__builtins__": {}}, {"r": record})  # noqa: S307
        except Exception:
            return
        if result:
            self._inner.write(record)

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
