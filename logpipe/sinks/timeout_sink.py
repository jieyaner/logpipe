"""TimeoutSink — drops or raises if the downstream write exceeds a deadline.

Unlike DeadlineSink (which uses an absolute wall-clock deadline shared across
many writes), TimeoutSink enforces a *per-write* maximum duration so that a
slow downstream cannot stall the pipeline indefinitely.
"""
from __future__ import annotations

import threading
from typing import Any

from logpipe.sinks import BaseSink


class WriteTimedOut(Exception):
    """Raised when a downstream write does not complete within *timeout_s*."""


class TimeoutSink(BaseSink):
    """Wrap *inner* and enforce a per-write wall-clock timeout.

    Parameters
    ----------
    inner:
        The downstream sink to delegate to.
    timeout_s:
        Maximum seconds allowed for a single ``write`` call.  Must be > 0.
    raise_on_timeout:
        When *True* (default) a :class:`WriteTimedOut` exception is raised.
        When *False* the record is silently dropped and processing continues.
    """

    def __init__(
        self,
        inner: BaseSink,
        timeout_s: float = 5.0,
        *,
        raise_on_timeout: bool = True,
    ) -> None:
        if timeout_s <= 0:
            raise ValueError("timeout_s must be greater than zero")
        self._inner = inner
        self._timeout_s = timeout_s
        self._raise_on_timeout = raise_on_timeout

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict[str, Any]) -> None:
        exc_box: list[BaseException] = []

        def _target() -> None:
            try:
                self._inner.write(record)
            except Exception as exc:  # noqa: BLE001
                exc_box.append(exc)

        t = threading.Thread(target=_target, daemon=True)
        t.start()
        t.join(self._timeout_s)

        if t.is_alive():
            # Thread is still blocked — downstream is too slow.
            if self._raise_on_timeout:
                raise WriteTimedOut(
                    f"write did not complete within {self._timeout_s}s"
                )
            return  # silently drop

        if exc_box:
            raise exc_box[0]

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()
