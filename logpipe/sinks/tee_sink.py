"""
TeeSink — writes every record to a primary sink and simultaneously
captures a copy to a secondary "tap" sink.  Useful for debugging or
audit logging without altering the main pipeline.
"""

from logpipe.sinks import BaseSink


class TeeSink(BaseSink):
    """Forward each record to *primary* and *tap* independently.

    Failures in the tap are swallowed (optionally re-raised) so that a
    debugging observer never disrupts the main data flow.

    Args:
        primary: The main sink that drives the pipeline.
        tap: The secondary sink that receives a copy of every record.
        silent_tap: When *True* (default) exceptions from *tap* are
            logged and suppressed.  Set to *False* to let them propagate.
    """

    def __init__(self, primary: BaseSink, tap: BaseSink, *, silent_tap: bool = True):
        self._primary = primary
        self._tap = tap
        self._silent_tap = silent_tap

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict) -> None:
        self._primary.write(record)
        self._tap_call("write", record)

    def flush(self) -> None:
        self._primary.flush()
        self._tap_call("flush")

    def close(self) -> None:
        self._primary.close()
        self._tap_call("close")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tap_call(self, method: str, *args):
        try:
            getattr(self._tap, method)(*args)
        except Exception as exc:  # noqa: BLE001
            if not self._silent_tap:
                raise
            import logging
            logging.getLogger(__name__).warning(
                "TeeSink: tap %s raised %s: %s", method, type(exc).__name__, exc
            )
