"""FanoutSink — forwards each record to multiple downstream sinks."""

from logpipe.sinks import BaseSink


class FanoutSink(BaseSink):
    """Write every record to all registered child sinks.

    Errors raised by individual sinks are collected and re-raised as a
    single ``FanoutError`` after all sinks have been attempted, so that
    one failing sink never silently drops records for the others.
    """

    def __init__(self, sinks):
        """Parameters
        ----------
        sinks:
            Iterable of :class:`~logpipe.sinks.BaseSink` instances.
        """
        if not sinks:
            raise ValueError("FanoutSink requires at least one child sink")
        self._sinks = list(sinks)

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record):
        errors = []
        for sink in self._sinks:
            try:
                sink.write(record)
            except Exception as exc:  # noqa: BLE001
                errors.append((sink, exc))
        if errors:
            raise FanoutError(errors)

    def flush(self):
        errors = []
        for sink in self._sinks:
            try:
                sink.flush()
            except Exception as exc:  # noqa: BLE001
                errors.append((sink, exc))
        if errors:
            raise FanoutError(errors)

    def close(self):
        errors = []
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as exc:  # noqa: BLE001
                errors.append((sink, exc))
        if errors:
            raise FanoutError(errors)

    def __repr__(self):  # pragma: no cover
        return f"FanoutSink(sinks={self._sinks!r})"


class FanoutError(Exception):
    """Raised when one or more child sinks fail during a fanout operation."""

    def __init__(self, errors):
        self.errors = errors  # list of (sink, exception) tuples
        details = "; ".join(f"{s!r}: {e}" for s, e in errors)
        super().__init__(f"Fanout failed for {len(errors)} sink(s): {details}")
