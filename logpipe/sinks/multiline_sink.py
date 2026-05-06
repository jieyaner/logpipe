"""MultilineSink — accumulates log lines into multi-line events.

Lines that do NOT match ``start_pattern`` are treated as continuations of
the previous event.  Once a new start line arrives (or flush/close is
called) the accumulated buffer is emitted as a single record whose
``message`` field is the joined text.
"""

import re
import time
from logpipe.sinks import BaseSink


class MultilineSink(BaseSink):
    """Coalesce continuation lines into a single structured record.

    Parameters
    ----------
    inner:
        Downstream sink that receives the merged records.
    start_pattern:
        Regex that marks the *beginning* of a new event.  Any line that
        does not match is appended to the current buffer.
    field:
        Record field that contains the raw log line (default ``"message"``).
    join:
        String used to join buffered lines (default ``"\\n"``).
    max_lines:
        Flush the buffer after this many lines even if no new start line
        has arrived.  ``0`` means unlimited (default ``0``).
    """

    def __init__(self, inner, start_pattern, field="message", join="\n", max_lines=0):
        self._inner = inner
        self._pattern = re.compile(start_pattern)
        self._field = field
        self._join = join
        self._max_lines = max_lines

        self._buffer: list[str] = []
        self._meta: dict = {}

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _flush_buffer(self):
        if not self._buffer:
            return
        record = dict(self._meta)
        record[self._field] = self._join.join(self._buffer)
        self._inner.write(record)
        self._buffer = []
        self._meta = {}

    def _start_new(self, record):
        self._meta = {k: v for k, v in record.items() if k != self._field}
        self._meta.setdefault("timestamp", time.time())
        line = record.get(self._field, "")
        self._buffer = [line]

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: dict):
        line = record.get(self._field, "")
        if self._pattern.search(line):
            self._flush_buffer()
            self._start_new(record)
        else:
            if not self._buffer:
                # No preceding start line — treat this line as its own event.
                self._start_new(record)
            else:
                self._buffer.append(line)

        if self._max_lines and len(self._buffer) >= self._max_lines:
            self._flush_buffer()

    def flush(self):
        self._flush_buffer()
        self._inner.flush()

    def close(self):
        self._flush_buffer()
        self._inner.close()
