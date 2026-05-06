"""MaskSink — redacts fields by replacing characters with a mask pattern.

Unlike RedactSink (which replaces an entire field value), MaskSink preserves
a configurable number of leading/trailing characters so values remain
partially recognisable (e.g. credit-card or token masking).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Optional

from logpipe.sinks import BaseSink


class MaskError(Exception):
    """Raised when MaskSink is misconfigured."""


class MaskSink(BaseSink):
    """Forward records to *downstream* after masking nominated fields.

    Parameters
    ----------
    downstream:
        Sink that receives the masked copy of each record.
    fields:
        List of dot-separated field paths to mask.
    mask_char:
        Character used to fill the masked portion (default ``'*'``).
    show_first:
        How many leading characters to leave visible (default ``0``).
    show_last:
        How many trailing characters to leave visible (default ``4``).
    min_mask:
        Minimum number of mask characters to insert regardless of value
        length (default ``3``).
    """

    def __init__(
        self,
        downstream: BaseSink,
        fields: List[str],
        *,
        mask_char: str = "*",
        show_first: int = 0,
        show_last: int = 4,
        min_mask: int = 3,
    ) -> None:
        if not fields:
            raise MaskError("fields must not be empty")
        if len(mask_char) != 1:
            raise MaskError("mask_char must be exactly one character")
        if show_first < 0 or show_last < 0 or min_mask < 0:
            raise MaskError("show_first, show_last, and min_mask must be >= 0")

        self._downstream = downstream
        self._fields = fields
        self._mask_char = mask_char
        self._show_first = show_first
        self._show_last = show_last
        self._min_mask = min_mask

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _mask_value(self, value: str) -> str:
        """Return a masked version of *value*."""
        total = len(value)
        first = min(self._show_first, total)
        last = min(self._show_last, total - first)
        mask_len = max(total - first - last, self._min_mask)
        return value[:first] + self._mask_char * mask_len + value[total - last :] if last else value[:first] + self._mask_char * mask_len

    def _apply(self, record: Dict[str, Any], path: str) -> None:
        """Mutate *record* in-place, masking the field at *path*."""
        parts = path.split(".")
        node: Any = record
        for part in parts[:-1]:
            if not isinstance(node, dict) or part not in node:
                return
            node = node[part]
        leaf = parts[-1]
        if isinstance(node, dict) and leaf in node and isinstance(node[leaf], str):
            node[leaf] = self._mask_value(node[leaf])

    # ------------------------------------------------------------------
    # BaseSink interface
    # ------------------------------------------------------------------

    def write(self, record: Dict[str, Any]) -> None:
        masked = copy.deepcopy(record)
        for field in self._fields:
            self._apply(masked, field)
        self._downstream.write(masked)

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
