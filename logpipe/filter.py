"""Record filtering support for logpipe pipelines."""

import fnmatch
import re
from typing import Any, Callable, Dict, List, Optional


class FilterError(Exception):
    """Raised when a filter cannot be compiled or evaluated."""


class FieldFilter:
    """Filters log records based on field value matching rules."""

    def __init__(
        self,
        field: str,
        pattern: str,
        match_type: str = "exact",
        invert: bool = False,
    ) -> None:
        """
        :param field:      dot-separated key path into the record dict.
        :param pattern:    value pattern to match against.
        :param match_type: one of 'exact', 'glob', 'regex'.
        :param invert:     if True, keep records that do NOT match.
        """
        if match_type not in ("exact", "glob", "regex"):
            raise FilterError(f"Unknown match_type: {match_type!r}")
        self.field = field
        self.pattern = pattern
        self.match_type = match_type
        self.invert = invert
        self._regex: Optional[re.Pattern] = (
            re.compile(pattern) if match_type == "regex" else None
        )

    def _get_field(self, record: Dict[str, Any]) -> Optional[str]:
        """Traverse dot-separated key path; return None if missing."""
        obj: Any = record
        for key in self.field.split("."):
            if not isinstance(obj, dict):
                return None
            obj = obj.get(key)
        return str(obj) if obj is not None else None

    def _matches(self, value: str) -> bool:
        if self.match_type == "exact":
            return value == self.pattern
        if self.match_type == "glob":
            return fnmatch.fnmatch(value, self.pattern)
        # regex
        return bool(self._regex.search(value))  # type: ignore[union-attr]

    def keep(self, record: Dict[str, Any]) -> bool:
        """Return True if the record should be forwarded downstream."""
        value = self._get_field(record)
        if value is None:
            return self.invert  # missing field: treat as no-match
        matched = self._matches(value)
        return (not matched) if self.invert else matched


class FilterChain:
    """Applies an ordered list of FieldFilters; ALL must pass (AND semantics)."""

    def __init__(self, filters: Optional[List[FieldFilter]] = None) -> None:
        self._filters: List[FieldFilter] = filters or []

    def add(self, f: FieldFilter) -> None:
        self._filters.append(f)

    def keep(self, record: Dict[str, Any]) -> bool:
        return all(f.keep(record) for f in self._filters)

    def __len__(self) -> int:
        return len(self._filters)
