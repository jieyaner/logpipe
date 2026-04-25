"""Record transformation utilities for logpipe."""

from __future__ import annotations

from typing import Any, Callable, Dict, List


class TransformError(Exception):
    """Raised when a transformation cannot be applied."""


class FieldTransformer:
    """Applies a series of named transformations to log record fields."""

    _BUILTINS: Dict[str, Callable[[Any], Any]] = {
        "uppercase": lambda v: v.upper() if isinstance(v, str) else v,
        "lowercase": lambda v: v.lower() if isinstance(v, str) else v,
        "strip": lambda v: v.strip() if isinstance(v, str) else v,
        "to_int": lambda v: int(v),
        "to_float": lambda v: float(v),
        "to_str": lambda v: str(v),
    }

    def __init__(self, rules: List[Dict[str, Any]]) -> None:
        """
        Parameters
        ----------
        rules:
            List of dicts with keys ``field`` and ``op`` (operation name).
            Optionally ``target`` to write the result to a different field.
        """
        self._rules = rules
        for rule in rules:
            op = rule.get("op")
            if op not in self._BUILTINS:
                raise TransformError(f"Unknown transform op: {op!r}")

    def apply(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a *new* dict with transformations applied."""
        out = dict(record)
        for rule in self._rules:
            field: str = rule["field"]
            op: str = rule["op"]
            target: str = rule.get("target", field)
            if field not in out:
                continue
            try:
                out[target] = self._BUILTINS[op](out[field])
            except (ValueError, TypeError) as exc:
                raise TransformError(
                    f"Transform {op!r} failed on field {field!r}: {exc}"
                ) from exc
        return out
