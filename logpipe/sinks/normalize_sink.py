"""NormalizeSink – coerce field values to a canonical type before forwarding.

Supported operations
--------------------
* ``to_str``   – cast value to ``str``
* ``to_int``   – cast value to ``int`` (truncates floats)
* ``to_float`` – cast value to ``float``
* ``to_bool``  – interpret common truthy strings as ``True``/``False``
* ``lower``    – lowercase a string field
* ``upper``    – uppercase a string field
* ``strip``    – strip leading/trailing whitespace from a string field
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from logpipe.sinks import BaseSink

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

_OPS = {"to_str", "to_int", "to_float", "to_bool", "lower", "upper", "strip"}


class NormalizeError(Exception):
    """Raised when a normalization rule is invalid or a coercion fails."""


class NormalizeSink(BaseSink):
    """Apply field-level type / string normalisations then forward records.

    Parameters
    ----------
    downstream:
        Sink that receives the (mutated copy of the) record.
    rules:
        List of ``(field, op)`` pairs applied in order.  Unknown fields are
        silently skipped; coercion errors raise :class:`NormalizeError`.
    """

    def __init__(self, downstream: BaseSink, rules: List[Tuple[str, str]]) -> None:
        for field, op in rules:
            if op not in _OPS:
                raise NormalizeError(f"Unknown normalization op {op!r} for field {field!r}")
        self._downstream = downstream
        self._rules: List[Tuple[str, str]] = list(rules)

    # ------------------------------------------------------------------
    def _apply(self, record: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(record)
        for field, op in self._rules:
            if field not in out:
                continue
            val = out[field]
            try:
                if op == "to_str":
                    out[field] = str(val)
                elif op == "to_int":
                    out[field] = int(float(val)) if not isinstance(val, int) else val
                elif op == "to_float":
                    out[field] = float(val)
                elif op == "to_bool":
                    if isinstance(val, bool):
                        pass
                    elif str(val).lower() in _TRUTHY:
                        out[field] = True
                    elif str(val).lower() in _FALSY:
                        out[field] = False
                    else:
                        raise NormalizeError(
                            f"Cannot coerce {val!r} to bool for field {field!r}"
                        )
                elif op == "lower":
                    out[field] = str(val).lower()
                elif op == "upper":
                    out[field] = str(val).upper()
                elif op == "strip":
                    out[field] = str(val).strip()
            except (ValueError, TypeError) as exc:
                raise NormalizeError(
                    f"Normalization {op!r} failed for field {field!r}: {exc}"
                ) from exc
        return out

    # ------------------------------------------------------------------
    def write(self, record: Dict[str, Any]) -> None:
        self._downstream.write(self._apply(record))

    def flush(self) -> None:
        self._downstream.flush()

    def close(self) -> None:
        self._downstream.close()
