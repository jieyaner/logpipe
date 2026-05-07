"""Builder helper for LookupSink.

Supports two ways to supply the lookup table:
  - ``table``      – inline dict in the config
  - ``table_file`` – path to a JSON or CSV file

CSV files must have exactly two columns: ``key`` and ``value``.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict

from logpipe.sinks import BaseSink
from logpipe.sinks.lookup_sink import LookupSink


def _load_table_file(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"lookup table file not found: {path}")
    suffix = p.suffix.lower()
    if suffix == ".json":
        with p.open(encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError(f"JSON lookup file must contain a top-level object: {path}")
        return data
    if suffix == ".csv":
        table: Dict[str, Any] = {}
        with p.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or set(reader.fieldnames) < {"key", "value"}:
                raise ValueError(
                    f"CSV lookup file must have 'key' and 'value' columns: {path}"
                )
            for row in reader:
                table[row["key"]] = row["value"]
        return table
    raise ValueError(f"Unsupported lookup table file format: {suffix!r} ({path})")


def build_lookup_sink(downstream: BaseSink, cfg: Dict[str, Any]) -> LookupSink:
    """Construct a :class:`LookupSink` from a config dict.

    Required keys:
      - ``src_field`` (str)
      - One of ``table`` (dict) or ``table_file`` (str)

    Optional keys:
      - ``dest_field`` (str)
      - ``on_miss``    ("skip" | "drop" | "error", default "skip")
    """
    src_field: str = cfg["src_field"]

    if "table" in cfg and "table_file" in cfg:
        raise ValueError("Specify either 'table' or 'table_file', not both.")
    if "table" in cfg:
        table = dict(cfg["table"])
    elif "table_file" in cfg:
        table = _load_table_file(cfg["table_file"])
    else:
        raise ValueError("LookupSink config must include 'table' or 'table_file'.")

    kwargs: Dict[str, Any] = {"src_field": src_field, "table": table}
    if "dest_field" in cfg:
        kwargs["dest_field"] = cfg["dest_field"]
    if "on_miss" in cfg:
        kwargs["on_miss"] = cfg["on_miss"]

    return LookupSink(downstream, **kwargs)
