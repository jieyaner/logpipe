"""Builder helper that constructs a :class:`RegexSink` from a config dict.

Expected config shape::

    {
        "type": "regex",
        "field": "level",          # required
        "pattern": "ERROR|WARN",   # required
        "invert": false,           # optional, default false
        "on_missing": "drop",      # optional: 'drop' | 'forward'
        "downstream": { ... }      # required — nested sink config
    }
"""

from logpipe.sinks.regex_sink import RegexSink, RegexError  # noqa: F401


def build_regex_sink(cfg, build_sink_fn):
    """Construct a :class:`RegexSink` from *cfg*.

    Parameters
    ----------
    cfg:
        Mapping with keys described in the module docstring.
    build_sink_fn:
        Callable ``(cfg) -> BaseSink`` used to construct the downstream sink
        (typically ``logpipe.builder._build_sink``).

    Returns
    -------
    RegexSink
    """
    field = cfg.get("field")
    if not field:
        raise RegexError("regex sink config requires 'field'")

    pattern = cfg.get("pattern")
    if pattern is None:
        raise RegexError("regex sink config requires 'pattern'")

    downstream_cfg = cfg.get("downstream")
    if downstream_cfg is None:
        raise RegexError("regex sink config requires 'downstream'")

    downstream = build_sink_fn(downstream_cfg)

    return RegexSink(
        downstream,
        field,
        pattern,
        invert=bool(cfg.get("invert", False)),
        on_missing=cfg.get("on_missing", "drop"),
    )
