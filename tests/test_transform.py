"""Tests for logpipe.transform.FieldTransformer."""

from __future__ import annotations

import pytest

from logpipe.transform import FieldTransformer, TransformError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_transformer(*rules):
    return FieldTransformer(list(rules))


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------

class TestFieldTransformerConstruction:
    def test_unknown_op_raises(self):
        with pytest.raises(TransformError, match="Unknown transform op"):
            _make_transformer({"field": "x", "op": "explode"})

    def test_known_ops_accepted(self):
        t = _make_transformer(
            {"field": "a", "op": "uppercase"},
            {"field": "b", "op": "to_int"},
        )
        assert t is not None


# ---------------------------------------------------------------------------
# string ops
# ---------------------------------------------------------------------------

class TestStringTransforms:
    def test_uppercase(self):
        t = _make_transformer({"field": "msg", "op": "uppercase"})
        assert t.apply({"msg": "hello"}) == {"msg": "HELLO"}

    def test_lowercase(self):
        t = _make_transformer({"field": "msg", "op": "lowercase"})
        assert t.apply({"msg": "WORLD"}) == {"msg": "world"}

    def test_strip(self):
        t = _make_transformer({"field": "msg", "op": "strip"})
        assert t.apply({"msg": "  hi  "}) == {"msg": "hi"}


# ---------------------------------------------------------------------------
# type coercions
# ---------------------------------------------------------------------------

class TestTypeCoercions:
    def test_to_int(self):
        t = _make_transformer({"field": "count", "op": "to_int"})
        assert t.apply({"count": "42"}) == {"count": 42}

    def test_to_float(self):
        t = _make_transformer({"field": "ratio", "op": "to_float"})
        result = t.apply({"ratio": "3.14"})
        assert abs(result["ratio"] - 3.14) < 1e-9

    def test_to_str(self):
        t = _make_transformer({"field": "code", "op": "to_str"})
        assert t.apply({"code": 200}) == {"code": "200"}

    def test_to_int_invalid_raises(self):
        t = _make_transformer({"field": "n", "op": "to_int"})
        with pytest.raises(TransformError):
            t.apply({"n": "not-a-number"})


# ---------------------------------------------------------------------------
# target field
# ---------------------------------------------------------------------------

class TestTargetField:
    def test_write_to_different_field(self):
        t = _make_transformer({"field": "raw", "op": "uppercase", "target": "raw_upper"})
        result = t.apply({"raw": "hello"})
        assert result["raw"] == "hello"
        assert result["raw_upper"] == "HELLO"


# ---------------------------------------------------------------------------
# missing fields
# ---------------------------------------------------------------------------

class TestMissingField:
    def test_missing_field_is_skipped(self):
        t = _make_transformer({"field": "missing", "op": "uppercase"})
        record = {"other": "value"}
        assert t.apply(record) == {"other": "value"}


# ---------------------------------------------------------------------------
# immutability
# ---------------------------------------------------------------------------

class TestImmutability:
    def test_original_record_not_mutated(self):
        t = _make_transformer({"field": "msg", "op": "uppercase"})
        original = {"msg": "hello"}
        t.apply(original)
        assert original["msg"] == "hello"
