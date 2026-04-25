import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nosql_delta_bridge.coerce import Action, CoerceConfig, coerce_document
from nosql_delta_bridge.infer import FieldSchema, infer_schema
from nosql_delta_bridge.flatten import flatten_document

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


def schema(*fields: tuple[str, str, bool]) -> dict[str, FieldSchema]:
    return {path: FieldSchema(dtype=dtype, nullable=null) for path, dtype, null in fields}


# --- matching types pass through unchanged ---

def test_correct_types_pass_through():
    doc = {"name": "Alice", "age": 30, "score": 9.5, "active": True}
    s = schema(("name", "string", False), ("age", "integer", False),
               ("score", "float", False), ("active", "boolean", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document == doc


# --- null handling ---

def test_null_on_nullable_field_passes():
    doc = {"email": None}
    s = schema(("email", "string", True))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["email"] is None


def test_null_on_non_nullable_field_rejects():
    doc = {"name": None}
    s = schema(("name", "string", False))
    result = coerce_document(doc, s)
    assert result.rejected
    assert "name" in result.reject_reason


# --- CAST action ---

def test_cast_string_to_integer():
    doc = {"age": "25"}
    s = schema(("age", "integer", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["age"] == 25
    assert isinstance(result.document["age"], int)


def test_cast_string_to_float():
    doc = {"score": "8.1"}
    s = schema(("score", "float", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["score"] == pytest.approx(8.1)


def test_cast_number_to_string():
    doc = {"age": 30}
    s = schema(("age", "string", False))
    result = coerce_document(doc, s)
    assert result.document["age"] == "30"


def test_cast_string_true_to_boolean():
    for val in ["true", "True", "yes", "YES", "1"]:
        doc = {"active": val}
        s = schema(("active", "boolean", False))
        result = coerce_document(doc, s)
        assert result.document["active"] is True


def test_cast_string_false_to_boolean():
    for val in ["false", "False", "no", "NO", "0"]:
        doc = {"active": val}
        s = schema(("active", "boolean", False))
        result = coerce_document(doc, s)
        assert result.document["active"] is False


def test_cast_lossless_float_to_integer():
    doc = {"age": 25.0}
    s = schema(("age", "integer", False))
    result = coerce_document(doc, s)
    assert result.document["age"] == 25


def test_cast_lossy_float_to_integer_rejects():
    doc = {"age": 25.7}
    s = schema(("age", "integer", False))
    result = coerce_document(doc, s)
    assert result.rejected
    assert "cast failed" in result.reject_reason


def test_cast_non_numeric_string_to_integer_rejects():
    doc = {"age": "twenty"}
    s = schema(("age", "integer", False))
    result = coerce_document(doc, s)
    assert result.rejected


def test_cast_ambiguous_string_to_boolean_rejects():
    doc = {"active": "maybe"}
    s = schema(("active", "boolean", False))
    result = coerce_document(doc, s)
    assert result.rejected


# --- REJECT action ---

def test_reject_action_on_mismatch():
    doc = {"age": "twenty-five"}
    s = schema(("age", "integer", False))
    config = CoerceConfig(field_rules={"age": Action.REJECT})
    result = coerce_document(doc, s, config)
    assert result.rejected
    assert "age" in result.reject_reason


# --- FLAG action ---

def test_flag_action_keeps_value_and_records_warning():
    doc = {"age": "25"}
    s = schema(("age", "integer", False))
    config = CoerceConfig(field_rules={"age": Action.FLAG})
    result = coerce_document(doc, s, config)
    assert not result.rejected
    assert result.document["age"] == "25"
    assert "age" in result.warnings


def test_flag_action_does_not_affect_other_fields():
    doc = {"age": "25", "name": "Alice"}
    s = schema(("age", "integer", False), ("name", "string", False))
    config = CoerceConfig(field_rules={"age": Action.FLAG})
    result = coerce_document(doc, s, config)
    assert result.document["name"] == "Alice"
    assert "name" not in result.warnings


# --- fields not in schema pass through ---

def test_unknown_field_passes_through():
    doc = {"name": "Alice", "extra_field": "surprise"}
    s = schema(("name", "string", False))
    result = coerce_document(doc, s)
    assert result.document["extra_field"] == "surprise"


# --- pipeline: infer + flatten + coerce ---

def test_pipeline_users_fixture():
    docs = load("users.json")
    schema_inferred = infer_schema(docs)
    flat = flatten_document(docs[1])  # Bob: age="25", email=None, score=None
    result = coerce_document(flat, schema_inferred)
    # age is string in inferred schema (widened), so "25" matches — no cast needed
    assert not result.rejected
    assert result.document["email"] is None


def test_pipeline_orders_fixture():
    docs = load("orders.json")
    schema_inferred = infer_schema(docs)
    flat = flatten_document(docs[0])  # clean order, all types correct
    result = coerce_document(flat, schema_inferred)
    assert not result.rejected


# --- datetime coercion ---

def test_datetime_object_passes_through():
    dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    doc = {"ts": dt}
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["ts"] == dt


def test_cast_iso_string_to_datetime():
    doc = {"ts": "2024-01-15T10:30:00+00:00"}
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert isinstance(result.document["ts"], datetime)
    assert result.document["ts"].year == 2024


def test_cast_iso_string_with_z_suffix_to_datetime():
    doc = {"ts": "2024-06-01T00:00:00Z"}
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["ts"].tzinfo is not None


def test_cast_naive_iso_string_assumes_utc():
    doc = {"ts": "2024-01-15T10:30:00"}
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert result.document["ts"].tzinfo == timezone.utc


def test_cast_integer_epoch_to_datetime():
    doc = {"ts": 1705316400}  # 2024-01-15T11:00:00Z
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert not result.rejected
    assert isinstance(result.document["ts"], datetime)
    assert result.document["ts"].tzinfo is not None


def test_cast_invalid_string_to_datetime_rejects():
    doc = {"ts": "not-a-date"}
    s = schema(("ts", "datetime", False))
    result = coerce_document(doc, s)
    assert result.rejected
    assert "ts" in result.reject_reason
