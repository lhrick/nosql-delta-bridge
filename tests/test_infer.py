import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nosql_delta_bridge.infer import FieldSchema, InferConfig, infer_schema, merge_schemas

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


# --- type widening ---

def test_widening_bool_to_int():
    docs = [{"x": True}, {"x": 1}]
    assert infer_schema(docs)["x"].dtype == "integer"


def test_widening_int_to_float():
    docs = [{"x": 1}, {"x": 1.5}]
    assert infer_schema(docs)["x"].dtype == "float"


def test_widening_number_to_string():
    docs = [{"x": 1}, {"x": "one"}]
    assert infer_schema(docs)["x"].dtype == "string"


def test_widening_incompatible_types_to_string():
    docs = [{"x": {"nested": 1}}, {"x": "flat"}]
    assert infer_schema(docs)["x"].dtype == "string"


# --- nullable ---

def test_nullable_when_field_missing():
    docs = [{"x": 1}, {"y": 2}]
    assert infer_schema(docs)["x"].nullable is True


def test_nullable_when_field_is_null():
    docs = [{"x": 1}, {"x": None}]
    schema = infer_schema(docs)
    assert schema["x"].nullable is True
    assert schema["x"].dtype == "integer"


def test_not_nullable_when_always_present():
    docs = [{"x": 1}, {"x": 2}]
    assert infer_schema(docs)["x"].nullable is False


# --- nested objects ---

def test_nested_field_paths():
    docs = [{"address": {"city": "SP", "zip": "01310"}}]
    schema = infer_schema(docs)
    assert "address.city" in schema
    assert "address.zip" in schema
    assert schema["address.city"].dtype == "string"


def test_nested_field_missing_in_some_docs():
    docs = [
        {"address": {"city": "SP", "country": "BR"}},
        {"address": {"city": "RJ"}},
    ]
    schema = infer_schema(docs)
    assert schema["address.country"].nullable is True
    assert schema["address.city"].nullable is False


# --- fixtures ---

def test_users_fixture_age_conflict():
    # age is int in most docs, string "25" in one, null in another
    docs = load("users.json")
    schema = infer_schema(docs)
    assert schema["age"].dtype == "string"
    assert schema["age"].nullable is True


def test_users_fixture_missing_fields_are_nullable():
    docs = load("users.json")
    schema = infer_schema(docs)
    assert schema["referral_code"].nullable is True
    assert schema["phone"].nullable is True


def test_users_fixture_score_widens_to_string():
    # score is float, string "8.1", and null across docs
    docs = load("users.json")
    schema = infer_schema(docs)
    assert schema["score"].dtype == "string"
    assert schema["score"].nullable is True


def test_orders_fixture_status_conflict():
    # status is string in most docs, int 3 in one
    docs = load("orders.json")
    schema = infer_schema(docs)
    assert schema["status"].dtype == "string"


def test_orders_fixture_total_widens_to_string():
    # total is float in some, string "89.90" in one, int 0 in another
    docs = load("orders.json")
    schema = infer_schema(docs)
    assert schema["total"].dtype == "string"


# --- datetime inference ---

def test_datetime_python_object_always_detected():
    docs = [{"ts": datetime(2024, 1, 15, tzinfo=timezone.utc)}]
    schema = infer_schema(docs)
    assert schema["ts"].dtype == "datetime"


def test_datetime_python_object_detected_without_config():
    # Python datetime objects are always typed as datetime regardless of config
    docs = [{"ts": datetime(2024, 1, 15, tzinfo=timezone.utc)}]
    assert infer_schema(docs)["ts"].dtype == "datetime"


def test_datetime_iso_string_detected_with_config():
    docs = [{"created_at": "2024-01-15T10:30:00+00:00"}]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["created_at"].dtype == "datetime"


def test_datetime_iso_string_not_detected_by_default():
    docs = [{"created_at": "2024-01-15T10:30:00+00:00"}]
    schema = infer_schema(docs)
    assert schema["created_at"].dtype == "string"


def test_datetime_z_suffix_detected():
    docs = [{"ts": "2024-06-01T00:00:00Z"}]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["ts"].dtype == "datetime"


def test_datetime_date_only_string_detected():
    docs = [{"date": "2024-01-15"}]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["date"].dtype == "datetime"


def test_datetime_widens_to_string_on_conflict_with_non_datetime():
    # one doc has a datetime string, another has a plain string
    docs = [
        {"ts": "2024-01-15T10:00:00Z"},
        {"ts": "not-a-date"},
    ]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["ts"].dtype == "string"


def test_datetime_widens_to_string_on_conflict_with_integer():
    docs = [{"ts": "2024-01-15T10:00:00Z"}, {"ts": 1234567890}]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["ts"].dtype == "string"


def test_all_datetime_strings_stay_datetime():
    docs = [
        {"ts": "2024-01-15T10:00:00Z"},
        {"ts": "2024-06-30T23:59:59+00:00"},
    ]
    schema = infer_schema(docs, InferConfig(detect_datetimes=True))
    assert schema["ts"].dtype == "datetime"


# --- merge_schemas ---

def s(dtype: str, nullable: bool) -> FieldSchema:
    return FieldSchema(dtype=dtype, nullable=nullable)


def test_merge_field_in_both_same_type():
    old = {"name": s("string", False)}
    new = {"name": s("string", False)}
    merged = merge_schemas(old, new)
    assert merged["name"].dtype == "string"
    assert merged["name"].nullable is False


def test_merge_field_in_both_widens_type():
    old = {"age": s("integer", False)}
    new = {"age": s("float", False)}
    merged = merge_schemas(old, new)
    assert merged["age"].dtype == "float"


def test_merge_field_in_both_unifies_nullable():
    old = {"name": s("string", False)}
    new = {"name": s("string", True)}
    merged = merge_schemas(old, new)
    assert merged["name"].nullable is True


def test_merge_field_only_in_old_becomes_nullable():
    old = {"name": s("string", False), "age": s("integer", False)}
    new = {"name": s("string", False)}
    merged = merge_schemas(old, new)
    assert "age" in merged
    assert merged["age"].nullable is True
    assert merged["age"].dtype == "integer"


def test_merge_field_only_in_new_added_as_nullable():
    old = {"name": s("string", False)}
    new = {"name": s("string", False), "address": s("object", False)}
    merged = merge_schemas(old, new)
    assert "address" in merged
    assert merged["address"].nullable is True
    assert merged["address"].dtype == "object"


def test_merge_both_empty():
    assert merge_schemas({}, {}) == {}


def test_merge_old_empty_takes_all_from_new():
    new = {"name": s("string", False), "age": s("integer", True)}
    merged = merge_schemas({}, new)
    assert set(merged) == {"name", "age"}
    # all fields from new, but marked nullable since they're "new"
    assert merged["name"].nullable is True


def test_merge_new_empty_keeps_old_as_nullable():
    old = {"name": s("string", False), "age": s("integer", False)}
    merged = merge_schemas(old, {})
    assert merged["name"].nullable is True
    assert merged["age"].nullable is True


def test_merge_preserves_datetime_type():
    old = {"ts": s("datetime", False)}
    new = {"ts": s("datetime", False)}
    merged = merge_schemas(old, new)
    assert merged["ts"].dtype == "datetime"


def test_merge_datetime_conflicts_with_string_widens_to_string():
    old = {"ts": s("datetime", False)}
    new = {"ts": s("string", False)}
    merged = merge_schemas(old, new)
    assert merged["ts"].dtype == "string"
