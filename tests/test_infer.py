import json
from pathlib import Path

import pytest

from nosql_delta_bridge.infer import FieldSchema, infer_schema

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
