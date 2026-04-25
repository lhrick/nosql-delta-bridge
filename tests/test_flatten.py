import json
from pathlib import Path

from nosql_delta_bridge.flatten import flatten_document, flatten_documents

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


# --- basic flattening ---

def test_flat_document_unchanged():
    doc = {"id": 1, "name": "Alice", "active": True}
    assert flatten_document(doc) == doc


def test_nested_object_flattened():
    doc = {"address": {"city": "SP", "zip": "01310"}}
    result = flatten_document(doc)
    assert result == {"address.city": "SP", "address.zip": "01310"}
    assert "address" not in result


def test_deeply_nested_object():
    doc = {"a": {"b": {"c": {"d": "deep"}}}}
    result = flatten_document(doc)
    assert result["a.b.c.d"] == "deep"


def test_null_value_preserved():
    doc = {"name": "Alice", "email": None}
    result = flatten_document(doc)
    assert result["email"] is None


# --- arrays ---

def test_array_of_scalars_kept_as_list():
    doc = {"tags": ["premium", "verified"]}
    result = flatten_document(doc)
    assert result["tags"] == ["premium", "verified"]


def test_array_of_dicts_kept_as_list():
    doc = {"items": [{"id": 1, "qty": 2}, {"id": 2, "qty": 1}]}
    result = flatten_document(doc)
    assert isinstance(result["items"], list)
    assert result["items"][0] == {"id": 1, "qty": 2}


def test_empty_array_preserved():
    doc = {"tags": []}
    result = flatten_document(doc)
    assert result["tags"] == []


def test_mixed_array_kept_as_list():
    doc = {"tags": ["premium", 42, ["vip", "earlybird"]]}
    result = flatten_document(doc)
    assert result["tags"] == ["premium", 42, ["vip", "earlybird"]]


# --- max_depth ---

def test_max_depth_stops_recursion():
    doc = {"a": {"b": {"c": "deep"}}}
    result = flatten_document(doc, max_depth=1)
    assert result == {"a": {"b": {"c": "deep"}}}


def test_max_depth_two():
    doc = {"a": {"b": {"c": "deep"}}}
    result = flatten_document(doc, max_depth=2)
    assert result == {"a.b": {"c": "deep"}}


def test_max_depth_full():
    doc = {"a": {"b": {"c": "deep"}}}
    result = flatten_document(doc, max_depth=5)
    assert result == {"a.b.c": "deep"}


# --- separator ---

def test_custom_separator():
    doc = {"address": {"city": "SP"}}
    result = flatten_document(doc, separator="__")
    assert "address__city" in result


# --- batch ---

def test_flatten_documents_returns_list():
    docs = [{"a": {"b": 1}}, {"a": {"b": 2}}]
    results = flatten_documents(docs)
    assert len(results) == 2
    assert results[0] == {"a.b": 1}
    assert results[1] == {"a.b": 2}


# --- fixtures ---

def test_users_fixture_address_flattened():
    docs = load("users.json")
    flat = flatten_document(docs[0])
    assert "address.city" in flat
    assert "address.street" in flat
    assert "address" not in flat


def test_users_fixture_deep_preferences_flattened():
    docs = load("users.json")
    # doc index 4 has preferences.notifications.email
    flat = flatten_document(docs[4])
    assert "preferences.notifications.email" in flat
    assert "preferences.notifications.sms" in flat
    assert flat["preferences.notifications.push"] == "only-deals"


def test_orders_fixture_nested_notes_flattened():
    docs = load("orders.json")
    # doc index 3 has notes.refund.amount
    flat = flatten_document(docs[3])
    assert "notes.refund.amount" in flat
    assert flat["notes.refund.processed"] is False


def test_orders_fixture_items_array_preserved():
    docs = load("orders.json")
    flat = flatten_document(docs[0])
    assert isinstance(flat["items"], list)
    assert flat["items"][0]["product_id"] == "p01"
