import json
from datetime import datetime
from pathlib import Path

import pytest

from nosql_delta_bridge.coerce import coerce_document
from nosql_delta_bridge.dlq import DeadLetterQueue, DLQEntry, DLQError
from nosql_delta_bridge.infer import FieldSchema


def schema_for(*fields: tuple[str, str, bool]) -> dict[str, FieldSchema]:
    return {path: FieldSchema(dtype=dtype, nullable=null) for path, dtype, null in fields}


# --- DLQEntry ---

def test_entry_failed_at_is_valid_iso_datetime():
    entry = DLQEntry(document={"id": 1}, reason="test", stage="coerce")
    assert entry.failed_at
    datetime.fromisoformat(entry.failed_at)  # raises if invalid


# --- append ---

def test_append_adds_to_buffer(tmp_path):
    dlq = DeadLetterQueue(tmp_path / "dlq.ndjson")
    assert len(dlq) == 0
    dlq.append({"id": 1}, reason="bad type", stage="coerce")
    assert len(dlq) == 1


def test_append_multiple(tmp_path):
    dlq = DeadLetterQueue(tmp_path / "dlq.ndjson")
    for i in range(3):
        dlq.append({"id": i}, reason="err", stage="coerce")
    assert len(dlq) == 3


# --- flush ---

def test_flush_writes_ndjson(tmp_path):
    path = tmp_path / "dlq.ndjson"
    dlq = DeadLetterQueue(path)
    dlq.append({"id": 1}, reason="cast failed", stage="coerce")
    count = dlq.flush()
    assert count == 1
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["document"] == {"id": 1}
    assert record["reason"] == "cast failed"
    assert record["stage"] == "coerce"
    assert "failed_at" in record


def test_flush_clears_buffer(tmp_path):
    dlq = DeadLetterQueue(tmp_path / "dlq.ndjson")
    dlq.append({"id": 1}, reason="err", stage="coerce")
    dlq.flush()
    assert len(dlq) == 0


def test_flush_empty_returns_zero(tmp_path):
    dlq = DeadLetterQueue(tmp_path / "dlq.ndjson")
    assert dlq.flush() == 0


def test_flush_empty_does_not_create_file(tmp_path):
    path = tmp_path / "dlq.ndjson"
    dlq = DeadLetterQueue(path)
    dlq.flush()
    assert not path.exists()


def test_flush_returns_entry_count(tmp_path):
    dlq = DeadLetterQueue(tmp_path / "dlq.ndjson")
    for i in range(5):
        dlq.append({"id": i}, reason="err", stage="flatten")
    assert dlq.flush() == 5


def test_flush_appends_across_calls(tmp_path):
    path = tmp_path / "dlq.ndjson"
    dlq = DeadLetterQueue(path)
    dlq.append({"id": 1}, reason="err1", stage="coerce")
    dlq.flush()
    dlq.append({"id": 2}, reason="err2", stage="coerce")
    dlq.flush()
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["document"]["id"] == 1
    assert json.loads(lines[1])["document"]["id"] == 2


def test_flush_creates_parent_directories(tmp_path):
    path = tmp_path / "nested" / "dir" / "dlq.ndjson"
    dlq = DeadLetterQueue(path)
    dlq.append({"id": 1}, reason="err", stage="infer")
    dlq.flush()
    assert path.exists()


def test_flush_each_line_is_valid_json(tmp_path):
    path = tmp_path / "dlq.ndjson"
    dlq = DeadLetterQueue(path)
    for i in range(3):
        dlq.append({"id": i, "nested": {"x": i}}, reason=f"err{i}", stage="coerce")
    dlq.flush()
    for line in path.read_text().splitlines():
        record = json.loads(line)
        assert "document" in record
        assert "reason" in record
        assert "stage" in record
        assert "failed_at" in record


# --- context manager ---

def test_context_manager_flushes_on_exit(tmp_path):
    path = tmp_path / "dlq.ndjson"
    with DeadLetterQueue(path) as dlq:
        dlq.append({"id": 1}, reason="err", stage="coerce")
    assert path.exists()
    assert len(path.read_text().splitlines()) == 1


def test_context_manager_flushes_on_exception(tmp_path):
    path = tmp_path / "dlq.ndjson"
    try:
        with DeadLetterQueue(path) as dlq:
            dlq.append({"id": 1}, reason="err", stage="coerce")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert path.exists()
    assert len(path.read_text().splitlines()) == 1


# --- integration: coerce rejection → dlq ---

def test_rejected_document_from_coerce_goes_to_dlq(tmp_path):
    doc = {"age": "not-a-number"}
    s = schema_for(("age", "integer", False))
    result = coerce_document(doc, s)
    assert result.rejected

    path = tmp_path / "dlq.ndjson"
    with DeadLetterQueue(path) as dlq:
        dlq.append(doc, reason=result.reject_reason, stage="coerce")

    record = json.loads(path.read_text().strip())
    assert record["stage"] == "coerce"
    assert "age" in record["reason"]
    assert record["document"] == doc


def test_non_nullable_null_rejection_goes_to_dlq(tmp_path):
    doc = {"name": None}
    s = schema_for(("name", "string", False))
    result = coerce_document(doc, s)
    assert result.rejected

    path = tmp_path / "dlq.ndjson"
    with DeadLetterQueue(path) as dlq:
        dlq.append(doc, reason=result.reject_reason, stage="coerce")

    record = json.loads(path.read_text().strip())
    assert "name" in record["reason"]
