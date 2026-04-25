import json
from pathlib import Path

import pandas as pd
import pytest
from deltalake import DeltaTable

from nosql_delta_bridge.coerce import CoerceConfig, coerce_document
from nosql_delta_bridge.flatten import flatten_document
from nosql_delta_bridge.infer import FieldSchema, infer_schema
from nosql_delta_bridge.writer import WriterConfig, WriterError, _schema_version, write_batch

FIXTURES = Path(__file__).parent / "fixtures"


def schema_for(*fields: tuple[str, str, bool]) -> dict[str, FieldSchema]:
    return {path: FieldSchema(dtype=dtype, nullable=null) for path, dtype, null in fields}


# --- empty batch ---

def test_empty_batch_returns_zero(tmp_path):
    config = WriterConfig(table_uri=tmp_path / "table", source_collection="users")
    assert write_batch([], schema_for(("name", "string", False)), config) == 0


def test_empty_batch_does_not_create_table(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    write_batch([], schema_for(("name", "string", False)), config)
    assert not path.exists()


# --- basic write ---

def test_returns_document_count(tmp_path):
    config = WriterConfig(table_uri=tmp_path / "table", source_collection="users")
    schema = schema_for(("name", "string", False), ("age", "integer", False))
    docs = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    assert write_batch(docs, schema, config) == 2


def test_written_rows_match_input(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    schema = schema_for(("name", "string", False), ("age", "integer", False))
    docs = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    write_batch(docs, schema, config)

    df = DeltaTable(str(path)).to_pandas()
    assert len(df) == 2
    assert set(df["name"].tolist()) == {"Alice", "Bob"}


# --- audit columns ---

def test_audit_columns_present(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="orders")
    write_batch([{"id": 1}], schema_for(("id", "integer", False)), config)

    df = DeltaTable(str(path)).to_pandas()
    assert "_ingested_at" in df.columns
    assert "_source_collection" in df.columns
    assert "_schema_version" in df.columns


def test_source_collection_value(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="my_collection")
    write_batch([{"id": 1}], schema_for(("id", "integer", False)), config)

    df = DeltaTable(str(path)).to_pandas()
    assert df["_source_collection"].iloc[0] == "my_collection"


def test_schema_version_is_eight_char_hex(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    write_batch([{"name": "Alice"}], schema_for(("name", "string", False)), config)

    df = DeltaTable(str(path)).to_pandas()
    version = df["_schema_version"].iloc[0]
    assert len(version) == 8
    int(version, 16)  # raises ValueError if not valid hex


# --- _schema_version ---

def test_schema_version_is_stable():
    s = schema_for(("name", "string", False), ("age", "integer", True))
    assert _schema_version(s) == _schema_version(s)


def test_schema_version_changes_when_field_added():
    s1 = schema_for(("name", "string", False))
    s2 = schema_for(("name", "string", False), ("age", "integer", False))
    assert _schema_version(s1) != _schema_version(s2)


def test_schema_version_changes_when_dtype_widens():
    s1 = schema_for(("age", "integer", False))
    s2 = schema_for(("age", "string", False))
    assert _schema_version(s1) != _schema_version(s2)


# --- append mode ---

def test_append_accumulates_rows(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    schema = schema_for(("name", "string", False))
    write_batch([{"name": "Alice"}], schema, config)
    write_batch([{"name": "Bob"}], schema, config)

    df = DeltaTable(str(path)).to_pandas()
    assert len(df) == 2


# --- schema evolution ---

def test_new_field_merged_not_breaking(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")

    schema_v1 = schema_for(("name", "string", False))
    write_batch([{"name": "Alice"}], schema_v1, config)

    schema_v2 = schema_for(("name", "string", False), ("age", "integer", True))
    write_batch([{"name": "Bob", "age": 30}], schema_v2, config)

    df = DeltaTable(str(path)).to_pandas()
    assert len(df) == 2
    assert "age" in df.columns
    alice_age = df.loc[df["name"] == "Alice", "age"].iloc[0]
    assert pd.isna(alice_age)


# --- object/array serialization ---

def test_array_field_serialized_to_json_string(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    schema = schema_for(("name", "string", False), ("tags", "array", True))
    write_batch([{"name": "Alice", "tags": ["x", "y"]}], schema, config)

    df = DeltaTable(str(path)).to_pandas()
    assert json.loads(df["tags"].iloc[0]) == ["x", "y"]


def test_object_field_serialized_to_json_string(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    schema = schema_for(("name", "string", False), ("meta", "object", True))
    write_batch([{"name": "Alice", "meta": {"role": "admin"}}], schema, config)

    df = DeltaTable(str(path)).to_pandas()
    assert json.loads(df["meta"].iloc[0]) == {"role": "admin"}


def test_null_array_field_stays_null(tmp_path):
    path = tmp_path / "table"
    config = WriterConfig(table_uri=path, source_collection="users")
    schema = schema_for(("name", "string", False), ("tags", "array", True))
    write_batch([{"name": "Alice", "tags": None}], schema, config)

    df = DeltaTable(str(path)).to_pandas()
    assert pd.isna(df["tags"].iloc[0])


# --- integration: infer + flatten + coerce + write ---

def test_full_pipeline_users_fixture(tmp_path):
    raw_docs = json.loads((FIXTURES / "users.json").read_text())
    schema = infer_schema(raw_docs)
    results = [coerce_document(flatten_document(d), schema, CoerceConfig()) for d in raw_docs]
    good = [r.document for r in results if not r.rejected]

    path = tmp_path / "users"
    config = WriterConfig(table_uri=path, source_collection="users")
    count = write_batch(good, schema, config)

    assert count == len(good)
    df = DeltaTable(str(path)).to_pandas()
    assert len(df) == count
    assert set(df.columns) >= {"_ingested_at", "_source_collection", "_schema_version"}
