import json
from pathlib import Path

import pytest
from deltalake import DeltaTable
from typer.testing import CliRunner

from nosql_delta_bridge.cli import app

FIXTURES = Path(__file__).parent / "fixtures"
runner = CliRunner()


# ── bridge ingest (happy path) ────────────────────────────────────────────────

def test_ingest_users_fixture(tmp_path):
    result = runner.invoke(app, ["ingest", str(FIXTURES / "users.json"), str(tmp_path / "table")])
    assert result.exit_code == 0
    assert "written" in result.output
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert len(df) > 0
    assert "_ingested_at" in df.columns
    assert "_source_collection" in df.columns


def test_ingest_orders_fixture(tmp_path):
    result = runner.invoke(app, ["ingest", str(FIXTURES / "orders.json"), str(tmp_path / "table")])
    assert result.exit_code == 0
    assert len(DeltaTable(str(tmp_path / "table")).to_pandas()) > 0


def test_collection_defaults_to_filename_stem(tmp_path):
    runner.invoke(app, ["ingest", str(FIXTURES / "users.json"), str(tmp_path / "table")])
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert (df["_source_collection"] == "users").all()


def test_collection_option_overrides_stem(tmp_path):
    runner.invoke(app, [
        "ingest", str(FIXTURES / "users.json"), str(tmp_path / "table"),
        "--collection", "mongo_users",
    ])
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert (df["_source_collection"] == "mongo_users").all()


def test_output_shows_written_and_rejected_counts(tmp_path):
    result = runner.invoke(app, ["ingest", str(FIXTURES / "users.json"), str(tmp_path / "table")])
    assert "written" in result.output
    assert "rejected" in result.output


# ── bridge ingest (DLQ) ───────────────────────────────────────────────────────

def test_dlq_option_accepted_and_reports_zero_rejected(tmp_path):
    # When schema is inferred from the full batch, coerce never rejects because
    # infer accommodates every value by widening types and marking nullable fields.
    docs = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))
    dlq_file = tmp_path / "dlq.ndjson"

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"), "--dlq", str(dlq_file),
    ])
    assert result.exit_code == 0
    assert "rejected:  0" in result.output
    assert not dlq_file.exists()


def test_no_dlq_file_when_no_rejections(tmp_path):
    docs = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))
    dlq_file = tmp_path / "dlq.ndjson"

    runner.invoke(app, ["ingest", str(input_file), str(tmp_path / "table"), "--dlq", str(dlq_file)])
    assert not dlq_file.exists()


# ── bridge ingest (error handling) ───────────────────────────────────────────

def test_missing_file_exits_with_error(tmp_path):
    result = runner.invoke(app, ["ingest", str(tmp_path / "nope.json"), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_invalid_json_exits_with_error(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json }")
    result = runner.invoke(app, ["ingest", str(bad), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_non_array_json_exits_with_error(tmp_path):
    obj = tmp_path / "obj.json"
    obj.write_text('{"key": "value"}')
    result = runner.invoke(app, ["ingest", str(obj), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_empty_array_exits_cleanly(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text("[]")
    result = runner.invoke(app, ["ingest", str(empty), str(tmp_path / "table")])
    assert result.exit_code == 0
    assert not (tmp_path / "table").exists()


# ── bridge ingest (options) ───────────────────────────────────────────────────

def test_detect_datetimes_flag(tmp_path):
    docs = [{"name": "Alice", "created_at": "2024-01-15T10:00:00Z"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"), "--detect-datetimes",
    ])
    assert result.exit_code == 0
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert hasattr(df["created_at"].dtype, "tz")


def test_overwrite_mode_replaces_data(tmp_path):
    input_v1 = tmp_path / "v1.json"
    input_v2 = tmp_path / "v2.json"
    input_v1.write_text(json.dumps([{"name": "Alice"}]))
    input_v2.write_text(json.dumps([{"name": "Bob"}, {"name": "Carol"}]))
    table = str(tmp_path / "table")

    runner.invoke(app, ["ingest", str(input_v1), table])
    runner.invoke(app, ["ingest", str(input_v2), table, "--mode", "overwrite"])

    df = DeltaTable(table).to_pandas()
    assert len(df) == 2
    assert set(df["name"].tolist()) == {"Bob", "Carol"}


# ── bridge infer ──────────────────────────────────────────────────────────────

def test_infer_creates_schema_file(tmp_path):
    result = runner.invoke(app, ["infer", str(FIXTURES / "users.json")])
    assert result.exit_code == 0
    schema_path = FIXTURES / "users.schema.json"
    assert schema_path.exists()
    schema_path.unlink()  # clean up


def test_infer_output_option(tmp_path):
    out = tmp_path / "my_schema.json"
    result = runner.invoke(app, ["infer", str(FIXTURES / "users.json"), "--output", str(out)])
    assert result.exit_code == 0
    assert out.exists()
    schema = json.loads(out.read_text())
    assert "name" in schema
    assert "dtype" in schema["name"]
    assert "nullable" in schema["name"]


def test_infer_schema_content(tmp_path):
    out = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(FIXTURES / "users.json"), "--output", str(out)])
    schema = json.loads(out.read_text())
    # users.json has age as string (widened from int + "25") and nullable
    assert schema["age"]["dtype"] == "string"
    assert schema["age"]["nullable"] is True


def test_infer_detect_datetimes_flag(tmp_path):
    docs = [{"name": "Alice", "ts": "2024-01-15T10:00:00Z"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))
    out = tmp_path / "schema.json"

    runner.invoke(app, ["infer", str(input_file), "--output", str(out), "--detect-datetimes"])
    schema = json.loads(out.read_text())
    assert schema["ts"]["dtype"] == "datetime"


def test_infer_missing_file_exits_with_error(tmp_path):
    result = runner.invoke(app, ["infer", str(tmp_path / "nope.json")])
    assert result.exit_code == 1


def test_infer_empty_array_exits_cleanly(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text("[]")
    result = runner.invoke(app, ["infer", str(empty)])
    assert result.exit_code == 0


# ── bridge ingest --schema (real DLQ rejections) ─────────────────────────────

def test_ingest_with_schema_rejects_type_violation(tmp_path):
    # Step 1: infer schema from clean reference data
    reference = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])
    assert schema_file.exists()

    # Step 2: ingest dirty docs against the fixed schema
    # "not-a-number" fails CAST to integer → rejected
    dirty = [
        {"name": "Carol", "age": 28},          # clean → written
        {"name": "Dave",  "age": "not-a-number"},  # cast fails → rejected
    ]
    dirty_file = tmp_path / "dirty.json"
    dirty_file.write_text(json.dumps(dirty))
    dlq_file = tmp_path / "dlq.ndjson"

    result = runner.invoke(app, [
        "ingest", str(dirty_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
        "--dlq", str(dlq_file),
    ])
    assert result.exit_code == 0
    assert "written:   1" in result.output
    assert "rejected:  1" in result.output
    assert dlq_file.exists()

    record = json.loads(dlq_file.read_text().strip())
    assert record["document"]["name"] == "Dave"
    assert "age" in record["reason"]


def test_ingest_with_schema_rejects_null_on_non_nullable(tmp_path):
    reference = [{"name": "Alice"}, {"name": "Bob"}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])

    dirty = [{"name": "Carol"}, {"name": None}]  # None on non-nullable → rejected
    dirty_file = tmp_path / "dirty.json"
    dirty_file.write_text(json.dumps(dirty))
    dlq_file = tmp_path / "dlq.ndjson"

    result = runner.invoke(app, [
        "ingest", str(dirty_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
        "--dlq", str(dlq_file),
    ])
    assert result.exit_code == 0
    assert "written:   1" in result.output
    assert "rejected:  1" in result.output


def test_ingest_missing_schema_file_exits_with_error(tmp_path):
    docs = [{"name": "Alice"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"),
        "--schema", str(tmp_path / "nope.schema.json"),
    ])
    assert result.exit_code == 1


def test_ingest_schema_evolves_with_new_field(tmp_path):
    # reference batch: no address field
    reference = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])

    # incoming batch: new documents include address
    incoming = [
        {"name": "Carol", "age": 28},
        {"name": "Dave",  "age": 33, "address": {"city": "SP", "zip": "01310"}},
    ]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(incoming))

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
    ])
    assert result.exit_code == 0
    assert "schema evolved" in result.output
    assert "address" in result.output

    # schema file must be updated with the new field
    updated = json.loads(schema_file.read_text())
    assert "address" in updated
    assert updated["address"]["nullable"] is True


def test_ingest_schema_file_unchanged_when_nothing_new(tmp_path):
    reference = [{"name": "Alice", "age": 30}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])

    original_content = schema_file.read_text()

    incoming = [{"name": "Bob", "age": 25}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(incoming))
    runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
    ])

    assert schema_file.read_text() == original_content


def test_ingest_warns_on_type_widening(tmp_path):
    reference = [{"name": "Alice", "age": 30}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])

    # incoming has age as string in some docs → batch_schema widens age to string
    incoming = [{"name": "Bob", "age": 25}, {"name": "Carol", "age": "twenty-eight"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(incoming))

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
    ])
    assert result.exit_code == 0
    assert "warning" in result.output
    assert "age" in result.output
    # age widening is detected but NOT applied to schema file
    schema = json.loads(schema_file.read_text())
    assert schema["age"]["dtype"] == "integer"


def test_ingest_output_shows_schema_source(tmp_path):
    reference = [{"name": "Alice"}]
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(json.dumps(reference))
    schema_file = tmp_path / "schema.json"
    runner.invoke(app, ["infer", str(ref_file), "--output", str(schema_file)])

    incoming = [{"name": "Bob"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(incoming))

    result = runner.invoke(app, [
        "ingest", str(input_file), str(tmp_path / "table"),
        "--schema", str(schema_file),
    ])
    assert "schema from" in result.output
    assert "schema.json" in result.output
