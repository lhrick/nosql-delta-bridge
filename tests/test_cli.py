import json
from pathlib import Path

import pytest
from deltalake import DeltaTable
from typer.testing import CliRunner

from nosql_delta_bridge.cli import app

FIXTURES = Path(__file__).parent / "fixtures"
runner = CliRunner()


# --- happy path ---

def test_ingest_users_fixture(tmp_path):
    result = runner.invoke(app, [str(FIXTURES / "users.json"), str(tmp_path / "table")])
    assert result.exit_code == 0
    assert "written" in result.output
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert len(df) > 0
    assert "_ingested_at" in df.columns
    assert "_source_collection" in df.columns


def test_ingest_orders_fixture(tmp_path):
    result = runner.invoke(app, [str(FIXTURES / "orders.json"), str(tmp_path / "table")])
    assert result.exit_code == 0
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert len(df) > 0


def test_collection_defaults_to_filename_stem(tmp_path):
    runner.invoke(app, [str(FIXTURES / "users.json"), str(tmp_path / "table")])
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert (df["_source_collection"] == "users").all()


def test_collection_option_overrides_stem(tmp_path):
    runner.invoke(app, [
        str(FIXTURES / "users.json"),
        str(tmp_path / "table"),
        "--collection", "mongo_users",
    ])
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert (df["_source_collection"] == "mongo_users").all()


def test_output_shows_written_and_rejected_counts(tmp_path):
    result = runner.invoke(app, [str(FIXTURES / "users.json"), str(tmp_path / "table")])
    assert "written" in result.output
    assert "rejected" in result.output


# --- rejections go to DLQ ---

def test_dlq_option_accepted_and_reports_zero_rejected(tmp_path):
    # When infer runs on the full batch, the schema accommodates every value
    # by widening types and marking nullable fields, so coerce never rejects.
    # DLQ population requires a pre-defined schema from a separate reference
    # batch — not supported by the CLI yet. This test verifies the --dlq
    # option is accepted and the output reports the correct count.
    docs = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))
    dlq_file = tmp_path / "dlq.ndjson"

    result = runner.invoke(app, [
        str(input_file),
        str(tmp_path / "table"),
        "--dlq", str(dlq_file),
    ])
    assert result.exit_code == 0
    assert "rejected:  0" in result.output
    assert not dlq_file.exists()


def test_no_dlq_file_when_no_rejections(tmp_path):
    docs = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))
    dlq_file = tmp_path / "dlq.ndjson"

    runner.invoke(app, [str(input_file), str(tmp_path / "table"), "--dlq", str(dlq_file)])
    assert not dlq_file.exists()


# --- error handling ---

def test_missing_file_exits_with_error(tmp_path):
    result = runner.invoke(app, [str(tmp_path / "nonexistent.json"), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_invalid_json_exits_with_error(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json }")
    result = runner.invoke(app, [str(bad), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_non_array_json_exits_with_error(tmp_path):
    obj = tmp_path / "obj.json"
    obj.write_text('{"key": "value"}')
    result = runner.invoke(app, [str(obj), str(tmp_path / "table")])
    assert result.exit_code == 1


def test_empty_array_exits_cleanly(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text("[]")
    result = runner.invoke(app, [str(empty), str(tmp_path / "table")])
    assert result.exit_code == 0
    assert not (tmp_path / "table").exists()


# --- options ---

def test_detect_datetimes_flag(tmp_path):
    docs = [{"name": "Alice", "created_at": "2024-01-15T10:00:00Z"}]
    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps(docs))

    result = runner.invoke(app, [
        str(input_file),
        str(tmp_path / "table"),
        "--detect-datetimes",
    ])
    assert result.exit_code == 0
    df = DeltaTable(str(tmp_path / "table")).to_pandas()
    assert hasattr(df["created_at"].dtype, "tz")


def test_overwrite_mode_replaces_data(tmp_path):
    docs_v1 = [{"name": "Alice"}]
    docs_v2 = [{"name": "Bob"}, {"name": "Carol"}]
    input_v1 = tmp_path / "v1.json"
    input_v2 = tmp_path / "v2.json"
    input_v1.write_text(json.dumps(docs_v1))
    input_v2.write_text(json.dumps(docs_v2))
    table = str(tmp_path / "table")

    runner.invoke(app, [str(input_v1), table])
    runner.invoke(app, [str(input_v2), table, "--mode", "overwrite"])

    df = DeltaTable(table).to_pandas()
    assert len(df) == 2
    assert set(df["name"].tolist()) == {"Bob", "Carol"}
