from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from nosql_delta_bridge.coerce import CoerceConfig, coerce_document
from nosql_delta_bridge.dlq import DeadLetterQueue
from nosql_delta_bridge.flatten import flatten_document
from nosql_delta_bridge.infer import (
    InferConfig,
    FieldSchema,
    infer_schema,
    merge_schemas,
    schema_from_dict,
    schema_to_dict,
)
from nosql_delta_bridge.writer import WriterConfig, WriterError, write_batch

app = typer.Typer(help="nosql-delta-bridge: ingest NoSQL documents into Delta Lake.")


def _parse_storage_options(options: list[str] | None) -> dict[str, str] | None:
    if not options:
        return None
    result: dict[str, str] = {}
    for opt in options:
        if "=" not in opt:
            typer.echo(f"error: --storage-option must be KEY=VALUE, got: {opt!r}", err=True)
            raise typer.Exit(1)
        key, _, value = opt.partition("=")
        if not key:
            typer.echo(f"error: --storage-option key cannot be empty, got: {opt!r}", err=True)
            raise typer.Exit(1)
        result[key] = value
    return result


@app.command()
def infer(
    input_file: Path = typer.Argument(..., help="JSON file to infer schema from"),
    output: Path = typer.Option(
        None, "--output", "-o",
        help="Output schema file (default: <input_stem>.schema.json)",
    ),
    detect_datetimes: bool = typer.Option(
        False, "--detect-datetimes",
        help="Attempt ISO 8601 datetime detection in string fields",
    ),
) -> None:
    """Infer a schema from a JSON file and save it for reuse.

    Use the saved schema with 'bridge ingest --schema' to coerce future
    documents against a fixed reference — enabling real DLQ rejections when
    new data violates the established schema.
    """
    try:
        raw: list = json.loads(input_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        typer.echo(f"error: file not found: {input_file}", err=True)
        raise typer.Exit(1)
    except json.JSONDecodeError as exc:
        typer.echo(f"error: invalid JSON in {input_file}: {exc}", err=True)
        raise typer.Exit(1)

    if not isinstance(raw, list):
        typer.echo(f"error: {input_file} must contain a JSON array of documents", err=True)
        raise typer.Exit(1)

    if not raw:
        typer.echo(f"warning: {input_file} contains no documents, nothing to infer")
        return

    schema = infer_schema(raw, InferConfig(detect_datetimes=detect_datetimes))

    out_path = output or input_file.with_suffix(".schema.json")
    out_path.write_text(json.dumps(schema_to_dict(schema), indent=2), encoding="utf-8")

    typer.echo(f"{input_file.name}  ·  {len(raw)} documents  ·  {len(schema)} fields")
    typer.echo(f"  schema written  →  {out_path}")


@app.command()
def ingest(
    input_file: Path = typer.Argument(..., help="JSON file containing an array of documents"),
    table_uri: str = typer.Argument(..., help="Delta Lake table path or URI (e.g. s3://bucket/path)"),
    schema_file: Optional[Path] = typer.Option(
        None, "--schema",
        help="Pre-inferred schema file (from 'bridge infer'). If omitted, schema is inferred from the input batch.",
    ),
    collection: Optional[str] = typer.Option(
        None, "--collection", "-c",
        help="Source collection name written to _source_collection (defaults to filename stem)",
    ),
    dlq_path: str = typer.Option(
        "dlq.ndjson", "--dlq",
        help="Dead letter queue path or URI (e.g. s3://bucket/dlq/failed.ndjson)",
    ),
    detect_datetimes: bool = typer.Option(
        False, "--detect-datetimes",
        help="Attempt ISO 8601 datetime detection (ignored when --schema is provided)",
    ),
    mode: str = typer.Option(
        "append", "--mode",
        help="Write mode: append (default) or overwrite",
    ),
    storage_option: Optional[list[str]] = typer.Option(
        None, "--storage-option",
        help="Storage credential as KEY=VALUE (repeatable). E.g. --storage-option AWS_ACCESS_KEY_ID=abc",
    ),
) -> None:
    """Ingest a JSON file into a Delta Lake table.

    Runs the full pipeline: schema → flatten → coerce → write.
    Rejected documents are written to the DLQ file instead of being dropped.

    Pass --schema to coerce against a fixed reference schema so that documents
    violating it are rejected to the DLQ rather than silently widening the schema.
    """
    # --- storage options ---
    storage_options = _parse_storage_options(storage_option)

    # --- load ---
    try:
        raw: list = json.loads(input_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        typer.echo(f"error: file not found: {input_file}", err=True)
        raise typer.Exit(1)
    except json.JSONDecodeError as exc:
        typer.echo(f"error: invalid JSON in {input_file}: {exc}", err=True)
        raise typer.Exit(1)

    if not isinstance(raw, list):
        typer.echo(f"error: {input_file} must contain a JSON array of documents", err=True)
        raise typer.Exit(1)

    if not raw:
        typer.echo(f"warning: {input_file} contains no documents, nothing to do")
        return

    source = collection or input_file.stem

    # --- schema ---
    infer_cfg = InferConfig(detect_datetimes=detect_datetimes)
    batch_schema = infer_schema(raw, infer_cfg)

    if schema_file is not None:
        try:
            old_schema: dict[str, FieldSchema] = schema_from_dict(
                json.loads(schema_file.read_text(encoding="utf-8"))
            )
        except FileNotFoundError:
            typer.echo(f"error: schema file not found: {schema_file}", err=True)
            raise typer.Exit(1)
        except (json.JSONDecodeError, KeyError) as exc:
            typer.echo(f"error: invalid schema file {schema_file}: {exc}", err=True)
            raise typer.Exit(1)

        # coerce_schema: old schema only — enforces existing field contracts strictly
        coerce_schema = old_schema

        # write_schema: old fields (types preserved) + new fields from batch (nullable)
        # type widening of existing columns is never applied automatically
        merged = merge_schemas(old_schema, batch_schema)
        write_schema: dict[str, FieldSchema] = {
            key: (
                FieldSchema(dtype=old_schema[key].dtype, nullable=merged[key].nullable)
                if key in old_schema
                else merged[key]
            )
            for key in merged
        }

        new_fields = set(write_schema) - set(old_schema)
        widened = [
            k for k in old_schema
            if k in batch_schema and old_schema[k].dtype != batch_schema[k].dtype
        ]
        schema_label = f"schema from {schema_file.name} ({len(write_schema)} fields)"
    else:
        coerce_schema = batch_schema
        write_schema = batch_schema
        old_schema = None
        new_fields = set()
        widened = []
        schema_label = f"{len(batch_schema)} fields inferred"

    typer.echo(f"{input_file.name}  ·  {len(raw)} documents  ·  {schema_label}")

    # --- flatten + coerce + route ---
    coerce_cfg = CoerceConfig()
    good: list = []
    rejected_count = 0

    with DeadLetterQueue(dlq_path, storage_options=storage_options) as dlq:
        for doc in raw:
            flat = flatten_document(doc)
            result = coerce_document(flat, coerce_schema, coerce_cfg)
            if result.rejected:
                dlq.append(doc, reason=result.reject_reason, stage="coerce")
                rejected_count += 1
            else:
                good.append(result.document)

    # --- write ---
    if good:
        writer_cfg = WriterConfig(table_uri=table_uri, source_collection=source, mode=mode, storage_options=storage_options)
        try:
            written = write_batch(good, write_schema, writer_cfg)
        except WriterError as exc:
            typer.echo(f"error: {exc}", err=True)
            raise typer.Exit(1)
        typer.echo(f"  written:   {written}  →  {table_uri}")
    else:
        typer.echo("  written:   0")

    if rejected_count:
        typer.echo(f"  rejected:  {rejected_count}  →  {dlq_path}")
    else:
        typer.echo("  rejected:  0")

    if schema_file is not None:
        if new_fields:
            # preserve old fields exactly — only append the new ones as nullable
            evolved_schema = {**old_schema, **{k: write_schema[k] for k in new_fields}}
            schema_file.write_text(
                json.dumps(schema_to_dict(evolved_schema), indent=2), encoding="utf-8"
            )
            typer.echo(
                f"  schema evolved: +{len(new_fields)} field(s) "
                f"{sorted(new_fields)}  →  {schema_file}"
            )
        if widened:
            typer.echo(f"  warning: type widening detected on fields {widened}")
            typer.echo(
                f"  to apply: re-run 'bridge infer' on a combined batch "
                f"and use --mode overwrite"
            )
