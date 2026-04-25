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
    schema_from_dict,
    schema_to_dict,
)
from nosql_delta_bridge.writer import WriterConfig, WriterError, write_batch

app = typer.Typer(help="nosql-delta-bridge: ingest NoSQL documents into Delta Lake.")


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
    table_uri: Path = typer.Argument(..., help="Delta Lake table path"),
    schema_file: Optional[Path] = typer.Option(
        None, "--schema",
        help="Pre-inferred schema file (from 'bridge infer'). If omitted, schema is inferred from the input batch.",
    ),
    collection: Optional[str] = typer.Option(
        None, "--collection", "-c",
        help="Source collection name written to _source_collection (defaults to filename stem)",
    ),
    dlq_path: Path = typer.Option(
        "dlq.ndjson", "--dlq",
        help="Dead letter queue output file for rejected documents",
    ),
    detect_datetimes: bool = typer.Option(
        False, "--detect-datetimes",
        help="Attempt ISO 8601 datetime detection (ignored when --schema is provided)",
    ),
    mode: str = typer.Option(
        "append", "--mode",
        help="Write mode: append (default) or overwrite",
    ),
) -> None:
    """Ingest a JSON file into a Delta Lake table.

    Runs the full pipeline: schema → flatten → coerce → write.
    Rejected documents are written to the DLQ file instead of being dropped.

    Pass --schema to coerce against a fixed reference schema so that documents
    violating it are rejected to the DLQ rather than silently widening the schema.
    """
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
    if schema_file is not None:
        try:
            schema: dict[str, FieldSchema] = schema_from_dict(
                json.loads(schema_file.read_text(encoding="utf-8"))
            )
        except FileNotFoundError:
            typer.echo(f"error: schema file not found: {schema_file}", err=True)
            raise typer.Exit(1)
        except (json.JSONDecodeError, KeyError) as exc:
            typer.echo(f"error: invalid schema file {schema_file}: {exc}", err=True)
            raise typer.Exit(1)
        schema_label = f"schema from {schema_file.name} ({len(schema)} fields)"
    else:
        schema = infer_schema(raw, InferConfig(detect_datetimes=detect_datetimes))
        schema_label = f"{len(schema)} fields inferred"

    typer.echo(f"{input_file.name}  ·  {len(raw)} documents  ·  {schema_label}")

    # --- flatten + coerce + route ---
    coerce_cfg = CoerceConfig()
    good: list = []
    rejected_count = 0

    with DeadLetterQueue(dlq_path) as dlq:
        for doc in raw:
            flat = flatten_document(doc)
            result = coerce_document(flat, schema, coerce_cfg)
            if result.rejected:
                dlq.append(doc, reason=result.reject_reason, stage="coerce")
                rejected_count += 1
            else:
                good.append(result.document)

    # --- write ---
    if good:
        writer_cfg = WriterConfig(table_uri=table_uri, source_collection=source, mode=mode)
        try:
            written = write_batch(good, schema, writer_cfg)
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
