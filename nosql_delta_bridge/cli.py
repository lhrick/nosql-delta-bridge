from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from nosql_delta_bridge.coerce import CoerceConfig, coerce_document
from nosql_delta_bridge.dlq import DeadLetterQueue
from nosql_delta_bridge.flatten import flatten_document
from nosql_delta_bridge.infer import InferConfig, infer_schema
from nosql_delta_bridge.writer import WriterConfig, WriterError, write_batch

app = typer.Typer(help="nosql-delta-bridge: ingest NoSQL documents into Delta Lake.")


@app.command()
def ingest(
    input_file: Path = typer.Argument(..., help="JSON file containing an array of documents"),
    table_uri: Path = typer.Argument(..., help="Delta Lake table path"),
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
        help="Attempt ISO 8601 datetime detection in string fields",
    ),
    mode: str = typer.Option(
        "append", "--mode",
        help="Write mode: append (default) or overwrite",
    ),
) -> None:
    """Ingest a JSON file into a Delta Lake table.

    Runs the full pipeline: infer schema → flatten → coerce → write.
    Rejected documents are written to the DLQ file instead of being dropped.
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

    # --- infer ---
    schema = infer_schema(raw, InferConfig(detect_datetimes=detect_datetimes))

    typer.echo(f"{input_file.name}  ·  {len(raw)} documents  ·  {len(schema)} fields inferred")

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
