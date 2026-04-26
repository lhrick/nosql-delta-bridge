from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from nosql_delta_bridge.infer import FieldSchema


class WriterError(Exception):
    pass


@dataclass
class WriterConfig:
    table_uri: str | Path
    source_collection: str
    mode: Literal["append", "overwrite"] = "append"
    storage_options: dict[str, str] | None = None


def write_batch(
    documents: list[dict[str, Any]],
    schema: dict[str, FieldSchema],
    config: WriterConfig,
) -> int:
    """Write a batch of coerced documents to a Delta Lake table.

    Adds audit columns (_ingested_at, _source_collection, _schema_version) to
    every row. Uses schema_mode='merge' so new fields are added to the table
    without breaking existing data.

    Returns the number of documents written. Does nothing and returns 0 if
    documents is empty (the Delta table is not created in that case).
    """
    if not documents:
        return 0

    ingested_at = datetime.now(timezone.utc)
    version = _schema_version(schema)

    enriched = [
        {
            **doc,
            "_ingested_at": ingested_at,
            "_source_collection": config.source_collection,
            "_schema_version": version,
        }
        for doc in documents
    ]

    arrow_table = _to_arrow(enriched, schema)

    conflicts = _type_conflicts(arrow_table.schema, config.table_uri, config.storage_options)
    if conflicts:
        conflict_list = "\n".join(f"  {c}" for c in conflicts)
        raise WriterError(
            f"type conflict with existing Delta table at {config.table_uri}:\n"
            f"{conflict_list}\n"
            f"The merged schema has widened a type that the table already stores "
            f"as a narrower type. Re-run 'bridge infer' on a combined batch and "
            f"use --mode overwrite to rewrite the table with the evolved schema."
        )

    try:
        write_deltalake(
            str(config.table_uri),
            arrow_table,
            mode=config.mode,
            schema_mode="merge",
            storage_options=config.storage_options,
        )
    except Exception as exc:
        raise WriterError(
            f"failed to write to Delta table at {config.table_uri}: {exc}"
        ) from exc

    return len(documents)


# --- internals ---

_DTYPE_TO_ARROW: dict[str, pa.DataType] = {
    "boolean":  pa.bool_(),
    "integer":  pa.int64(),
    "float":    pa.float64(),
    "string":   pa.string(),
    "datetime": pa.timestamp("us", tz="UTC"),
    "object":   pa.string(),  # serialized to JSON string
    "array":    pa.string(),  # serialized to JSON string
}

_AUDIT_FIELDS: list[tuple[str, pa.DataType]] = [
    ("_ingested_at",       pa.timestamp("us", tz="UTC")),
    ("_source_collection", pa.string()),
    ("_schema_version",    pa.string()),
]


def _type_conflicts(
    arrow_schema: pa.Schema,
    table_uri: str | Path,
    storage_options: dict[str, str] | None = None,
) -> list[str]:
    """Return descriptions of columns whose types differ from the existing Delta table."""
    try:
        existing = DeltaTable(str(table_uri), storage_options=storage_options).schema().to_arrow()
    except Exception:
        return []  # table doesn't exist yet — no conflicts

    conflicts = []
    for field in arrow_schema:
        try:
            existing_field = existing.field(field.name)
        except KeyError:
            continue  # new column, will be added via schema_mode="merge"
        if existing_field.type != field.type:
            conflicts.append(
                f"'{field.name}': table={existing_field.type}, incoming={field.type}"
            )
    return conflicts


def _schema_version(schema: dict[str, FieldSchema]) -> str:
    """8-char SHA256 of sorted field_path:dtype pairs. Changes when fields are added or types widen."""
    payload = ",".join(f"{k}:{v.dtype}" for k, v in sorted(schema.items()))
    return hashlib.sha256(payload.encode()).hexdigest()[:8]


def _build_arrow_schema(schema: dict[str, FieldSchema]) -> pa.Schema:
    fields = [
        pa.field(name, _DTYPE_TO_ARROW.get(fs.dtype, pa.string()), nullable=fs.nullable)
        for name, fs in schema.items()
    ]
    fields += [pa.field(name, arrow_type, nullable=False) for name, arrow_type in _AUDIT_FIELDS]
    return pa.schema(fields)


def _to_arrow(documents: list[dict[str, Any]], schema: dict[str, FieldSchema]) -> pa.Table:
    df = pd.DataFrame(documents)

    # serialize object/array values to JSON strings before Arrow conversion
    for name, fs in schema.items():
        if fs.dtype in ("object", "array") and name in df.columns:
            df[name] = df[name].apply(lambda v: json.dumps(v) if v is not None else None)

    # convert datetime columns to UTC-aware pandas timestamps for Arrow
    for name, fs in schema.items():
        if fs.dtype == "datetime" and name in df.columns:
            df[name] = pd.to_datetime(df[name], utc=True, errors="coerce")

    arrow_schema = _build_arrow_schema(schema)

    # align DataFrame columns to the schema: drop unknowns, fill missing with NaN
    expected_columns = [f.name for f in arrow_schema]
    df = df.reindex(columns=expected_columns)

    try:
        return pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise WriterError(f"failed to build Arrow table: {exc}") from exc
