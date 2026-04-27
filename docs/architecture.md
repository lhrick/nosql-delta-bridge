# Architecture

## Overview

`nosql-delta-bridge` is a sequential pipeline of five modules. Data flows in one direction: raw documents enter at `infer`, and either land in the Delta table via `writer` or are routed to the dead-letter queue via `dlq`. Nothing is silently dropped at any stage.

```
raw JSON documents
        │
        ▼
┌───────────────┐
│    infer.py   │  Examines a batch and infers a unified schema.
│               │  Conflict resolution: widest type wins, all fields
└───────┬───────┘  nullable by default.
        │  dict[str, FieldSchema]
        ▼
┌───────────────┐
│   flatten.py  │  Flattens nested objects into dot-notation columns.
│               │  {"address": {"city": "SP"}} → {"address.city": "SP"}
└───────┬───────┘  Only leaf fields. Arrays kept as lists.
        │  flat documents
        ▼
┌───────────────┐
│   coerce.py   │  Applies per-field type coercion against the schema.
│               │  Cast silently when possible. Reject to DLQ otherwise.
└───────┬───────┘
        │                          │
   typed docs                 rejected docs
        │                          │
        ▼                          ▼
┌───────────────┐        ┌─────────────────────┐
│   writer.py   │        │       dlq.py         │
│               │        │                      │
│  Delta Lake   │        │  NDJSON (local or    │
│  via delta-rs │        │  S3 / Azure / GCS)   │
└───────────────┘        └─────────────────────┘
```

---

## Modules

### `infer.py` — Schema Inference

Examines a batch of raw documents and produces a `dict[str, FieldSchema]` where every key is a dot-notation field path.

**Type widening order** (narrowest → widest):
```
boolean < integer < float < string
```
Incompatible types (e.g. `object` vs `integer`) widen to `string`. `datetime` conflicts with any other type and also widens to `string`.

**Nullable rules:**
- A field missing from any document in the batch → `nullable=True`
- A field explicitly `null` in any document → `nullable=True`
- A field present and non-null in all documents → `nullable=False`

**Nested objects** are expanded recursively. Only leaf fields (non-dict values) produce schema entries — parent paths are excluded. `{"address": {"city": "SP"}}` produces `address.city`, not `address`.

**`merge_schemas(old, new)`** combines two schemas monotonically:
- Field in both → widen dtype; `nullable = old.nullable OR new.nullable`
- Field only in old → keep dtype, mark `nullable=True`
- Field only in new → add dtype, mark `nullable=True`

Types only ever widen — the merged schema is always a superset of both inputs.

---

### `flatten.py` — Document Flattening

Converts nested documents into a flat dict with dot-notation keys before coercion and write.

**Parameters:**
- `max_depth` (default: `5`) — recursion limit. Beyond the limit, the remaining sub-object is kept as an opaque value in a single column.
- `separator` (default: `.`) — key segment joiner.

**Arrays** are never recursed into. They are kept as Python lists and stored as list columns in Delta Lake. Exploding arrays (one document → many rows) is a deliberate transform that changes document cardinality — not a side effect of flattening.

---

### `coerce.py` — Type Coercion

Applies the schema to each flat document field by field. Three outcomes per field:

| Outcome | Condition | Result |
|---|---|---|
| Cast silently | Value can be losslessly converted | Document passes with coerced value |
| Reject | Value cannot be converted, or null on non-nullable field | Document goes to DLQ with reason |
| Pass through | Field not in schema (new field) | Value kept as-is |

**Lossless cast rules:**
- `"25"` → integer: `int("25") = 25` ✓
- `"twenty-five"` → integer: raises `ValueError` → reject
- `30.0` → integer: `float.is_integer() = True` → cast to 30 ✓
- `25.7` → integer: `float.is_integer() = False` → reject (lossy)

The coerce stage runs against a fixed schema — never the schema inferred from the current batch. This is what makes the DLQ meaningful: a schema inferred from the same batch would accommodate every value in it.

---

### `writer.py` — Delta Lake Writer

Writes typed documents to a Delta Lake table using `delta-rs`. Adds three audit columns to every row:

| Column | Value |
|---|---|
| `_ingested_at` | UTC timestamp of the write |
| `_source_collection` | Name of the source collection (from CLI or config) |
| `_schema_version` | Short hash of the schema used for this batch |

Uses `schema_mode="merge"` — new fields are added to the table as nullable columns without breaking existing data. Historical rows get `null` for new columns.

**Type conflict detection:** before writing, the writer reads the existing Delta table schema and compares it against the incoming batch schema. If any existing column would change type, `WriterError` is raised with a descriptive message before delta-rs attempts the write.

**Cloud storage:** accepts `storage_options: dict[str, str]` in the same format as AWS environment variables. Both `write_deltalake` and `DeltaTable` receive the same dict.

---

### `dlq.py` — Dead Letter Queue

Buffers rejected documents in memory and flushes them to an NDJSON file at context manager exit. Every entry includes the original document, a rejection reason, the pipeline stage that produced the rejection, and a UTC timestamp.

NDJSON is used because rejected documents have no guaranteed schema — that is why they were rejected. NDJSON holds arbitrary shapes, is appendable across runs, and is directly queryable by DuckDB's `read_ndjson`.

**Cloud storage:** uses `fsspec` for all I/O. Credential translation per backend:

| Backend | Path prefix | Translation |
|---|---|---|
| Local | plain path | none |
| AWS S3 | `s3://` | `_s3fs_kwargs` → `key`, `secret`, `endpoint_url`, `client_kwargs` |
| Azure Blob | `az://`, `abfs://` | `_adlfs_kwargs` → `account_name`, `account_key`, `connection_string` |
| GCS | `gs://` | no translation (untested) |

The emulator case for Azure (`AZURE_STORAGE_USE_EMULATOR=true`) automatically builds the Azurite connection string from the account name and key.

---

## Two-Schema Design

The most important design decision in the pipeline. When a pre-inferred schema is provided via `--schema`, the CLI derives two schemas from a merge of the old and incoming batch schemas:

```
old_schema  (loaded from --schema file)
batch_schema (inferred from the incoming JSON)
merged = merge_schemas(old_schema, batch_schema)

coerce_schema = old_schema
write_schema  = {
    existing fields: old dtype + merged nullable  (types NEVER change)
    new fields:      merged dtype, nullable=True
}
```

**`coerce_schema = old_schema`** — enforces the established contract. Values that violate the old schema go to the DLQ. Old types are never relaxed silently.

**`write_schema`** — keeps existing column types identical to the old schema (preventing type conflicts with the Delta table) while adding new fields as nullable columns. The Delta table grows via `schema_mode="merge"`.

This split means the pipeline can handle a document batch containing both new fields (which land safely) and type violations (which go to the DLQ) in the same run, without either silently corrupting the schema or crashing.

---

## Schema Evolution Contract

The schema file is a contract, not a snapshot. Two rules enforced by `bridge ingest`:

1. **New fields are added automatically.** When a new field appears in a batch, it is appended to the schema file with `nullable=True`. Existing fields are copied from the old schema verbatim — their `nullable` and `dtype` are never changed by a write run.

2. **Type widening requires operator action.** When a field's type changes (partial or full), the pipeline warns and stops ingesting the affected documents. The operator re-infers from authoritative data and uses `--mode overwrite`.

---

## CLI

Two commands:

```
bridge infer  <input.json> [--output schema.json] [--detect-datetimes]
bridge ingest <input.json> <table_uri> [--schema] [--dlq] [--collection] [--mode] [--storage-option KEY=VALUE ...]
```

`bridge infer` is a one-time setup step. `bridge ingest` is the repeating operational step. Running `bridge ingest` without `--schema` infers the schema from the batch itself — useful for first-pass bronze ingestion but produces an empty DLQ because the schema accommodates every value in the batch.

---

## Cloud Backend Support

| Backend | URI scheme | Writer | DLQ |
|---|---|---|---|
| Local | plain path | ✓ | ✓ |
| AWS S3 / S3-compatible | `s3://` | ✓ | ✓ |
| Azure Blob Storage | `az://` | ✓ | ✓ |
| GCS | `gs://` | ✓ (delta-rs) | untested |
