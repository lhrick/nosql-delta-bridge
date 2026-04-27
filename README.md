# nosql-delta-bridge

A Python library for ingesting schema-free NoSQL documents into Delta Lake with predictable, auditable results.

## The Problem

In my previous role I ran ETL jobs pulling data from MongoDB into Delta Lake using Spark. The pipeline worked fine — until it didn't. A single document where `age` was `"thirty-two"` instead of `32`, or where a previously stable `status` field suddenly appeared as an integer, was enough to break the entire write. The Spark job would fail with a cryptic cast error, the table was left in an inconsistent state, and ops scrambled to figure out which document was the culprit.

The fix was always the same: widen the schema, re-run, and hope the same thing didn't happen tomorrow. There was no DLQ, no audit trail, and no contract that said "this field must be an integer." Bad documents were either silently coerced, dropped, or they crashed everything.

`nosql-delta-bridge` is built to eliminate that class of problem. Every document either lands in the Delta table or in a dead-letter queue with an explicit rejection reason. Nothing is silently dropped. Nothing silently crashes the pipeline.

---

## Architecture

```
raw JSON documents
        │
        ▼
┌───────────────┐
│    infer.py   │  Examines a batch and infers a unified schema.
│               │  Conflict resolution: widest type wins, all fields 
└───────┬───────┘  nullable by default.
        │  schema: dict[str, FieldSchema]
        ▼
┌───────────────┐
│   flatten.py  │  Flattens nested objects into dot-notation columns.
│               │  address.city, preferences.notifications.email, etc.
└───────┬───────┘
        │  flat documents
        ▼
┌───────────────┐
│   coerce.py   │  Applies per-field type coercion against the schema.
│               │  cast (silent), reject (→ DLQ), or flag.
└───────┬───────┘
        │  typed documents          rejected documents
        ▼                                   ▼
┌───────────────┐               ┌───────────────────┐
│   writer.py   │               │      dlq.py       │
│               │               │                   │
│  Delta Lake   │               │  NDJSON file or   │
│  (delta-rs)   │               │  S3/GCS/Azure Blob│
└───────────────┘               └───────────────────┘
```

Audit metadata columns are added automatically on every write:

| Column | Description |
|---|---|
| `_ingested_at` | UTC timestamp of the write |
| `_source_collection` | Name of the source collection |
| `_schema_version` | Short hash of the schema used for this batch |

---

## Quickstart

```bash
pip install -e ".[dev]"
```

### Infer a schema from known-good data

```bash
bridge infer historical.json --output users.schema.json
```

```
historical.json  ·  500 documents  ·  8 fields
  schema written  →  users.schema.json
```

The schema file is a plain JSON contract that controls what the pipeline will accept:

```json
{
  "name":  {"dtype": "string",  "nullable": false},
  "age":   {"dtype": "integer", "nullable": false},
  "email": {"dtype": "string",  "nullable": true}
}
```

### Ingest new documents against the fixed schema

```bash
bridge ingest incoming.json ./delta/users --schema users.schema.json --dlq rejected.ndjson
```

```
incoming.json  ·  11 documents  ·  schema from users.schema.json (8 fields)
  written:   8  →  delta/users
  rejected:  3  →  rejected.ndjson
  schema evolved: +4 field(s) ['address', 'address.city', 'address.street', 'address.zip']  →  users.schema.json
```

Every rejected document in `rejected.ndjson` has an explicit reason:

```json
{"_dlq_reason": "cast failed on 'age': invalid literal for int() with base 10: 'thirty-two'", "_dlq_stage": "coerce", ...}
{"_dlq_reason": "null value on non-nullable field 'name'", "_dlq_stage": "coerce", ...}
```

### Without a schema file

```bash
bridge ingest batch.json ./delta/users
```

Schema is inferred from the batch itself. All documents land; the DLQ stays empty. Useful for first-pass bronze ingestion.

---

## Pipeline Behavior

The two-command workflow (`bridge infer` + `bridge ingest --schema`) is what makes the DLQ meaningful. When the schema is inferred from the same batch being ingested, it always accommodates every value in that batch — types widen, nulls make fields nullable — so nothing is ever rejected. The DLQ only has teeth when coerce runs against a pre-established schema contract.

| Situation | Written | DLQ | Schema file |
|---|---|---|---|
| Clean batch | all | empty | unchanged |
| New optional field appears | all | empty | updated (field added, nullable) |
| Null on a required field | valid docs | nulled docs | unchanged |
| Castable type mismatch (`"25"` → integer) | all, cast silently | empty | unchanged |
| Non-castable mismatch (`"twenty-five"` → integer) | valid docs | bad docs | unchanged + warning |
| Partial type widening (some docs changed type) | valid-type docs | bad-type docs | unchanged + warning |
| Full type migration (all docs changed type) | 0 | all docs | unchanged + warning |
| Lossy float → integer cast (`25.7` → int) | clean docs | lossy docs | unchanged |

**Type widening is never automatic.** When a field goes from `integer` to `string`, the pipeline cannot distinguish a legitimate migration from a data quality incident. Both look identical from the outside. The correct response is to stop, warn, and surface the decision to a human:

```bash
# re-infer from new authoritative data and overwrite the table
bridge infer new_batch.json --output users.schema.json
bridge ingest new_batch.json ./delta/users --schema users.schema.json --mode overwrite
```

---

## Flattening

Nested objects are expanded into dot-notation columns before coercion and write. Only leaf fields appear in the schema — parent keys are excluded when their children are tracked.

```python
# {"address": {"city": "SP", "zip": "01310"}}
# becomes:
# {"address.city": "SP", "address.zip": "01310"}
# "address" does NOT appear as a column
```

### `max_depth`

Controls how many levels deep the flattener recurses. The default is `5`, which covers most real-world documents. Beyond the limit, the remaining sub-object is kept as a single opaque column instead of being expanded further.

Given this document:

```python
{"user": {"profile": {"address": {"city": "São Paulo"}}}}
```

| `max_depth` | Result |
|---|---|
| `5` (default) | `user.profile.address.city = "São Paulo"` |
| `2` | `user.profile = {"address": {"city": "São Paulo"}}` |
| `1` | `user = {"profile": {"address": {"city": "São Paulo"}}}` |

The column still exists in Delta when the limit is hit — it holds a serialized object instead of a scalar. You can query it, but not with simple column predicates.

Lower `max_depth` when documents are extremely wide or deeply nested across many schema variants. A product catalog with 10-level-deep `specs` objects across 500 product types can easily produce hundreds of distinct column names with the default depth. Capping the recursion keeps the schema manageable; the deep structure lands in an opaque column you can process separately.

### Arrays

Arrays are never recursed into — they are kept as Python lists and stored as list columns in Delta Lake:

```python
{"tags": ["python", "delta", "etl"]}
# stays as:
{"tags": ["python", "delta", "etl"]}

{"orders": [{"id": 1, "total": 99.0}, {"id": 2, "total": 45.0}]}
# also stays as:
{"orders": [{"id": 1, "total": 99.0}, {"id": 2, "total": 45.0}]}
```

Exploding an array (one document → many rows) is a cardinality change that breaks joins and aggregations on all other columns. That is a deliberate transform — a `LATERAL VIEW EXPLODE` in Spark or `UNNEST` in SQL — not a side effect of flattening. Do it explicitly before feeding documents into the pipeline if needed.

### Custom separator

```python
from nosql_delta_bridge.flatten import flatten_document

flat = flatten_document(doc, max_depth=2, separator="__")
# produces: address__city, address__zip
```

---

## Schema Evolution

New fields are added automatically and safely. Existing field types and nullability are never changed by an ingestion run — the schema file is a contract, not a snapshot.

When a new field appears in a batch:
- It is added to the Delta table as a nullable column via `schema_mode="merge"`.
- Historical rows get `null` for that column — no data loss.
- The schema file is updated to include the new field.
- Existing fields in the schema file are written back verbatim — nullable flags are never silently widened.

---

## Writing to S3 / Cloudflare R2 / GCS

Pass `storage_options` through `WriterConfig` and `DeadLetterQueue`:

```python
from nosql_delta_bridge.writer import WriterConfig, write_batch
from nosql_delta_bridge.dlq import DeadLetterQueue

storage_options = {
    "AWS_ACCESS_KEY_ID":     "...",
    "AWS_SECRET_ACCESS_KEY": "...",
    "AWS_ENDPOINT_URL":      "https://<account>.r2.cloudflarestorage.com",
    "AWS_REGION":            "auto",
}

config = WriterConfig(
    table_uri="s3://my-bucket/delta/users",
    source_collection="users",
    storage_options=storage_options,
)

with DeadLetterQueue("s3://my-bucket/dlq/users.ndjson", storage_options=storage_options) as dlq:
    ...
```

The DLQ uses `fsspec` internally, so the same `storage_options` key format works across S3, GCS, and Azure Blob without any code changes.

---

## Running Tests

```bash
pytest
```

137 tests across all five modules and the CLI. Fixtures in `tests/fixtures/` are fully synthetic — documents with missing fields, type conflicts, deeply nested structures, and mixed-type arrays. No external services required.

---

## Key Decisions

**Two schemas per ingestion run, not one.** When a fixed schema is provided, the pipeline derives two schemas from a merge:
- `coerce_schema` = old schema only. Enforces the established contract strictly.
- `write_schema` = old fields (types and nullability preserved) + new fields from the batch (added as nullable). Prevents type conflicts with the existing Delta table while allowing safe growth.

**Type widening is operator-triggered, not automatic.** The pipeline cannot tell whether a field going from `integer` to `string` is intentional (the source system changed) or accidental (bad data). Both look identical from the outside. The correct response is to stop, warn, and surface the decision to a human.

**DLQ as NDJSON, not Delta.** Rejected documents have no guaranteed schema — that is why they were rejected. NDJSON is the only format that can hold arbitrary shapes, is appendable across runs without schema negotiation, and is directly queryable by DuckDB's `read_ndjson`.

**`delta-rs` over Spark.** No cluster required. The full pipeline runs locally on a laptop and writes to a Delta table that any Spark or DuckDB reader can consume. This makes it reproducible for anyone cloning the repo.

**Lossy float-to-integer casts are rejections, not silent truncation.** `30.0 → 30` is lossless and accepted. `25.7 → 25` silently drops `.7`. In a financial or scientific context that is a data quality bug. The coerce layer enforces `float.is_integer()` before casting.

---

## What I Would Do Differently

**`--storage-options` on the CLI.** Right now, cloud credentials must be passed via the Python API or environment variables. A `bridge ingest --storage-options key=val` flag would let the CLI write directly to S3/R2 without Airflow as the wrapper.

**Publish to PyPI.** Currently installed via `pip install -e .`. Publishing removes the volume-mount workaround needed to use the library inside Airflow containers.

**`--allow-widening` flag for explicit type migrations.** For the case where all documents in a batch changed type, an opt-in flag could auto-rewrite the Delta table with the evolved schema instead of requiring two manual commands. Kept out of scope deliberately — the conservative default should be explicit, not opt-out.

---

## Project Structure

```
nosql_delta_bridge/
├── infer.py       # Schema inference and merging
├── coerce.py      # Type coercion with cast / reject / flag
├── flatten.py     # Nested object and array flattening
├── writer.py      # Delta Lake writer with schema evolution
├── dlq.py         # Dead letter queue (local or cloud via fsspec)
└── cli.py         # bridge infer / bridge ingest commands

tests/
└── fixtures/      # Synthetic messy JSON documents (no external deps)

examples/
└── mongo_to_delta.py  # End-to-end demo: MongoDB Atlas → Delta Lake on Cloudflare R2
```
