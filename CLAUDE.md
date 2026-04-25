# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`nosql-delta-bridge` is a Python library that ingests schema-free NoSQL documents (MongoDB, Firestore, DynamoDB) into Delta Lake with predictable, auditable results. It solves real production pain: fields appear/disappear across documents, types conflict, nested objects vary in depth, and arrays hold mixed types.

See `portfolio-projects.md` for the full vision and rationale.

## Planned Stack

| Layer | Tool |
|---|---|
| Processing | Pandas + PyArrow (or PySpark) |
| Sink | Delta Lake via `delta-rs` (no Spark cluster required) |
| Orchestration | CLI (Click or Typer), optionally Prefect later |
| Testing | `pytest` with synthetic JSON fixtures in `tests/fixtures/` |

## Architecture

The library is a sequential pipeline of five modules in `nosql_delta_bridge/`:

1. **`infer.py`** — Examines a batch of documents and infers a unified schema. Conflict resolution: widest type wins, all fields nullable by default.
2. **`coerce.py`** — Applies per-field type coercion rules: `cast`, `reject`, or `flag` a value when it doesn't match the expected type.
3. **`flatten.py`** — Flattens nested objects and arrays into a tabular structure with configurable depth.
4. **`writer.py`** — Writes processed documents to a Delta Lake table using `delta-rs`. Handles schema evolution (new fields are merged, not breaking).
5. **`dlq.py`** — Routes documents that fail processing to a dead letter queue. Nothing is silently dropped; all failures are logged with a reason.

Data flows: raw documents → `infer` → `coerce` → `flatten` → `writer`; failures at any stage → `dlq`.

Audit metadata columns (`_ingested_at`, _`source_collection`, `_schema_version`) are added by the writer.

## Development Setup

No build system exists yet. When adding one, the expected commands are:

```bash
pip install -e ".[dev]"   # install package + dev dependencies
pytest                    # run all tests
pytest tests/test_infer.py::test_name  # run a single test
```

Linting should use `ruff` (fast, covers both lint and format). If a `pyproject.toml` is added, configure `ruff` and `pytest` there.

## Key Conventions to Follow

- Use type hints throughout (target Python 3.10+).
- Configuration for coercion rules and flattening depth should be dataclasses or Pydantic models, not raw dicts.
- Raise custom exception types (not bare `Exception`) so callers can catch specific failures.
- Test fixtures live in `tests/fixtures/` as synthetic messy JSON files — documents with missing fields, type conflicts, and deeply nested structures.
