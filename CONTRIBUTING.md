# Contributing

## Setup

```bash
git clone https://github.com/lhrick/nosql-delta-bridge
cd nosql-delta-bridge
pip install -e ".[dev]"
```

## Running tests

```bash
pytest                   # unit tests only
pytest -m integration    # requires Docker (Azurite for Azure tests)
```

## Linting

```bash
ruff check .
ruff format .
```

CI runs both on every pull request. A PR will not be merged if either fails.

## Making a change

1. Fork the repo and create a branch from `main`.
2. Add or update tests for whatever you changed. The test suite lives in `tests/` with synthetic fixtures in `tests/fixtures/` — no external services required for unit tests.
3. Open a pull request against `main`. Describe what the change does and why.

## Good first contributions

- **GCS DLQ support** — `dlq.py` translates `AWS_*` keys to s3fs kwargs via `_s3fs_kwargs` and Azure keys via `_adlfs_kwargs`. GCS needs the same: a `_gcsfs_kwargs` translator and integration tests against [`fake-gcs-server`](https://github.com/fsouza/fake-gcs-server).
- **`--allow-widening` flag** — `bridge ingest` currently warns and stops when all documents in a batch changed type. An opt-in flag could auto-rewrite the Delta table with the evolved schema. See `docs/roadmap.md` for context.
