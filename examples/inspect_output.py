"""Inspect the output produced by mongo_to_delta.py.

Run from the project root:
  .venv/bin/python examples/inspect_output.py
"""

import json
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

OUTPUT = Path(__file__).parent / "output"


def section(title: str) -> None:
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def check(path: Path, label: str) -> bool:
    if not path.exists():
        print(f"\n  ⚠  {label} not found at {path}")
        print("     Run examples/mongo_to_delta.py first.")
        return False
    return True


# ── Schema ────────────────────────────────────────────────────────────────────

schema_path = OUTPUT / "schema.json"
if check(schema_path, "schema.json"):
    section("schema.json — inferred from 100 reference documents")
    schema = json.loads(schema_path.read_text())
    print(f"\n  {'field':<35} {'dtype':<12} {'nullable'}")
    print(f"  {'─'*35} {'─'*12} {'─'*8}")
    for field, props in sorted(schema.items()):
        nullable = "yes" if props["nullable"] else "no"
        print(f"  {field:<35} {props['dtype']:<12} {nullable}")


# ── Delta table ───────────────────────────────────────────────────────────────

table_path = OUTPUT / "users_delta"
if check(table_path, "users_delta/"):
    section("users_delta — Delta table")
    dt = DeltaTable(str(table_path))
    df = dt.to_pandas()

    print(f"\n  {len(df)} rows  ·  {len(df.columns)} columns")
    print(f"  Delta version: {dt.version()}\n")

    # column types
    print("  Column types:")
    for col, dtype in df.dtypes.items():
        print(f"    {col:<35} {dtype}")

    # data
    print(f"\n  Rows:")
    display_cols = ["_id", "name", "age", "plan", "score", "active",
                    "created_at", "_source_collection", "_schema_version"]
    available = [c for c in display_cols if c in df.columns]
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    pd.set_option("display.max_colwidth", 24)
    print(df[available].to_string(index=False))


# ── DLQ ───────────────────────────────────────────────────────────────────────

dlq_path = OUTPUT / "failed.ndjson"
if dlq_path.exists():
    section("failed.ndjson — rejected documents")
    entries = [json.loads(line) for line in dlq_path.read_text().splitlines()]
    print(f"\n  {len(entries)} rejection(s)\n")
    for i, entry in enumerate(entries, 1):
        doc = entry["document"]
        print(f"  [{i}] _id: {doc.get('_id', '?')}")
        print(f"      stage:  {entry['stage']}")
        print(f"      reason: {entry['reason']}")
        print(f"      failed_at: {entry['failed_at']}")
        print(f"      document: {json.dumps(doc)}")
        print()
else:
    section("failed.ndjson — rejected documents")
    print("\n  No rejections — DLQ file was not created.")
