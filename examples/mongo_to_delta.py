"""End-to-end demo: MongoDB users collection → Delta Lake.

Two-phase workflow:

  Phase 1 — bootstrap (run once against historical data)
    bridge infer  data/reference.json --output output/schema.json

  Phase 2 — recurring ingestion (run against each new batch)
    bridge ingest data/incoming.json output/users_delta \\
        --schema  output/schema.json \\
        --collection users \\
        --dlq output/failed.ndjson

Files produced in examples/output/:
  schema.json      inferred schema from 100 reference documents
  users_delta/     Delta Lake table (Parquet + _delta_log/)
  failed.ndjson    rejected documents with reasons (if any)

Run from the project root:
  .venv/bin/python examples/mongo_to_delta.py
"""

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
DATA = HERE / "data"
OUTPUT = HERE / "output"
BRIDGE = Path(__file__).parent.parent / ".venv" / "bin" / "bridge"

OUTPUT.mkdir(exist_ok=True)


def run(cmd: list[str]) -> None:
    result = subprocess.run([str(BRIDGE)] + cmd, capture_output=True, text=True)
    print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        sys.exit(result.returncode)


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Phase 1: infer schema from 100 clean reference documents ──────────────────

section("Phase 1 — infer schema from reference.json (100 docs)")

run([
    "infer",
    str(DATA / "reference.json"),
    "--output", str(OUTPUT / "schema.json"),
    "--detect-datetimes",
])

schema = json.loads((OUTPUT / "schema.json").read_text())
print(f"\nSchema snapshot ({len(schema)} fields):")
for field, props in sorted(schema.items()):
    nullable = "nullable" if props["nullable"] else "required"
    print(f"  {field:<35} {props['dtype']:<10} {nullable}")


# ── Phase 2: ingest incoming batch against the fixed schema ───────────────────

section("Phase 2 — ingest incoming.json (10 docs, 3 with violations)")

run([
    "ingest",
    str(DATA / "incoming.json"),
    str(OUTPUT / "users_delta"),
    "--schema",     str(OUTPUT / "schema.json"),
    "--collection", "users",
    "--dlq",        str(OUTPUT / "failed.ndjson"),
    "--detect-datetimes",
])


# ── Inspect: written rows in the Delta table ──────────────────────────────────

section("Delta table — written rows")

from deltalake import DeltaTable
df = DeltaTable(str(OUTPUT / "users_delta")).to_pandas()

print(f"\n{len(df)} rows written\n")
cols = ["_id", "name", "age", "plan", "score", "_schema_version", "_source_collection"]
print(df[cols].to_string(index=False))


# ── Inspect: rejected documents in the DLQ ────────────────────────────────────

dlq_path = OUTPUT / "failed.ndjson"
if dlq_path.exists():
    section("DLQ — rejected documents")
    entries = [json.loads(line) for line in dlq_path.read_text().splitlines()]
    print(f"\n{len(entries)} document(s) rejected\n")
    for entry in entries:
        doc = entry["document"]
        print(f"  _id: {doc.get('_id', '?'):<8}  name: {str(doc.get('name')):<12}"
              f"  reason: {entry['reason']}")
