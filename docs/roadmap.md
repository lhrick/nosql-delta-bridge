# Roadmap & Design Reflections

## What I Would Do Differently

**PySpark-native flatten and coerce.** The library uses Pandas + PyArrow, which works well for batch sizes up to a few million documents per run. The original problem this solves — Spark jobs pulling from MongoDB into Delta Lake — operates at a different scale. A PySpark variant of the `flatten` and `coerce` stages expressed as column transformations or UDFs would allow the same schema enforcement logic to run distributed across a cluster. Schema inference could still run driver-side on a representative sample and then be broadcast to workers. The DLQ in that context would write to a partitioned Delta table rather than NDJSON. The current design made a deliberate trade-off: no cluster required means anyone can clone the repo and run the full pipeline on a laptop. That reproducibility has value, but it is not the right choice for a 100M-document-per-hour production job.

**`--allow-widening` flag for explicit type migrations.** For the case where all documents in a batch changed type, an opt-in flag could auto-rewrite the Delta table with the evolved schema instead of requiring two manual commands. Kept out of scope deliberately — the conservative default should be explicit, not opt-out.

## Planned Features

- **GCS DLQ support** — the writer supports GCS via delta-rs; the DLQ needs a `_gcsfs_kwargs` translator and integration tests against `fake-gcs-server`.
- **`--allow-widening` flag** — opt-in auto-overwrite of the Delta table when all documents in a batch agree on the new type (Scenario 7 in the pipeline behaviour table).
