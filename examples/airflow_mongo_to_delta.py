from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


R2_BUCKET   = "s3://nosql-delta-bridge"
SCHEMA_PATH = "/opt/airflow/config/users.schema.json"
COLLECTION  = "users"


def r2_storage_options() -> dict:
    from airflow.models import Variable

    account_id = Variable.get("r2_account_id")
    return {
        "AWS_ACCESS_KEY_ID":     Variable.get("r2_access_key_id"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("r2_secret_access_key"),
        "AWS_ENDPOINT_URL":      f"https://{account_id}.r2.cloudflarestorage.com",
        "AWS_REGION":            "auto",
    }


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nosql-delta-bridge"],
)
def mongo_to_delta():

    @task
    def extract() -> str:
        """Pull all documents from MongoDB (demo: no date filter)."""
        import json
        import tempfile
        from pathlib import Path

        from airflow.models import Variable
        from pymongo import MongoClient

        client = MongoClient(Variable.get("mongo_uri"))
        docs = list(client.mydb[COLLECTION].find())

        for doc in docs:
            doc["_id"] = str(doc["_id"])

        path = Path(tempfile.mktemp(suffix=".json"))
        path.write_text(json.dumps(docs, default=str))
        print(f"Extracted {len(docs)} documents")
        return str(path)

    @task
    def infer_if_needed(batch_path: str) -> None:
        """Run infer on the first run when no schema file exists yet."""
        import json
        from pathlib import Path

        from nosql_delta_bridge.infer import InferConfig, infer_schema, schema_to_dict

        if not Path(SCHEMA_PATH).exists():
            print("No schema file — inferring from first batch")
            raw = json.loads(Path(batch_path).read_text())
            schema = infer_schema(raw, InferConfig(detect_datetimes=True))
            Path(SCHEMA_PATH).write_text(json.dumps(schema_to_dict(schema), indent=2))
            print(f"Schema written to {SCHEMA_PATH} ({len(schema)} fields)")
        else:
            print(f"Schema exists at {SCHEMA_PATH} — skipping infer")

    @task
    def ingest(batch_path: str) -> None:
        """Coerce against schema, write to R2, route rejections to DLQ."""
        import json
        from pathlib import Path

        from nosql_delta_bridge.coerce import CoerceConfig, coerce_document
        from nosql_delta_bridge.dlq import DeadLetterQueue
        from nosql_delta_bridge.flatten import flatten_document
        from nosql_delta_bridge.infer import (
            FieldSchema,
            InferConfig,
            infer_schema,
            merge_schemas,
            schema_from_dict,
            schema_to_dict,
        )
        from nosql_delta_bridge.writer import WriterConfig, write_batch

        raw = json.loads(Path(batch_path).read_text())
        if not raw:
            print("Empty batch — nothing to do")
            return

        storage      = r2_storage_options()
        old_schema   = schema_from_dict(json.loads(Path(SCHEMA_PATH).read_text()))
        batch_schema = infer_schema(raw, InferConfig(detect_datetimes=True))
        merged       = merge_schemas(old_schema, batch_schema)

        write_schema = {
            key: (
                FieldSchema(dtype=old_schema[key].dtype, nullable=merged[key].nullable)
                if key in old_schema
                else merged[key]
            )
            for key in merged
        }

        new_fields = set(write_schema) - set(old_schema)
        widened    = [
            k for k in old_schema
            if k in batch_schema and old_schema[k].dtype != batch_schema[k].dtype
        ]

        good: list[dict] = []
        rejected_count   = 0
        dlq_uri          = f"{R2_BUCKET}/dlq/{COLLECTION}.ndjson"

        with DeadLetterQueue(dlq_uri, storage_options=storage) as dlq:
            for doc in raw:
                flat   = flatten_document(doc)
                result = coerce_document(flat, old_schema, CoerceConfig())
                if result.rejected:
                    dlq.append(doc, reason=result.reject_reason, stage="coerce")
                    rejected_count += 1
                else:
                    good.append(result.document)

        if good:
            config = WriterConfig(
                table_uri=f"{R2_BUCKET}/delta/{COLLECTION}",
                source_collection=COLLECTION,
                storage_options=storage,
            )
            written = write_batch(good, write_schema, config)
            print(f"Written: {written}  Rejected: {rejected_count}")

        if new_fields:
            # preserve old fields exactly — only append the new ones as nullable
            evolved_schema = {**old_schema, **{k: write_schema[k] for k in new_fields}}
            Path(SCHEMA_PATH).write_text(json.dumps(schema_to_dict(evolved_schema), indent=2))
            print(f"Schema evolved: +{len(new_fields)} field(s) {sorted(new_fields)}")

        if widened:
            print(f"WARNING: type widening detected on {widened}")
            print("Re-run bridge infer on a combined batch and use --mode overwrite to apply")

    path = extract()
    infer_if_needed(path) >> ingest(path)


mongo_to_delta()
