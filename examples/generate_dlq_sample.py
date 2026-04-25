"""Run a batch of intentionally broken documents through the pipeline and write
rejections to dlq_sample.ndjson so you can inspect what DLQ entries look like.

Schema is inferred from clean reference documents only. Dirty documents are
then coerced against that fixed schema — violations produce real rejections.
"""

from pathlib import Path

from nosql_delta_bridge.coerce import CoerceConfig, coerce_document
from nosql_delta_bridge.dlq import DeadLetterQueue
from nosql_delta_bridge.flatten import flatten_document
from nosql_delta_bridge.infer import infer_schema

# clean reference batch — schema is inferred from these only
REFERENCE = [
    {"id": 1, "name": "Alice", "age": 30, "score": 9.5, "active": True},
    {"id": 2, "name": "Bob",   "age": 25, "score": 8.0, "active": False},
]

# incoming batch — some documents violate the schema inferred above
INCOMING = [
    # clean — passes
    {"id": 3, "name": "Carol", "age": 28, "score": 7.5, "active": True},
    # age is a non-numeric string → cast to integer fails → rejected
    {"id": 4, "name": "Dave",  "age": "twenty", "score": 6.0, "active": False},
    # score is a string that can't be cast to float → rejected
    {"id": 5, "name": "Eve",   "age": 33, "score": "n/a", "active": True},
    # name is null but the reference schema marks it non-nullable → rejected
    {"id": 6, "name": None,    "age": 41, "score": 5.5, "active": False},
    # clean — passes
    {"id": 7, "name": "Frank", "age": 19, "score": 9.0, "active": True},
]

OUTPUT = Path("dlq_sample.ndjson")


def main() -> None:
    schema = infer_schema(REFERENCE)

    print("Inferred schema:")
    for path, field_schema in schema.items():
        print(f"  {path:<10} dtype={field_schema.dtype:<8} nullable={field_schema.nullable}")
    print()

    passed = 0
    rejected = 0

    with DeadLetterQueue(OUTPUT) as dlq:
        for doc in INCOMING:
            flat = flatten_document(doc)
            result = coerce_document(flat, schema, CoerceConfig())

            if result.rejected:
                dlq.append(doc, reason=result.reject_reason, stage="coerce")
                rejected += 1
                print(f"  REJECTED  id={doc['id']}  reason: {result.reject_reason}")
            else:
                passed += 1
                print(f"  ok        id={doc['id']}")

    print(f"\n{passed} passed, {rejected} rejected → {OUTPUT}")


if __name__ == "__main__":
    main()
