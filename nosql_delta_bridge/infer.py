from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Widening order for scalar types: each type can absorb anything to its left.
_WIDEN_ORDER = ["boolean", "integer", "float", "string"]


@dataclass
class InferConfig:
    detect_datetimes: bool = False


@dataclass
class FieldSchema:
    dtype: str   # boolean | integer | float | string | object | array | datetime
    nullable: bool = False

    def __repr__(self) -> str:
        null = "?" if self.nullable else ""
        return f"FieldSchema({self.dtype}{null})"


def merge_schemas(
    old: dict[str, FieldSchema],
    new: dict[str, FieldSchema],
) -> dict[str, FieldSchema]:
    """Merge two schemas into a cumulative schema using widest-type-wins rules.

    - Field in both   → widen dtype if needed; nullable if either side is nullable
    - Field only in old → keep it, mark nullable (absent from new batch)
    - Field only in new → add it, mark nullable (not in historical data)

    Types only ever widen — the merged schema is always a superset of both inputs.
    """
    merged: dict[str, FieldSchema] = {}
    for key in set(old) | set(new):
        if key in old and key in new:
            merged[key] = FieldSchema(
                dtype=_widen(old[key].dtype, new[key].dtype),
                nullable=old[key].nullable or new[key].nullable,
            )
        elif key in old:
            merged[key] = FieldSchema(dtype=old[key].dtype, nullable=True)
        else:
            merged[key] = FieldSchema(dtype=new[key].dtype, nullable=True)
    return merged


def schema_to_dict(schema: dict[str, FieldSchema]) -> dict:
    """Serialize a schema to a plain dict suitable for JSON export."""
    return {path: {"dtype": fs.dtype, "nullable": fs.nullable} for path, fs in schema.items()}


def schema_from_dict(data: dict) -> dict[str, FieldSchema]:
    """Deserialize a schema from a plain dict (as produced by schema_to_dict)."""
    return {path: FieldSchema(dtype=v["dtype"], nullable=v["nullable"]) for path, v in data.items()}


def infer_schema(
    documents: list[dict],
    config: InferConfig | None = None,
) -> dict[str, FieldSchema]:
    """Infer a unified schema from a batch of documents.

    Uses widest-type-wins conflict resolution:
      bool < int < float < string
    Incompatible types (e.g. object vs scalar) widen to string.
    A field missing from any document is marked nullable.

    Pass InferConfig(detect_datetimes=True) to attempt ISO 8601 string detection.
    """
    if config is None:
        config = InferConfig()

    all_keys: set[str] = set()
    for doc in documents:
        all_keys.update(_extract_paths(doc))

    schema: dict[str, FieldSchema] = {}

    for path in all_keys:
        values = [_get_path(doc, path) for doc in documents]
        present = [v for v in values if v is not _MISSING]
        missing_in_some = len(present) < len(documents)

        dtype, has_null = _infer_dtype(present, config.detect_datetimes)
        schema[path] = FieldSchema(dtype=dtype, nullable=missing_in_some or has_null)

    return schema


# --- internals ---

_MISSING = object()


def _extract_paths(doc: dict, prefix: str = "") -> list[str]:
    paths = []
    for key, value in doc.items():
        full = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            paths.extend(_extract_paths(value, full))
        else:
            paths.append(full)
    return paths


def _get_path(doc: dict, path: str) -> Any:
    parts = path.split(".")
    current: Any = doc
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _looks_like_datetime(value: str) -> bool:
    try:
        datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _python_type_to_dtype(value: Any, detect_datetimes: bool = False) -> str | None:
    if value is None:
        return None  # signals nullable
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, str):
        if detect_datetimes and _looks_like_datetime(value):
            return "datetime"
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"  # fallback for unknown types


def _widen(a: str, b: str) -> str:
    """Return the wider of two dtypes."""
    if a == b:
        return a
    # datetime conflicts with every other type → string
    if a == "datetime" or b == "datetime":
        return "string"
    # Both scalars in the widening order
    if a in _WIDEN_ORDER and b in _WIDEN_ORDER:
        return _WIDEN_ORDER[max(_WIDEN_ORDER.index(a), _WIDEN_ORDER.index(b))]
    # Incompatible types (e.g. object vs string, array vs integer) → string
    return "string"


def _infer_dtype(values: list[Any], detect_datetimes: bool = False) -> tuple[str, bool]:
    """Return (dtype, has_null) for a list of present values."""
    has_null = False
    dtype: str | None = None

    for value in values:
        vtype = _python_type_to_dtype(value, detect_datetimes)
        if vtype is None:
            has_null = True
            continue
        dtype = vtype if dtype is None else _widen(dtype, vtype)

    return (dtype or "string", has_null)
