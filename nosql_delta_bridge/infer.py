from dataclasses import dataclass
from typing import Any

# Widening order for scalar types: each type can absorb anything to its left.
_WIDEN_ORDER = ["boolean", "integer", "float", "string"]


@dataclass
class FieldSchema:
    dtype: str   # boolean | integer | float | string | object | array
    nullable: bool = False

    def __repr__(self) -> str:
        null = "?" if self.nullable else ""
        return f"FieldSchema({self.dtype}{null})"


def infer_schema(documents: list[dict]) -> dict[str, FieldSchema]:
    """Infer a unified schema from a batch of documents.

    Uses widest-type-wins conflict resolution:
      bool < int < float < string
    Incompatible types (e.g. object vs scalar) widen to string.
    A field missing from any document is marked nullable.
    """
    all_keys: set[str] = set()
    for doc in documents:
        all_keys.update(_extract_paths(doc))

    schema: dict[str, FieldSchema] = {}

    for path in all_keys:
        values = [_get_path(doc, path) for doc in documents]
        present = [v for v in values if v is not _MISSING]
        missing_in_some = len(present) < len(documents)

        dtype, has_null = _infer_dtype(present)
        schema[path] = FieldSchema(dtype=dtype, nullable=missing_in_some or has_null)

    return schema


# --- internals ---

_MISSING = object()


def _extract_paths(doc: dict, prefix: str = "") -> list[str]:
    paths = []
    for key, value in doc.items():
        full = f"{prefix}.{key}" if prefix else key
        paths.append(full)
        if isinstance(value, dict):
            paths.extend(_extract_paths(value, full))
    return paths


def _get_path(doc: dict, path: str) -> Any:
    parts = path.split(".")
    current: Any = doc
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _python_type_to_dtype(value: Any) -> str | None:
    if value is None:
        return None  # signals nullable
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
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
    # Both scalars in the widening order
    if a in _WIDEN_ORDER and b in _WIDEN_ORDER:
        return _WIDEN_ORDER[max(_WIDEN_ORDER.index(a), _WIDEN_ORDER.index(b))]
    # Incompatible types (e.g. object vs string, array vs integer) → string
    return "string"


def _infer_dtype(values: list[Any]) -> tuple[str, bool]:
    """Return (dtype, has_null) for a list of present values."""
    has_null = False
    dtype: str | None = None

    for value in values:
        vtype = _python_type_to_dtype(value)
        if vtype is None:
            has_null = True
            continue
        dtype = vtype if dtype is None else _widen(dtype, vtype)

    return (dtype or "string", has_null)
