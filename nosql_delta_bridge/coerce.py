from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from nosql_delta_bridge.infer import FieldSchema


class Action(str, Enum):
    CAST = "cast"     # attempt type cast; on failure, reject the document
    REJECT = "reject" # reject the entire document immediately
    FLAG = "flag"     # keep the original value, record a warning


@dataclass
class CoerceConfig:
    default_action: Action = Action.CAST
    field_rules: dict[str, Action] = field(default_factory=dict)

    def action_for(self, field_path: str) -> Action:
        return self.field_rules.get(field_path, self.default_action)


@dataclass
class CoerceResult:
    document: dict | None       # None when rejected
    warnings: dict[str, str]    # field_path → warning message (FLAG action)
    rejected: bool = False
    reject_reason: str | None = None


def coerce_document(
    doc: dict,
    schema: dict[str, FieldSchema],
    config: CoerceConfig | None = None,
) -> CoerceResult:
    """Apply type coercion rules to a flat document against a schema.

    For each field, if the value's type doesn't match the schema dtype:
      - CAST:   attempt a type cast; reject the document if the cast fails
      - REJECT: reject the document immediately, no further processing
      - FLAG:   keep the original value and record a warning
    Fields not in the schema are passed through unchanged.
    Null values on nullable fields are passed through unchanged.
    """
    if config is None:
        config = CoerceConfig()

    result: dict = {}
    warnings: dict[str, str] = {}

    for field_path, value in doc.items():
        field_schema = schema.get(field_path)

        if field_schema is None:
            result[field_path] = value
            continue

        if value is None:
            if field_schema.nullable:
                result[field_path] = None
            else:
                return CoerceResult(
                    document=None,
                    warnings={},
                    rejected=True,
                    reject_reason=f"null value on non-nullable field '{field_path}'",
                )
            continue

        if _matches_dtype(value, field_schema.dtype):
            result[field_path] = value
            continue

        action = config.action_for(field_path)

        if action == Action.REJECT:
            return CoerceResult(
                document=None,
                warnings={},
                rejected=True,
                reject_reason=f"type mismatch on '{field_path}': expected {field_schema.dtype}, got {type(value).__name__}",
            )

        if action == Action.FLAG:
            warnings[field_path] = (
                f"expected {field_schema.dtype}, got {type(value).__name__} — value kept as-is"
            )
            result[field_path] = value
            continue

        # CAST
        try:
            result[field_path] = _cast(value, field_schema.dtype)
        except (ValueError, TypeError) as exc:
            return CoerceResult(
                document=None,
                warnings={},
                rejected=True,
                reject_reason=f"cast failed on '{field_path}': {exc}",
            )

    return CoerceResult(document=result, warnings=warnings)


# --- internals ---

_BOOL_TRUE = {"true", "yes", "1"}
_BOOL_FALSE = {"false", "no", "0"}


def _matches_dtype(value: Any, dtype: str) -> bool:
    if dtype == "boolean":
        return isinstance(value, bool)
    if dtype == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if dtype == "float":
        return isinstance(value, float)
    if dtype == "string":
        return isinstance(value, str)
    if dtype == "datetime":
        return isinstance(value, datetime)
    # object and array — no coercion attempted
    return True


def _cast(value: Any, target: str) -> Any:
    if target == "string":
        return str(value)

    if target == "integer":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(f"cannot losslessly cast float {value} to integer")
            return int(value)
        return int(value)  # raises ValueError for non-numeric strings

    if target == "float":
        return float(value)  # raises ValueError for non-numeric strings

    if target == "boolean":
        if isinstance(value, int):
            return bool(value)
        if isinstance(value, str):
            low = value.strip().lower()
            if low in _BOOL_TRUE:
                return True
            if low in _BOOL_FALSE:
                return False
        raise ValueError(f"cannot cast {value!r} to boolean")

    if target == "datetime":
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                raise ValueError(f"cannot parse {value!r} as an ISO 8601 datetime")
        raise ValueError(f"cannot cast {type(value).__name__} to datetime")

    raise ValueError(f"unsupported target dtype '{target}'")
