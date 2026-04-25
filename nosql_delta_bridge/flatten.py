from typing import Any


def flatten_document(doc: dict, max_depth: int = 5, separator: str = ".") -> dict:
    """Flatten a nested document into a single-level dict.

    Nested objects are expanded using dot notation up to max_depth.
    Beyond max_depth, the value is kept as-is (dict or list).
    Arrays are kept as Python lists — their elements are not recursed into.
    """
    result = {}
    _flatten_recursive(doc, prefix="", depth=0, max_depth=max_depth, separator=separator, out=result)
    return result


def flatten_documents(documents: list[dict], max_depth: int = 5, separator: str = ".") -> list[dict]:
    return [flatten_document(doc, max_depth=max_depth, separator=separator) for doc in documents]


# --- internals ---

def _flatten_recursive(
    obj: Any,
    prefix: str,
    depth: int,
    max_depth: int,
    separator: str,
    out: dict,
) -> None:
    if not isinstance(obj, dict) or depth >= max_depth:
        out[prefix] = obj
        return

    for key, value in obj.items():
        full_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(value, dict) and depth + 1 < max_depth:
            _flatten_recursive(value, full_key, depth + 1, max_depth, separator, out)
        else:
            out[full_key] = value
