from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fsspec


class DLQError(Exception):
    pass


# deltalake uses AWS env-var style keys; s3fs uses different names
def _s3fs_kwargs(path: str, storage_options: dict[str, str]) -> dict:
    """Translate object-store style storage_options to fsspec/s3fs kwargs for S3 paths."""
    if not path.startswith("s3://"):
        return storage_options

    result: dict = {}
    for k, v in storage_options.items():
        if k == "AWS_ACCESS_KEY_ID":
            result["key"] = v
        elif k == "AWS_SECRET_ACCESS_KEY":
            result["secret"] = v
        elif k == "AWS_ENDPOINT_URL":
            result["endpoint_url"] = v
        elif k == "AWS_REGION":
            result.setdefault("client_kwargs", {})["region_name"] = v
        # unknown keys are silently dropped to avoid unexpected kwarg errors
    return result


@dataclass
class DLQEntry:
    document: dict[str, Any]
    reason: str
    stage: str
    failed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class DeadLetterQueue:
    """Buffers rejected documents and flushes them to a NDJSON file.

    Accepts any path supported by fsspec: local paths, s3://, gs://, az://, etc.
    Pass storage_options to authenticate with cloud backends (same format as
    WriterConfig.storage_options).

    Nothing is silently dropped — every rejection lands here with its reason
    and the pipeline stage that produced it.
    """

    def __init__(
        self,
        path: Path | str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._path = str(path)
        self._storage_options: dict[str, str] = storage_options or {}
        self._buffer: list[DLQEntry] = []

    def append(self, document: dict[str, Any], reason: str, stage: str) -> None:
        self._buffer.append(DLQEntry(document=document, reason=reason, stage=stage))

    def flush(self) -> int:
        """Write buffered entries to the NDJSON file and clear the buffer.

        Returns the number of entries written. Does nothing and returns 0 if
        the buffer is empty (the output file is not created in that case).
        """
        if not self._buffer:
            return 0

        # create parent directories for local paths only
        if not self._path.startswith(("s3://", "gs://", "az://", "abfs://")):
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        count = len(self._buffer)

        open_kwargs = _s3fs_kwargs(self._path, self._storage_options)

        try:
            with fsspec.open(self._path, "a", encoding="utf-8", **open_kwargs) as fh:
                for entry in self._buffer:
                    fh.write(json.dumps(asdict(entry)) + "\n")
        except Exception as exc:
            raise DLQError(
                f"failed to write DLQ entries to {self._path}: {exc}"
            ) from exc

        self._buffer.clear()
        return count

    def __len__(self) -> int:
        return len(self._buffer)

    def __enter__(self) -> DeadLetterQueue:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.flush()
