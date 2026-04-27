from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fsspec


class DLQError(Exception):
    pass


# deltalake uses object-store style env-var keys; fsspec backends use different names.
# Each _*_kwargs function translates to the format the relevant fsspec backend expects.

def _s3fs_kwargs(storage_options: dict[str, str]) -> dict:
    """Translate to fsspec/s3fs kwargs for S3 paths."""
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


def _adlfs_kwargs(storage_options: dict[str, str]) -> dict:
    """Translate to fsspec/adlfs kwargs for Azure paths (az://, abfs://)."""
    # Explicit connection string takes precedence — covers Azurite and SAS scenarios
    if "AZURE_STORAGE_CONNECTION_STRING" in storage_options:
        return {"connection_string": storage_options["AZURE_STORAGE_CONNECTION_STRING"]}

    # Emulator: build Azurite connection string from account name + key
    if storage_options.get("AZURE_STORAGE_USE_EMULATOR", "").lower() == "true":
        account = storage_options.get("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1")
        key = storage_options.get("AZURE_STORAGE_ACCOUNT_KEY", "")
        return {
            "connection_string": (
                f"DefaultEndpointsProtocol=http;"
                f"AccountName={account};"
                f"AccountKey={key};"
                f"BlobEndpoint=http://127.0.0.1:10000/{account};"
            )
        }

    result: dict = {}
    if "AZURE_STORAGE_ACCOUNT_NAME" in storage_options:
        result["account_name"] = storage_options["AZURE_STORAGE_ACCOUNT_NAME"]
    if "AZURE_STORAGE_ACCOUNT_KEY" in storage_options:
        result["account_key"] = storage_options["AZURE_STORAGE_ACCOUNT_KEY"]
    if "AZURE_STORAGE_SAS_TOKEN" in storage_options:
        result["sas_token"] = storage_options["AZURE_STORAGE_SAS_TOKEN"]
    return result


def _open_kwargs(path: str, storage_options: dict[str, str]) -> dict:
    """Return fsspec-compatible kwargs for the given path scheme."""
    if path.startswith("s3://"):
        return _s3fs_kwargs(storage_options)
    if path.startswith(("az://", "abfs://")):
        return _adlfs_kwargs(storage_options)
    return storage_options


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

        open_kwargs = _open_kwargs(self._path, self._storage_options)

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
