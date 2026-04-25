from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DLQError(Exception):
    pass


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

    Nothing is silently dropped — every rejection lands here with its reason
    and the pipeline stage that produced it.
    """

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
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

        self._path.parent.mkdir(parents=True, exist_ok=True)
        count = len(self._buffer)

        try:
            with self._path.open("a", encoding="utf-8") as fh:
                for entry in self._buffer:
                    fh.write(json.dumps(asdict(entry)) + "\n")
        except OSError as exc:
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
