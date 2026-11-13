"""Durable per-question logs, separate from approved conversation history."""

import json
import os
from pathlib import Path
import re
from threading import Lock


class JsonlAudit:
    """Use one sink per application; request UUIDs isolate concurrent conversations."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self._lock = Lock()

    def __call__(self, event: dict) -> None:
        request_id = event.get("request_id")
        if not isinstance(request_id, str) or not re.fullmatch(r"[a-f0-9]{32}", request_id):
            raise ValueError("Audit request_id must be a UUID hex string")
        if event.get("event") not in {"snapshot", "attempt", "terminal"}:
            raise ValueError("Unknown audit event")
        line = json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
        with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            descriptor = os.open(
                self.directory / f"{request_id}.jsonl",
                os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600,
            )
            with os.fdopen(descriptor, "a", encoding="utf-8", newline="\n") as output:
                output.write(line)
                output.flush()
                # A successful audit call is the gate before publishing an approved answer.
                os.fsync(output.fileno())
