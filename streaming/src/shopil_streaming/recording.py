"""A StreamEngine that also records what it was asked to ingest, in order.

Experiments need to prove that run_live / run_microbatch hand records to the
engine in source order and what each record's disposition was. Subclassing only
observes calls: ingest() itself is the unmodified course implementation.
"""
from __future__ import annotations

from typing import Any

from streaming_lab import StreamEngine


class RecordingEngine(StreamEngine):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.trace: list[dict[str, Any]] = []

    def ingest(self, event, *, offset: int, crash_at: str | None = None) -> dict[str, Any]:
        result = super().ingest(event, offset=offset, crash_at=crash_at)
        event_id = getattr(event, "event_id", None) or (event.get("event_id") if isinstance(event, dict) else None)
        self.trace.append({"offset": offset, "event_id": event_id, "status": result["status"],
                           "checkpoint": result["checkpoint"]})
        return result

    @property
    def offsets(self) -> list[int]:
        return [row["offset"] for row in self.trace]
