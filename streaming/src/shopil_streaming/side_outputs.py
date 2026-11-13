"""SHOPIL-22: late_events and dead_letters side outputs, read from the engine DB.

* late_events  - VALID, first-seen events whose window had already closed
                 (watermark >= window_end + grace when they arrived). They are
                 kept with input, offset and reason, and excluded from windows.
* dead_letters - inputs that failed the event contract, with input, offset and reason.

Both are written by StreamEngine.ingest in the same SQLite transaction that
advances the checkpoint, so neither kind of record blocks the stream or is
re-processed on restart. A retried late event_id is a duplicate, not a second
late row.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .contract import dead_letter_report


def late_report(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"offset": row["source_offset"], "event_id": row["event_id"], "kind": row["kind"],
             "event_time": row["event_time"], "window_start": row["window_start"], "window_end": row["window_end"],
             "watermark_at_arrival": row["watermark"], "amount_cents": row["amount_cents"],
             "reason": row["reason"], "input": row["payload"]}
            for row in snapshot["late_events"]]


def export_side_outputs(snapshot: dict[str, Any], directory: Path) -> dict[str, Path]:
    """Write late_events.csv and dead_letters.csv (input as JSON text)."""
    directory.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, rows in (("late_events", late_report(snapshot)), ("dead_letters", dead_letter_report(snapshot))):
        path = directory / f"{name}.csv"
        columns = list(rows[0]) if rows else ["offset", "reason", "input"]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                                 for k, v in row.items()})
        paths[name] = path
    return paths
