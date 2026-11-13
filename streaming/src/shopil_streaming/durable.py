"""SHOPIL-23: read the DURABLE state straight from the SQLite file.

`durable_state` opens the database read-only in a new connection, so it only
sees committed data, never an in-memory Python object. Its fingerprint lets
experiments compare durable state byte for byte across replay, reopen and crashes.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

TABLES = ("stream_meta", "counters", "seen_events", "order_state", "review_state", "realtime_revenue",
          "review_windows", "review_alerts", "late_events", "dead_letters", "accepted_reviews")


def durable_state(db_path: Path | str) -> dict[str, list[list[Any]]]:
    con = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        state = {}
        for table in TABLES:
            columns = [row[1] for row in con.execute(f"PRAGMA table_info({table})")]
            state[table] = [list(row) for row in con.execute(
                f"SELECT * FROM {table} ORDER BY {', '.join(columns)}")]
        return state
    finally:
        con.close()


def fingerprint(db_path: Path | str) -> str:
    text = json.dumps(durable_state(db_path), ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
