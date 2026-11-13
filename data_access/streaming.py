"""Read the committed streaming state from an existing SQLite database."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3


WINDOW_TABLES = (
    ("order_state", "window_start_ms"),
    ("realtime_revenue", "window_start_ms"),
    ("review_state", "window_start_ms, product_id"),
    ("review_windows", "window_start_ms, product_id"),
    ("review_alerts", "window_start_ms, product_id"),
)


def read_stream_snapshot(database: Path) -> dict:
    """Return detached rows and their progress position from one read transaction."""
    uri = database.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")
        meta = connection.execute("""
            SELECT checkpoint, watermark_ms, config_json, finalized
            FROM stream_meta WHERE singleton = 1
        """).fetchone()
        if meta is None:
            raise ValueError("Streaming database has no committed metadata")
        counters = dict(connection.execute("SELECT name, value FROM counters"))
        # Table names and ordering come only from the module's fixed query list.
        rows = {
            table: [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")]
            for table, order in WINDOW_TABLES
        }
        connection.commit()

    return {
        "checkpoint": meta["checkpoint"],
        "watermark_ms": meta["watermark_ms"],
        "config": json.loads(meta["config_json"]),
        "finalized": bool(meta["finalized"]),
        "counters": counters,
        "rows": rows,
    }
