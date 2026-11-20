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


def read_stream_snapshot(database: Path, *, window_table=None, product_id=None, alert=False) -> dict:
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
        if window_table is None:
            rows = {
                table: [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")]
                for table, order in WINDOW_TABLES
            }
        else:
            if window_table not in dict(WINDOW_TABLES) or window_table == "review_alerts":
                raise ValueError("Unknown streaming window table")
            predicate = "WHERE product_id = ?" if product_id is not None else ""
            parameters = () if product_id is None else (product_id,)
            window = connection.execute(
                f"SELECT * FROM {window_table} {predicate} ORDER BY window_start_ms DESC LIMIT 1",
                parameters,
            ).fetchone()
            rows = {table: [] for table, _ in WINDOW_TABLES}
            if window is not None:
                rows[window_table] = [dict(window)]
                if alert:
                    rows["review_alerts"] = [dict(row) for row in connection.execute(
                        "SELECT * FROM review_alerts WHERE window_start_ms = ? AND product_id = ?",
                        (window["window_start_ms"], product_id),
                    )]
        connection.commit()

    return {
        "checkpoint": meta["checkpoint"],
        "watermark_ms": meta["watermark_ms"],
        "config": json.loads(meta["config_json"]),
        "finalized": bool(meta["finalized"]),
        "counters": counters,
        "rows": rows,
    }
