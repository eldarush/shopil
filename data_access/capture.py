"""Compose one question's evidence from the completed batch and live stream."""

import asyncio
import sqlite3

import duckdb
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .batch import read_batch_sources
from .evidence import EvidencePackage, EvidenceSource
from .streaming import read_stream_snapshot


def _timestamp(milliseconds: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)


def _capture(batch: Path, stream: Path, start: date, end: date,
             product_id: int | None, question: str) -> dict:
    historical = read_batch_sources(batch, start=start, end=end, product_id=product_id)
    snapshot = read_stream_snapshot(stream)
    watermark = snapshot["watermark_ms"]
    width = snapshot["config"]["window_seconds"] * 1000
    sources = list(historical)
    for table, rows in snapshot["rows"].items():
        for row in rows:
            window_start = row["window_start_ms"]
            window_end = row.get("window_end_ms", window_start + width)
            is_closed = table in ("realtime_revenue", "review_windows")
            if table == "review_alerts":
                is_closed = any(
                    item["window_start_ms"] == window_start
                    and item["product_id"] == row["product_id"]
                    for item in snapshot["rows"]["review_windows"]
                )
            sources.append(EvidenceSource(
                table=table, metric="stream_window", units="agorot and counts",
                identifiers=("product_id",) if "product_id" in row else (),
                time_start=_timestamp(window_start), time_end=_timestamp(window_end),
                rows=[row], checkpoint=snapshot["checkpoint"],
                watermark=_timestamp(watermark) if watermark is not None else None,
                window_state="closed" if is_closed else "open",
            ))
    # Preserve progress even before the first window exists.
    if not snapshot["rows"]["order_state"] and not any(snapshot["rows"].values()):
        start_time = datetime.now(UTC)
        sources.append(EvidenceSource(
            table="stream_meta", metric="stream_progress", units="offset and UTC milliseconds",
            identifiers=(), time_start=start_time, time_end=start_time + timedelta(microseconds=1),
            rows=[{"checkpoint": snapshot["checkpoint"], "watermark_ms": watermark}],
            checkpoint=snapshot["checkpoint"],
            watermark=_timestamp(watermark) if watermark is not None else None,
        ))
    return EvidencePackage(question=question, sources=tuple(sources)).model_dump(mode="json")


def make_capture(batch: Path, stream: Path, *, start: date, end: date,
                 product_id: int | None = None):
    """Build an async ChatService capture callable for the configured databases."""
    async def capture(question: str, history: list[dict[str, str]]) -> dict:
        try:
            return await asyncio.to_thread(_capture, batch, stream, start, end, product_id, question)
        except (duckdb.Error, sqlite3.Error) as exc:
            raise OSError("Could not read evidence databases") from exc

    return capture
