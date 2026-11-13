"""Compose fixed historical scope and a fresh consistent stream snapshot."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
import sqlite3

import duckdb

from data_access.batch import read_batch_sources
from data_access.evidence import EvidencePackage, EvidenceSource
from data_access.streaming import read_stream_snapshot

from .model import ModelError

MAX_SOURCE_ROWS = 256
MAX_REVIEW_TEXT_ROWS = 12
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _time(milliseconds):
    return None if milliseconds is None else _EPOCH + timedelta(milliseconds=milliseconds)


async def capture_stream(database: Path, *, latest_windows: int = 3) -> dict:
    """Read once and return ``sources`` plus ``stream`` metadata for composition.

    Scope is the latest N distinct event-time window starts across all five
    tables, keeping every product in those windows. Older windows are explicitly
    omitted; these are not all-time revenue totals. More than 256 selected rows
    fails closed instead of silently dropping products. Counters remain labeled
    as all-time engine metadata. Empty streams have no invented date bounds.

    The existing reader materializes its full SQL snapshot before this adapter
    bounds model evidence; push filtering into that reader if storage grows.
    """
    if type(latest_windows) is not int or latest_windows < 1:
        raise ValueError("latest_windows must be a positive integer")
    try:
        snapshot = await asyncio.to_thread(read_stream_snapshot, Path(database))
        width = snapshot["config"]["window_seconds"]
        if type(width) is not int or width <= 0:
            raise ValueError("Invalid window width")
        width_ms = width * 1000
        starts = sorted({row["window_start_ms"] for rows in snapshot["rows"].values() for row in rows})
        selected = set(starts[-latest_windows:])
        rows = {table: [dict(row) for row in values if row["window_start_ms"] in selected]
                for table, values in snapshot["rows"].items()}
        if sum(map(len, rows.values())) > MAX_SOURCE_ROWS:
            raise ModelError("Selected streaming windows exceed the evidence row limit; reduce latest_windows")
        watermark = _time(snapshot["watermark_ms"])
        sources = []
        scope = f"latest {latest_windows} distinct event-time windows across stream tables; all products retained"

        def add(table, values, state, metric, units, identifiers):
            if not values:
                return
            normalized = []
            for original in values:
                row = dict(original)
                row.setdefault("window_end_ms", row["window_start_ms"] + width_ms)
                row["window_start"] = _time(row["window_start_ms"]).isoformat()
                row["window_end"] = _time(row["window_end_ms"]).isoformat()
                if "emitted_at_watermark_ms" in row:
                    emitted = _time(row["emitted_at_watermark_ms"])
                    row["emitted_at_watermark"] = emitted.isoformat() if emitted else None
                if "revenue_cents" in row:
                    row["revenue_ils"] = format(Decimal(row["revenue_cents"]) / 100, ".2f")
                if table == "review_alerts":
                    row["negative_reviews_at_trigger"] = row.pop("negative_reviews")
                normalized.append(row)
            sources.append(EvidenceSource(
                table=table, metric=f"{metric}; scope: {scope}", units=units,
                identifiers=identifiers,
                time_start=_time(min(row["window_start_ms"] for row in normalized)),
                time_end=_time(max(row["window_end_ms"] for row in normalized)),
                rows=normalized, checkpoint=snapshot["checkpoint"], watermark=watermark,
                window_state=state,
            ))

        for table, state in (("order_state", "open"), ("realtime_revenue", "closed")):
            add(table, rows[table], state,
                "accepted stream order amounts per window; no payment status; not historical paid revenue",
                "revenue_cents: integer agorot; revenue_ils: ILS; orders: count", ("window_start_ms",))
        for table, state in (("review_state", "open"), ("review_windows", "closed")):
            add(table, rows[table], state, "accepted reviews and current negative count (rating <= 2) per product/window",
                "reviews and negative_reviews: counts", ("window_start_ms", "product_id"))
        open_reviews = {(row["window_start_ms"], row["product_id"]) for row in rows["review_state"]}
        closed_reviews = {(row["window_start_ms"], row["product_id"]) for row in rows["review_windows"]}
        alert_groups = {"open": [], "closed": []}
        for row in rows["review_alerts"]:
            key = (row["window_start_ms"], row["product_id"])
            if (key in open_reviews) == (key in closed_reviews):
                raise ValueError("Alert has no unambiguous review window")
            alert_groups["open" if key in open_reviews else "closed"].append(row)
        for state, alerts in alert_groups.items():
            add("review_alerts", alerts, state,
                "once-per-product/window alert; negative_reviews_at_trigger is the count at emission, not the current count",
                "negative_reviews_at_trigger: count", ("window_start_ms", "product_id", "trigger_event_id"))
        return {"sources": tuple(sources), "stream": {
            "checkpoint": snapshot["checkpoint"], "watermark": watermark.isoformat() if watermark else None,
            "config": snapshot["config"], "finalized": snapshot["finalized"],
            "counters": snapshot["counters"], "counters_scope": "all-time engine counters, not selected-window totals",
            "scope": scope, "selected_window_starts": [_time(start).isoformat() for start in sorted(selected)],
            "omitted_window_count": len(starts) - len(selected),
            "empty_tables_in_scope": [table for table, values in rows.items() if not values],
        }}
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError, OverflowError):
        raise ModelError("Unable to read valid streaming evidence") from None


def build_capture(
    warehouse: Path,
    stream_database: Path,
    *,
    start: date,
    end: date,
    latest_windows: int = 3,
) -> Callable[[str, list[dict[str, str]]], Awaitable[dict]]:
    """Build a per-question capture for a completed, no-longer-written warehouse.

    Dates fix the paid aggregate/review-text scope; the full product mart retains
    its own coverage and every product row. Questions/history never generate SQL
    or select product IDs. History is interpreted by the Actor using this fresh
    evidence. The latest twelve cleaned review texts are a labeled global sample,
    not the complete review list or a product-balanced sample. Both readers still
    materialize their full query results before these evidence limits are applied.

    Each call reads each database once, then creates one EvidencePackage. The
    completed batch and one SQLite read are not a cross-database transaction;
    startup must finish batch writes before chat begins. ChatService freezes the
    returned package and metadata for all attempts of that question.
    """
    if type(start) is not date or type(end) is not date or start >= end:
        raise ValueError("Expected a nonempty range of calendar dates")
    if type(latest_windows) is not int or latest_windows < 1:
        raise ValueError("latest_windows must be a positive integer")
    warehouse, stream_database = Path(warehouse).resolve(), Path(stream_database).resolve()

    async def capture(question: str, history: list[dict[str, str]]) -> dict:
        try:
            historical = await asyncio.to_thread(read_batch_sources, warehouse, start=start, end=end)
            batch_sources = []
            review_sample = None
            for source in historical:
                if source.table == "staging.stg_reviews":
                    available = len(source.rows)
                    selected = min(available, MAX_REVIEW_TEXT_ROWS)
                    review_sample = {
                        "available": available, "selected": selected, "omitted": available - selected,
                        "limit": MAX_REVIEW_TEXT_ROWS,
                        "selection": "latest by created_at then review_id across all products; not product-balanced",
                    }
                    fields = source.model_dump(exclude={"rows"})
                    fields["rows"] = [dict(row) for row in source.rows[-MAX_REVIEW_TEXT_ROWS:]]
                    fields["metric"] = (
                        f"{source.metric}; latest text sample across all products in configured date range; "
                        f"available={available}; selected={selected}; omitted={available - selected}; "
                        "not the complete review list when rows are omitted"
                    )
                    source = EvidenceSource(**fields)
                batch_sources.append(source)
        except (duckdb.Error, OSError, ValueError):
            raise ModelError("Unable to read valid historical evidence") from None

        stream = await capture_stream(stream_database, latest_windows=latest_windows)
        package = EvidencePackage(question=question, sources=tuple(batch_sources) + stream["sources"])
        payload = package.model_dump(mode="json")
        payload["stream"] = stream["stream"]
        payload["batch"] = {
            "time_start": start.isoformat(), "time_end": end.isoformat(),
            "scope": "fixed [start,end) paid aggregate/review text; full product mart has independent source coverage",
            "review_text_sample": review_sample,
            "cross_source_product_mapping": "unverified; sources remain separate",
        }
        return payload

    return capture
