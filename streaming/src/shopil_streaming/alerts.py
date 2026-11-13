"""SHOPIL-25: negative-review alerts per product and event-time window.

Rule (course engine): a review with rating <= 2 is negative. When the negative
count of (product_id, window) reaches alert_threshold (3), ONE alert row is
inserted with the triggering event_id. The primary key (product_id,
window_start) makes it once-per-product-window: further negatives raise the
window counter but never a second alert. Duplicates and late reviews never
count. Everything is in the same transaction as the checkpoint, so replay
after a restart cannot emit an alert twice.
"""
from __future__ import annotations

from typing import Any


def alert_report(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"product_id": a["product_id"], "window_start": a["window_start"], "window_end": a["window_end"],
             "negative_reviews_at_alert": a["negative_reviews"], "trigger_event_id": a["trigger_event_id"],
             "emitted_at_watermark": a["emitted_at_watermark"]} for a in snapshot["alerts"]]


def negative_counters(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Current negative-review counters for every product-window (open and closed)."""
    threshold = snapshot["config"]["alert_threshold"]
    alerted = {(a["product_id"], a["window_start_ms"]) for a in snapshot["alerts"]}
    rows = [dict(w, state="open") for w in snapshot["open_review_windows"]]
    rows += [dict(w, state="closed") for w in snapshot["review_windows"]]
    return sorted(({"product_id": r["product_id"], "window_start": r["window_start"], "state": r["state"],
                    "reviews": r["reviews"], "negative_reviews": r["negative_reviews"], "threshold": threshold,
                    "alerted": (r["product_id"], r["window_start_ms"]) in alerted} for r in rows),
                  key=lambda r: (r["window_start"], r["product_id"]))
