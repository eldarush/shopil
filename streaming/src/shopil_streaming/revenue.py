"""SHOPIL-24: streaming revenue windows, window-state monitor and reconciliation.

Money stays integer agorot inside the engine; conversion to shekels happens only
for display (`shekels`). Accepted revenue = closed windows + still-open windows;
summing only closed windows under-reports while the stream is live.

Reconciliation is INSIDE the stream: accepted + late (side output) must equal a
full recomputation over the same valid, unique order events (`batch_truth`,
which ignores arrival order). This is NOT the ShopIL historical Batch revenue
(fct_orders, status='paid', ILS): stream order events have no payment status,
so the two are reported separately and never summed.
"""
from __future__ import annotations

from typing import Any, Iterable

from streaming_lab import Delivery, batch_truth
from streaming_lab.events import iso_time

from .runs import shekels

SCOPE_NOTE = ("Stream reconciliation only: accepted + late vs. a full recomputation of the same valid, unique "
              "stream order events. Not comparable to or additive with Batch paid revenue (fct_orders).")


def revenue_windows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    grace_ms = snapshot["config"]["grace_seconds"] * 1000
    rows = []
    for w in snapshot["revenue_windows"]:
        rows.append({"window_start": w["window_start"], "window_end": w["window_end"], "state": "closed",
                     "orders": w["orders"], "revenue_cents": w["revenue_cents"], "revenue_ils": shekels(w["revenue_cents"]),
                     "closure_reason": w["closure_reason"], "emitted_at_watermark": w["emitted_at_watermark"],
                     "closes_at_watermark": iso_time(w["window_end_ms"] + grace_ms)})
    for w in snapshot["open_windows"]:
        rows.append({"window_start": w["window_start"], "window_end": w["window_end"], "state": "open",
                     "orders": w["orders"], "revenue_cents": w["revenue_cents"], "revenue_ils": shekels(w["revenue_cents"]),
                     "closure_reason": "", "emitted_at_watermark": "",
                     "closes_at_watermark": iso_time(w["window_end_ms"] + grace_ms)})
    return sorted(rows, key=lambda r: r["window_start"])


def window_monitor(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Compact live view: checkpoint, watermark and per-state totals."""
    rows = revenue_windows(snapshot)
    closed = [r for r in rows if r["state"] == "closed"]
    open_ = [r for r in rows if r["state"] == "open"]
    return {"checkpoint": snapshot["checkpoint"], "watermark": snapshot["watermark"], "finalized": snapshot["finalized"],
            "closed_windows": len(closed), "closed_revenue_ils": shekels(sum(r["revenue_cents"] for r in closed)),
            "open_windows": len(open_), "open_revenue_ils": shekels(sum(r["revenue_cents"] for r in open_)),
            "accepted_revenue_ils": shekels(snapshot["totals"]["revenue_cents"]),
            "next_close_at_watermark": min((r["closes_at_watermark"] for r in open_), default=None)}


def reconcile(snapshot: dict[str, Any], deliveries: Iterable[Delivery]) -> dict[str, Any]:
    truth = batch_truth(list(deliveries))
    t = snapshot["totals"]
    accepted, late = t["revenue_cents"], t["late_order_cents"]
    return {"accepted_orders": t["orders"], "accepted_cents": accepted, "accepted_ils": shekels(accepted),
            "late_orders": t["late_orders"], "late_cents": late, "late_ils": shekels(late),
            "stream_total_ils": shekels(accepted + late),
            "recomputed_orders": truth["orders"], "recomputed_cents": truth["revenue_cents"],
            "recomputed_ils": shekels(truth["revenue_cents"]),
            "orders_match": t["orders"] + t["late_orders"] == truth["orders"],
            "revenue_match": accepted + late == truth["revenue_cents"], "scope": SCOPE_NOTE}
