"""SHOPIL-24 evidence: revenue windows in shekels, live window state, reconciliation.

Run:  uv run python experiments/shopil_24_revenue_reconciliation.py
  1. Teaching fixture (17 deliveries): mid-stream monitor after offset 12, then
     finalize -> 122.00 ILS accepted, 13.00 ILS late, 135.00 ILS full recompute.
  2. Course generator (300 orders + 17 retries, seed 7, grace 10): accepted
     57,954.30 + late = full recompute 61,241.70.
Fresh DB per run; 60 s windows, out_of_order 0, grace 10.
"""
from __future__ import annotations

import sys

from shopil_streaming.revenue import SCOPE_NOTE, reconcile, revenue_windows, window_monitor
from shopil_streaming.runs import fresh_db, md_table, write_json, write_text
from streaming_lab import StreamEngine, generate_order_fixture, teaching_fixture
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-24"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}
COLS = ["window_start", "window_end", "state", "orders", "revenue_cents", "revenue_ils", "closure_reason",
        "emitted_at_watermark", "closes_at_watermark"]


def main() -> int:
    teaching = teaching_fixture()
    with StreamEngine(fresh_db("shopil-24-teaching"), **CONFIG) as engine:
        for item in teaching[:13]:
            engine.ingest(item.event, offset=item.offset)
        mid = engine.snapshot()
        mid_monitor, mid_windows = window_monitor(mid), revenue_windows(mid)
        for item in teaching[13:]:
            engine.ingest(item.event, offset=item.offset)
        final = engine.finalize_demo()
    t_rec, t_windows = reconcile(final, teaching), revenue_windows(final)

    orders = generate_order_fixture(300)
    with StreamEngine(fresh_db("shopil-24-orders300"), **CONFIG) as engine:
        for item in orders:
            engine.ingest(item.event, offset=item.offset)
        big = engine.finalize_demo()
    g_rec = reconcile(big, orders)

    checks = {
        "teaching: accepted 122.00 ILS": t_rec["accepted_ils"] == "122.00",
        "teaching: late 13.00 ILS": t_rec["late_ils"] == "13.00",
        "teaching: full recompute of valid unique orders 135.00 ILS == accepted + late":
            t_rec["recomputed_ils"] == "135.00" and t_rec["revenue_match"] and t_rec["orders_match"],
        "teaching: closed window revenue sums to accepted after finalize":
            sum(w["revenue_cents"] for w in t_windows) == final["totals"]["revenue_cents"],
        "mid-stream: accepted revenue includes open windows (closed-only would under-report)":
            mid["totals"]["revenue_cents"] == sum(w["revenue_cents"] for w in mid_windows)
            and any(w["state"] == "open" for w in mid_windows),
        "orders300: accepted 57,954.30 ILS (grace 10 reference)": g_rec["accepted_ils"] == "57,954.30",
        "orders300: accepted + late == recompute 61,241.70 ILS":
            g_rec["recomputed_ils"] == "61,241.70" and g_rec["revenue_match"] and g_rec["orders_match"],
    }
    write_json(TICKET, "revenue.json", {"teaching": {"reconciliation": t_rec, "windows": t_windows,
                                                     "mid_stream_monitor": mid_monitor, "mid_stream_windows": mid_windows,
                                                     "canonical_results": canonical_results(final)},
                                        "orders300": {"reconciliation": g_rec, "windows": revenue_windows(big)},
                                        "checks": checks})
    passed = all(checks.values())
    rec_cols = ["accepted_orders", "accepted_ils", "late_orders", "late_ils", "stream_total_ils",
                "recomputed_orders", "recomputed_ils", "orders_match", "revenue_match"]
    report = f"""# SHOPIL-24 — revenue windows and reconciliation

Command: `uv run python experiments/shopil_24_revenue_reconciliation.py`. Fresh DB per run; 60 s windows, out_of_order 0, grace 10. Amounts are integer agorot in the engine and converted to ₪ only for display.

> {SCOPE_NOTE}

## Teaching fixture — mid-stream (after offset 12, before finalize)
Monitor: {md_table([mid_monitor])}

{md_table(mid_windows, COLS)}

## Teaching fixture — after finalize_demo
{md_table(t_windows, COLS)}

Reconciliation: {md_table([t_rec], rec_cols)}

Hand check: accepted = order-a 10.00 + order-c 15.00 + order-b 20.00 + order-d 30.00 + order-e 25.00 + order-f 10.00 + order-h 12.00 = ₪122.00; late = order-late 5.00 + order-boundary 8.00 = ₪13.00; duplicates (order-a, order-late retries) and bad-money (19.9, invalid) are excluded from both sides → 9 valid unique orders = ₪135.00.

## Course generator — 300 orders, 17 retries, seed 7
{md_table([g_rec], rec_cols)}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
