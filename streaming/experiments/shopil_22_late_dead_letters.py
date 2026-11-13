"""SHOPIL-22 evidence: late events and dead letters with input, offset and reason.

Run:  uv run python experiments/shopil_22_late_dead_letters.py
Input: the course teaching fixture (offsets 0-16, source order) plus two extra
deliveries appended at offsets 17-18: a late NEGATIVE review for product 5 and a
naive-timestamp order. Fresh DB, 60 s windows, out_of_order 0, grace 10.
Writes evidence/SHOPIL-22/{README.md, side_outputs.json, late_events.csv, dead_letters.csv}.
"""
from __future__ import annotations

from datetime import timedelta
import sys

from shopil_streaming.contract import dead_letter_report
from shopil_streaming.runs import EVIDENCE_DIR, fresh_db, md_table, write_json, write_text
from shopil_streaming.side_outputs import export_side_outputs, late_report
from streaming_lab import Delivery, Event, StreamEngine, teaching_fixture
from streaming_lab.events import T0

TICKET = "SHOPIL-22"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def deliveries() -> list[Delivery]:
    extra = [
        Event("review-late", "review", T0 + timedelta(seconds=30), 5, rating=1, text="איחור בביקורת"),
        {"event_id": "order-naive", "kind": "order", "event_time": "2026-11-01T10:03:20", "product_id": 5,
         "amount_cents": 700},
    ]
    base = teaching_fixture()
    return base + [Delivery(len(base) + i, e) for i, e in enumerate(extra)]


def main() -> int:
    data = deliveries()
    path = fresh_db("shopil-22")
    trace = []
    with StreamEngine(path, **CONFIG) as engine:
        for item in data:
            before = engine.snapshot()
            result = engine.ingest(item.event, offset=item.offset)
            after = engine.snapshot()
            trace.append({"offset": item.offset, "status": result["status"], "checkpoint": after["checkpoint"],
                          "late_rows": len(after["late_events"]), "dead_rows": len(after["dead_letters"]),
                          "accepted_revenue_cents": after["totals"]["revenue_cents"],
                          "watermark_unchanged": before["watermark_ms"] == after["watermark_ms"]})
    with StreamEngine(path, **CONFIG) as reopened:          # side outputs are durable
        snap = reopened.snapshot()
    late, dead = late_report(snap), dead_letter_report(snap)
    paths = export_side_outputs(snap, EVIDENCE_DIR / TICKET)
    side = {r["offset"] for r in trace if r["status"] in ("late", "invalid")}
    checks = {
        "late_events: offsets 9 (order-late), 15 (order-boundary), 17 (review-late)":
            [r["offset"] for r in late] == [9, 15, 17],
        "dead_letters: offsets 11 (float money), 18 (naive time)": [d["offset"] for d in dead] == [11, 18],
        "every side-output row has input, offset and reason":
            all(r["input"] and r["reason"] and r["offset"] is not None for r in late + dead),
        "late rows record the watermark at arrival and it was >= window_end + grace":
            all(r["watermark_at_arrival"] >= r["window_end"] for r in late) and all(
                row["watermark_ms"] >= row["window_end_ms"] + 10_000 for row in snap["late_events"]),
        "checkpoint advanced past every late and invalid record": all(
            r["checkpoint"] == r["offset"] for r in trace if r["offset"] in side),
        "late/invalid records did not change accepted revenue or the watermark": all(
            r["watermark_unchanged"] and r["accepted_revenue_cents"] == trace[r["offset"] - 1]["accepted_revenue_cents"]
            for r in trace if r["offset"] in side),
        "retry of a late event (offset 14) is a duplicate, not a second late row":
            trace[14]["status"] == "duplicate" and sum(r["event_id"] == "order-late" for r in late) == 1,
        "late negative review not counted toward the product-5 window/alert":
            snap["totals"]["negative_reviews"] == 4 and snap["totals"]["late_reviews"] == 1,
        "late order side output totals 13.00 ILS (500 + 800 agorot)": snap["totals"]["late_order_cents"] == 1300,
        "side outputs survive reopening the DB": len(late) == 3 and len(dead) == 2,
    }
    write_json(TICKET, "side_outputs.json", {"trace": trace, "late_events": late, "dead_letters": dead,
                                             "checks": checks, "checkpoint": snap["checkpoint"]})
    passed = all(checks.values())
    report = f"""# SHOPIL-22 — late events and dead letters

Command: `uv run python experiments/shopil_22_late_dead_letters.py`. Input: course teaching fixture (offsets 0–16) plus a late negative review (offset 17) and a naive-timestamp order (offset 18), in source order, on a fresh DB. Config: 60 s windows, out_of_order 0, grace 10. Exported to [`late_events.csv`]({paths['late_events'].name}) and [`dead_letters.csv`]({paths['dead_letters'].name}).

## late_events (valid, first-seen, window already closed)
{md_table(late, ['offset', 'event_id', 'kind', 'event_time', 'window_start', 'window_end', 'watermark_at_arrival', 'amount_cents', 'reason', 'input'])}

## dead_letters (contract violations)
{md_table(dead, ['offset', 'reason', 'input'])}

## Checkpoint progression
{md_table(trace)}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
