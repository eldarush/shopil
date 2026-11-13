"""SHOPIL-25 evidence: one alert per product-window at three negative reviews.

Run:  uv run python experiments/shopil_25_review_alerts.py
Input (source order, fresh DB, 60 s windows, grace 10, threshold 3):
  offsets 0-5  course review fixture: product 5 gets review-1 (1), review-2 (2),
               a RETRY of review-1, review-3 (1) -> 3rd negative -> ALERT,
               review-4 (1) -> 4th negative, counter 4, no new alert;
               review-5 is a positive review for product 3.
  offsets 6-10 extension: product 2 gets two negatives (below threshold, no
               alert); product 5 gets three negatives in the NEXT window
               [10:01, 10:02) -> a separate alert for that window.
Then the DB is reopened and every offset replayed: alerts must not change.
"""
from __future__ import annotations

from datetime import timedelta
import sys

from shopil_streaming.alerts import alert_report, negative_counters
from shopil_streaming.runs import fresh_db, md_table, write_json, write_text
from streaming_lab import Delivery, Event, StreamEngine, generate_review_fixture
from streaming_lab.events import T0, iso_time, window_start_ms

TICKET = "SHOPIL-25"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def deliveries() -> list[Delivery]:
    base = generate_review_fixture()
    def neg(key: str, second: int, product: int) -> Event:
        return Event(key, "review", T0 + timedelta(seconds=second), product, rating=1, text="לא עובד")
    extra = [neg("p2-neg-1", 20, 2), neg("p2-neg-2", 25, 2),
             neg("p5-w2-1", 66, 5), neg("p5-w2-2", 70, 5), neg("p5-w2-3", 75, 5)]
    return base + [Delivery(len(base) + i, e) for i, e in enumerate(extra)]


def main() -> int:
    data = deliveries()
    path = fresh_db("shopil-25")
    trace = []
    with StreamEngine(path, **CONFIG) as engine:
        for item in data:
            result = engine.ingest(item.event, offset=item.offset)
            snap = engine.snapshot()
            counter = next((c for c in negative_counters(snap) if c["product_id"] == item.event.product_id
                            and c["window_start"] == iso_time(window_start_ms(item.event.event_time))), None)
            trace.append({"offset": item.offset, "event_id": item.event.event_id, "product_id": item.event.product_id,
                          "rating": item.event.rating, "status": result["status"],
                          "window_negatives": counter["negative_reviews"] if counter else "",
                          "alerts_total": len(snap["alerts"])})
        alerts_live = alert_report(engine.snapshot())
    with StreamEngine(path, **CONFIG) as engine:
        replay = {engine.ingest(i.event, offset=i.offset)["status"] for i in data}
        snap = engine.snapshot()
    alerts, counters = alert_report(snap), negative_counters(snap)
    w1 = [c for c in counters if c["product_id"] == 5 and c["window_start"].endswith("10:00:00.000+00:00")]
    checks = {
        "product 5 window [10:00,10:01): exactly one alert, triggered by review-3 (3rd negative)":
            [(a["product_id"], a["trigger_event_id"], a["negative_reviews_at_alert"]) for a in alerts
             if a["window_start"].endswith("10:00:00.000+00:00")] == [(5, "review-3", 3)],
        "4th negative (review-4) raised the window counter to 4 without a second alert":
            trace[4]["window_negatives"] == 4 and trace[4]["alerts_total"] == trace[3]["alerts_total"] == 1
            and w1 and w1[0]["negative_reviews"] == 4,
        "retried review-1 is a duplicate and did not change the counter (stays 2)": trace[2]["status"] == "duplicate"
            and trace[2]["window_negatives"] == trace[1]["window_negatives"] == 2,
        "product 2 with 2 negatives (below threshold) has no alert": all(a["product_id"] != 2 for a in alerts),
        "positive review (product 3) has no alert": all(a["product_id"] != 3 for a in alerts),
        "product 5 next window gets its own single alert": [a["trigger_event_id"] for a in alerts
            if a["product_id"] == 5 and a["window_start"].endswith("10:01:00.000+00:00")] == ["p5-w2-3"],
        "reopen + replay of every offset left alerts unchanged": replay == {"replayed"} and alerts == alerts_live,
        "alerts_emitted counter == alert rows": snap["stats"]["alerts_emitted"] == len(alerts) == 2,
    }
    write_json(TICKET, "alerts.json", {"trace": trace, "alerts": alerts, "counters": counters, "checks": checks})
    passed = all(checks.values())
    report = f"""# SHOPIL-25 — negative-review alerts

Command: `uv run python experiments/shopil_25_review_alerts.py`. Fresh DB; 60 s windows, out_of_order 0, grace 10, alert threshold 3 negatives (rating ≤ 2) per product per window. Input: the course review fixture (offsets 0–5, slides 25/36/41) plus five extra reviews (offsets 6–10).

## Per-record trace
{md_table(trace)}

## review_alerts
{md_table(alerts)}

## Negative counters per product-window
{md_table(counters)}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
