"""SHOPIL-40 evidence: window boundaries, a hand-computed window, and grace 0/10/120.

Run:  uv run python experiments/shopil_40_window_grace.py
Part 1 - hand calculation of window [10:00:00, 10:01:00) on the teaching fixture.
Part 2 - boundary cases on a tiny hand-built input (fresh DB):
         10:00:59.999 belongs to [10:00,10:01), 10:01:00.000 to [10:01,10:02);
         watermark 10:01:09.999 keeps [10:00,10:01) open, 10:01:10.000 closes it;
         an event for the closed window is late and goes to late_events.
Part 3 - grace policy change: the committed orders300 fixture (same input, same
         order, other parameters fixed) with grace 0, 10 and 120 s, fresh DB each.
Part 4 - reconciliation per grace: accepted + late == full recompute (same valid unique events).
"""
from __future__ import annotations

from datetime import timedelta
import sys

from shopil_streaming.fixtures import fresh_engine, load_fixture
from shopil_streaming.runs import md_table, shekels, write_json, write_text
from streaming_lab import Delivery, Event, batch_truth
from streaming_lab.events import T0, epoch_ms, iso_time

TICKET = "SHOPIL-40"
BASE = {"window_seconds": 60, "out_of_order_seconds": 0, "alert_threshold": 3}


def hm(ms) -> str:
    return iso_time(ms)[11:23] if ms is not None else "-"


def part1() -> dict:
    data = load_fixture("teaching")
    w_start, w_end, grace = epoch_ms(T0), epoch_ms(T0) + 60_000, 10_000
    # Manual: walk the deliveries in source order with pen-and-paper rules.
    seen, wm, members, late, closed_at = set(), None, [], [], None
    for d in data:
        e = d.event
        if not isinstance(e, Event) or e.event_id in seen:
            continue
        seen.add(e.event_id)
        t = epoch_ms(e.event_time)
        in_window = w_start <= t < w_end
        if in_window and closed_at is not None:
            late.append(e.event_id)
            continue
        wm = t if wm is None else max(wm, t)
        if in_window and e.kind == "order":
            members.append((e.event_id, e.amount_cents))
        if closed_at is None and wm >= w_end + grace:
            closed_at = (d.offset, e.event_id, wm)
    manual = {"orders": len(members), "revenue_cents": sum(c for _, c in members), "members": members,
              "closed_at_offset": closed_at[0], "closed_by": closed_at[1], "closed_at_watermark": hm(closed_at[2]),
              "late_for_this_window": late}
    with fresh_engine("shopil-40-hand", grace_seconds=10, **BASE) as engine:
        trace = {}
        for d in data:
            before = len(engine.snapshot()["revenue_windows"])
            engine.ingest(d.event, offset=d.offset)
            if len(engine.snapshot()["revenue_windows"]) > before and "closed_offset" not in trace:
                trace["closed_offset"] = d.offset
        snap = engine.snapshot()
    win = next(w for w in snap["revenue_windows"] if w["window_start_ms"] == w_start)
    engine_view = {"orders": win["orders"], "revenue_cents": win["revenue_cents"],
                   "closed_at_offset": trace["closed_offset"], "closed_at_watermark": hm(win["emitted_at_watermark_ms"]),
                   "late_for_this_window": [r["event_id"] for r in snap["late_events"] if r["window_start_ms"] == w_start]}
    return {"manual": manual, "engine": engine_view,
            "match": (manual["orders"], manual["revenue_cents"], manual["closed_at_offset"], manual["late_for_this_window"])
            == (engine_view["orders"], engine_view["revenue_cents"], engine_view["closed_at_offset"],
                engine_view["late_for_this_window"])}


def part2() -> dict:
    def order(key, delta):
        return Event(key, "order", T0 + delta, 1, amount_cents=100)
    steps = [
        ("last ms of window 1", order("b-0059999", timedelta(seconds=59, milliseconds=999))),
        ("exact start of window 2", order("b-0100000", timedelta(seconds=60))),
        ("watermark one ms before end+grace", order("b-0109999", timedelta(seconds=69, milliseconds=999))),
        ("watermark == end+grace (closes window 1)", order("b-0110000", timedelta(seconds=70))),
        ("event for closed window 1 (late)", order("b-late", timedelta(seconds=30))),
        ("event for still-open window 2", order("b-0115000", timedelta(seconds=75))),
    ]
    rows = []
    with fresh_engine("shopil-40-boundary", grace_seconds=10, **BASE) as engine:
        for offset, (label, event) in enumerate(steps):
            result = engine.ingest(event, offset=offset)
            snap = engine.snapshot()
            rows.append({"offset": offset, "case": label, "event_time": hm(epoch_ms(event.event_time)),
                         "status": result["status"], "window_start": (result.get("window_start") or "")[11:19],
                         "watermark": hm(snap["watermark_ms"]),
                         "closed_windows": [w["window_start"][11:19] for w in snap["revenue_windows"]]})
        late = [{"offset": r["source_offset"], "event_id": r["event_id"], "event_time": r["event_time"],
                 "window_start": r["window_start"], "watermark": r["watermark"], "reason": r["reason"]}
                for r in snap["late_events"]]
    ok = ([r["window_start"] for r in rows[:2]] == ["10:00:00", "10:01:00"]
          and rows[2]["closed_windows"] == [] and rows[3]["closed_windows"] == ["10:00:00"]
          and rows[4]["status"] == "late" and rows[5]["status"] == "accepted" and len(late) == 1)
    return {"rows": rows, "late_events": late, "ok": ok}


def part3() -> list[dict]:
    data = load_fixture("orders300")
    truth = batch_truth(data)
    rows = []
    for grace in (0, 10, 120):
        with fresh_engine(f"shopil-40-grace{grace}", grace_seconds=grace, **BASE) as engine:
            for d in data:
                engine.ingest(d.event, offset=d.offset)
            live = engine.snapshot()
            final = engine.finalize_demo()
        by_wm = [w for w in final["revenue_windows"] if w["closure_reason"] == "watermark"]
        lags = [(w["emitted_at_watermark_ms"] - w["window_end_ms"]) / 1000 for w in by_wm]
        t = final["totals"]
        rows.append({"grace_s": grace, "accepted_orders": t["orders"], "late_orders": t["late_orders"],
                     "accepted_ils": shekels(t["revenue_cents"]), "late_ils": shekels(t["late_order_cents"]),
                     "windows_closed_by_watermark": len(by_wm),
                     "windows_open_at_end_of_input": len(live["open_windows"]),
                     "avg_close_after_window_end_s": round(sum(lags) / len(lags), 1) if lags else None,
                     "min_close_after_window_end_s": min(lags) if lags else None,
                     "accepted_plus_late_ils": shekels(t["revenue_cents"] + t["late_order_cents"]),
                     "full_recompute_ils": shekels(truth["revenue_cents"]),
                     "reconciles": t["revenue_cents"] + t["late_order_cents"] == truth["revenue_cents"]})
    return rows


def main() -> int:
    p1, p2, p3 = part1(), part2(), part3()
    g = {r["grace_s"]: r for r in p3}
    checks = {
        "part 1: hand calculation == engine (2 orders, 25.00 ILS, closed at offset 8, late order-late)": p1["match"]
            and p1["manual"]["revenue_cents"] == 2500,
        "part 2: half-open boundary and inclusive end+grace closure behave as specified": p2["ok"],
        "part 3: reference late counts 21 / 14 / 0 for grace 0 / 10 / 120":
            (g[0]["late_orders"], g[10]["late_orders"], g[120]["late_orders"]) == (21, 14, 0),
        "part 3: reference accepted 56,916.90 / 57,954.30 / 61,241.70 ILS":
            (g[0]["accepted_ils"], g[10]["accepted_ils"], g[120]["accepted_ils"]) == ("56,916.90", "57,954.30", "61,241.70"),
        "part 3: larger grace closes windows later (every close lag >= grace)":
            all(g[x]["min_close_after_window_end_s"] >= x for x in g),
        "part 3: larger grace leaves more windows open at end of input":
            g[0]["windows_open_at_end_of_input"] <= g[10]["windows_open_at_end_of_input"] <= g[120]["windows_open_at_end_of_input"],
        "part 4: accepted + late == full recompute for every grace": all(r["reconciles"] for r in p3),
    }
    write_json(TICKET, "window_grace.json", {"hand_window": p1, "boundary": p2, "grace": p3, "checks": checks})
    passed = all(checks.values())
    m = p1["manual"]
    report = f"""# SHOPIL-40 — window boundaries and grace policy

Command: `uv run python experiments/shopil_40_window_grace.py`. Fresh DB per run. Fixed: 60 s windows, out_of_order 0, alert threshold 3. Only grace changes in part 3.

## 1. Hand calculation: window [10:00:00, 10:01:00) on the teaching fixture (grace 10)
Rule: the window closes when watermark ≥ 10:01:00 + 10 s = 10:01:10.
Walking the deliveries in source order: order-a 10:00:05 (+10.00), review-1, order-b 10:01:05 (other window, watermark → 10:01:05 < 10:01:10, still open), order-c 10:00:20 (+15.00, accepted because the window is still open), reviews, the order-a retry (duplicate), then order-d 10:01:15 moves the watermark to 10:01:15 ≥ 10:01:10 → **closed at offset 8**. order-late 10:00:25 arrives at offset 9, after closure → late.

| | orders | revenue | closed at offset | closed at watermark | late for this window |
|---|---|---|---|---|---|
| by hand | {m['orders']} | ₪{shekels(m['revenue_cents'])} | {m['closed_at_offset']} | {m['closed_at_watermark']} | {m['late_for_this_window']} |
| engine | {p1['engine']['orders']} | ₪{shekels(p1['engine']['revenue_cents'])} | {p1['engine']['closed_at_offset']} | {p1['engine']['closed_at_watermark']} | {p1['engine']['late_for_this_window']} |

## 2. Boundary cases (hand-built input, grace 10)
{md_table(p2['rows'])}

late_events output:
{md_table(p2['late_events'])}

## 3–4. Grace policy change on the same input (orders300 fixture, 317 deliveries, source order)
{md_table(p3)}

**Explanation.** A window is emitted once the watermark passes `end + grace`. With grace 0, windows close as soon as event time reaches their end, so every out-of-order order that arrives later (the fixture delays ~10 % of orders by 20–90 s) is late: 21 late orders. Grace 10 waits 10 s longer, which rescues the orders delayed by less than that margin past the window end: 14 late. Grace 120 is longer than the largest delay, so nothing is late, but each window is emitted at least 120 s of event time after it ends, and more windows are still open when input stops (they only close via `finalize_demo`). The trade-off is completeness against latency. In every case accepted + late equals the full recomputation of the same valid, unique events.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
