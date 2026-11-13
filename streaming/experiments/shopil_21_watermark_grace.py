"""SHOPIL-21 evidence: watermark and grace windows (60 s, out_of_order 0, grace 10).

Run:  uv run python experiments/shopil_21_watermark_grace.py
Feeds the teaching fixture (source order) to a fresh StreamEngine and, in
parallel, to the independent WindowModel. After every record it compares status,
max event time, watermark and which order windows closed. Then it verifies:
  * the watermark never decreases;
  * a window closes exactly when watermark >= window_end + grace (not earlier,
    and at the first record that makes it true);
  * invalid / duplicate / late records never move max event time;
  * wall-clock waiting does not move the watermark;
  * finalize_demo closes the remaining windows as demo_end_of_input without
    changing the watermark.
"""
from __future__ import annotations

import sys
import time

from shopil_streaming.runs import fresh_db, md_table, write_json, write_text
from shopil_streaming.windows import WindowModel
from streaming_lab import StreamEngine, teaching_fixture
from streaming_lab.events import Event, iso_time

TICKET = "SHOPIL-21"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def hhmmss(ms: int | None) -> str:
    return "-" if ms is None else iso_time(ms)[11:19]


def main() -> int:
    data = teaching_fixture()
    model = WindowModel(60, 10, 0)
    rows, mismatches = [], []
    with StreamEngine(fresh_db("shopil-21"), **CONFIG) as engine:
        closed_before: set[int] = set()
        for item in data:
            result = engine.ingest(item.event, offset=item.offset)
            predicted = model.step(item.event)
            snap = engine.snapshot()
            closed_now = sorted({w["window_start_ms"] for w in snap["revenue_windows"]} - closed_before)
            closed_before |= set(closed_now)
            event = item.event if isinstance(item.event, Event) else None
            row = {"offset": item.offset, "event_id": event.event_id if event else item.event.get("event_id"),
                   "event_time": event.event_time.isoformat()[11:19] if event else "(invalid)",
                   "status": result["status"], "model_status": predicted["status"],
                   "max_event_time": hhmmss(snap["max_event_time_ms"]), "watermark": hhmmss(snap["watermark_ms"]),
                   "model_watermark": hhmmss(predicted["watermark_ms"]),
                   "closed_now": ", ".join(f"[{hhmmss(s)},{hhmmss(s + 60_000)})" for s in closed_now) or "",
                   "_wm": snap["watermark_ms"], "_max": snap["max_event_time_ms"], "_closed": closed_now,
                   "_model_closed": predicted["closed_now"]}
            if (row["status"], row["_wm"], row["_max"], closed_now) != (
                    predicted["status"], predicted["watermark_ms"], predicted["max_event_time_ms"], predicted["closed_now"]):
                mismatches.append(item.offset)
            rows.append(row)
        before_wait = engine.snapshot()["watermark_ms"]
        time.sleep(1.2)
        after_wait = engine.snapshot()["watermark_ms"]
        open_before_final = engine.snapshot()["open_windows"]
        final = engine.finalize_demo()
    emitted = final["revenue_windows"]
    wms = [r["_wm"] for r in rows if r["_wm"] is not None]
    by_start = {r: i for i, row in enumerate(rows) for r in row["_closed"]}
    first_opportunity = all(
        rows[i]["_wm"] >= s + 70_000 and (i == 0 or rows[i - 1]["_wm"] is None or rows[i - 1]["_wm"] < s + 70_000)
        for s, i in by_start.items())
    non_moving = [r for r in rows if r["status"] in ("invalid", "duplicate", "late")]
    checks = {
        "engine matches independent model on every record (status, max time, watermark, closures)": not mismatches,
        "watermark never decreases": wms == sorted(wms),
        "every watermark-closed window satisfied watermark >= end + grace at emission":
            all(w["emitted_at_watermark_ms"] >= w["window_end_ms"] + 10_000
                for w in emitted if w["closure_reason"] == "watermark"),
        "each window closed at the first record that satisfied the rule": first_opportunity,
        "invalid/duplicate/late records did not move max event time":
            all(rows[r["offset"] - 1]["_max"] == r["_max"] for r in non_moving),
        "1.2 s of wall-clock idle time did not move the watermark": before_wait == after_wait,
        "finalize_demo closed remaining windows as demo_end_of_input": bool(open_before_final) and all(
            w["closure_reason"] == "demo_end_of_input" for w in emitted
            if w["window_start_ms"] in {o["window_start_ms"] for o in open_before_final}),
        "finalize_demo did not change the watermark": final["watermark_ms"] == after_wait,
    }
    windows = [{"window": f"[{w['window_start'][11:19]}, {w['window_end'][11:19]})", "orders": w["orders"],
                "revenue_cents": w["revenue_cents"], "closure_reason": w["closure_reason"],
                "emitted_at_watermark": hhmmss(w["emitted_at_watermark_ms"]),
                "close_threshold (end+grace)": hhmmss(w["window_end_ms"] + 10_000)} for w in emitted]
    public = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    write_json(TICKET, "watermark.json", {"trace": public, "windows": windows, "checks": checks,
                                          "final_watermark": final["watermark"]})
    passed = all(checks.values())
    report = f"""# SHOPIL-21 — watermark and grace windows

Command: `uv run python experiments/shopil_21_watermark_grace.py`. Input: course teaching fixture (17 arrivals, source order). Fresh DB. Times below are UTC on 2026-11-01.

Policy: `watermark = max(previous, max_seen_event_time - 0 s)`; window `[start, start+60s)` closes when `watermark >= end + 10 s`. `model_*` columns come from `shopil_streaming.windows.WindowModel`, an independent implementation of the same rule.

## Per-record trace
{md_table(public)}

## Emitted revenue windows
{md_table(windows)}

Hand check: `[10:00:00, 10:01:00)` needs watermark ≥ 10:01:10. `order-d` (10:01:15, offset 8) is the first accepted event at or after that, so the window closes there with 2 orders (`order-a` 1000 + `order-c` 1500 = 2500 agorot = ₪25.00; the duplicate `order-a` is not counted again). Between offsets 2 and 7 the watermark sits at 10:01:05 < 10:01:10, so the window stays open and `order-c` (10:00:20, out of order) is still accepted. `order-late` (10:00:25, offset 9) then arrives after closure and is late. `order-boundary` at exactly 10:01:00 belongs to `[10:01:00, 10:02:00)` (half-open windows), which had already closed by watermark 10:02:15, so it is late too.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
