"""SHOPIL-41 evidence: duplicate event_ids, replayed offsets and restart change nothing.

Run:  uv run python experiments/shopil_41_dedup_offsets_restart.py
Input: committed orders300 fixture (300 orders + 17 producer retries at new
offsets), teaching config, fresh DB per scenario.

  A. retries       - the 17 retried event_ids are duplicates; revenue equals
                     the sum over unique event_ids.
  B. altered retry - event ev-0000 re-sent at a NEW offset with a different
                     amount: still a duplicate, revenue unchanged (first wins).
  C. restart       - a run_live stream is CANCELLED mid-run (task cancel, slow
                     sink); the DB is reopened, the producer resumes from the
                     durable checkpoint (engine.next_offset) and the final
                     result equals an uninterrupted run.
  D. replay        - after reopening, all 317 saved offsets are delivered again:
                     every one is "replayed" and every durable table is unchanged.
"""
from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
import sqlite3
import sys

from shopil_streaming.fixtures import TEACHING_CONFIG, load_fixture
from shopil_streaming.runs import fresh_db, md_table, shekels, write_json, write_text
from streaming_lab import Delivery, Event, StreamEngine, batch_truth, run_live
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-41"


def dump(path: Path) -> dict:
    """Committed contents of every table via a separate read-only connection."""
    con = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {t: con.execute(f"SELECT * FROM {t} ORDER BY 1, 2").fetchall() for t in tables}
    finally:
        con.close()


def sequential(path: Path, deliveries) -> list[str]:
    with StreamEngine(path, **TEACHING_CONFIG) as engine:
        return [engine.ingest(d.event, offset=d.offset)["status"] for d in deliveries]


def main() -> int:
    data = load_fixture("orders300")
    truth = batch_truth(data)

    # A + B
    path_ab = fresh_db("shopil-41-dedup")
    altered = Delivery(len(data), Event("ev-0000", "order", data[0].event.event_time, data[0].event.product_id,
                                        amount_cents=999_999, order_id=5000))
    statuses = sequential(path_ab, data + [altered])
    with StreamEngine(path_ab, **TEACHING_CONFIG) as engine:
        snap_ab = engine.snapshot()
        first_ev0000 = engine.con.execute("SELECT source_offset, payload_json FROM seen_events WHERE event_id='ev-0000'").fetchone()
    dup_offsets = [d.offset for d, s in zip(data, statuses) if s == "duplicate"]
    accepted_plus_late = snap_ab["totals"]["revenue_cents"] + snap_ab["totals"]["late_order_cents"]

    # C: cancelled live run, then restart from the durable checkpoint
    reference_path = fresh_db("shopil-41-reference")
    sequential(reference_path, data)
    with StreamEngine(reference_path, **TEACHING_CONFIG) as engine:
        expected = canonical_results(engine.finalize_demo())
    path_c = fresh_db("shopil-41-restart")
    async def cancelled_run() -> int:
        with StreamEngine(path_c, **TEACHING_CONFIG) as engine:
            task = asyncio.create_task(run_live(engine, data, duration_s=None, interval_s=0.0,
                                                processing_delay_s=0.002, queue_maxsize=8))
            await asyncio.sleep(0.25)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            return engine.checkpoint
    checkpoint_at_stop = asyncio.run(cancelled_run())
    async def resumed_run() -> tuple[int, dict, dict]:
        with StreamEngine(path_c, **TEACHING_CONFIG) as engine:   # fresh Python object, durable state only
            resume_from = engine.next_offset
            state = await run_live(engine, data[resume_from:], duration_s=None, interval_s=0.0, finalize=True)
            return resume_from, state, canonical_results(state["snapshot"])
    resume_from, resumed_state, restarted = asyncio.run(resumed_run())

    # D: replay every saved offset after reopen
    before = dump(reference_path)
    with StreamEngine(reference_path, **TEACHING_CONFIG) as engine:
        replay = {engine.ingest(d.event, offset=d.offset)["status"] for d in data}
    after = dump(reference_path)

    checks = {
        "A: exactly the 17 producer retries are duplicates": statuses[:len(data)].count("duplicate") == 17,
        "A: accepted + late revenue == sum over unique event_ids (61,241.70 ILS)":
            accepted_plus_late == truth["revenue_cents"] == 6_124_170,
        "B: altered retry of ev-0000 at a new offset is a duplicate": statuses[-1] == "duplicate",
        "B: revenue unchanged by the altered retry (first payload kept)":
            first_ev0000[0] == next(d.offset for d in data if d.event.event_id == "ev-0000") and "999999" not in first_ev0000[1] and accepted_plus_late == truth["revenue_cents"],
        "C: stream was really interrupted mid-run": 0 <= checkpoint_at_stop < len(data) - 1,
        "C: restart resumed exactly at the durable checkpoint": resume_from == checkpoint_at_stop + 1,
        "C: restarted result == uninterrupted result": restarted == expected,
        "D: every saved offset replayed after reopen": replay == {"replayed"},
        "D: every durable table unchanged by the replay": before == after,
    }
    dup_rows = [{"offset": d.offset, "event_id": d.event.event_id, "status": s}
                for d, s in zip(data, statuses) if s == "duplicate"]
    summary = {"deliveries": len(data), "unique_orders": truth["orders"], "duplicates": len(dup_offsets),
               "accepted_ils": shekels(snap_ab["totals"]["revenue_cents"]),
               "late_ils": shekels(snap_ab["totals"]["late_order_cents"]),
               "unique_recompute_ils": shekels(truth["revenue_cents"])}
    restart = {"checkpoint_at_cancel": checkpoint_at_stop, "resumed_from_offset": resume_from,
               "records_after_restart": resumed_state["consumed"],
               "final_accepted_ils": shekels(restarted["totals"]["revenue_cents"]),
               "uninterrupted_accepted_ils": shekels(expected["totals"]["revenue_cents"]),
               "identical": restarted == expected}
    write_json(TICKET, "dedup_restart.json", {"summary": summary, "duplicates": dup_rows, "restart": restart,
                                              "altered_retry_status": statuses[-1], "checks": checks})
    passed = all(checks.values())
    report = f"""# SHOPIL-41 — event deduplication, offsets and restart

Command: `uv run python experiments/shopil_41_dedup_offsets_restart.py`. Input: committed `fixtures/streaming/orders300.jsonl` (317 deliveries in source order). Teaching config (60 s, out_of_order 0, grace 10), a fresh DB per scenario.

## A/B. Duplicate event_ids at new offsets
{md_table([summary])}

{md_table(dup_rows)}

Altered retry: `ev-0000` re-sent at offset {altered.offset} with amount 999,999 agorot → `{statuses[-1]}`. The first stored payload (offset {first_ev0000[0]}) is kept and revenue does not change.

## C. Cancel mid-run, reopen, resume from the durable checkpoint
{md_table([restart])}

## D. Replay of saved offsets after reopen
All 317 offsets delivered again to a reopened engine → statuses {sorted(replay)}; every SQLite table compared row by row before/after: identical = {before == after}.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
