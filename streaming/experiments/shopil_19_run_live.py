"""SHOPIL-19 evidence: run_live hands every record to the engine in source order.

Run:  uv run python experiments/shopil_19_run_live.py
For each input (teaching fixture, 17 arrivals; course generator, 300 orders + retries):
  * reference  - plain sequential engine.ingest in source order on a fresh DB;
  * live       - the course run_live (real producer task, bounded queue, consumer
                 task) on another fresh DB, while the producer keeps admitting.
Checks: same offsets in the same order, same per-record disposition, identical
canonical_results, produced == consumed == input size, and consumption observed
before the producer finished. Input is never sorted by event time.
"""
from __future__ import annotations

import asyncio
import sys

from shopil_streaming.recording import RecordingEngine
from shopil_streaming.runs import fresh_db, md_table, shekels, write_json, write_text
from streaming_lab import generate_order_fixture, run_live, teaching_fixture
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-19"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def reference(name: str, deliveries) -> tuple[list[dict], dict]:
    with RecordingEngine(fresh_db(f"shopil-19-{name}-reference"), **CONFIG) as engine:
        for item in deliveries:
            engine.ingest(item.event, offset=item.offset)
        return engine.trace, canonical_results(engine.finalize_demo())


async def live(name: str, deliveries) -> tuple[list[dict], dict, dict, list[dict]]:
    samples: list[dict] = []
    with RecordingEngine(fresh_db(f"shopil-19-{name}-live"), **CONFIG) as engine:
        state = await run_live(engine, deliveries, duration_s=None, interval_s=0.002, queue_maxsize=4,
                               finalize=True,
                               on_snapshot=lambda s: samples.append({k: s["queue"][k] for k in ("produced", "consumed", "depth")}))
        return engine.trace, canonical_results(state.pop("snapshot")), state, samples


async def compare(name: str, deliveries) -> dict:
    ref_trace, ref_canon = reference(name, deliveries)
    live_trace, live_canon, state, samples = await live(name, deliveries)
    source_offsets = [d.offset for d in deliveries]
    checks = {
        "engine received offsets in source order": [r["offset"] for r in live_trace] == source_offsets,
        "same disposition per record as reference":
            [(r["offset"], r["status"]) for r in live_trace] == [(r["offset"], r["status"]) for r in ref_trace],
        "canonical_results identical to sequential reference": live_canon == ref_canon,
        "produced == consumed == input size": state["produced"] == state["consumed"] == len(deliveries),
        "consumption observed while producer was still admitting":
            any(s["consumed"] >= 1 and s["produced"] < len(deliveries) for s in samples),
        "stop reason end_of_input": state["stop_reason"] == "end_of_input",
    }
    totals = live_canon["totals"]
    return {"name": name, "records": len(deliveries), "checks": checks, "trace": live_trace,
            "state": state, "totals": totals, "stats": live_canon["stats"],
            "summary": {"input": name, "records": len(deliveries), "produced": state["produced"],
                        "consumed": state["consumed"], "max_queue_depth": state["max_queue_depth"],
                        "backpressure_waits": state["backpressure_waits"],
                        "accepted_revenue_ils": shekels(totals["revenue_cents"]),
                        "late_revenue_ils": shekels(totals["late_order_cents"]),
                        "duplicates": live_canon["stats"]["duplicates"], "invalid": live_canon["stats"]["invalid"]}}


async def main_async() -> int:
    results = [await compare("teaching", teaching_fixture()), await compare("orders300", generate_order_fixture(300))]
    write_json(TICKET, "run_live.json", {r["name"]: {k: v for k, v in r.items() if k != "name"} for r in results})
    passed = all(all(r["checks"].values()) for r in results)
    check_rows = [{"input": r["name"], "check": k, "result": "PASS" if v else "FAIL"}
                  for r in results for k, v in r["checks"].items()]
    report = f"""# SHOPIL-19 — run_live, one record at a time

Command: `uv run python experiments/shopil_19_run_live.py`. Config: 60 s windows, out_of_order 0, grace 10. Each run uses its own fresh SQLite DB. `run_live` is the unmodified course runner (producer task, bounded `asyncio.Queue` of 4, consumer task). `RecordingEngine` only observes `ingest` calls.

## Teaching fixture: per-record trace under run_live (source order, not sorted by event time)
{md_table(results[0]['trace'], ['offset', 'event_id', 'status', 'checkpoint'])}

## Summary
{md_table([r['summary'] for r in results])}

## Checks
{md_table(check_rows)}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
