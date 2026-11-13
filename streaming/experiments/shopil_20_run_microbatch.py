"""SHOPIL-20 evidence: run_microbatch release conditions (max_size, max_wait, final_drain).

Run:  uv run python experiments/shopil_20_run_microbatch.py
Three scenarios on the teaching fixture (source order, never sorted), each on a
fresh DB, using the unmodified course run_microbatch:

  1. max_size    - records arrive back to back; batches fill to max_batch_size=5.
  2. max_wait    - records arrive every 0.5 s but max_wait_s=0.1; every batch is
                   released by its timer holding ONE record, while no further
                   record had been produced yet (proof: produced == last_offset+1
                   at release time).
  3. final_drain - 7 records, max_batch_size=5, max_wait_s=10: one full batch, then
                   the producer stops and the 2-record partial batch drains.

The collection timer starts when the consumer pulls the FIRST record of a batch.
Each record is still committed in its own transaction, so canonical_results
match a sequential reference. A batch is a processing buffer, not a time window:
final_drain does not close any event-time window (windows stay open without finalize).
"""
from __future__ import annotations

import asyncio
import sys

from shopil_streaming.recording import RecordingEngine
from shopil_streaming.runs import fresh_db, md_table, write_json, write_text
from streaming_lab import run_microbatch, teaching_fixture
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-20"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}

SCENARIOS = {
    "max_size": dict(records=17, interval_s=0.0, max_batch_size=5, max_wait_s=5.0, queue_maxsize=8),
    "max_wait": dict(records=5, interval_s=0.5, max_batch_size=5, max_wait_s=0.1, queue_maxsize=8),
    "final_drain": dict(records=7, interval_s=0.0, max_batch_size=5, max_wait_s=10.0, queue_maxsize=8),
}


def reference(deliveries) -> dict:
    with RecordingEngine(fresh_db("shopil-20-reference"), **CONFIG) as engine:
        for item in deliveries:
            engine.ingest(item.event, offset=item.offset)
        return canonical_results(engine.snapshot())


async def scenario(name: str, records: int, **kwargs) -> dict:
    deliveries = teaching_fixture()[:records]
    releases: list[dict] = []

    def on_batch(snapshot: dict) -> None:
        b, q = snapshot["batch"], snapshot["queue"]
        releases.append({**b, "produced_at_release": q["produced"], "consumed_at_release": q["consumed"],
                         "checkpoint_at_release": snapshot["checkpoint"],
                         "open_windows": len(snapshot["open_windows"]),
                         "closed_windows": len(snapshot["revenue_windows"])})
    with RecordingEngine(fresh_db(f"shopil-20-{name}"), **CONFIG) as engine:
        state = await run_microbatch(engine, deliveries, duration_s=None, finalize=False,
                                     on_snapshot=on_batch, **kwargs)
        offsets = engine.offsets
    canon = canonical_results(state["snapshot"])
    return {"name": name, "settings": {"records": records, **kwargs}, "releases": releases,
            "batch_sizes": state["batch_sizes"], "flush_counts": state["flush_counts"],
            "produced": state["produced"], "consumed": state["consumed"],
            "offsets_in_source_order": offsets == [d.offset for d in deliveries],
            "matches_reference": canon == reference(deliveries),
            "open_windows_after_run": len(canon["open_windows"])}


async def main_async() -> int:
    runs = {name: await scenario(name, **cfg) for name, cfg in SCENARIOS.items()}
    s, w, d = runs["max_size"], runs["max_wait"], runs["final_drain"]
    checks = {
        "max_size: full batches released by size [5,5,5] then final_drain 2":
            s["batch_sizes"] == [5, 5, 5, 2] and s["flush_counts"] == {"max_size": 3, "max_wait": 0, "final_drain": 1},
        "max_wait: every batch released by timer holding 1 record":
            w["batch_sizes"] == [1] * 5 and w["flush_counts"]["max_wait"] == 5,
        "max_wait: released WITHOUT a further event (produced == last_offset + 1 at release)":
            all(r["produced_at_release"] == r["last_offset"] + 1 for r in w["releases"]),
        "max_wait: collection wait ~= max_wait_s (>= 0.1 s, well below 0.5 s arrival gap)":
            all(0.1 <= r["collection_wait_s"] < 0.45 for r in w["releases"]),
        "final_drain: [5 max_size, 2 final_drain]":
            d["batch_sizes"] == [5, 2] and [r["flush_reason"] for r in d["releases"]] == ["max_size", "final_drain"],
        "final_drain did not close event-time windows (still open without finalize)": d["open_windows_after_run"] > 0,
        "every scenario: sum(batch_sizes) == consumed == produced":
            all(sum(r["batch_sizes"]) == r["consumed"] == r["produced"] for r in runs.values()),
        "every scenario: engine saw offsets in source order": all(r["offsets_in_source_order"] for r in runs.values()),
        "every scenario: canonical_results == sequential reference (per-record transactions)":
            all(r["matches_reference"] for r in runs.values()),
    }
    write_json(TICKET, "microbatch.json", {"runs": runs, "checks": checks})
    passed = all(checks.values())
    cols = ["index", "size", "flush_reason", "collection_wait_s", "first_offset", "last_offset",
            "produced_at_release", "checkpoint_at_release", "open_windows", "closed_windows"]
    sections = "\n\n".join(
        f"### {name}\nSettings: `{r['settings']}` → batch_sizes={r['batch_sizes']}, flush_counts={r['flush_counts']}, "
        f"produced={r['produced']}, consumed={r['consumed']}\n\n{md_table(r['releases'], cols)}"
        for name, r in runs.items())
    report = f"""# SHOPIL-20 — run_microbatch and release conditions

Command: `uv run python experiments/shopil_20_run_microbatch.py`. Input: course teaching fixture in source order. Config: 60 s windows, out_of_order 0, grace 10, fresh DB per run.

A batch is released at `max_batch_size` or `max_wait_s` after the consumer pulled its first record, whichever comes first; on producer stop the remaining partial batch is released as `final_drain`. Each record is still its own SQLite transaction. A batch is not an event-time window: release never closes a window.

{sections}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
