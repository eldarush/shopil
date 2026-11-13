"""SHOPIL-39 evidence: run_live vs run_microbatch give the same canonical_results.

Run:  uv run python experiments/shopil_39_live_vs_microbatch.py
For each committed fixture (teaching, reviews, orders300), same input and order,
each run on a NEW database (teaching config: 60 s, out_of_order 0, grace 10):
  * reference   - sequential engine.ingest
  * live        - course run_live (queue 4)
  * microbatch  - course run_microbatch with three settings:
                  (5, 0.2 s), (1, 0.2 s) and (50, 0.01 s, slow arrivals -> timer flushes)
All runs finish gracefully and finalize_demo. Collected: produced, consumed,
batch_sizes and release reasons. Business results must be identical.
"""
from __future__ import annotations

import asyncio
from collections import Counter
import sys

from shopil_streaming.fixtures import TEACHING_CONFIG, fresh_engine, load_fixture
from shopil_streaming.runs import md_table, shekels, write_json, write_text
from streaming_lab import run_live, run_microbatch
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-39"
MICRO = {"mb(5,0.2s)": dict(max_batch_size=5, max_wait_s=0.2, interval_s=0.0),
         "mb(1,0.2s)": dict(max_batch_size=1, max_wait_s=0.2, interval_s=0.0),
         "mb(50,0.01s)": dict(max_batch_size=50, max_wait_s=0.01, interval_s=0.003)}


def reference(name: str) -> dict:
    with fresh_engine(f"shopil-39-{name}-ref", **TEACHING_CONFIG) as engine:
        for item in load_fixture(name):
            engine.ingest(item.event, offset=item.offset)
        return canonical_results(engine.finalize_demo())


async def live(name: str) -> tuple[dict, dict]:
    with fresh_engine(f"shopil-39-{name}-live", **TEACHING_CONFIG) as engine:
        state = await run_live(engine, load_fixture(name), duration_s=None, interval_s=0.0, queue_maxsize=4,
                               finalize=True)
    return canonical_results(state.pop("snapshot")), state


async def micro(name: str, label: str, settings: dict) -> tuple[dict, dict]:
    with fresh_engine(f"shopil-39-{name}-{label.replace('(', '-').replace(')', '').replace(',', '-')}",
                      **TEACHING_CONFIG) as engine:
        state = await run_microbatch(engine, load_fixture(name), duration_s=None, queue_maxsize=8,
                                     finalize=True, **settings)
    return canonical_results(state.pop("snapshot")), state


def size_summary(sizes: list[int]) -> str:
    if len(sizes) <= 8:
        return str(sizes)
    common = ", ".join(f"{size}×{count}" for size, count in sorted(Counter(sizes).items(), reverse=True))
    return f"{len(sizes)} batches: {common}"


async def main_async() -> int:
    rows, checks, raw = [], {}, {}
    for name in ("teaching", "reviews", "orders300"):
        n = len(load_fixture(name))
        ref = reference(name)
        runs = {"live": await live(name)}
        for label, settings in MICRO.items():
            runs[label] = await micro(name, label, settings)
        raw[name] = {"reference_totals": ref["totals"]}
        for label, (canon, state) in runs.items():
            sizes = state.get("batch_sizes", [1] * state["consumed"])
            rows.append({"fixture": name, "mode": label, "produced": state["produced"], "consumed": state["consumed"],
                         "batches": len(sizes), "batch_sizes": size_summary(sizes),
                         "release_reasons": state.get("flush_counts", "per record"),
                         "accepted_ils": shekels(canon["totals"]["revenue_cents"]),
                         "late_ils": shekels(canon["totals"]["late_order_cents"]),
                         "alerts": canon["stats"]["alerts_emitted"], "same_as_reference": canon == ref})
            raw[name][label] = {"produced": state["produced"], "consumed": state["consumed"],
                                "batch_sizes": state.get("batch_sizes"), "flush_counts": state.get("flush_counts"),
                                "batch_details": state.get("batch_details"), "canonical_equal": canon == ref}
            checks[f"{name} / {label}: canonical_results == reference"] = canon == ref
            checks[f"{name} / {label}: produced == consumed == {n}, batch sizes sum to consumed"] = (
                state["produced"] == state["consumed"] == n == sum(sizes))
    checks["mb(50,0.01s) used the timer (max_wait) at least once on every fixture"] = all(
        raw[name]["mb(50,0.01s)"]["flush_counts"]["max_wait"] > 0 for name in raw)
    checks["mb(5,0.2s) on orders300 released mostly by size"] = (
        raw["orders300"]["mb(5,0.2s)"]["flush_counts"]["max_size"] >= 60)
    write_json(TICKET, "comparison.json", {"runs": raw, "checks": checks})
    passed = all(checks.values())
    report = f"""# SHOPIL-39 — live vs microbatch canonical_results

Command: `uv run python experiments/shopil_39_live_vs_microbatch.py`. Inputs: committed fixtures from SHOPIL-37 (`fixtures/streaming/*.jsonl`), same order for every mode, a NEW SQLite DB per run, teaching config (60 s, out_of_order 0, grace 10). Every run stops gracefully and calls `finalize_demo`.

{md_table(rows)}

Batches only change WHEN records are processed, not HOW. Each record is still its own transaction, in source order, so watermark, windows, late events, dedupe and alerts are identical. The batch release reasons (`max_size` / `max_wait` / `final_drain`) depend on arrival timing; the business results do not.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
