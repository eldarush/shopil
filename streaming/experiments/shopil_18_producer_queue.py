"""SHOPIL-18 evidence: continuous producer + bounded asyncio.Queue.

Run:  uv run python experiments/shopil_18_producer_queue.py
Writes evidence/SHOPIL-18/. Two runs on fresh DBs with the endless ShopIL source:
  A. normal pace     - consumption advances while the producer is still admitting.
  B. slow sink       - per-record processing delay > arrival interval, so the
                        bounded queue fills, the producer waits (backpressure) and
                        queue depth never exceeds its maxsize.
Timings are machine dependent; the checks are not.
"""
from __future__ import annotations

import asyncio
import sys
import time

from shopil_streaming.runs import fresh_db, md_table, shekels, write_json, write_text
from shopil_streaming.service import DEFAULT_CONFIG, run
from streaming_lab import StreamEngine

TICKET = "SHOPIL-18"


async def observed_run(tag: str, **kwargs) -> dict:
    samples: list[dict] = []
    started = time.perf_counter()

    def sample(snapshot: dict) -> None:
        q = snapshot["queue"]
        samples.append({"t_s": round(time.perf_counter() - started, 3), "produced": q["produced"],
                        "consumed": q["consumed"], "depth": q["depth"], "maxsize": q["maxsize"],
                        "backpressure_waits": q["backpressure_waits"], "checkpoint": snapshot["checkpoint"],
                        "revenue": shekels(snapshot["totals"]["revenue_cents"])})
    with StreamEngine(fresh_db(tag), **DEFAULT_CONFIG) as engine:
        state = await run(engine, mode="live", finalize=False, on_snapshot=sample,
                          batch_size=1, max_wait_s=1.0, **kwargs)
    state.pop("snapshot")
    return {"state": state, "samples": samples}


def thin(samples: list[dict], n: int = 12) -> list[dict]:
    if len(samples) <= n:
        return samples
    step = (len(samples) - 1) / (n - 1)
    return [samples[round(i * step)] for i in range(n)]


async def main_async() -> int:
    a = await observed_run("shopil-18-normal", seconds=2.0, interval_s=0.01, queue_size=8, processing_delay_s=0.0)
    b = await observed_run("shopil-18-backpressure", seconds=2.0, interval_s=0.0, queue_size=4,
                           processing_delay_s=0.02)
    sa, sb = a["state"], b["state"]
    first = a["samples"][0]
    checks = {
        "A: first record consumed while producer was still admitting (produced kept rising afterwards)":
            first["consumed"] >= 1 and sa["produced"] > first["produced"],
        "A: consumed advanced across many observations during production":
            len({s["consumed"] for s in a["samples"]}) > 10,
        "A: graceful stop drained the queue (produced == consumed)": sa["produced"] == sa["consumed"] > 0,
        "B: bounded queue never exceeded maxsize":
            sb["max_queue_depth"] <= sb["queue_maxsize"] and all(s["depth"] <= s["maxsize"] for s in b["samples"]),
        "B: producer experienced backpressure (waits > 0)": sb["backpressure_waits"] > 0,
        "B: graceful stop drained the queue (produced == consumed)": sb["produced"] == sb["consumed"] > 0,
    }
    write_json(TICKET, "runs.json", {"normal": a, "backpressure": b, "checks": checks})
    cols = ["t_s", "produced", "consumed", "depth", "maxsize", "backpressure_waits", "checkpoint", "revenue"]
    keys = ["produced", "consumed", "queue_maxsize", "max_queue_depth", "backpressure_waits",
            "producer_wait_s", "stop_reason", "elapsed_s"]
    passed = all(checks.values())
    report = f"""# SHOPIL-18 — continuous producer and bounded asyncio.Queue

Command: `uv run python experiments/shopil_18_producer_queue.py`. The endless deterministic ShopIL source (`shopil_streaming.source`) feeds the course `run_live`. Each run uses a fresh SQLite DB. For a long interactive run use `uv run python -m shopil_streaming.service --mode live --seconds 30`.

## A. Normal pace (interval 0.01 s, queue 8, no sink delay, 2 s admission)
{md_table(thin(a['samples']), cols)}

Summary: {md_table([{k: sa[k] for k in keys}])}

## B. Slow sink (interval 0 s, queue 4, 0.02 s per record, 2 s admission)
{md_table(thin(b['samples']), cols)}

Summary: {md_table([{k: sb[k] for k in keys}])}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
