"""SHOPIL-17 evidence: every contract case goes through a fresh course StreamEngine.

Run:  uv run python experiments/shopil_17_contract.py
Writes evidence/SHOPIL-17/{contract_run.json, README.md}. Exit code 1 on mismatch.
"""
from __future__ import annotations

import json
import sys

from shopil_streaming.contract import CONTRACT, PAYMENT_STATUS_NOTE, check, dead_letter_report
from shopil_streaming.runs import STREAMING_ROOT, fresh_db, md_table, write_json, write_text
from streaming_lab import StreamEngine

TICKET = "SHOPIL-17"


def run() -> dict:
    cases = json.loads((STREAMING_ROOT / "fixtures" / "contract_cases.json").read_text(encoding="utf-8"))
    db = fresh_db("shopil-17")
    rows, failures = [], []
    with StreamEngine(db) as engine:
        for offset, case in enumerate(cases):
            result = engine.ingest(case["event"], offset=offset)
            ok = result["status"] == case["expect"]
            if not ok:
                failures.append(case["case"])
            rows.append({"offset": offset, "case": case["case"], "expected": case["expect"],
                         "engine_status": result["status"], "reason": result.get("reason", ""),
                         "precheck_matches_engine": check(case["event"]) == result.get("reason"),
                         "checkpoint_after": result["checkpoint"]})
        snap = engine.snapshot()
    dead = dead_letter_report(snap)
    invalid_offsets = [r["offset"] for r in rows if r["expected"] == "invalid"]
    checks = {
        "every case has the expected status": not failures,
        "every invalid input is in dead_letters with input, offset and reason":
            [d["offset"] for d in dead] == invalid_offsets and all(d["reason"] and d["input"] is not None for d in dead),
        "checkpoint advanced over invalid input (last offset)": snap["checkpoint"] == len(cases) - 1,
        "a future-dated invalid event (11:00) did not move max event time / watermark":
            snap["max_event_time"] == snap["watermark"] == "2026-11-01T10:00:20.000+00:00",
        "contract.check() reproduces the engine reason": all(r["precheck_matches_engine"] for r in rows),
        "stats.invalid == dead letters": snap["stats"]["invalid"] == len(dead),
    }
    return {"db": str(db), "cases": rows, "dead_letters": dead, "checks": checks,
            "stats": snap["stats"], "checkpoint": snap["checkpoint"], "watermark": snap["watermark"]}


def main() -> int:
    out = run()
    write_json(TICKET, "contract_run.json", {k: v for k, v in out.items() if k != "db"})
    passed = all(out["checks"].values())
    report = f"""# SHOPIL-17 — streaming event contract

Command: `uv run python experiments/shopil_17_contract.py` (fresh SQLite DB per run, course `StreamEngine` unmodified)

## Contract
{md_table(CONTRACT)}

{PAYMENT_STATUS_NOTE}

## Run: {len(out['cases'])} deliveries in source order
{md_table(out['cases'], ['offset', 'case', 'expected', 'engine_status', 'reason', 'checkpoint_after'])}

## dead_letters (input, offset, reason)
{md_table(out['dead_letters'], ['offset', 'reason', 'input'])}

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in out['checks'].items()])}

Final: checkpoint={out['checkpoint']}, watermark={out['watermark']}, stats={json.dumps(out['stats'])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
