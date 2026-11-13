"""SHOPIL-23 evidence: one SQLite transaction per event; dedupe, replay and reopen.

Run:  uv run python experiments/shopil_23_sqlite_replay.py
All runs use the course teaching fixture (17 deliveries, source order) and fresh
DBs. Durable state is read from the file through a separate read-only connection.

  A. transaction boundary - for EVERY offset k, crash before COMMIT; reopen;
     durable state must equal a clean run that stopped after k-1 (nothing of
     record k leaked: not its dedupe row, window state, side output or
     checkpoint). Crash after COMMIT must equal a clean run that stopped after k.
     Then resume from the checkpoint; the final result must equal the baseline.
  B. duplicate event_id at a new offset - accepted once; the retry is a
     duplicate, revenue unchanged, checkpoint still advances.
  C. replay of already-saved offsets after reopening - every record "replayed",
     durable state fingerprint unchanged.
  D. offset gap - rejected with OffsetGapError and rolled back.
"""
from __future__ import annotations

import sys

from shopil_streaming.durable import durable_state, fingerprint
from shopil_streaming.runs import fresh_db, md_table, shekels, write_json, write_text
from streaming_lab import InjectedCrash, OffsetGapError, StreamEngine, teaching_fixture
from streaming_lab.reporting import canonical_results

TICKET = "SHOPIL-23"
CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def clean_prefix(n: int):
    """Durable state of a clean run that committed offsets 0..n-1."""
    path = fresh_db(f"shopil-23-prefix{n}")
    with StreamEngine(path, **CONFIG) as engine:
        for item in teaching_fixture()[:n]:
            engine.ingest(item.event, offset=item.offset)
    return durable_state(path)


def baseline():
    with StreamEngine(fresh_db("shopil-23-baseline"), **CONFIG) as engine:
        for item in teaching_fixture():
            engine.ingest(item.event, offset=item.offset)
        return canonical_results(engine.finalize_demo())


def crash_case(stage: str, k: int, prefixes: dict[int, dict], expected_final: dict) -> dict:
    data = teaching_fixture()
    path = fresh_db(f"shopil-23-{stage}-{k}")
    with StreamEngine(path, **CONFIG) as engine:
        try:
            for item in data:
                engine.ingest(item.event, offset=item.offset, crash_at=stage if item.offset == k else None)
        except InjectedCrash:
            pass
    after_crash = durable_state(path)
    with StreamEngine(path, **CONFIG) as engine:   # restart from durable state only
        checkpoint = engine.checkpoint
        statuses = [engine.ingest(item.event, offset=item.offset)["status"] for item in data]
        final = canonical_results(engine.finalize_demo())
    want = k if stage == "before_commit" else k + 1
    return {"stage": stage, "crash_offset": k, "checkpoint_after_restart": checkpoint,
            "durable_equals_clean_prefix": after_crash == prefixes[want],
            "replayed_on_resume": statuses.count("replayed"),
            "final_equals_baseline": final == expected_final}


def main() -> int:
    data = teaching_fixture()
    expected = baseline()
    prefixes = {n: clean_prefix(n) for n in range(len(data) + 1)}
    crash_rows = [crash_case(stage, k, prefixes, expected)
                  for stage in ("before_commit", "after_commit") for k in range(len(data))]

    # B. duplicate event_id at a new offset (order-a at 0 and 5; order-late at 9 and 14)
    with StreamEngine(fresh_db("shopil-23-dup"), **CONFIG) as engine:
        dup_rows = []
        for item in data:
            before = engine.snapshot()["totals"]["revenue_cents"]
            result = engine.ingest(item.event, offset=item.offset)
            if result["status"] == "duplicate":
                dup_rows.append({"offset": item.offset, "event_id": item.event.event_id,
                                 "first_seen_offset": engine.con.execute(
                                     "SELECT source_offset FROM seen_events WHERE event_id=?",
                                     (item.event.event_id,)).fetchone()[0],
                                 "revenue_before": shekels(before),
                                 "revenue_after": shekels(engine.snapshot()["totals"]["revenue_cents"]),
                                 "checkpoint": result["checkpoint"]})
        dup_stats = engine.snapshot()["stats"]

    # C. replay after reopen + D. offset gap
    path = fresh_db("shopil-23-replay")
    with StreamEngine(path, **CONFIG) as engine:
        for item in data:
            engine.ingest(item.event, offset=item.offset)
    before = fingerprint(path)
    with StreamEngine(path, **CONFIG) as engine:
        replay = [engine.ingest(item.event, offset=item.offset)["status"] for item in data]
    after_replay = fingerprint(path)
    with StreamEngine(path, **CONFIG) as engine:
        try:
            engine.ingest(data[0].event, offset=len(data) + 3)
            gap = "accepted (unexpected)"
        except OffsetGapError as exc:
            gap = str(exc)
    after_gap = fingerprint(path)

    checks = {
        "A: crash before COMMIT at every offset leaves exactly the previous committed state":
            all(r["durable_equals_clean_prefix"] for r in crash_rows if r["stage"] == "before_commit"),
        "A: crash after COMMIT at every offset keeps exactly that record":
            all(r["durable_equals_clean_prefix"] for r in crash_rows if r["stage"] == "after_commit"),
        "A: restart resumes at the durable checkpoint and finishes equal to the baseline":
            all(r["final_equals_baseline"] and r["replayed_on_resume"] == r["checkpoint_after_restart"] + 1
                for r in crash_rows),
        "B: same event_id at a new offset does not add revenue":
            len(dup_rows) == 2 and all(r["revenue_before"] == r["revenue_after"] for r in dup_rows),
        "B: duplicate still advances the checkpoint": all(r["checkpoint"] == r["offset"] for r in dup_rows),
        "C: replay of saved offsets after reopen is a no-op": replay == ["replayed"] * len(data) and before == after_replay,
        "D: offset gap rejected and rolled back": "expected offset 17" in gap and after_gap == before,
        "baseline: 122.00 ILS accepted, 13.00 ILS late": expected["totals"]["revenue_cents"] == 12_200
            and expected["totals"]["late_order_cents"] == 1_300,
    }
    write_json(TICKET, "replay.json", {"crash_matrix": crash_rows, "duplicates": dup_rows, "dup_stats": dup_stats,
                                       "replay_statuses": replay, "fingerprint_before": before,
                                       "fingerprint_after_replay": after_replay, "offset_gap": gap, "checks": checks})
    passed = all(checks.values())
    report = f"""# SHOPIL-23 — SQLite transaction, checkpoint and replay

Command: `uv run python experiments/shopil_23_sqlite_replay.py`. Input: course teaching fixture, fresh DBs, 60 s windows, out_of_order 0, grace 10. Durable state is read from the file via a separate read-only connection (`shopil_streaming.durable`).

Transaction boundary: `StreamEngine.ingest` runs `BEGIN IMMEDIATE` → dedupe row, window state, alert, closed windows, late/dead side output, counters and checkpoint → `COMMIT`. Either all of a record's effects are durable, or none are.

## A. Crash matrix (34 runs: each offset × before/after COMMIT)
{md_table(crash_rows)}

## B. Duplicate event_id at a new offset
{md_table(dup_rows)}

## C/D. Replay after reopen and offset gap
- Durable fingerprint after first pass: `{before}`; after reopening and replaying offsets 0–16: `{after_replay}` (statuses: all `replayed` = {replay == ['replayed'] * len(data)}).
- Ingesting offset 20 when 17 is expected: `{gap}`; fingerprint afterwards `{after_gap}`.

This proves idempotent LOCAL processing of an at-least-once source. It is not a claim of exactly-once delivery to an external system.

## Checks
{md_table([{'check': k, 'result': 'PASS' if v else 'FAIL'} for k, v in checks.items()])}

**Result: {'PASS' if passed else 'FAIL'}**
"""
    write_text(TICKET, "README.md", report)
    print(report)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
