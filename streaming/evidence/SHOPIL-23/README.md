# SHOPIL-23 — SQLite transaction, checkpoint and replay

Command: `uv run python experiments/shopil_23_sqlite_replay.py`. Input: course teaching fixture, fresh DBs, 60 s windows, out_of_order 0, grace 10. Durable state is read from the file via a separate read-only connection (`shopil_streaming.durable`).

Transaction boundary: `StreamEngine.ingest` runs `BEGIN IMMEDIATE` → dedupe row, window state, alert, closed windows, late/dead side output, counters and checkpoint → `COMMIT`. Either all of a record's effects are durable, or none are.

## A. Crash matrix (34 runs: each offset × before/after COMMIT)
| stage | crash_offset | checkpoint_after_restart | durable_equals_clean_prefix | replayed_on_resume | final_equals_baseline |
|---|---|---|---|---|---|
| before_commit | 0 | -1 | True | 0 | True |
| before_commit | 1 | 0 | True | 1 | True |
| before_commit | 2 | 1 | True | 2 | True |
| before_commit | 3 | 2 | True | 3 | True |
| before_commit | 4 | 3 | True | 4 | True |
| before_commit | 5 | 4 | True | 5 | True |
| before_commit | 6 | 5 | True | 6 | True |
| before_commit | 7 | 6 | True | 7 | True |
| before_commit | 8 | 7 | True | 8 | True |
| before_commit | 9 | 8 | True | 9 | True |
| before_commit | 10 | 9 | True | 10 | True |
| before_commit | 11 | 10 | True | 11 | True |
| before_commit | 12 | 11 | True | 12 | True |
| before_commit | 13 | 12 | True | 13 | True |
| before_commit | 14 | 13 | True | 14 | True |
| before_commit | 15 | 14 | True | 15 | True |
| before_commit | 16 | 15 | True | 16 | True |
| after_commit | 0 | 0 | True | 1 | True |
| after_commit | 1 | 1 | True | 2 | True |
| after_commit | 2 | 2 | True | 3 | True |
| after_commit | 3 | 3 | True | 4 | True |
| after_commit | 4 | 4 | True | 5 | True |
| after_commit | 5 | 5 | True | 6 | True |
| after_commit | 6 | 6 | True | 7 | True |
| after_commit | 7 | 7 | True | 8 | True |
| after_commit | 8 | 8 | True | 9 | True |
| after_commit | 9 | 9 | True | 10 | True |
| after_commit | 10 | 10 | True | 11 | True |
| after_commit | 11 | 11 | True | 12 | True |
| after_commit | 12 | 12 | True | 13 | True |
| after_commit | 13 | 13 | True | 14 | True |
| after_commit | 14 | 14 | True | 15 | True |
| after_commit | 15 | 15 | True | 16 | True |
| after_commit | 16 | 16 | True | 17 | True |

## B. Duplicate event_id at a new offset
| offset | event_id | first_seen_offset | revenue_before | revenue_after | checkpoint |
|---|---|---|---|---|---|
| 5 | order-a | 0 | 45.00 | 45.00 | 5 |
| 14 | order-late | 9 | 110.00 | 110.00 | 14 |

## C/D. Replay after reopen and offset gap
- Durable fingerprint after first pass: `c35f73428746428f`; after reopening and replaying offsets 0–16: `c35f73428746428f` (statuses: all `replayed` = True).
- Ingesting offset 20 when 17 is expected: `expected offset 17, received 20`; fingerprint afterwards `c35f73428746428f`.

This proves idempotent LOCAL processing of an at-least-once source. It is not a claim of exactly-once delivery to an external system.

## Checks
| check | result |
|---|---|
| A: crash before COMMIT at every offset leaves exactly the previous committed state | PASS |
| A: crash after COMMIT at every offset keeps exactly that record | PASS |
| A: restart resumes at the durable checkpoint and finishes equal to the baseline | PASS |
| B: same event_id at a new offset does not add revenue | PASS |
| B: duplicate still advances the checkpoint | PASS |
| C: replay of saved offsets after reopen is a no-op | PASS |
| D: offset gap rejected and rolled back | PASS |
| baseline: 122.00 ILS accepted, 13.00 ILS late | PASS |

**Result: PASS**
