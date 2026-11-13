# SHOPIL-19 — run_live, one record at a time

Command: `uv run python experiments/shopil_19_run_live.py`. Config: 60 s windows, out_of_order 0, grace 10. Each run uses its own fresh SQLite DB. `run_live` is the unmodified course runner (producer task, bounded `asyncio.Queue` of 4, consumer task). `RecordingEngine` only observes `ingest` calls.

## Teaching fixture: per-record trace under run_live (source order, not sorted by event time)
| offset | event_id | status | checkpoint |
|---|---|---|---|
| 0 | order-a | accepted | 0 |
| 1 | review-1 | accepted | 1 |
| 2 | order-b | accepted | 2 |
| 3 | order-c | accepted | 3 |
| 4 | review-2 | accepted | 4 |
| 5 | order-a | duplicate | 5 |
| 6 | review-3 | accepted | 6 |
| 7 | review-4 | accepted | 7 |
| 8 | order-d | accepted | 8 |
| 9 | order-late | late | 9 |
| 10 | order-e | accepted | 10 |
| 11 | bad-money | invalid | 11 |
| 12 | order-f | accepted | 12 |
| 13 | review-5 | accepted | 13 |
| 14 | order-late | duplicate | 14 |
| 15 | order-boundary | late | 15 |
| 16 | order-h | accepted | 16 |

## Summary
| input | records | produced | consumed | max_queue_depth | backpressure_waits | accepted_revenue_ils | late_revenue_ils | duplicates | invalid |
|---|---|---|---|---|---|---|---|---|---|
| teaching | 17 | 17 | 17 | 1 | 0 | 122.00 | 13.00 | 2 | 1 |
| orders300 | 317 | 317 | 317 | 1 | 0 | 57,954.30 | 3,287.40 | 17 | 0 |

## Checks
| input | check | result |
|---|---|---|
| teaching | engine received offsets in source order | PASS |
| teaching | same disposition per record as reference | PASS |
| teaching | canonical_results identical to sequential reference | PASS |
| teaching | produced == consumed == input size | PASS |
| teaching | consumption observed while producer was still admitting | PASS |
| teaching | stop reason end_of_input | PASS |
| orders300 | engine received offsets in source order | PASS |
| orders300 | same disposition per record as reference | PASS |
| orders300 | canonical_results identical to sequential reference | PASS |
| orders300 | produced == consumed == input size | PASS |
| orders300 | consumption observed while producer was still admitting | PASS |
| orders300 | stop reason end_of_input | PASS |

**Result: PASS**
