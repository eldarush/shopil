# SHOPIL-20 — run_microbatch and release conditions

Command: `uv run python experiments/shopil_20_run_microbatch.py`. Input: course teaching fixture in source order. Config: 60 s windows, out_of_order 0, grace 10, fresh DB per run.

A batch is released at `max_batch_size` or `max_wait_s` after the consumer pulled its first record, whichever comes first; on producer stop the remaining partial batch is released as `final_drain`. Each record is still its own SQLite transaction. A batch is not an event-time window: release never closes a window.

### max_size
Settings: `{'records': 17, 'interval_s': 0.0, 'max_batch_size': 5, 'max_wait_s': 5.0, 'queue_maxsize': 8}` → batch_sizes=[5, 5, 5, 2], flush_counts={'max_size': 3, 'max_wait': 0, 'final_drain': 1}, produced=17, consumed=17

| index | size | flush_reason | collection_wait_s | first_offset | last_offset | produced_at_release | checkpoint_at_release | open_windows | closed_windows |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 5 | max_size | 7e-05 | 0 | 4 | 8 | 4 | 2 | 0 |
| 2 | 5 | max_size | 0.0002 | 5 | 9 | 16 | 9 | 1 | 1 |
| 3 | 5 | max_size | 4e-05 | 10 | 14 | 16 | 14 | 1 | 2 |
| 4 | 2 | final_drain | 8.7e-05 | 15 | 16 | 17 | 16 | 1 | 3 |

### max_wait
Settings: `{'records': 5, 'interval_s': 0.5, 'max_batch_size': 5, 'max_wait_s': 0.1, 'queue_maxsize': 8}` → batch_sizes=[1, 1, 1, 1, 1], flush_counts={'max_size': 0, 'max_wait': 5, 'final_drain': 0}, produced=5, consumed=5

| index | size | flush_reason | collection_wait_s | first_offset | last_offset | produced_at_release | checkpoint_at_release | open_windows | closed_windows |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | max_wait | 0.110712 | 0 | 0 | 1 | 0 | 1 | 0 |
| 2 | 1 | max_wait | 0.111082 | 1 | 1 | 2 | 1 | 1 | 0 |
| 3 | 1 | max_wait | 0.105737 | 2 | 2 | 3 | 2 | 2 | 0 |
| 4 | 1 | max_wait | 0.109512 | 3 | 3 | 4 | 3 | 2 | 0 |
| 5 | 1 | max_wait | 0.108797 | 4 | 4 | 5 | 4 | 2 | 0 |

### final_drain
Settings: `{'records': 7, 'interval_s': 0.0, 'max_batch_size': 5, 'max_wait_s': 10.0, 'queue_maxsize': 8}` → batch_sizes=[5, 2], flush_counts={'max_size': 1, 'max_wait': 0, 'final_drain': 1}, produced=7, consumed=7

| index | size | flush_reason | collection_wait_s | first_offset | last_offset | produced_at_release | checkpoint_at_release | open_windows | closed_windows |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 5 | max_size | 3.4e-05 | 0 | 4 | 7 | 4 | 2 | 0 |
| 2 | 2 | final_drain | 2.9e-05 | 5 | 6 | 7 | 6 | 2 | 0 |

## Checks
| check | result |
|---|---|
| max_size: full batches released by size [5,5,5] then final_drain 2 | PASS |
| max_wait: every batch released by timer holding 1 record | PASS |
| max_wait: released WITHOUT a further event (produced == last_offset + 1 at release) | PASS |
| max_wait: collection wait ~= max_wait_s (>= 0.1 s, well below 0.5 s arrival gap) | PASS |
| final_drain: [5 max_size, 2 final_drain] | PASS |
| final_drain did not close event-time windows (still open without finalize) | PASS |
| every scenario: sum(batch_sizes) == consumed == produced | PASS |
| every scenario: engine saw offsets in source order | PASS |
| every scenario: canonical_results == sequential reference (per-record transactions) | PASS |

**Result: PASS**
