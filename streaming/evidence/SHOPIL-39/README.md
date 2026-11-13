# SHOPIL-39 — live vs microbatch canonical_results

Command: `uv run python experiments/shopil_39_live_vs_microbatch.py`. Inputs: committed fixtures from SHOPIL-37 (`fixtures/streaming/*.jsonl`), same order for every mode, a NEW SQLite DB per run, teaching config (60 s, out_of_order 0, grace 10). Every run stops gracefully and calls `finalize_demo`.

| fixture | mode | produced | consumed | batches | batch_sizes | release_reasons | accepted_ils | late_ils | alerts | same_as_reference |
|---|---|---|---|---|---|---|---|---|---|---|
| teaching | live | 17 | 17 | 17 | 17 batches: 1×17 | per record | 122.00 | 13.00 | 1 | True |
| teaching | mb(5,0.2s) | 17 | 17 | 4 | [5, 5, 5, 2] | {"max_size": 3, "max_wait": 0, "final_drain": 1} | 122.00 | 13.00 | 1 | True |
| teaching | mb(1,0.2s) | 17 | 17 | 17 | 17 batches: 1×17 | {"max_size": 17, "max_wait": 0, "final_drain": 0} | 122.00 | 13.00 | 1 | True |
| teaching | mb(50,0.01s) | 17 | 17 | 13 | 13 batches: 2×4, 1×9 | {"max_size": 0, "max_wait": 12, "final_drain": 1} | 122.00 | 13.00 | 1 | True |
| reviews | live | 6 | 6 | 6 | [1, 1, 1, 1, 1, 1] | per record | 0.00 | 0.00 | 1 | True |
| reviews | mb(5,0.2s) | 6 | 6 | 2 | [5, 1] | {"max_size": 1, "max_wait": 0, "final_drain": 1} | 0.00 | 0.00 | 1 | True |
| reviews | mb(1,0.2s) | 6 | 6 | 6 | [1, 1, 1, 1, 1, 1] | {"max_size": 6, "max_wait": 0, "final_drain": 0} | 0.00 | 0.00 | 1 | True |
| reviews | mb(50,0.01s) | 6 | 6 | 4 | [1, 1, 2, 2] | {"max_size": 0, "max_wait": 4, "final_drain": 0} | 0.00 | 0.00 | 1 | True |
| orders300 | live | 317 | 317 | 317 | 317 batches: 1×317 | per record | 57,954.30 | 3,287.40 | 0 | True |
| orders300 | mb(5,0.2s) | 317 | 317 | 64 | 64 batches: 5×63, 2×1 | {"max_size": 63, "max_wait": 0, "final_drain": 1} | 57,954.30 | 3,287.40 | 0 | True |
| orders300 | mb(1,0.2s) | 317 | 317 | 317 | 317 batches: 1×317 | {"max_size": 317, "max_wait": 0, "final_drain": 0} | 57,954.30 | 3,287.40 | 0 | True |
| orders300 | mb(50,0.01s) | 317 | 317 | 208 | 208 batches: 3×12, 2×85, 1×111 | {"max_size": 0, "max_wait": 208, "final_drain": 0} | 57,954.30 | 3,287.40 | 0 | True |

Batches only change WHEN records are processed, not HOW. Each record is still its own transaction, in source order, so watermark, windows, late events, dedupe and alerts are identical. The batch release reasons (`max_size` / `max_wait` / `final_drain`) depend on arrival timing; the business results do not.

## Checks
| check | result |
|---|---|
| teaching / live: canonical_results == reference | PASS |
| teaching / live: produced == consumed == 17, batch sizes sum to consumed | PASS |
| teaching / mb(5,0.2s): canonical_results == reference | PASS |
| teaching / mb(5,0.2s): produced == consumed == 17, batch sizes sum to consumed | PASS |
| teaching / mb(1,0.2s): canonical_results == reference | PASS |
| teaching / mb(1,0.2s): produced == consumed == 17, batch sizes sum to consumed | PASS |
| teaching / mb(50,0.01s): canonical_results == reference | PASS |
| teaching / mb(50,0.01s): produced == consumed == 17, batch sizes sum to consumed | PASS |
| reviews / live: canonical_results == reference | PASS |
| reviews / live: produced == consumed == 6, batch sizes sum to consumed | PASS |
| reviews / mb(5,0.2s): canonical_results == reference | PASS |
| reviews / mb(5,0.2s): produced == consumed == 6, batch sizes sum to consumed | PASS |
| reviews / mb(1,0.2s): canonical_results == reference | PASS |
| reviews / mb(1,0.2s): produced == consumed == 6, batch sizes sum to consumed | PASS |
| reviews / mb(50,0.01s): canonical_results == reference | PASS |
| reviews / mb(50,0.01s): produced == consumed == 6, batch sizes sum to consumed | PASS |
| orders300 / live: canonical_results == reference | PASS |
| orders300 / live: produced == consumed == 317, batch sizes sum to consumed | PASS |
| orders300 / mb(5,0.2s): canonical_results == reference | PASS |
| orders300 / mb(5,0.2s): produced == consumed == 317, batch sizes sum to consumed | PASS |
| orders300 / mb(1,0.2s): canonical_results == reference | PASS |
| orders300 / mb(1,0.2s): produced == consumed == 317, batch sizes sum to consumed | PASS |
| orders300 / mb(50,0.01s): canonical_results == reference | PASS |
| orders300 / mb(50,0.01s): produced == consumed == 317, batch sizes sum to consumed | PASS |
| mb(50,0.01s) used the timer (max_wait) at least once on every fixture | PASS |
| mb(5,0.2s) on orders300 released mostly by size | PASS |

**Result: PASS**
