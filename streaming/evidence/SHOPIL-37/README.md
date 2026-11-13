# SHOPIL-37 — reproducible fixtures and independent databases

Commands: `uv run python experiments/shopil_37_fixtures.py --write` regenerates the files; `uv run python experiments/shopil_37_fixtures.py` verifies them. Fixtures live in `fixtures/streaming/` (JSON Lines, one delivery per line, in source order) with `manifest.json` holding generator, sha256, config and expected results. `shopil_streaming.fixtures.load_fixture` refuses any file whose offsets are not `0..n-1` in file order; `fresh_engine` always opens a new DB under `runs/`.

## Fixtures (config: 60 s windows, out_of_order 0, grace 10, threshold 3)
| fixture | records | sha256 | accepted_ils | late_ils | duplicates | invalid | late | alerts |
|---|---|---|---|---|---|---|---|---|
| teaching | 17 | 2cd7612efb47eaf7 | 122.00 | 13.00 | 2 | 1 | 2 | 1 |
| reviews | 6 | ee94597a90282524 | 0.00 | 0.00 | 1 | 0 | 0 | 1 |
| orders300 | 317 | 045a29f9a5201ef4 | 57,954.30 | 3,287.40 | 17 | 0 | 14 | 0 |

## Control: why input must not be sorted by event time
| fixture | source_order_late | sorted_late | source_order_accepted_ils | sorted_accepted_ils |
|---|---|---|---|---|
| teaching | 2 | 0 | 122.00 | 135.00 |
| orders300 | 14 | 0 | 57,954.30 | 61,241.70 |

Sorting removes the disorder the experiments are about. Late events become on-time and the accepted revenue changes, so a sorted run would no longer test the assignment's lateness rules.

## Checks
| check | result |
|---|---|
| teaching: regenerated == committed file (sha256) | PASS |
| teaching: loaded deliveries == generator output, same order | PASS |
| teaching: two independent fresh DBs -> identical canonical_results | PASS |
| teaching: results match manifest expectations | PASS |
| reviews: regenerated == committed file (sha256) | PASS |
| reviews: loaded deliveries == generator output, same order | PASS |
| reviews: two independent fresh DBs -> identical canonical_results | PASS |
| reviews: results match manifest expectations | PASS |
| orders300: regenerated == committed file (sha256) | PASS |
| orders300: loaded deliveries == generator output, same order | PASS |
| orders300: two independent fresh DBs -> identical canonical_results | PASS |
| orders300: results match manifest expectations | PASS |
| control: sorting by event_time changes late counts (so we never sort) | PASS |

**Result: PASS**
