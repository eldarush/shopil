# SHOPIL-40 — window boundaries and grace policy

Command: `uv run python experiments/shopil_40_window_grace.py`. Fresh DB per run. Fixed: 60 s windows, out_of_order 0, alert threshold 3. Only grace changes in part 3.

## 1. Hand calculation: window [10:00:00, 10:01:00) on the teaching fixture (grace 10)
Rule: the window closes when watermark ≥ 10:01:00 + 10 s = 10:01:10.
Walking the deliveries in source order: order-a 10:00:05 (+10.00), review-1, order-b 10:01:05 (other window, watermark → 10:01:05 < 10:01:10, still open), order-c 10:00:20 (+15.00, accepted because the window is still open), reviews, the order-a retry (duplicate), then order-d 10:01:15 moves the watermark to 10:01:15 ≥ 10:01:10 → **closed at offset 8**. order-late 10:00:25 arrives at offset 9, after closure → late.

| | orders | revenue | closed at offset | closed at watermark | late for this window |
|---|---|---|---|---|---|
| by hand | 2 | ₪25.00 | 8 | 10:01:15.000 | ['order-late'] |
| engine | 2 | ₪25.00 | 8 | 10:01:15.000 | ['order-late'] |

## 2. Boundary cases (hand-built input, grace 10)
| offset | case | event_time | status | window_start | watermark | closed_windows |
|---|---|---|---|---|---|---|
| 0 | last ms of window 1 | 10:00:59.999 | accepted | 10:00:00 | 10:00:59.999 | [] |
| 1 | exact start of window 2 | 10:01:00.000 | accepted | 10:01:00 | 10:01:00.000 | [] |
| 2 | watermark one ms before end+grace | 10:01:09.999 | accepted | 10:01:00 | 10:01:09.999 | [] |
| 3 | watermark == end+grace (closes window 1) | 10:01:10.000 | accepted | 10:01:00 | 10:01:10.000 | ["10:00:00"] |
| 4 | event for closed window 1 (late) | 10:00:30.000 | late | 10:00:00 | 10:01:10.000 | ["10:00:00"] |
| 5 | event for still-open window 2 | 10:01:15.000 | accepted | 10:01:00 | 10:01:15.000 | ["10:00:00"] |

late_events output:
| offset | event_id | event_time | window_start | watermark | reason |
|---|---|---|---|---|---|
| 4 | b-late | 2026-11-01T10:00:30.000+00:00 | 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:10.000+00:00 | watermark >= window_end + grace |

## 3–4. Grace policy change on the same input (orders300 fixture, 317 deliveries, source order)
| grace_s | accepted_orders | late_orders | accepted_ils | late_ils | windows_closed_by_watermark | windows_open_at_end_of_input | avg_close_after_window_end_s | min_close_after_window_end_s | accepted_plus_late_ils | full_recompute_ils | reconciles |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 279 | 21 | 56,916.90 | 4,324.80 | 9 | 1 | 0.9 | 0.081 | 61,241.70 | 61,241.70 | True |
| 10 | 286 | 14 | 57,954.30 | 3,287.40 | 9 | 1 | 11.4 | 10.301 | 61,241.70 | 61,241.70 | True |
| 120 | 300 | 0 | 61,241.70 | 0.00 | 7 | 3 | 120.7 | 120.081 | 61,241.70 | 61,241.70 | True |

**Explanation.** A window is emitted once the watermark passes `end + grace`. With grace 0, windows close as soon as event time reaches their end, so every out-of-order order that arrives later (the fixture delays ~10 % of orders by 20–90 s) is late: 21 late orders. Grace 10 waits 10 s longer, which rescues the orders delayed by less than that margin past the window end: 14 late. Grace 120 is longer than the largest delay, so nothing is late, but each window is emitted at least 120 s of event time after it ends, and more windows are still open when input stops (they only close via `finalize_demo`). The trade-off is completeness against latency. In every case accepted + late equals the full recomputation of the same valid, unique events.

## Checks
| check | result |
|---|---|
| part 1: hand calculation == engine (2 orders, 25.00 ILS, closed at offset 8, late order-late) | PASS |
| part 2: half-open boundary and inclusive end+grace closure behave as specified | PASS |
| part 3: reference late counts 21 / 14 / 0 for grace 0 / 10 / 120 | PASS |
| part 3: reference accepted 56,916.90 / 57,954.30 / 61,241.70 ILS | PASS |
| part 3: larger grace closes windows later (every close lag >= grace) | PASS |
| part 3: larger grace leaves more windows open at end of input | PASS |
| part 4: accepted + late == full recompute for every grace | PASS |

**Result: PASS**
