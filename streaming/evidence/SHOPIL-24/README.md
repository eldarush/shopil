# SHOPIL-24 — revenue windows and reconciliation

Command: `uv run python experiments/shopil_24_revenue_reconciliation.py`. Fresh DB per run; 60 s windows, out_of_order 0, grace 10. Amounts are integer agorot in the engine and converted to ₪ only for display.

> Stream reconciliation only: accepted + late vs. a full recomputation of the same valid, unique stream order events. Not comparable to or additive with Batch paid revenue (fct_orders).

## Teaching fixture — mid-stream (after offset 12, before finalize)
Monitor: | checkpoint | watermark | finalized | closed_windows | closed_revenue_ils | open_windows | open_revenue_ils | accepted_revenue_ils | next_close_at_watermark |
|---|---|---|---|---|---|---|---|---|
| 12 | 2026-11-01T10:02:15.000+00:00 | False | 2 | 75.00 | 1 | 35.00 | 110.00 | 2026-11-01T10:03:10.000+00:00 |

| window_start | window_end | state | orders | revenue_cents | revenue_ils | closure_reason | emitted_at_watermark | closes_at_watermark |
|---|---|---|---|---|---|---|---|---|
| 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | closed | 2 | 2500 | 25.00 | watermark | 2026-11-01T10:01:15.000+00:00 | 2026-11-01T10:01:10.000+00:00 |
| 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:02:00.000+00:00 | closed | 2 | 5000 | 50.00 | watermark | 2026-11-01T10:02:15.000+00:00 | 2026-11-01T10:02:10.000+00:00 |
| 2026-11-01T10:02:00.000+00:00 | 2026-11-01T10:03:00.000+00:00 | open | 2 | 3500 | 35.00 |  |  | 2026-11-01T10:03:10.000+00:00 |

## Teaching fixture — after finalize_demo
| window_start | window_end | state | orders | revenue_cents | revenue_ils | closure_reason | emitted_at_watermark | closes_at_watermark |
|---|---|---|---|---|---|---|---|---|
| 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | closed | 2 | 2500 | 25.00 | watermark | 2026-11-01T10:01:15.000+00:00 | 2026-11-01T10:01:10.000+00:00 |
| 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:02:00.000+00:00 | closed | 2 | 5000 | 50.00 | watermark | 2026-11-01T10:02:15.000+00:00 | 2026-11-01T10:02:10.000+00:00 |
| 2026-11-01T10:02:00.000+00:00 | 2026-11-01T10:03:00.000+00:00 | closed | 2 | 3500 | 35.00 | watermark | 2026-11-01T10:03:15.000+00:00 | 2026-11-01T10:03:10.000+00:00 |
| 2026-11-01T10:03:00.000+00:00 | 2026-11-01T10:04:00.000+00:00 | closed | 1 | 1200 | 12.00 | demo_end_of_input | 2026-11-01T10:03:15.000+00:00 | 2026-11-01T10:04:10.000+00:00 |

Reconciliation: | accepted_orders | accepted_ils | late_orders | late_ils | stream_total_ils | recomputed_orders | recomputed_ils | orders_match | revenue_match |
|---|---|---|---|---|---|---|---|---|
| 7 | 122.00 | 2 | 13.00 | 135.00 | 9 | 135.00 | True | True |

Hand check: accepted = order-a 10.00 + order-c 15.00 + order-b 20.00 + order-d 30.00 + order-e 25.00 + order-f 10.00 + order-h 12.00 = ₪122.00; late = order-late 5.00 + order-boundary 8.00 = ₪13.00; duplicates (order-a, order-late retries) and bad-money (19.9, invalid) are excluded from both sides → 9 valid unique orders = ₪135.00.

## Course generator — 300 orders, 17 retries, seed 7
| accepted_orders | accepted_ils | late_orders | late_ils | stream_total_ils | recomputed_orders | recomputed_ils | orders_match | revenue_match |
|---|---|---|---|---|---|---|---|---|
| 286 | 57,954.30 | 14 | 3,287.40 | 61,241.70 | 300 | 61,241.70 | True | True |

## Checks
| check | result |
|---|---|
| teaching: accepted 122.00 ILS | PASS |
| teaching: late 13.00 ILS | PASS |
| teaching: full recompute of valid unique orders 135.00 ILS == accepted + late | PASS |
| teaching: closed window revenue sums to accepted after finalize | PASS |
| mid-stream: accepted revenue includes open windows (closed-only would under-report) | PASS |
| orders300: accepted 57,954.30 ILS (grace 10 reference) | PASS |
| orders300: accepted + late == recompute 61,241.70 ILS | PASS |

**Result: PASS**
