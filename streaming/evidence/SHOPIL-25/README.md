# SHOPIL-25 — negative-review alerts

Command: `uv run python experiments/shopil_25_review_alerts.py`. Fresh DB; 60 s windows, out_of_order 0, grace 10, alert threshold 3 negatives (rating ≤ 2) per product per window. Input: the course review fixture (offsets 0–5, slides 25/36/41) plus five extra reviews (offsets 6–10).

## Per-record trace
| offset | event_id | product_id | rating | status | window_negatives | alerts_total |
|---|---|---|---|---|---|---|
| 0 | review-1 | 5 | 1 | accepted | 1 | 0 |
| 1 | review-2 | 5 | 2 | accepted | 2 | 0 |
| 2 | review-1 | 5 | 1 | duplicate | 2 | 0 |
| 3 | review-3 | 5 | 1 | accepted | 3 | 1 |
| 4 | review-4 | 5 | 1 | accepted | 4 | 1 |
| 5 | review-5 | 3 | 5 | accepted | 0 | 1 |
| 6 | p2-neg-1 | 2 | 1 | accepted | 1 | 1 |
| 7 | p2-neg-2 | 2 | 1 | accepted | 2 | 1 |
| 8 | p5-w2-1 | 5 | 1 | accepted | 1 | 1 |
| 9 | p5-w2-2 | 5 | 1 | accepted | 2 | 1 |
| 10 | p5-w2-3 | 5 | 1 | accepted | 3 | 2 |

## review_alerts
| product_id | window_start | window_end | negative_reviews_at_alert | trigger_event_id | emitted_at_watermark |
|---|---|---|---|---|---|
| 5 | 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | 3 | review-3 | 2026-11-01T10:00:15.000+00:00 |
| 5 | 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:02:00.000+00:00 | 3 | p5-w2-3 | 2026-11-01T10:01:15.000+00:00 |

## Negative counters per product-window
| product_id | window_start | state | reviews | negative_reviews | threshold | alerted |
|---|---|---|---|---|---|---|
| 2 | 2026-11-01T10:00:00.000+00:00 | closed | 2 | 2 | 3 | False |
| 5 | 2026-11-01T10:00:00.000+00:00 | closed | 4 | 4 | 3 | True |
| 3 | 2026-11-01T10:01:00.000+00:00 | open | 1 | 0 | 3 | False |
| 5 | 2026-11-01T10:01:00.000+00:00 | open | 3 | 3 | 3 | True |

## Checks
| check | result |
|---|---|
| product 5 window [10:00,10:01): exactly one alert, triggered by review-3 (3rd negative) | PASS |
| 4th negative (review-4) raised the window counter to 4 without a second alert | PASS |
| retried review-1 is a duplicate and did not change the counter (stays 2) | PASS |
| product 2 with 2 negatives (below threshold) has no alert | PASS |
| positive review (product 3) has no alert | PASS |
| product 5 next window gets its own single alert | PASS |
| reopen + replay of every offset left alerts unchanged | PASS |
| alerts_emitted counter == alert rows | PASS |

**Result: PASS**
