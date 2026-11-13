# SHOPIL-22 — late events and dead letters

Command: `uv run python experiments/shopil_22_late_dead_letters.py`. Input: course teaching fixture (offsets 0–16) plus a late negative review (offset 17) and a naive-timestamp order (offset 18), in source order, on a fresh DB. Config: 60 s windows, out_of_order 0, grace 10. Exported to [`late_events.csv`](late_events.csv) and [`dead_letters.csv`](dead_letters.csv).

## late_events (valid, first-seen, window already closed)
| offset | event_id | kind | event_time | window_start | window_end | watermark_at_arrival | amount_cents | reason | input |
|---|---|---|---|---|---|---|---|---|---|
| 9 | order-late | order | 2026-11-01T10:00:25.000+00:00 | 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:01:15.000+00:00 | 500 | watermark >= window_end + grace | {"amount_cents": 500, "event_id": "order-late", "event_time": "2026-11-01T10:00:25+00:00", "kind": "order", "product_id": 5} |
| 15 | order-boundary | order | 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:02:00.000+00:00 | 2026-11-01T10:02:15.000+00:00 | 800 | watermark >= window_end + grace | {"amount_cents": 800, "event_id": "order-boundary", "event_time": "2026-11-01T10:01:00+00:00", "kind": "order", "product_id": 5} |
| 17 | review-late | review | 2026-11-01T10:00:30.000+00:00 | 2026-11-01T10:00:00.000+00:00 | 2026-11-01T10:01:00.000+00:00 | 2026-11-01T10:03:15.000+00:00 | None | watermark >= window_end + grace | {"event_id": "review-late", "event_time": "2026-11-01T10:00:30+00:00", "kind": "review", "product_id": 5, "rating": 1, "text": "איחור בביקורת"} |

## dead_letters (contract violations)
| offset | reason | input |
|---|---|---|
| 11 | amount_cents must be an integer >= 0 | {"amount_cents": 19.9, "event_id": "bad-money", "event_time": "2026-11-01T10:02:10+00:00", "kind": "order", "product_id": 5} |
| 18 | event_time must include a timezone | {"amount_cents": 700, "event_id": "order-naive", "event_time": "2026-11-01T10:03:20", "kind": "order", "product_id": 5} |

## Checkpoint progression
| offset | status | checkpoint | late_rows | dead_rows | accepted_revenue_cents | watermark_unchanged |
|---|---|---|---|---|---|---|
| 0 | accepted | 0 | 0 | 0 | 1000 | False |
| 1 | accepted | 1 | 0 | 0 | 1000 | False |
| 2 | accepted | 2 | 0 | 0 | 3000 | False |
| 3 | accepted | 3 | 0 | 0 | 4500 | True |
| 4 | accepted | 4 | 0 | 0 | 4500 | True |
| 5 | duplicate | 5 | 0 | 0 | 4500 | True |
| 6 | accepted | 6 | 0 | 0 | 4500 | True |
| 7 | accepted | 7 | 0 | 0 | 4500 | True |
| 8 | accepted | 8 | 0 | 0 | 7500 | False |
| 9 | late | 9 | 1 | 0 | 7500 | True |
| 10 | accepted | 10 | 1 | 0 | 10000 | False |
| 11 | invalid | 11 | 1 | 1 | 10000 | True |
| 12 | accepted | 12 | 1 | 1 | 11000 | False |
| 13 | accepted | 13 | 1 | 1 | 11000 | True |
| 14 | duplicate | 14 | 1 | 1 | 11000 | True |
| 15 | late | 15 | 2 | 1 | 11000 | True |
| 16 | accepted | 16 | 2 | 1 | 12200 | False |
| 17 | late | 17 | 3 | 1 | 12200 | True |
| 18 | invalid | 18 | 3 | 2 | 12200 | True |

## Checks
| check | result |
|---|---|
| late_events: offsets 9 (order-late), 15 (order-boundary), 17 (review-late) | PASS |
| dead_letters: offsets 11 (float money), 18 (naive time) | PASS |
| every side-output row has input, offset and reason | PASS |
| late rows record the watermark at arrival and it was >= window_end + grace | PASS |
| checkpoint advanced past every late and invalid record | PASS |
| late/invalid records did not change accepted revenue or the watermark | PASS |
| retry of a late event (offset 14) is a duplicate, not a second late row | PASS |
| late negative review not counted toward the product-5 window/alert | PASS |
| late order side output totals 13.00 ILS (500 + 800 agorot) | PASS |
| side outputs survive reopening the DB | PASS |

**Result: PASS**
