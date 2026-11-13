# SHOPIL-21 — watermark and grace windows

Command: `uv run python experiments/shopil_21_watermark_grace.py`. Input: course teaching fixture (17 arrivals, source order). Fresh DB. Times below are UTC on 2026-11-01.

Policy: `watermark = max(previous, max_seen_event_time - 0 s)`; window `[start, start+60s)` closes when `watermark >= end + 10 s`. `model_*` columns come from `shopil_streaming.windows.WindowModel`, an independent implementation of the same rule.

## Per-record trace
| offset | event_id | event_time | status | model_status | max_event_time | watermark | model_watermark | closed_now |
|---|---|---|---|---|---|---|---|---|
| 0 | order-a | 10:00:05 | accepted | accepted | 10:00:05 | 10:00:05 | 10:00:05 |  |
| 1 | review-1 | 10:00:10 | accepted | accepted | 10:00:10 | 10:00:10 | 10:00:10 |  |
| 2 | order-b | 10:01:05 | accepted | accepted | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 3 | order-c | 10:00:20 | accepted | accepted | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 4 | review-2 | 10:00:12 | accepted | accepted | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 5 | order-a | 10:00:05 | duplicate | duplicate | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 6 | review-3 | 10:00:15 | accepted | accepted | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 7 | review-4 | 10:00:18 | accepted | accepted | 10:01:05 | 10:01:05 | 10:01:05 |  |
| 8 | order-d | 10:01:15 | accepted | accepted | 10:01:15 | 10:01:15 | 10:01:15 | [10:00:00,10:01:00) |
| 9 | order-late | 10:00:25 | late | late | 10:01:15 | 10:01:15 | 10:01:15 |  |
| 10 | order-e | 10:02:05 | accepted | accepted | 10:02:05 | 10:02:05 | 10:02:05 |  |
| 11 | bad-money | (invalid) | invalid | invalid | 10:02:05 | 10:02:05 | 10:02:05 |  |
| 12 | order-f | 10:02:15 | accepted | accepted | 10:02:15 | 10:02:15 | 10:02:15 | [10:01:00,10:02:00) |
| 13 | review-5 | 10:02:05 | accepted | accepted | 10:02:15 | 10:02:15 | 10:02:15 |  |
| 14 | order-late | 10:00:25 | duplicate | duplicate | 10:02:15 | 10:02:15 | 10:02:15 |  |
| 15 | order-boundary | 10:01:00 | late | late | 10:02:15 | 10:02:15 | 10:02:15 |  |
| 16 | order-h | 10:03:15 | accepted | accepted | 10:03:15 | 10:03:15 | 10:03:15 | [10:02:00,10:03:00) |

## Emitted revenue windows
| window | orders | revenue_cents | closure_reason | emitted_at_watermark | close_threshold (end+grace) |
|---|---|---|---|---|---|
| [10:00:00, 10:01:00) | 2 | 2500 | watermark | 10:01:15 | 10:01:10 |
| [10:01:00, 10:02:00) | 2 | 5000 | watermark | 10:02:15 | 10:02:10 |
| [10:02:00, 10:03:00) | 2 | 3500 | watermark | 10:03:15 | 10:03:10 |
| [10:03:00, 10:04:00) | 1 | 1200 | demo_end_of_input | 10:03:15 | 10:04:10 |

Hand check: `[10:00:00, 10:01:00)` needs watermark ≥ 10:01:10. `order-d` (10:01:15, offset 8) is the first accepted event at or after that, so the window closes there with 2 orders (`order-a` 1000 + `order-c` 1500 = 2500 agorot = ₪25.00; the duplicate `order-a` is not counted again). Between offsets 2 and 7 the watermark sits at 10:01:05 < 10:01:10, so the window stays open and `order-c` (10:00:20, out of order) is still accepted. `order-late` (10:00:25, offset 9) then arrives after closure and is late. `order-boundary` at exactly 10:01:00 belongs to `[10:01:00, 10:02:00)` (half-open windows), which had already closed by watermark 10:02:15, so it is late too.

## Checks
| check | result |
|---|---|
| engine matches independent model on every record (status, max time, watermark, closures) | PASS |
| watermark never decreases | PASS |
| every watermark-closed window satisfied watermark >= end + grace at emission | PASS |
| each window closed at the first record that satisfied the rule | PASS |
| invalid/duplicate/late records did not move max event time | PASS |
| 1.2 s of wall-clock idle time did not move the watermark | PASS |
| finalize_demo closed remaining windows as demo_end_of_input | PASS |
| finalize_demo did not change the watermark | PASS |

**Result: PASS**
