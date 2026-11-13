# SHOPIL-17 — streaming event contract

Command: `uv run python experiments/shopil_17_contract.py` (fresh SQLite DB per run, course `StreamEngine` unmodified)

## Contract
| field | applies_to | type | rule |
|---|---|---|---|
| event_id | all | non-empty string | Business identity. Duplicates are ignored by the engine, whatever their offset. |
| kind | all | 'order' \| 'review' | Any other value is invalid. |
| event_time | all | ISO-8601 with timezone (or tz-aware datetime) | Naive timestamps are invalid. Normalised to UTC; windows are computed in integer milliseconds. |
| product_id | all | integer >= 1 | Strings, floats and booleans are invalid. Same id space as ShopIL products.product_id. |
| amount_cents | order | integer >= 0 (agorot) | Never a float. 12200 agorot = 122.00 ILS. Orders must not carry a rating. |
| order_id | order (optional) | integer >= 1 | Informational only. |
| rating | review | integer 1..5 | Reviews must not carry amount_cents. rating <= 2 counts as negative. |
| text | review | string | Defaults to ''. Must be a string. |

Streaming order events carry amount_cents but no payment status; they are not equivalent to Batch paid revenue (ILS, status='paid').

## Run: 23 deliveries in source order
| offset | case | expected | engine_status | reason | checkpoint_after |
|---|---|---|---|---|---|
| 0 | valid order (Israel time, +02:00) | accepted | accepted |  | 0 |
| 1 | valid review (UTC Z suffix) | accepted | accepted |  | 1 |
| 2 | missing event_id | invalid | invalid | missing required field: event_id | 2 |
| 3 | blank event_id | invalid | invalid | event_id must be a nonempty string | 3 |
| 4 | unknown kind | invalid | invalid | kind must be 'order' or 'review' | 4 |
| 5 | event_time without timezone | invalid | invalid | event_time must include a timezone | 5 |
| 6 | event_time not ISO-8601 | invalid | invalid | event_time must be ISO-8601 | 6 |
| 7 | missing product_id | invalid | invalid | missing required field: product_id | 7 |
| 8 | product_id as string | invalid | invalid | product_id must be an integer >= 1 | 8 |
| 9 | product_id zero | invalid | invalid | product_id must be an integer >= 1 | 9 |
| 10 | amount in shekels as float | invalid | invalid | amount_cents must be an integer >= 0 | 10 |
| 11 | negative amount | invalid | invalid | amount_cents must be an integer >= 0 | 11 |
| 12 | boolean amount | invalid | invalid | amount_cents must be an integer >= 0 | 12 |
| 13 | order missing amount | invalid | invalid | amount_cents must be an integer >= 0 | 13 |
| 14 | order carrying a rating | invalid | invalid | an order must not contain a rating | 14 |
| 15 | review rating 0 | invalid | invalid | rating must be an integer >= 1 | 15 |
| 16 | review rating 7 | invalid | invalid | rating must be between 1 and 5 | 16 |
| 17 | review missing rating | invalid | invalid | rating must be an integer >= 1 | 17 |
| 18 | review carrying amount_cents | invalid | invalid | a review must not contain amount_cents | 18 |
| 19 | review text not a string | invalid | invalid | text must be a string | 19 |
| 20 | payload is not an object | invalid | invalid | event must be an object | 20 |
| 21 | invalid event one hour in the future (must not move watermark) | invalid | invalid | amount_cents must be an integer >= 0 | 21 |
| 22 | valid order after the bad input (stream keeps going) | accepted | accepted |  | 22 |

## dead_letters (input, offset, reason)
| offset | reason | input |
|---|---|---|
| 2 | missing required field: event_id | {"amount_cents": 100, "event_time": "2026-11-01T10:00:11Z", "kind": "order", "product_id": 5} |
| 3 | event_id must be a nonempty string | {"amount_cents": 100, "event_id": "  ", "event_time": "2026-11-01T10:00:11Z", "kind": "order", "product_id": 5} |
| 4 | kind must be 'order' or 'review' | {"amount_cents": 100, "event_id": "c-refund", "event_time": "2026-11-01T10:00:12Z", "kind": "refund", "product_id": 5} |
| 5 | event_time must include a timezone | {"amount_cents": 100, "event_id": "c-naive", "event_time": "2026-11-01T10:00:13", "kind": "order", "product_id": 5} |
| 6 | event_time must be ISO-8601 | {"amount_cents": 100, "event_id": "c-badtime", "event_time": "yesterday", "kind": "order", "product_id": 5} |
| 7 | missing required field: product_id | {"amount_cents": 100, "event_id": "c-noproduct", "event_time": "2026-11-01T10:00:14Z", "kind": "order"} |
| 8 | product_id must be an integer >= 1 | {"amount_cents": 100, "event_id": "c-strproduct", "event_time": "2026-11-01T10:00:14Z", "kind": "order", "product_id": "5"} |
| 9 | product_id must be an integer >= 1 | {"amount_cents": 100, "event_id": "c-zeroproduct", "event_time": "2026-11-01T10:00:14Z", "kind": "order", "product_id": 0} |
| 10 | amount_cents must be an integer >= 0 | {"amount_cents": 19.9, "event_id": "c-float", "event_time": "2026-11-01T10:00:15Z", "kind": "order", "product_id": 5} |
| 11 | amount_cents must be an integer >= 0 | {"amount_cents": -100, "event_id": "c-negative", "event_time": "2026-11-01T10:00:15Z", "kind": "order", "product_id": 5} |
| 12 | amount_cents must be an integer >= 0 | {"amount_cents": true, "event_id": "c-bool", "event_time": "2026-11-01T10:00:15Z", "kind": "order", "product_id": 5} |
| 13 | amount_cents must be an integer >= 0 | {"event_id": "c-noamount", "event_time": "2026-11-01T10:00:15Z", "kind": "order", "product_id": 5} |
| 14 | an order must not contain a rating | {"amount_cents": 100, "event_id": "c-orderrating", "event_time": "2026-11-01T10:00:16Z", "kind": "order", "product_id": 5, "rating": 3} |
| 15 | rating must be an integer >= 1 | {"event_id": "c-rating0", "event_time": "2026-11-01T10:00:17Z", "kind": "review", "product_id": 5, "rating": 0, "text": "x"} |
| 16 | rating must be between 1 and 5 | {"event_id": "c-rating7", "event_time": "2026-11-01T10:00:17Z", "kind": "review", "product_id": 5, "rating": 7, "text": "x"} |
| 17 | rating must be an integer >= 1 | {"event_id": "c-norating", "event_time": "2026-11-01T10:00:17Z", "kind": "review", "product_id": 5, "text": "x"} |
| 18 | a review must not contain amount_cents | {"amount_cents": 100, "event_id": "c-reviewamount", "event_time": "2026-11-01T10:00:18Z", "kind": "review", "product_id": 5, "rating": 4, "text": "x"} |
| 19 | text must be a string | {"event_id": "c-textnum", "event_time": "2026-11-01T10:00:18Z", "kind": "review", "product_id": 5, "rating": 4, "text": 123} |
| 20 | event must be an object | ["c-list", "order", "2026-11-01T10:00:19Z", 5, 100] |
| 21 | amount_cents must be an integer >= 0 | {"amount_cents": 1.5, "event_id": "c-future", "event_time": "2026-11-01T11:00:00Z", "kind": "order", "product_id": 5} |

## Checks
| check | result |
|---|---|
| every case has the expected status | PASS |
| every invalid input is in dead_letters with input, offset and reason | PASS |
| checkpoint advanced over invalid input (last offset) | PASS |
| a future-dated invalid event (11:00) did not move max event time / watermark | PASS |
| contract.check() reproduces the engine reason | PASS |
| stats.invalid == dead letters | PASS |

Final: checkpoint=22, watermark=2026-11-01T10:00:20.000+00:00, stats={"received": 23, "accepted": 3, "accepted_orders": 2, "accepted_reviews": 1, "duplicates": 0, "late": 0, "invalid": 20, "revenue_windows_emitted": 0, "review_windows_emitted": 0, "alerts_emitted": 0}

**Result: PASS**
