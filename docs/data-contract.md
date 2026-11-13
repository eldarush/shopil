# ShopIL data and evidence contract

This contract describes the implemented DuckDB/SQLite readers and immutable evidence types. Track open cross-source mapping decisions in SHOPIL-9. The DuckDB names reflect the pinned ETL lab and authored review models; the SQLite names reflect the course streaming engine.

## Historical DuckDB

| Relation | Row grain and fields | Time and units |
|---|---|---|
| `staging.stg_products` | One row per integer `product_id`. Name, category, list price. | `list_price` is a decimal amount in ILS. |
| `marts.fct_orders` | One valid historical order per integer `order_id`. Includes `product_id`, `customer_id`, `status`, `quantity` and `amount`. | `order_date` is a calendar date. Paid revenue sums `amount` in ILS for `status = 'paid'`. Amount already includes quantity. |
| `raw.reviews`, `raw.reviews__tags` | Raw review rows may repeat `review_id`. The nested product key is `product__id`. Tag rows join to the specific raw review on `_dlt_parent_id = _dlt_id`. | `created_at` is stored as a timestamp with time zone by dlt for the reference fixture. |
| `staging.stg_reviews` | One valid row per integer `review_id`, with `product_id`, `order_id`, `rating`, `review_text` and `created_at`. | Earliest valid `created_at` per ID, exposed as a timestamp with time zone. Ratings are 1–5. |
| `marts.mart_product_reviews` | One row per product. Review count, average rating, negative count, paid order count and paid revenue. | The reader reports full-history coverage independently of the configured question range. A product with zero reviews has a null average. |

## Streaming SQLite

| Relation | Row grain and fields |
|---|---|
| `stream_meta` | Singleton containing committed `checkpoint`, `watermark_ms`, `max_event_time_ms` and finalization status. The checkpoint is the last committed source offset and starts at −1. |
| `order_state`, `realtime_revenue` | Open and closed order windows, respectively, keyed by `window_start_ms`. Includes order counts and integer `revenue_cents`. |
| `review_state`, `review_windows`, `review_alerts` | Open review state, closed review state and alerts, keyed by product and window start. Ratings ≤ 2 count as negative. Alert rows include the triggering `event_id`. |
| `accepted_reviews`, `seen_events` | String `event_id` identifies a business event. `seen_events.source_offset` identifies its first recorded delivery. |
| `late_events`, `dead_letters` | Late valid events and invalid input with their reasons and source offsets. |

Stream `event_time` must carry a timezone. The engine converts it to UTC epoch milliseconds. Window bounds are `[start, end)`. An order event carries integer `amount_cents` in agorot and has no payment status. Closed-window and open-window totals must be labeled separately. Keep stream order amounts separate from historical paid revenue. Validate product ID mappings before joining sources. A stream event ID or source offset has no declared correspondence to a historical review ID or order ID.

## Per-question evidence payload

The streaming ingestion layer rejects values that its durable representation cannot store or display: signed 64-bit ID/amount overflow, per-window revenue overflow, unpaired Unicode surrogates, or unrenderable configured window/closing/watermark dates. These records advance the checkpoint into dead letters without changing business state. Stateless `contract.check` verifies only the event schema; the ingestion outcome is authoritative for state-dependent capacity. Stream reconciliation reports recorded dead-letter exclusions and schema-only input totals separately. See [storage bounds and attribution](../streaming/README.md#teaching-configuration).

- `snapshot_id`, `captured_at` in UTC, and `question`.
- A `sources` list of result sets. Each has `table`, `metric`, `units`, `identifiers`, `time_start`, `time_end`, and `rows`. The default `time_coverage="bounded"` requires ordered, same-type half-open bounds. If the complete historical warehouse has no paid orders or cleaned reviews, product mart rows still retain known zero counts/revenue and null averages; they carry `time_coverage="no_observed_history"` and both time bounds are null. This explicit historical-only case does not invent a date range and cannot carry streaming checkpoint/watermark/window metadata. Date-filtered paid totals and review queries retain their configured bounds. An empty result set and a numeric zero carry distinct meanings.
- For streaming result sets, include `checkpoint`, `watermark` and `window_state` for window results. Encode exact historical ILS amounts as decimal strings and streaming amounts as integer agorot. Read those fields and the corresponding rows in one SQLite read transaction.
- Preserve this same immutable payload for Actor, Critic and every retry. Read a fresh payload for a new question. Calculate counts and sums in SQL or Python.

`chat.capture.build_capture` composes both readers after Batch completes. Historical order filters use calendar dates; review timestamp filters use UTC midnight bounds. All product metrics are retained, with the latest 12 cleaned review texts across products as an explicitly labeled sample. Streaming evidence includes all products in the latest three distinct window starts by default, with omission counts and separate all-time counters. The source package and scope metadata are frozen together for each question. There is no cross-database transaction: the historical warehouse must remain unchanged during chat. Product mapping remains unverified, so sources are kept separate. See [runtime configuration](../README.md#evidence-scope-and-configuration).

Reference implementations and tracking issues

[ETL lab](https://github.com/paulavrahami/etl-course-demo/tree/6788526695bbb20ac40f46a947413fff3545b86a), [streaming SQLite schema](https://github.com/eldarush/shopil/blob/5335ee59f83aeffeb0e6bafa1d045d2f753aee78/streaming/src/streaming_lab/engine.py), [SHOPIL-9](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-9), [SHOPIL-28](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-28).
