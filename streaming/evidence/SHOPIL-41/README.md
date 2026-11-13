# SHOPIL-41 — event deduplication, offsets and restart

Command: `uv run python experiments/shopil_41_dedup_offsets_restart.py`. Input: committed `fixtures/streaming/orders300.jsonl` (317 deliveries in source order). Teaching config (60 s, out_of_order 0, grace 10), a fresh DB per scenario.

## A/B. Duplicate event_ids at new offsets
| deliveries | unique_orders | duplicates | accepted_ils | late_ils | unique_recompute_ils |
|---|---|---|---|---|---|
| 317 | 300 | 17 | 57,954.30 | 3,287.40 | 61,241.70 |

| offset | event_id | status |
|---|---|---|
| 34 | ev-0003 | duplicate |
| 63 | ev-0064 | duplicate |
| 79 | ev-0078 | duplicate |
| 86 | ev-0085 | duplicate |
| 96 | ev-0091 | duplicate |
| 115 | ev-0109 | duplicate |
| 133 | ev-0126 | duplicate |
| 134 | ev-0125 | duplicate |
| 161 | ev-0150 | duplicate |
| 166 | ev-0156 | duplicate |
| 178 | ev-0166 | duplicate |
| 212 | ev-0201 | duplicate |
| 217 | ev-0202 | duplicate |
| 267 | ev-0254 | duplicate |
| 280 | ev-0264 | duplicate |
| 308 | ev-0297 | duplicate |
| 310 | ev-0294 | duplicate |

Altered retry: `ev-0000` re-sent at offset 317 with amount 999,999 agorot → `duplicate`. The first stored payload (offset 1) is kept and revenue does not change.

## C. Cancel mid-run, reopen, resume from the durable checkpoint
| checkpoint_at_cancel | resumed_from_offset | records_after_restart | final_accepted_ils | uninterrupted_accepted_ils | identical |
|---|---|---|---|---|---|
| 15 | 16 | 301 | 57,954.30 | 57,954.30 | True |

## D. Replay of saved offsets after reopen
All 317 offsets delivered again to a reopened engine → statuses ['replayed']; every SQLite table compared row by row before/after: identical = True.

## Checks
| check | result |
|---|---|
| A: exactly the 17 producer retries are duplicates | PASS |
| A: accepted + late revenue == sum over unique event_ids (61,241.70 ILS) | PASS |
| B: altered retry of ev-0000 at a new offset is a duplicate | PASS |
| B: revenue unchanged by the altered retry (first payload kept) | PASS |
| C: stream was really interrupted mid-run | PASS |
| C: restart resumed exactly at the durable checkpoint | PASS |
| C: restarted result == uninterrupted result | PASS |
| D: every saved offset replayed after reopen | PASS |
| D: every durable table unchanged by the replay | PASS |

**Result: PASS**
