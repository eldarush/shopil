# SHOPIL-18 — continuous producer and bounded asyncio.Queue

Command: `uv run python experiments/shopil_18_producer_queue.py`. The endless deterministic ShopIL source (`shopil_streaming.source`) feeds the course `run_live`. Each run uses a fresh SQLite DB. For a long interactive run use `uv run python -m shopil_streaming.service --mode live --seconds 30`.

## A. Normal pace (interval 0.01 s, queue 8, no sink delay, 2 s admission)
| t_s | produced | consumed | depth | maxsize | backpressure_waits | checkpoint | revenue |
|---|---|---|---|---|---|---|---|
| 0.036 | 1 | 1 | 0 | 8 | 0 | 0 | 698.00 |
| 0.228 | 13 | 13 | 0 | 8 | 0 | 12 | 2,103.60 |
| 0.398 | 24 | 24 | 0 | 8 | 0 | 23 | 4,595.50 |
| 0.585 | 36 | 36 | 0 | 8 | 0 | 35 | 6,419.20 |
| 0.768 | 48 | 48 | 0 | 8 | 0 | 47 | 7,893.90 |
| 0.942 | 59 | 59 | 0 | 8 | 0 | 58 | 9,349.40 |
| 1.127 | 71 | 71 | 0 | 8 | 0 | 70 | 11,242.20 |
| 1.295 | 82 | 82 | 0 | 8 | 0 | 81 | 12,447.90 |
| 1.485 | 94 | 94 | 0 | 8 | 0 | 93 | 14,410.70 |
| 1.674 | 106 | 106 | 0 | 8 | 0 | 105 | 16,215.20 |
| 1.844 | 117 | 117 | 0 | 8 | 0 | 116 | 17,870.00 |
| 2.026 | 129 | 129 | 0 | 8 | 0 | 128 | 20,341.80 |

Summary: | produced | consumed | queue_maxsize | max_queue_depth | backpressure_waits | producer_wait_s | stop_reason | elapsed_s |
|---|---|---|---|---|---|---|---|
| 129 | 129 | 8 | 1 | 0 | 0.0073 | duration | 2.0077 |

## B. Slow sink (interval 0 s, queue 4, 0.02 s per record, 2 s admission)
| t_s | produced | consumed | depth | maxsize | backpressure_waits | checkpoint | revenue |
|---|---|---|---|---|---|---|---|
| 0.054 | 5 | 1 | 4 | 4 | 2 | 0 | 698.00 |
| 0.242 | 11 | 7 | 4 | 4 | 8 | 6 | 1,614.80 |
| 0.429 | 17 | 13 | 4 | 4 | 14 | 12 | 2,103.60 |
| 0.645 | 24 | 20 | 4 | 4 | 21 | 19 | 3,359.50 |
| 0.834 | 30 | 26 | 4 | 4 | 27 | 25 | 4,944.50 |
| 1.018 | 36 | 32 | 4 | 4 | 33 | 31 | 5,930.40 |
| 1.209 | 42 | 38 | 4 | 4 | 39 | 37 | 6,538.20 |
| 1.397 | 48 | 44 | 4 | 4 | 45 | 43 | 7,116.90 |
| 1.582 | 54 | 50 | 4 | 4 | 51 | 49 | 8,342.70 |
| 1.801 | 61 | 57 | 4 | 4 | 58 | 56 | 9,209.60 |
| 1.986 | 67 | 63 | 4 | 4 | 64 | 62 | 10,126.40 |
| 2.171 | 69 | 69 | 0 | 4 | 66 | 68 | 10,385.20 |

Summary: | produced | consumed | queue_maxsize | max_queue_depth | backpressure_waits | producer_wait_s | stop_reason | elapsed_s |
|---|---|---|---|---|---|---|---|
| 69 | 69 | 4 | 4 | 66 | 1.997 | duration | 2.1433 |

## Checks
| check | result |
|---|---|
| A: first record consumed while producer was still admitting (produced kept rising afterwards) | PASS |
| A: consumed advanced across many observations during production | PASS |
| A: graceful stop drained the queue (produced == consumed) | PASS |
| B: bounded queue never exceeded maxsize | PASS |
| B: producer experienced backpressure (waits > 0) | PASS |
| B: graceful stop drained the queue (produced == consumed) | PASS |

**Result: PASS**
