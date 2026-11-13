# ShopIL streaming

Continuous ingestion and processing of ShopIL order and review events (assignment part B), owned by the [Streaming epic SHOPIL-3](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-3).

It runs the course `StreamEngine` with the storage validation additions attributed in [SOURCES.md](SOURCES.md): a producer feeds a bounded `asyncio.Queue`, a consumer processes either one record at a time (`run_live`) or in small batches (`run_microbatch`), and every event is committed to SQLite in its own transaction together with dedupe state, window state, side outputs and the checkpoint.

## Setup (Windows / PowerShell, uv)

Run from this `streaming/` directory:

```powershell
uv python install 3.13.12
uv sync --locked
uv run python -m pytest -q
```

## Run the continuous stream

```powershell
# one record at a time, 30 s of admission, progress every 0.5 s
uv run python -m shopil_streaming.service --mode live --seconds 30
# small batches (released at 5 records or 0.2 s after the first record, whichever is first)
uv run python -m shopil_streaming.service --mode microbatch --batch-size 5 --max-wait 0.2 --seconds 30
# keep running next to the chat; Ctrl+C stops it, re-running with the same --db resumes
uv run python -m shopil_streaming.service --db runs\chat\stream.db --forever
```

The source (`shopil_streaming.source`) is endless and deterministic per offset, uses the ShopIL catalog product ids 1–8 and prices in agorot, and includes producer retries (same `event_id`, new offset) and out-of-order events.

## Layout

| Path | What |
| --- | --- |
| `src/streaming_lab/` | Course engine with documented storage validation additions |
| `src/shopil_streaming/` | Our ShopIL layer (contract, source, runners, reports) |
| `fixtures/` | Versioned, deterministic experiment inputs (source order preserved, never sorted by event time) |
| `experiments/shopil_<n>_*.py` | One script per Jira task; each opens fresh DBs and writes `evidence/SHOPIL-<n>/` |
| `evidence/` | Committed experiment outputs (README.md + JSON per task) |
| `runs/` | Fresh SQLite databases, git-ignored |
| `tests/` | `pytest`; `tests/course/` are the course's own engine tests |

## Teaching configuration

Windows of 60 s, `out_of_order_seconds=0`, `grace_seconds=10`. Windows are `[start, end)`.

```text
watermark    = max(previous_watermark, max_seen_event_time - out_of_order)
close_window = watermark >= window_end + grace
```

Only valid, unique, accepted events move the maximum event time; the watermark never goes back; neither the wall clock nor the micro-batch timer advance it. `finalize_demo()` closes remaining windows with reason `demo_end_of_input` without moving the watermark.

SQLite stores signed 64-bit integers. Event amounts and product/order IDs must fit that range; a window's accumulated revenue must also fit. An order that would exceed its window's capacity is rejected into `dead_letters` with its payload and reason, advancing the checkpoint but changing no revenue, dedupe or watermark. It is not counted as accepted money. Schema-valid duplicates and late deliveries retain their separate treatment; schema-invalid retries remain invalid, as with invalid ratings or missing fields. Reconciliation excludes recorded dead-letter offsets before recomputing unique valid events, and separately exposes those offsets and the schema-only input truth. Passing arithmetic does not prove that the rejection classification was correct. The supplied course fixtures do not reach the storage limits.

Window starts/ends, grace-adjusted closing thresholds and resulting watermarks must be representable in Python's UTC years 1–9999. Unrepresentable events are dead-lettered before committing business state. Time-policy settings cannot exceed 315,537,897,600 seconds (the complete datetime domain); oversized configuration is rejected before opening a database. These bounds prevent a committed checkpoint from referring to state that the evidence reader cannot display. `contract.check` is only a stateless schema precheck; configured time bounds and current revenue capacity are additional ingestion checks. The recorded ingestion result/dead-letter reason is authoritative.

Event IDs and review text must be valid Unicode without unpaired surrogates. Invalid text is rejected; JSON escaping preserves the original malformed payload in dead letters without breaking SQLite's UTF-8 storage. Hebrew and other valid Unicode remain unchanged when decoded.

## Experiments by task

Every Jira task has `experiments/shopil_<n>_<topic>.py` and `evidence/SHOPIL-<n>/README.md`. Run any of them with:

```powershell
uv run python experiments/shopil_<n>_<topic>.py
```

Each script opens fresh databases under `runs/`, prints its report, rewrites its evidence folder, and exits non-zero if any check fails. Run them all with:

```powershell
Get-ChildItem experiments\shopil_*.py | ForEach-Object { uv run python $_.FullName }
```
