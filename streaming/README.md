# ShopIL streaming

Continuous ingestion and processing of ShopIL order and review events (assignment part B), owned by the [Streaming epic SHOPIL-3](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-3).

It runs the course `StreamEngine` unchanged (see [SOURCES.md](SOURCES.md)): a producer feeds a bounded `asyncio.Queue`, a consumer processes either one record at a time (`run_live`) or in small batches (`run_microbatch`), and every event is committed to SQLite in its own transaction together with dedupe state, window state, side outputs and the checkpoint.

## Setup (Windows / PowerShell, uv)

Run from this `streaming/` directory:

```powershell
uv python install 3.13.12
uv sync --locked
uv run python -m pytest -q
```

## Layout

| Path | What |
| --- | --- |
| `src/streaming_lab/` | Course engine, copied verbatim |
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

## Experiments by task

Every Jira task has `experiments/shopil_<n>_<topic>.py` and `evidence/SHOPIL-<n>/README.md`. Run any of them with:

```powershell
uv run python experiments/shopil_<n>_<topic>.py
```

Each script opens fresh databases under `runs/`, prints its report, rewrites its evidence folder, and exits non-zero if any check fails. Run them all with:

```powershell
Get-ChildItem experiments\shopil_*.py | ForEach-Object { uv run python $_.FullName }
```
