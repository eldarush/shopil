# ShopIL

A Hebrew business assistant combining historical ShopIL data and a continuously processed event stream. The application explains computed evidence through an Actor/Critic loop. This repository implements Assignment 1 in AI Systems Engineering.

## Setup

Install Git and [uv](https://docs.astral.sh/uv/getting-started/installation/), then clone this repository. Commands below work in PowerShell on Windows and a terminal on macOS/Linux, from the repository root:

```text
uv python install 3.14.10 3.13.12
uv sync --locked
uv sync --project streaming --locked
uv run --locked python -m integration.smoke
```

The application environment uses Python 3.14.10. The existing course streaming package uses its own Python 3.13.12 environment and lockfile. `uv --project` selects that environment without activating a shell. Do not mix these environments or run bare `pip install` into a system interpreter. ETL's environment will be documented with its implementation.

The smoke test runs live and microbatch ingestion against temporary SQLite databases and checks committed checkpoints. It needs no model credentials and removes its temporary data. CI runs it on Windows, macOS and Linux.

## Run streaming independently

```text
uv run --project streaming --locked python -m shopil_streaming.service --mode live --seconds 30 --db runs/live.db
uv run --project streaming --locked python -m shopil_streaming.service --mode microbatch --seconds 30 --db runs/microbatch.db
```

Use different databases to compare modes. Finite runs stop admitting records and drain the queue. Normal completion leaves business windows open; `--finalize` declares demo EOF and should not be used on a database you intend to resume. See [streaming](streaming/README.md) for experiment inputs and source attribution.

Batch ETL and evidence retrieval are still being integrated. The complete startup sequence is batch completion, streaming producer/consumer startup, then chat; streaming must continue during model calls. A combined launcher will follow that sequence. Do not treat the smoke test as verification of the complete assignment.

## Tests

```text
uv run --locked python -m pytest -q
uv run --directory streaming --locked python -m pytest -q
uv run --locked python -m integration.smoke
```

## Layout and collaboration

`etl/` owns batch loading/models; `streaming/` owns continuous processing; `data_access/` owns evidence queries; `chat/` owns the interface and Actor/Critic; `integration/` owns startup and cross-component checks. Deterministic fixtures and course attribution remain tracked. Databases, virtual environments, credentials and new run artifacts stay ignored.

Use the [ShopIL plugin](https://github.com/eldarush/shopil-plugin) and its Jira workflow/Ponytail skills. Work only on your assigned Jira tickets. Create one branch and PR per ticket, for example `feature/SHOPIL-8-runtime-setup`, targeting `master`. Commit subjects use Conventional Commits and the Jira key: `build: configure isolated runtimes [SHOPIL-8]`.

Install local Git checks once per clone:

```text
uv run --locked python tools/workflow_guard.py install-hooks
```

Run tests before pushing, include the Jira URL and verification in the PR, and wait for current-head CI. Pull `master` after each merge. Local hooks are guardrails; required remote checks and review remain the merge boundary. Never commit credentials or generated databases.
