# ShopIL

A Hebrew business assistant for Assignment 1 in AI Systems Engineering. Batch ingestion builds historical ShopIL metrics in DuckDB; a continuous producer and consumer persist event-time windows in SQLite. Each question captures fresh evidence once, and an Actor/Critic loop uses that unchanged snapshot for at most three attempts. Only approved answers enter conversation history; every attempt is audited separately.

## 1. Install and prepare the runtimes

Install Git and [uv](https://docs.astral.sh/uv/getting-started/installation/). Run these commands in PowerShell on Windows or a terminal on macOS/Linux:

```text
git clone https://github.com/eldarush/shopil.git
cd shopil
uv python install 3.14.10 3.13.12
uv sync --locked
uv sync --project streaming --locked
uv run --locked python -m integration.smoke
```

Run all remaining commands from this repository root. Check that each command succeeds before continuing. The application uses Python **3.14.10** and the course streaming package uses isolated Python **3.13.12**. The smoke test exercises both streaming modes using temporary databases; it needs no model service.

Prepare the pinned course checkout and a third environment for ETL. Use unused sibling paths on first setup; if they already exist, verify their contents and revision before reusing them.

```text
git clone https://github.com/paulavrahami/etl-course-demo.git ../etl-course-demo
git -C ../etl-course-demo checkout --detach 6788526695bbb20ac40f46a947413fff3545b86a
uv venv --python 3.14.10 ../.venv-shopil-etl
```

Windows PowerShell:

```powershell
uv pip install --python ../.venv-shopil-etl/Scripts/python.exe -r etl/requirements.txt
$etlPython = (Resolve-Path '../.venv-shopil-etl/Scripts/python.exe').Path
```

macOS/Linux:

```sh
uv pip install --python ../.venv-shopil-etl/bin/python -r etl/requirements.txt
etlPython="$(cd ../.venv-shopil-etl && pwd)/bin/python"
```

The ETL requirements pin `dlt[duckdb]==1.32.0`, `duckdb==1.5.6`, `dbt-core==1.12.7` and `dbt-duckdb==1.11.1`. Keep these environments separate; no shell activation is required. The `$etlPython` variable below works in either shell after the corresponding setup block; set it again in a new terminal.

## 2. Prepare all historical data

One command runs offline base ingestion, reviews replacement, all dbt models/tests, the baseline checker, course checks 1–3 and the exact-row reviews checker:

```text
uv run --locked python -m integration.batch --lab-dir ../etl-course-demo --etl-python "$etlPython" --run-dir runs/batch-1
```

The warehouse is `../etl-course-demo/warehouse.duckdb`. Each new run directory contains an exported pinned course dbt project, the authored model overlays, and dbt logs/results. Existing course model files remain untouched. For an idempotence check, rerun with `--run-dir runs/batch-2`, preserving the same warehouse and dlt state. Existing run directories are never overwritten. The command stops on failure; let it finish and release DuckDB before starting chat.

Reference results: **600 raw orders → 549 valid orders; 402 paid orders totaling ILS 97,210.10; 310 raw reviews → 304 valid ratings → 298 nonempty texts → 289 deduplicated reviews; eight product rows**. Paid orders and reviews are aggregated separately to avoid multiplying revenue through joins. The course relationship test intentionally warns about 19 orders without matching customers; the fact model excludes them. [ETL details and source attribution](etl/README.md).

## 3. Provide a local model

Install and start stable **Ollama 0.13.0 or newer** using its [official setup instructions](https://docs.ollama.com/quickstart). Select and download a local model capable of Hebrew and structured JSON using Ollama itself. `ollama list` shows installed model names; replace `YOUR_INSTALLED_MODEL` below with one of them. The default endpoint is `http://127.0.0.1:11434`; if the desktop app is not serving it, start `ollama serve` in another terminal.

ShopIL uses the local [Ollama chat API](https://docs.ollama.com/api/chat), accepts only loopback endpoints, and requires no API key. It does not install Ollama, download models or substitute mock answers. Requests use an 8,192-token context and a 1,024-token output cap; the transport verifies the server version and rejects context overflow, truncated output, tool calls and malformed evaluations. Errors are audited and do not publish an unapproved answer. Choose a model that fits your machine and verify its Hebrew answers against the displayed sources.

## 4. Run streaming and chat separately

After Batch preparation, start either continuous streaming mode in its own terminal:

```text
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-live.db --mode live
```

Or use microbatch collection:

```text
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-microbatch.db --mode microbatch
```

Keep the chosen process running. Console output shows produced/consumed counts, queue depth, checkpoint and watermark. Press Enter or Ctrl+C in that terminal to stop admission and drain queued records; stdin EOF also requests shutdown. Normal shutdown does not finalize business windows, so the database remains resumable. Use separate database paths when comparing modes. [Streaming experiments](streaming/README.md).

In a second terminal at the repository root, launch chat against the matching stream database:

```text
uv run --locked python -m chat --warehouse ../etl-course-demo/warehouse.duckdb --stream-db runs/stream-live.db --history-start 2026-01-01 --history-end 2027-01-01 --model YOUR_INSTALLED_MODEL
```

For microbatch, change `--stream-db` to `runs/stream-microbatch.db`. Open **http://127.0.0.1:7860**. Ctrl+C closes this standalone chat server; stop the separately launched stream in its own terminal. `--managed` makes chat accept newline/EOF for use by the supervisor.

## 5. Or run the coordinated stack

With the environments, course checkout and Ollama service ready, this command performs Batch preparation, starts the stream, waits for a newly committed checkpoint, and launches chat:

```text
uv run --locked python -m integration.run --lab-dir ../etl-course-demo --etl-python "$etlPython" --batch-run-dir runs/batch-stack-1 --warehouse ../etl-course-demo/warehouse.duckdb --stream-db runs/stream-stack.db --history-start 2026-01-01 --history-end 2027-01-01 --model YOUR_INSTALLED_MODEL --mode live
```

Use `--mode microbatch` for the other runner and a distinct stream path for comparisons. Choose a new `--batch-run-dir` each invocation. The warehouse must be `warehouse.duckdb` inside `--lab-dir`. This workflow replaces the separately launched processes above; do not run two writers against the same stream database.

Ctrl+C in the supervisor closes chat and drains the stream. Enter controls a managed component directly, not the supervisor's own stdin. Forced termination or a failed component is reported as an error, not a successful drain. Batch does not run concurrently with chat's historical reads. [Lifecycle details](integration/README.md).

Chat stops accepting questions during shutdown and waits up to five seconds for active questions to record a cancellation/error terminal event before closing the server. A failed audit or shutdown deadline makes the process fail. A failed or blank question leaves the last approved answer's source and snapshot fields unchanged.

## Evidence scope and configuration

The examples use the fixed half-open calendar range **[2026-01-01, 2027-01-01)**, which covers the pinned fixture's order dates and review timestamps. It controls paid-order totals and cleaned review text. Product mart metrics retain their independently reported full-history coverage. Questions and follow-ups do not generate SQL or silently alter the configured date range.

When the catalog has products but there are no historical orders or reviews, their known zero counts and null averages remain available. The mart source explicitly reports `no_observed_history` with null time bounds instead of inventing coverage dates.

Every question reads fresh data. All product metrics are included; review text is the **latest 12 cleaned reviews across all products**, with available/selected/omitted counts. It is a global sample, not a complete or balanced review list. Streaming evidence defaults to the **latest three distinct event-time windows**, keeping all products in those windows and explicitly reporting omissions. Open/closed windows and alert trigger counts remain distinct. Historical paid ILS and streaming cents are never added together; the cross-source product mapping is marked unverified. Missing data is not presented as zero. [Evidence contract](data_access/README.md).

CLI options override environment defaults: `--warehouse`, `--history-start`, `--history-end`, `--model`, `--stream-db`, `--ollama-url`, and `--audit-dir`. `.env.example` lists their corresponding `SHOPIL_*` variables. **The file is not loaded automatically**: set variables in your shell, or use the explicit flags above. For example:

```powershell
# PowerShell
$env:SHOPIL_MODEL = 'YOUR_INSTALLED_MODEL'
```

```sh
# macOS/Linux
export SHOPIL_MODEL='YOUR_INSTALLED_MODEL'
```

`--port` (default 7860) and `--latest-windows` (default 3) are CLI-only options. Audit JSONL files default to `runs/audit`, one per question, containing the frozen evidence, drafts, Critic feedback and terminal result. UI history contains only approved turns. Keep audits, credentials, databases and environments out of Git.

## Tests and validation

```text
uv run --locked python -m pytest -q
uv run --directory streaming --locked python -m pytest -q
uv run --locked python -m integration.smoke
```

CI runs application tests, streaming tests, smoke checks and complete Batch preparation twice on Windows, macOS and Linux; inspect the current PR checks for their actual status. The macOS job uses the standard `macos-26-intel` runner after hosted ARM capacity failures, retaining its existing required check name. The verified Batch fixture produces eight models, 25 passing data tests, the one expected relationship warning and zero errors; all 14 course checks pass. Independent product metrics and revenue remain identical across reruns.

Server tests use explicitly scripted models and HTTP contract tests use mocked transport. They do not establish that a real Ollama endpoint is installed or functioning. The separate [teaching experiments](experiments/README.md) document real Actor/Critic correction and manual SQL comparisons; they are not a fallback backend for the application.

The [architecture report](docs/report.md) explains loading policies, event-time processing, transaction and snapshot boundaries, Actor/Critic feedback, and verified results with their limits. Its two-page explanation is followed by separate architecture and results appendices in the generated submission PDF.

Use the [submission guide](docs/submission.md) to select the required logs, screenshots and report, then package them with an exact committed source revision. Review the evidence and its real-model limitations before submission; the packaging command only checks files and hashes and does not upload anything.

## Contributing

`etl/` contains loaders/models, `streaming/` event processing, `data_access/` read-only queries/evidence types, `chat/` the interface and Actor/Critic, and `integration/` startup supervision. Course starter attribution and deterministic inputs remain tracked; generated run artifacts stay ignored. Instructor solutions are excluded from independent work.

Install the [ShopIL plugin](https://github.com/eldarush/shopil-plugin) and follow its Jira workflow and Ponytail skills. Resolve your authenticated Jira identity and modify only tickets assigned to your account ID. Do not create or claim tasks unless explicitly requested. Use one ticket branch/PR targeting `master`, such as `feature/SHOPIL-5-chat-runtime`, and Conventional Commit subjects containing the ticket: `feat(chat): add local model transport [SHOPIL-5]`.

Install local Git checks once per clone:

```text
uv run --locked python tools/workflow_guard.py install-hooks
```

Run relevant tests, include the Jira URL and actual verification in the PR, and wait for current-head CI and review before merging. Pull `master` after each merge. Hooks are local guardrails; they do not replace remote checks or review. Do not commit secrets, warehouses, environments or generated audit logs.
