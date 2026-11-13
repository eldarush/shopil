# Process lifecycle

Complete batch loading and dbt before starting chat. The batch process must exit successfully and release DuckDB. Streaming then starts and must commit a new checkpoint before the chat process launches; it continues processing while chat waits for model responses.

`integration.run.run_stack(batch_command, chat_command, stream_db=..., mode="live")` supervises this order. Commands are explicit argument lists, executed without a shell. The caller supplies the real batch command and the chat's managed command; there is no mock or reviews-only substitute for the complete batch pipeline. Set `mode="microbatch"` for the other ingestion mode.

The supervisor uses `streaming/.venv` directly, so first run `uv sync --project streaming --locked`. It inherits component output for visible produced/consumed/checkpoint progress. A startup failure prevents chat from starting; a stream failure while chat is active stops the stack.

## Concrete Batch command

Prepare the pinned course checkout and isolated ETL dependencies as described in [ETL setup](../etl/README.md). From the repository root on Windows:

```text
uv run --locked python -m integration.batch --lab-dir ../etl-course-demo --etl-python ../.venv-shopil-etl/Scripts/python.exe --run-dir runs/batch-1
```

On macOS/Linux, use `../.venv-shopil-etl/bin/python` for `--etl-python`. This runs base ingestion, reviews replacement, a full dbt build, the baseline checker, course checks 1–3 and the exact-row staging checker. Failure stops subsequent stages. A new run directory contains an exported copy of the pinned course dbt project plus the authored model overlays, logs and dbt results. Existing course model files remain unchanged. Reuse the warehouse and dlt state with a new output directory such as `runs/batch-2` to verify rerun behavior. Existing output directories are never overwritten.

## Run the complete stack

With the isolated environments prepared and an installed model served by stable Ollama >=0.13.0, run:

```text
uv run --locked python -m integration.run --lab-dir ../etl-course-demo --etl-python ../.venv-shopil-etl/Scripts/python.exe --batch-run-dir runs/batch-stack-1 --warehouse ../etl-course-demo/warehouse.duckdb --stream-db runs/stream-live.db --history-start 2026-01-01 --history-end 2027-01-01 --model YOUR_INSTALLED_MODEL --mode live
```

Use the macOS/Linux interpreter path above on those platforms. `--mode microbatch` selects the other runner; use a distinct SQLite path for comparisons. The warehouse must be the Batch output in `--lab-dir`. All child commands are argument lists, without a shell. Open `http://127.0.0.1:7860` after startup; Ctrl+C in the supervisor closes chat and drains the stream. Enter/EOF controls apply when running a managed component directly, not to the supervisor's own stdin.

## Run streaming independently

From the repository root in PowerShell or a macOS/Linux terminal:

```text
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-live.db --mode live
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-microbatch.db --mode microbatch
```

Press Enter or Ctrl+C to stop admission and drain accepted queue entries. EOF on standard input also requests graceful shutdown, which is how the supervisor controls its child. The chat command must support the same control contract (`--managed` in the application launcher).

Orderly shutdown closes chat, then stops streaming admission and waits for its producer/consumer drain. It does not call `finalize_demo`: open event-time windows remain resumable. A deadline that forces termination is reported as an error, never as successful draining. SQLite's committed checkpoint remains the restart boundary.

Before closing Gradio, chat rejects new admission, cancels active questions and waits up to five seconds for their terminal audit records. Cancellation preserves approved-only history. If a request ignores cancellation or its audit cannot be written, shutdown fails visibly.

Windows commands run inside a Job Object, joining before they can start descendants; the supervisor retains the job handle and terminates remaining members during cleanup. POSIX commands use a new process group. Cleanup also runs when the original leader has already exited, and completed groups are not signaled again. Batch ownership is released before starting streaming or chat. These controls cover normal component descendants, not programs deliberately escaping their POSIX process group.

## Verification

```text
uv run --locked python -m pytest tests/test_lifecycle.py -q
uv run --directory streaming --locked python -m pytest tests/test_managed.py -q
```

Tests run real worker processes with bounded queues, assert that all admitted records drain, and exercise failed batch startup, continuing stream progress during a chat stub, unexpected stream failure, startup deadlines and uncooperative child shutdown. CLI tests verify concrete command wiring. CI also runs the complete Batch command twice in separate output directories against the same warehouse/state on each supported OS. Local Batch validation passed twice with eight product rows, 289 cleaned reviews, 402 paid orders and ILS 97,210.10. Scripted-model server tests and Batch runs do not prove a real local model deployment; the combined demonstration still needs that configured service.

Regression tests also stop an actual Gradio server during Actor/Critic work and verify terminal audit events, and launch descendants whose original batch/chat leader exits first. A held socket verifies descendant termination rather than merely checking the direct child handle.
