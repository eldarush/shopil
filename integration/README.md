# Process lifecycle

Complete batch loading and dbt before starting chat. The batch process must exit successfully and release DuckDB. Streaming then starts and must commit a new checkpoint before the chat process launches; it continues processing while chat waits for model responses.

`integration.run.run_stack(batch_command, chat_command, stream_db=..., mode="live")` supervises this order. Commands are explicit argument lists, executed without a shell. The caller supplies the real batch command and the chat's managed command; there is no mock or reviews-only substitute for the complete batch pipeline. Set `mode="microbatch"` for the other ingestion mode.

The supervisor uses `streaming/.venv` directly, so first run `uv sync --project streaming --locked`. It inherits component output for visible produced/consumed/checkpoint progress. A startup failure prevents chat from starting; a stream failure while chat is active stops the stack.

## Run streaming independently

From the repository root in PowerShell or a macOS/Linux terminal:

```text
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-live.db --mode live
uv run --project streaming --locked python -X utf8 -m shopil_streaming.managed --db runs/stream-microbatch.db --mode microbatch
```

Press Enter or Ctrl+C to stop admission and drain accepted queue entries. EOF on standard input also requests graceful shutdown, which is how the supervisor controls its child. The chat command must support the same control contract (`--managed` in the application launcher).

Orderly shutdown closes chat, then stops streaming admission and waits for its producer/consumer drain. It does not call `finalize_demo`: open event-time windows remain resumable. A deadline that forces termination is reported as an error, never as successful draining. SQLite's committed checkpoint remains the restart boundary.

## Verification

```text
uv run --locked python -m pytest tests/test_lifecycle.py -q
uv run --directory streaming --locked python -m pytest tests/test_managed.py -q
```

Tests run real worker processes with bounded queues, assert that all admitted records drain, and exercise failed batch startup, continuing stream progress during a chat stub, unexpected stream failure, startup deadlines and uncooperative child shutdown. These checks do not establish that the final ETL/chat command pair has been integrated; that requires the completed data pipeline and configured model service.
