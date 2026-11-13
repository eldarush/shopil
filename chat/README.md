# Hebrew chat

SHOPIL-31–36 implement the Gradio interface, Actor/Critic and at most three attempts per question. Only approved answers enter conversation history; all attempts retain the same evidence snapshot and are logged separately.

`Actor(model).draft(question, evidence_json, history, previous_draft=None, feedback=None)` creates a candidate, not an approved answer. The injected model implements async `ChatModel.complete(messages, schema=None)`. Transport failures raise `ModelError`; missing or malformed inputs fail before a model call. The Actor passes the frozen JSON unchanged and uses approved history only for conversational references.

Actor instructions live in `chat/actor.py`. They require Hebrew, current evidence over stale history, source/time/units, missing-data and open-window caveats, and separation of historical paid revenue from streaming event amounts. Unit tests use recording models; factual grounding requires the real Actor/Critic experiment and manual comparison with query results.

`JsonlAudit(Path("runs/chat/audit"))` is the local audit sink. Each question gets a separate UUID-named JSONL file containing its frozen evidence, drafts, decisions, feedback and terminal state. Share one sink within the application; writes are synchronized and flushed to disk before an approved answer is published. Storage failures propagate so the chat can fail closed. These logs can contain conversation/review text and are kept outside Git, separate from approved UI history.

`build_app(service)` in `chat/app.py` creates the Hebrew Gradio interface. Each browser session retains its own approved history; rejected/error turns appear only in the status field, with the question retained for retry. Clearing the conversation also clears session history and provenance. Source details and snapshot ID/time appear beside the conversation.

## Run the application

Complete the [Batch preparation](../integration/README.md), start streaming, and provide an installed model served by a stable **Ollama 0.13.0 or newer** on loopback. The application does not install a server, download models or fall back to a mock. Replace `YOUR_INSTALLED_MODEL` with an actual model name:

```text
uv run --locked python -m chat --warehouse ../etl-course-demo/warehouse.duckdb --stream-db runs/stream-live.db --history-start 2026-01-01 --history-end 2027-01-01 --model YOUR_INSTALLED_MODEL
```

Open `http://127.0.0.1:7860`. Ctrl+C stops the standalone server. `--managed` is for the supervisor and instead accepts stdin newline/EOF. `--ollama-url`, `--audit-dir`, `--port` and `--latest-windows` override the local endpoint, audit directory, UI port and stream evidence scope. The model transport sends text only, checks the server version, disables prompt truncation/context shifting, and rejects incomplete output. Context overflow ends the question with an audited error rather than silently dropping evidence. These controls are supported by the [released Ollama API](https://github.com/ollama/ollama/blob/v0.13.0/api/types.go).

## Evidence scope

`build_capture` reads the completed historical warehouse and a fresh SQLite snapshot once per question, off the event loop. It creates one `EvidencePackage`; `ChatService` then freezes that package and its metadata for all retries. Historical and streaming databases are separate snapshots, so finish all Batch writes before starting chat.

The configured historical range is half-open: start included, end excluded. Paid-order totals and review text use that range; the product mart retains its independently reported full-history coverage. All product metrics are included. Only the latest twelve cleaned review texts across all products are included, with available/selected/omitted counts; this is a global sample, not a complete or balanced review list. Questions do not generate SQL or silently change this configured scope.

Stream evidence defaults to the latest three distinct event-time window starts across the reader's five tables, retaining every product within them. Open/closed sources stay separate. Alert counts at emission are distinguished from current review counts. Empty sources have no invented dates. If selected rows exceed the limit, capture fails instead of dropping products silently. Underlying readers currently materialize full query results before these evidence limits; large deployments need database-side selection. Historical product IDs and streaming IDs are not assumed to be a verified mapping, and financial values are never combined across sources.

Tests cover real DuckDB/SQLite capture and running Gradio processes with explicitly scripted models; HTTP contract tests do not establish the availability or quality of a real local model. The separate [teaching experiment](../experiments/README.md) retains the actual Actor/Critic correction demonstration.
