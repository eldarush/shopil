# Chat evaluation

Run the deterministic mechanics tests after syncing both Python environments:

```sh
uv run python -m pytest tests/test_chat_evaluation.py -q
```

For the required real rejection/correction cycle, install and authenticate Codex CLI, then run:

```sh
uv run python -m experiments.codex_teaching
```

This optional experiment uses the CLI's configured live model and normal authentication/rules/hooks. It sends only fixed, public numeric teaching data and the project's actual Actor/Critic prompts. It accepts no arbitrary user questions or review text and is not the chat application's model backend. Read-only mode is not a complete tool sandbox: the experiment requests no tools and fails verification if a tool call appears in its log. CLI host warnings remain visible in the saved stderr/events.

Each run creates a new ignored `runs/chat-evaluation-*` directory. The fixture's normalized UTF-8/LF hash is checked against its committed manifest before processing. The streaming interpreter processes the 17 deliveries in source order using 60-second windows, zero out-of-order allowance and 10-second grace. It finalizes only this finite fixture. SQL reads the resulting windows, late events, identifiers, actual configuration and checkpoint in one transaction; it verifies 12,200 accepted cents, 1,300 late cents and checkpoint 16 before calling a model. Results record the CLI version, Git revision, dirty-tree flag and experiment-file hashes. The configured model is used; its resolved identifier is explicitly unknown when the CLI does not expose it.

The deliberately injected first draft incorrectly says 135 ILS. The real Critic must reject it; the real Actor then receives that draft, feedback and the identical evidence; a real Critic evaluates the correction. `result.json` reports `cycle_passed=false` if the bad draft is approved, the Actor repeats it unchanged, or no correction is approved within three attempts. This checks the cycle, not factual correctness. A separate case explicitly mocks both roles and checks exactly three rejections with no published answer/history.

Review `snapshot.json`, `audit/*.jsonl`, the model responses and `result.json` manually. Match every numerical/source/time claim to the SQL-derived rows. An approved JSON schema or passing loop check is not proof of semantic correctness. Preserve the required output in the submission bundle, rather than committing generated databases or logs. Model responses can vary between runs.

For another live provider, pass its `ChatModel` implementation to `rejection_experiment(model, snapshot, audit_directory)`; the production Actor, Critic and ChatService are unchanged.

## Integrated live-model rehearsal

The opt-in runner below starts the actual Batch preparation, continuous stream supervisor,
production evidence capture, Gradio page and Actor/Critic service. It uses an already
authenticated Codex CLI for three fixed public-data questions. The regular application
still uses the configured Ollama endpoint. This runner is a controlled experiment, not a
general-purpose Codex chat backend.

Complete the root/streaming environment setup and isolated ETL dependencies from the main
README first. Supply a local checkout of the public course repository; the runner clones
it into a **new** run directory and checks out the pinned course revision, so local data
edits and previous warehouse state are excluded. The model flag must select a model
available to your authenticated CLI; check your installed CLI configuration/help.

```powershell
uv run --locked python -m experiments.integrated_codex launch `
  --course-checkout ../etl-course-demo --etl-python "$etlPython" `
  --run-dir runs/integrated-live-review --model YOUR_AVAILABLE_CODEX_MODEL --port 7860
```

Use the `$etlPython` variable set by the main README's platform-specific setup block.
On macOS/Linux use ordinary shell line continuation. Open
`http://127.0.0.1:7860` and ask these questions **in order**, in the same conversation:

1. `מה ההכנסה מהזמנות ששולמו?`
2. `ומה לגבי מוצר 5?`
3. `מהן עלויות המשלוח?`

Each run permits one conversation. Clearing the UI, opening another session or
retrying after a failed question requires a fresh run directory/restart. Later
questions must exactly match the history durably approved in the service audit;
rejected drafts and Critic approvals without a successful terminal audit do not qualify.

The first candidate deliberately fabricates 999999 ILS and combines historical and
streaming revenue. It is explicitly recorded in `call-1.injection.json`, not labeled a
real response. Its rejection, the corrected draft and subsequent evaluations must come
from real model calls. A model accepting the faulty candidate is a failed experiment:
inspect the audits; do not count the UI's approval alone as success. Other questions or
forged assistant history are rejected before reaching the CLI. Normal CLI rules, trusted
hooks, authentication and configuration stay active. Read-only mode does not disable
all tools: requests instruct the model not to use tools, and any observed tool activity
fails verification. Use only this fixed public course dataset.

Each question saves its production snapshot. `call-*.request.json`, `.txt`, `.events.jsonl`
and `.stderr.txt` preserve the prompt, actual response and host warnings.
`call-*.progress.json` records stream checkpoints around each real call; verify they
advance while all attempts use the saved snapshot checkpoint. Verify later questions
have newer snapshots. Calls have a 180-second deadline and owned process-tree cleanup
on cancellation/error. Ctrl+C gracefully stops chat and drains the stream; `--managed`
also allows launcher stdin newline/EOF for automated testing.

`environment.json` records the requested model, CLI version, pinned course revision,
source commit/dirty state and file hashes. Server-side model routing remains unknown.
Manually compare at least two accepted answers to SQL, source names, units and time
ranges. Capture the **running** Gradio answer and streaming output, then package these
files with the audit JSONL and Batch results. Keep any failed attempts and distinguish
them from the successful run. These generated artifacts remain outside Git; this
command neither packages nor submits to Moodle.

The runner's deterministic boundary and real-process cleanup tests require no model:

```text
uv run --locked python -m pytest tests/test_integrated_codex.py -q
```
