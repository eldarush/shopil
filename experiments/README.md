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
