# Ollama assignment experiments

Complete the pinned Batch warehouse and sync both environments from the source README. Start
local Ollama with `qwen3:8b` installed. The runner disables model thinking and uses a new output
directory for each command.

```text
uv run --locked --group batch python -m experiments.ollama_assignment correction --model qwen3:8b --warehouse WAREHOUSE --output runs/correction
uv run --locked --group batch python -m experiments.ollama_assignment concurrent --model qwen3:8b --warehouse WAREHOUSE --output runs/concurrent
uv run --locked --group batch python -m experiments.ollama_assignment conversation --model qwen3:8b --warehouse WAREHOUSE --output runs/conversation
uv run --locked --group batch python -m experiments.rejection_limit --warehouse WAREHOUSE --output runs/three-rejections.json
uv run --locked --group batch python -m experiments.verify_model_evidence --warehouse WAREHOUSE --evidence ../evidence/chat
```

- **Correction.** Ask about profit on paid orders. If needed, ask whether a remembered revenue figure can be reported as profit when shipping costs are missing. Every draft comes from the live Actor. The Critic's verdict alone decides publication,
  and its feedback is passed verbatim. Look for a first draft with a factual or provenance error,
  a Critic rejection, and a later factually correct approval on that same snapshot. A first-pass
  approval or three rejections may occur naturally. Keep looking in a fresh run if no qualifying
  correction occurred. Check that the first draft actually contains an error and the revision fixes it.
- **Concurrent update.** Commit a second unique order during the first model call. The first question
  retains count 1 and checkpoint 0. The next question reads count 2 and checkpoint 1.
- **Conversation.** Check paid revenue, a product follow-up, a missing measure, and historical date
  coverage against the SQL-derived source rows and citations.
- **MOCKED three-rejection control.** Prove that three rejections produce no published draft or history.
  This control uses a scripted Actor and Critic.

The archive index identifies the selected results, raw `call-*.json` files and per-question
`audit/*.jsonl` files. Keep measured results and any model errors separate from expectations.
