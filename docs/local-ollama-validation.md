# Normal app with local Ollama

Validated on 2026-11-13 using the production `integration.run` → Batch → continuous stream → Gradio → `OllamaModel` path. The normal UI submitted all questions; no response fixtures, Codex backend, injected drafts or mocked model transport were used for these three answers. The historical input was the pinned course revision documented in the README.

## Configuration

- Ollama 0.40.16; `qwen3:4b-instruct`, Q4_K_M, 2,497,293,803 bytes on disk.
- Model digest: `0edcdef34593eac1aa2be9c7d06c432dcf81945adca5eca2f27662c18f168ba0`.
- Windows, 32 GB RAM, GTX 1070 Ti 8 GB; Vulkan backend. `/api/ps` reported 5,102,307,901 bytes of GPU memory at a 16,384-token context.
- Installed model sampling defaults, `--no-thinking`, 4,096 output tokens, 240-second deadline per request. Actor and Critic use the same local model; loopback only.
- Historical paid aggregate/review-text range `[2026-01-01, 2027-01-01)`; product mart retains its independent `[2026-01-01, 2026-10-09)` coverage.

The previous 1,024-token generation cap and forced greedy decoding were unsuitable for the tested reasoners. The transport now preserves model sampling defaults and allows an explicit thinking override. Context overflow and incomplete output remain terminal errors. The Actor repeats the question after the unchanged evidence; the Critic schema generates feedback before its boolean verdict. A separate compact index repeats each source's own date bounds from the same frozen evidence, without replacing or modifying the original evidence JSON. This addressed a real false approval that confused the global batch range with the product mart's coverage. These changes help instruction following; they cannot guarantee factual truth.

## Actual UI checks

Each answer was approved on its first attempt, then manually compared with the source tables and frozen audit. Separate read-only DuckDB queries confirmed 402 paid orders and ILS 97,210.10, product 5 revenue ILS 8,925.00 and average rating 3.52, and the product mart coverage ending exclusively on 2026-10-09. Shipping cost has no source metric.

| Question | Observed answer | UI latency | Stream checkpoint during generation | Accepted events during generation |
| --- | --- | --- | --- | --- |
| מה ההכנסה מהזמנות ששולמו? | ILS 97,210.10; `fct_orders`; correct configured range | 52.189 s | 193 → 1036 | 170 → 908 |
| ומה לגבי מוצר 5? | ILS 8,925.00; USB-C fast charger; `marts.mart_product_reviews`; correct independent coverage | 39.316 s | 1060 → 1684 | 929 → 1474 |
| מהן עלויות המשלוח? | Explicitly unavailable; no invented cost | 45.368 s | 1701 → 2445 | 1489 → 2139 |

Snapshots, respectively: `769d50547987405c83cdd4c6567ee5f6`, `e3a6954dc7784348810aeac5109eb277`, `06e449c0f4f1488b96c6cda06401a15e`. SQLite monitoring used one read transaction per sample. Each question had a distinct fresh snapshot, unchanged across its Actor/Critic calls. Screenshots confirmed the answers, source panel and snapshot metadata were rendered in the actual Hebrew UI.

## Model selection and limits

Qwen3 0.6B/1.7B and Qwen3.5 0.8B/2B produced wrong figures, invented shipping costs, misleading missing-data claims or exhausted generation limits in the tested configurations. Some false answers were also approved by their Critics. Qwen3.5 4B with thinking exhausted the Critic's generation limit; without thinking it returned the core figures correctly but used awkward Hebrew. The selected text-only Qwen3 4B instruction model downloads fewer bytes than the tested Qwen3.5 2B/4B multimodal models.

This is the smallest **verified candidate in this comparison**, not a claim about every available model. The selected model still occasionally uses awkward or mixed-language wording. Three conversational spot checks do not establish accuracy for every question; model output is stochastic, and even a valid structured approval can be wrong. Keep checking consequential answers against their source rows. Unit/CI tests use scripted models and establish software behavior separately from these real-model observations.

## Reproduce

Follow the README environment and pinned-course setup, pull `qwen3:4b-instruct`, and start Ollama. Run the coordinated stack with that model, `--no-thinking`, fresh run/database paths and an initially absent `--stop-file`. Open the normal local Gradio page and submit the three questions above in one conversation. Compare the actual answers with the DuckDB tables, their source-specific dates and the JSONL audit; do not infer correctness solely from the approval label. Sample SQLite checkpoints/counters during requests to confirm streaming remains active.

Create the stop file from another terminal and wait for the supervisor to exit. Confirm equal produced/consumed counts, closed app port and `stream_meta.finalized = 0`, which leaves the stream resumable rather than falsely closing event-time windows.
