# Hebrew chat

SHOPIL-31–36 implement the Gradio interface, Actor/Critic and at most three attempts per question. Only approved answers enter conversation history; all attempts retain the same evidence snapshot and are logged separately.

`Actor(model).draft(question, evidence_json, history, previous_draft=None, feedback=None)` creates a candidate, not an approved answer. The injected model implements async `ChatModel.complete(messages, schema=None)`. Transport failures raise `ModelError`; missing or malformed inputs fail before a model call. The Actor passes the frozen JSON unchanged and uses approved history only for conversational references.

Actor instructions live in `chat/actor.py`. They require Hebrew, current evidence over stale history, source/time/units, missing-data and open-window caveats, and separation of historical paid revenue from streaming event amounts. Unit tests use recording models; factual grounding requires the real Actor/Critic experiment and manual comparison with query results.

`JsonlAudit(Path("runs/chat/audit"))` is the local audit sink. Each question gets a separate UUID-named JSONL file containing its frozen evidence, drafts, decisions, feedback and terminal state. Share one sink within the application; writes are synchronized and flushed to disk before an approved answer is published. Storage failures propagate so the chat can fail closed. These logs can contain conversation/review text and are kept outside Git, separate from approved UI history.
