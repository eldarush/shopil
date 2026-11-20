# Streaming implementation and course attribution

The event-time engine, asynchronous live and microbatch runners, and the original course tests come from the pinned [course streaming laboratory](https://github.com/paulavrahami/streaming-course-demo/tree/c27b4fc1b343e5bfca1880ad3c5ae34e97f60b71). The Python distribution keeps its original `streaming_lab` module structure.

| Component | Course behavior and application changes |
| --- | --- |
| `src/streaming_lab/events.py` and `engine.py` | The course engine owns ordering, deduplication, event-time assignment, closure, outputs and atomic checkpoint commits. Storage guards reject integers or window sums outside SQLite's integer range, invalid Unicode and timestamps that cannot be represented. Rejected payloads and reasons enter dead letters in the existing transaction. |
| `src/streaming_lab/live.py` and `microbatch.py` | The course queue, release rules and per-event transactions are retained. The service uses compact progress reads and aggregate counters. Finite experiments retain the per-batch timing records. |
| `src/shopil_streaming/` | The project's deterministic continuous source, read-only progress view, controls and reconciliation. |
| `experiments/`, `fixtures/`, `tests/` | Project experiments, deterministic fixture inputs and regression tests. The course engine tests are under `tests/course/`. |

The parity experiment loads each version in an isolated Python process. For each input offset it compares a trace digest over the disposition and complete decoded snapshot. It also includes the final demo snapshot. The three input streams have 17, 6 and 317 deliveries. At Grace 0, 10 and 120, the 1,020 offset comparisons and nine finalizations match. Storage-limit cases receive separate tests.

The unmodified modules used by the comparison are in `course_reference/`. The comparison verifies their hashes before loading them and runs without a second checkout:

```powershell
uv run --locked python experiments/course_parity.py
```
