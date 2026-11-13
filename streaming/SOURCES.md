# Streaming sources and attribution

| Path | Origin | Changes |
| --- | --- | --- |
| `src/streaming_lab/` | [paulavrahami/streaming-course-demo](https://github.com/paulavrahami/streaming-course-demo) commit `c27b4fc1b343e5bfca1880ad3c5ae34e97f60b71`, `src/streaming_lab/` | Course base retained. SHOPIL-49 adds signed 64-bit integer validation in `events.py` and storage/window timestamp capacity checks in `engine.py`; invalid records retain their payload and reason in dead letters. Other course modules remain unchanged. |
| `tests/course/test_core.py` | Same commit, `tests/test_core.py` | None. Kept to prove the copied engine still passes its own course tests here. |
| `.python-version` | Same commit | None (3.13.12). |

Everything under `src/shopil_streaming/`, `experiments/`, `fixtures/`, `evidence/` and `tests/` (outside `tests/course/`) is our own work for the SHOPIL Streaming epic.

ShopIL-specific behaviour (contract reporting, the continuous ShopIL source, experiment runners, reconciliation) wraps the course engine. The narrowly scoped storage validation changes above prevent poison inputs from stopping ingestion or committing unreadable evidence. The original course tests remain intact, alongside regression tests for these additions; passing them establishes covered behavior rather than proving every semantic property.
