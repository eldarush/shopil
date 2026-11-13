# Streaming sources and attribution

| Path | Origin | Changes |
| --- | --- | --- |
| `src/streaming_lab/` | [paulavrahami/streaming-course-demo](https://github.com/paulavrahami/streaming-course-demo) commit `c27b4fc1b343e5bfca1880ad3c5ae34e97f60b71`, `src/streaming_lab/` | None. Copied verbatim so the assignment runs the course `StreamEngine`, `run_live` and `run_microbatch` exactly as taught. |
| `tests/course/test_core.py` | Same commit, `tests/test_core.py` | None. Kept to prove the copied engine still passes its own course tests here. |
| `.python-version` | Same commit | None (3.13.12). |

Everything under `src/shopil_streaming/`, `experiments/`, `fixtures/`, `evidence/` and `tests/` (outside `tests/course/`) is our own work for the SHOPIL Streaming epic.

The course engine is not modified. ShopIL-specific behaviour (contract reporting, the continuous ShopIL source, experiment runners, reconciliation) wraps it instead, so any change to course semantics would show up as a failing course test.
