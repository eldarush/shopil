"""SHOPIL-20: microbatch release reasons and equivalence with record-wise processing."""
from __future__ import annotations

import asyncio

from shopil_streaming.recording import RecordingEngine
from streaming_lab import run_microbatch, teaching_fixture


def _run(tmp_path, name, records, **kwargs):
    with RecordingEngine(tmp_path / f"{name}.db") as engine:
        state = asyncio.run(run_microbatch(engine, teaching_fixture()[:records], duration_s=None, **kwargs))
        assert engine.offsets == list(range(records))
    assert sum(state["batch_sizes"]) == state["consumed"] == state["produced"] == records
    return state


def test_size_then_final_drain(tmp_path):
    state = _run(tmp_path, "size", 7, interval_s=0, max_batch_size=5, max_wait_s=10)
    assert state["batch_sizes"] == [5, 2]
    assert [b["flush_reason"] for b in state["batch_details"]] == ["max_size", "final_drain"]
    assert state["snapshot"]["open_windows"], "final_drain must not close event-time windows"


def test_timer_releases_partial_batch_without_next_event(tmp_path):
    seen = []
    state = _run(tmp_path, "wait", 3, interval_s=0.3, max_batch_size=5, max_wait_s=0.05,
                 on_snapshot=lambda s: seen.append((s["batch"]["last_offset"], s["queue"]["produced"])))
    assert state["flush_counts"]["max_wait"] == 3 and state["batch_sizes"] == [1, 1, 1]
    assert all(produced == last + 1 for last, produced in seen)
