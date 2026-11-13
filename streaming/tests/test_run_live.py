"""SHOPIL-19: run_live preserves source order and matches sequential processing."""
from __future__ import annotations

import asyncio

from shopil_streaming.recording import RecordingEngine
from streaming_lab import run_live, teaching_fixture
from streaming_lab.reporting import canonical_results


def test_run_live_equals_sequential_reference(tmp_path):
    data = teaching_fixture()
    with RecordingEngine(tmp_path / "ref.db") as ref:
        for item in data:
            ref.ingest(item.event, offset=item.offset)
        expected = canonical_results(ref.finalize_demo())
    with RecordingEngine(tmp_path / "live.db") as engine:
        state = asyncio.run(run_live(engine, data, duration_s=None, interval_s=0, queue_maxsize=2, finalize=True))
        assert engine.offsets == list(range(len(data)))
        assert [r["status"] for r in engine.trace] == [r["status"] for r in ref.trace]
    assert state["produced"] == state["consumed"] == len(data)
    assert canonical_results(state["snapshot"]) == expected
    assert expected["totals"]["revenue_cents"] == 12_200
