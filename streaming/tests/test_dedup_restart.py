"""SHOPIL-41: retries never re-count, and a cancelled stream resumes to the same result."""
from __future__ import annotations

import asyncio
import contextlib

from shopil_streaming.fixtures import load_fixture
from streaming_lab import StreamEngine, run_live
from streaming_lab.reporting import canonical_results


def test_cancel_and_resume_equals_uninterrupted(tmp_path):
    data = load_fixture("orders300")
    with StreamEngine(tmp_path / "ref.db") as engine:
        for d in data:
            engine.ingest(d.event, offset=d.offset)
        expected = canonical_results(engine.finalize_demo())
        assert engine.snapshot()["stats"]["duplicates"] == 17

    async def interrupted():
        with StreamEngine(tmp_path / "s.db") as engine:
            task = asyncio.create_task(run_live(engine, data, duration_s=None, interval_s=0,
                                                processing_delay_s=0.002, queue_maxsize=4))
            await asyncio.sleep(0.1)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            return engine.checkpoint

    stopped = asyncio.run(interrupted())
    assert stopped < len(data) - 1
    with StreamEngine(tmp_path / "s.db") as engine:
        start = engine.next_offset
        assert start == stopped + 1
        state = asyncio.run(run_live(engine, data[start:], duration_s=None, interval_s=0, finalize=True))
    assert canonical_results(state["snapshot"]) == expected
