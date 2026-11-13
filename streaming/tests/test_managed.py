import asyncio

import pytest

from shopil_streaming.managed import run_managed
from streaming_lab import StreamEngine


@pytest.mark.parametrize("mode", ["live", "microbatch"])
def test_stop_with_backlog_drains_every_admitted_record_without_finalizing(tmp_path, mode):
    stopped = False
    pending_at_stop = None

    def on_snapshot(snapshot):
        nonlocal stopped, pending_at_stop
        if not stopped:
            pending_at_stop = snapshot["queue"]["produced"] - snapshot["queue"]["consumed"]
            stopped = True

    database = tmp_path / "stream.db"
    with StreamEngine(database) as engine:
        state = asyncio.run(run_managed(
            engine, lambda: stopped, mode=mode, interval_s=0, queue_size=2,
            batch_size=4, max_wait_s=0.05, processing_delay_s=0.01,
            on_snapshot=on_snapshot,
        ))
        assert pending_at_stop > 0
        assert state["produced"] == state["consumed"] > 1
        assert engine.checkpoint == state["consumed"] - 1
        assert engine.con.execute("SELECT finalized FROM stream_meta").fetchone()[0] == 0
        if mode == "microbatch":
            assert sum(state["batch_sizes"]) == state["consumed"]
            assert state["flush_counts"]["final_drain"] == 1
    with StreamEngine(database) as engine:
        starting = engine.checkpoint

        def stop_after_next(snapshot):
            nonlocal stopped
            stopped = True

        stopped = False
        asyncio.run(run_managed(engine, lambda: stopped, interval_s=0.01,
                                on_snapshot=stop_after_next))
        assert engine.checkpoint > starting


@pytest.mark.parametrize("mode", ["live", "microbatch"])
def test_stop_before_admission_is_empty_and_does_not_finalize(tmp_path, mode):
    with StreamEngine(tmp_path / "stream.db") as engine:
        state = asyncio.run(run_managed(engine, lambda: True, mode=mode))
        assert state["produced"] == state["consumed"] == 0
        assert engine.checkpoint == -1
        assert engine.con.execute("SELECT finalized FROM stream_meta").fetchone()[0] == 0
