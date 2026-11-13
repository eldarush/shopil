"""SHOPIL-39: same fixture, same order -> identical canonical_results in both modes."""
from __future__ import annotations

import asyncio

from shopil_streaming.fixtures import load_fixture
from streaming_lab import StreamEngine, run_live, run_microbatch
from streaming_lab.reporting import canonical_results


def test_live_and_microbatch_match(tmp_path):
    data = load_fixture("orders300")
    with StreamEngine(tmp_path / "live.db") as engine:
        live = asyncio.run(run_live(engine, data, duration_s=None, interval_s=0, finalize=True))
    with StreamEngine(tmp_path / "mb.db") as engine:
        mb = asyncio.run(run_microbatch(engine, data, duration_s=None, interval_s=0, max_batch_size=7,
                                        max_wait_s=0.05, finalize=True))
    assert canonical_results(live["snapshot"]) == canonical_results(mb["snapshot"])
    assert live["produced"] == live["consumed"] == mb["consumed"] == sum(mb["batch_sizes"]) == len(data)
