"""SHOPIL-18: ShopIL source determinism, contract validity and resumable runs."""
from __future__ import annotations

import asyncio
from itertools import islice

from shopil_streaming.contract import check
from shopil_streaming.service import DEFAULT_CONFIG, progress_line, run
from shopil_streaming.source import CATALOG_CENTS, delivery_at, shopil_source
from streaming_lab import StreamEngine


def test_source_is_a_pure_function_of_offset_and_valid():
    first = list(islice(shopil_source(), 200))
    assert first == list(islice(shopil_source(), 200))
    assert list(islice(shopil_source(150), 50)) == first[150:]
    assert [d.offset for d in first] == list(range(200))
    assert all(check(d.event) is None for d in first)
    assert {d.event.product_id for d in first} == set(CATALOG_CENTS)


def test_source_contains_retries_and_out_of_order_events():
    retry = delivery_at(12)
    assert retry.event == delivery_at(11).event and retry.offset == 12
    assert delivery_at(18).event.event_time < delivery_at(17).event.event_time


def test_live_run_then_resume_continues_from_checkpoint(tmp_path):
    path = tmp_path / "s.db"
    lines: list[str] = []
    with StreamEngine(path, **DEFAULT_CONFIG) as engine:
        state = asyncio.run(run(engine, mode="live", seconds=0.3, interval_s=0.005, queue_size=4, batch_size=1,
                                max_wait_s=1, processing_delay_s=0, finalize=False,
                                on_snapshot=lambda s: lines.append(progress_line(s))))
        first_checkpoint = engine.checkpoint
    assert state["produced"] == state["consumed"] == first_checkpoint + 1
    assert lines and "produced=" in lines[-1]
    with StreamEngine(path, **DEFAULT_CONFIG) as engine:
        state = asyncio.run(run(engine, mode="microbatch", seconds=0.3, interval_s=0.005, queue_size=4,
                                batch_size=3, max_wait_s=0.05, processing_delay_s=0, finalize=False))
        assert engine.checkpoint == first_checkpoint + state["consumed"]
        assert engine.snapshot()["stats"]["received"] == engine.checkpoint + 1
