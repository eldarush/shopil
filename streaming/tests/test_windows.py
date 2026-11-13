"""SHOPIL-21: engine time policy equals the independent model; finalize keeps watermark."""
from __future__ import annotations

from datetime import timedelta

import pytest

from shopil_streaming.windows import WindowModel
from streaming_lab import Event, StreamEngine, generate_order_fixture, teaching_fixture
from streaming_lab.events import T0


@pytest.mark.parametrize("grace", [0, 10, 120])
@pytest.mark.parametrize("fixture", ["teaching", "orders300"])
def test_engine_matches_model(tmp_path, grace, fixture):
    data = teaching_fixture() if fixture == "teaching" else generate_order_fixture(300)
    model = WindowModel(60, grace, 0)
    with StreamEngine(tmp_path / "s.db", grace_seconds=grace) as engine:
        for item in data:
            status = engine.ingest(item.event, offset=item.offset)["status"]
            predicted = model.step(item.event)
            assert status == predicted["status"], item.offset
            assert engine.snapshot()["watermark_ms"] == predicted["watermark_ms"]
        closed = {w["window_start_ms"] for w in engine.snapshot()["revenue_windows"]}
        assert closed == set(model.closed_windows)


def test_grace_boundary_is_inclusive(tmp_path):
    with StreamEngine(tmp_path / "s.db") as engine:
        engine.ingest(Event("a", "order", T0 + timedelta(seconds=5), 1, amount_cents=100), offset=0)
        engine.ingest(Event("b", "order", T0 + timedelta(seconds=69, milliseconds=999), 1, amount_cents=100), offset=1)
        assert engine.snapshot()["revenue_windows"] == []          # 10:01:09.999 < end + grace
        engine.ingest(Event("c", "order", T0 + timedelta(seconds=70), 1, amount_cents=100), offset=2)
        assert len(engine.snapshot()["revenue_windows"]) == 1      # 10:01:10 == end + grace closes
        wm = engine.snapshot()["watermark_ms"]
        assert engine.finalize_demo()["watermark_ms"] == wm
