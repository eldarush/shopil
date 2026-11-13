"""SHOPIL-24: shekel reporting and stream reconciliation (122 + 13 = 135)."""
from __future__ import annotations

import pytest

from shopil_streaming.revenue import reconcile, revenue_windows, window_monitor
from shopil_streaming.runs import shekels
from streaming_lab import StreamEngine, generate_order_fixture, teaching_fixture


def test_teaching_reconciliation(tmp_path):
    data = teaching_fixture()
    with StreamEngine(tmp_path / "s.db") as engine:
        for item in data:
            engine.ingest(item.event, offset=item.offset)
        live = window_monitor(engine.snapshot())
        final = engine.finalize_demo()
    assert live["open_windows"] == 1 and live["accepted_revenue_ils"] == "122.00"
    rec = reconcile(final, data)
    assert (rec["accepted_ils"], rec["late_ils"], rec["recomputed_ils"]) == ("122.00", "13.00", "135.00")
    assert rec["revenue_match"] and rec["orders_match"]
    assert all(w["state"] == "closed" for w in revenue_windows(final))


@pytest.mark.parametrize("grace,accepted", [(0, "56,916.90"), (10, "57,954.30"), (120, "61,241.70")])
def test_generator_reconciles_for_each_grace(tmp_path, grace, accepted):
    data = generate_order_fixture(300)
    with StreamEngine(tmp_path / "s.db", grace_seconds=grace) as engine:
        for item in data:
            engine.ingest(item.event, offset=item.offset)
        rec = reconcile(engine.finalize_demo(), data)
    assert rec["accepted_ils"] == accepted and rec["recomputed_ils"] == "61,241.70" and rec["revenue_match"]


def test_shekels_is_exact():
    assert shekels(12_200) == "122.00" and shekels(5) == "0.05" and shekels(-150) == "-1.50"
    with pytest.raises(TypeError):
        shekels(1.5)
