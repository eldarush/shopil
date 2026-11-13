"""SHOPIL-24: shekel reporting and stream reconciliation (122 + 13 = 135)."""
from __future__ import annotations

import pytest

from shopil_streaming.revenue import reconcile, revenue_windows, window_monitor
from shopil_streaming.runs import shekels
from streaming_lab import Delivery, Event, StreamEngine, generate_order_fixture, teaching_fixture
from streaming_lab.events import SQLITE_MAX_INTEGER, T0


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


def test_reconciliation_discloses_storage_rejections_without_counting_them_as_accepted(tmp_path):
    data = [Delivery(0, Event("full", "order", T0, 1, amount_cents=SQLITE_MAX_INTEGER)),
            Delivery(1, Event("overflow", "order", T0, 1, amount_cents=1))]
    with StreamEngine(tmp_path / "bounded.db") as engine:
        for item in data:
            engine.ingest(item.event, offset=item.offset)
        rec = reconcile(engine.snapshot(), data)
    assert rec["orders_match"] and rec["revenue_match"]
    assert rec["accepted_cents"] == rec["recomputed_cents"] == SQLITE_MAX_INTEGER
    assert rec["input_schema_truth"]["revenue_cents"] == SQLITE_MAX_INTEGER + 1
    assert rec["excluded_dead_letter_offsets"] == [1]


def test_rejected_identity_can_later_be_accepted_with_corrected_payload(tmp_path):
    data = [Delivery(0, Event("full", "order", T0, 1, amount_cents=SQLITE_MAX_INTEGER)),
            Delivery(1, Event("corrected", "order", T0, 1, amount_cents=1)),
            Delivery(2, Event("corrected", "order", T0, 1, amount_cents=0))]
    with StreamEngine(tmp_path / "corrected.db") as engine:
        for item in data:
            engine.ingest(item.event, offset=item.offset)
        rec = reconcile(engine.snapshot(), data)
    assert rec["orders_match"] and rec["revenue_match"]
    assert rec["recomputed_orders"] == 2
    assert rec["excluded_dead_letter_offsets"] == [1]
