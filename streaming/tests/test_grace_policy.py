"""SHOPIL-40: grace 0/10/120 on the committed 300-order fixture (course reference values)."""
from __future__ import annotations

import pytest

from shopil_streaming.fixtures import load_fixture
from streaming_lab import StreamEngine


@pytest.mark.parametrize("grace,accepted,late", [(0, 279, 21), (10, 286, 14), (120, 300, 0)])
def test_grace_reference(tmp_path, grace, accepted, late):
    with StreamEngine(tmp_path / "s.db", grace_seconds=grace) as engine:
        for d in load_fixture("orders300"):
            engine.ingest(d.event, offset=d.offset)
        snap = engine.finalize_demo()
    assert (snap["totals"]["orders"], snap["totals"]["late_orders"]) == (accepted, late)
    assert snap["totals"]["revenue_cents"] + snap["totals"]["late_order_cents"] == 6_124_170
