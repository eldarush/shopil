"""SHOPIL-22: late events / dead letters carry input, offset, reason and export to CSV."""
from __future__ import annotations

import csv

from shopil_streaming.contract import dead_letter_report
from shopil_streaming.side_outputs import export_side_outputs, late_report
from streaming_lab import StreamEngine, teaching_fixture


def test_teaching_side_outputs(tmp_path):
    with StreamEngine(tmp_path / "s.db") as engine:
        for item in teaching_fixture():
            engine.ingest(item.event, offset=item.offset)
        snap = engine.snapshot()
    late, dead = late_report(snap), dead_letter_report(snap)
    assert [(r["offset"], r["event_id"], r["amount_cents"]) for r in late] == [(9, "order-late", 500),
                                                                                (15, "order-boundary", 800)]
    assert [(d["offset"], d["reason"]) for d in dead] == [(11, "amount_cents must be an integer >= 0")]
    assert all(r["input"]["event_id"] == r["event_id"] and r["reason"] for r in late)
    paths = export_side_outputs(snap, tmp_path / "out")
    with paths["late_events"].open(encoding="utf-8") as handle:
        assert [row["offset"] for row in csv.DictReader(handle)] == ["9", "15"]
    with paths["dead_letters"].open(encoding="utf-8") as handle:
        assert '"bad-money"' in handle.read()
