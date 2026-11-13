"""SHOPIL-25: product 5 alerts once at the 3rd negative; the 4th only raises the counter."""
from __future__ import annotations

from shopil_streaming.alerts import alert_report, negative_counters
from streaming_lab import StreamEngine, generate_review_fixture, teaching_fixture


def test_review_fixture_single_alert(tmp_path):
    with StreamEngine(tmp_path / "s.db") as engine:
        alerts_after = []
        for item in generate_review_fixture():
            engine.ingest(item.event, offset=item.offset)
            alerts_after.append(len(engine.snapshot()["alerts"]))
        snap = engine.snapshot()
    assert alerts_after == [0, 0, 0, 1, 1, 1]
    assert [(a["product_id"], a["trigger_event_id"]) for a in alert_report(snap)] == [(5, "review-3")]
    p5 = [c for c in negative_counters(snap) if c["product_id"] == 5]
    assert p5[0]["negative_reviews"] == 4 and p5[0]["alerted"]


def test_teaching_fixture_has_one_alert(tmp_path):
    with StreamEngine(tmp_path / "s.db") as engine:
        for item in teaching_fixture():
            engine.ingest(item.event, offset=item.offset)
        snap = engine.finalize_demo()
    assert snap["stats"]["alerts_emitted"] == 1
    assert alert_report(snap)[0]["product_id"] == 5
