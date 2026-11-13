"""SHOPIL-17: contract enforcement and dead-letter routing."""
from __future__ import annotations

import json

import pytest

from shopil_streaming.contract import check, dead_letter_report
from shopil_streaming.runs import STREAMING_ROOT
from streaming_lab import StreamEngine

CASES = json.loads((STREAMING_ROOT / "fixtures" / "contract_cases.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", CASES, ids=[c["case"] for c in CASES])
def test_precheck_agrees_with_expected(case):
    assert (check(case["event"]) is None) == (case["expect"] == "accepted")


def test_invalid_events_go_to_dead_letters_with_offset_and_reason(tmp_path):
    with StreamEngine(tmp_path / "s.db") as engine:
        statuses = [engine.ingest(c["event"], offset=i)["status"] for i, c in enumerate(CASES)]
        snap = engine.snapshot()
    assert statuses == [c["expect"] for c in CASES]
    dead = dead_letter_report(snap)
    assert [d["offset"] for d in dead] == [i for i, c in enumerate(CASES) if c["expect"] == "invalid"]
    for d in dead:
        assert d["reason"] == check(CASES[d["offset"]]["event"])
        assert d["input"] == CASES[d["offset"]]["event"]
    assert snap["checkpoint"] == len(CASES) - 1
    assert snap["stats"]["accepted"] == 3


def test_dead_letters_survive_reopen(tmp_path):
    path = tmp_path / "s.db"
    with StreamEngine(path) as engine:
        engine.ingest({"event_id": "x", "kind": "order", "event_time": "2026-11-01T10:00:00", "product_id": 1,
                       "amount_cents": 1}, offset=0)
    with StreamEngine(path) as engine:
        dead = dead_letter_report(engine.snapshot())
    assert dead == [{"offset": 0, "reason": "event_time must include a timezone",
                     "input": {"event_id": "x", "kind": "order", "event_time": "2026-11-01T10:00:00",
                               "product_id": 1, "amount_cents": 1}}]
