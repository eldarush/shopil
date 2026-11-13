import asyncio
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from chat.service import ChatService
from data_access.evidence import EvidencePackage, EvidenceSource


FIXTURE = Path(__file__).parent / "fixtures" / "evidence_snapshot.json"


def test_evidence_copies_rows_and_serializes_dates_and_exact_ils():
    rows = [{"product_id": 5, "paid_revenue": "0.00", "avg_rating": None}]
    source = EvidenceSource(
        table="marts.fct_orders",
        metric="historical_paid_revenue",
        units="ILS",
        identifiers=("product_id",),
        time_start=date(2026, 7, 1),
        time_end=date(2026, 8, 1),
        rows=rows,
    )
    package = EvidencePackage(question="What was the paid revenue?", sources=(source,))
    rows[0]["paid_revenue"] = "999.00"
    assert package.model_dump(mode="json")["sources"][0]["rows"] == [
        {"product_id": 5, "paid_revenue": "0.00", "avg_rating": None}
    ]
    assert EvidencePackage.model_validate_json(package.model_dump_json()) == package


def test_evidence_rejects_window_without_checkpoint():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["sources"][1]["checkpoint"] = None
    with pytest.raises(ValidationError, match="require a checkpoint"):
        EvidencePackage.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize("changes", [
    {},
    {"time_coverage": "no_observed_history", "time_start": "2026-01-01"},
    {"time_coverage": "no_observed_history", "time_end": "2027-01-01"},
    {"time_coverage": "no_observed_history", "checkpoint": 0},
    {"time_coverage": "no_observed_history", "watermark": "2026-01-01T00:00:00Z"},
    {"time_coverage": "no_observed_history", "window_state": "open"},
])
def test_null_coverage_is_explicit_paired_and_never_streaming(changes):
    source = json.loads(FIXTURE.read_text(encoding="utf-8"))["sources"][0]
    source.update(time_start=None, time_end=None)
    source.update(changes)
    with pytest.raises(ValidationError):
        EvidenceSource.model_validate_json(json.dumps(source))


@pytest.mark.parametrize("start,end", [
    ("2026-01-01", "2026-01-01"),
    ("2027-01-01", "2026-01-01"),
    ("2026-01-01", "2027-01-01T00:00:00Z"),
])
def test_bounded_coverage_keeps_ordered_same_type_invariant(start, end):
    source = json.loads(FIXTURE.read_text(encoding="utf-8"))["sources"][0]
    source.update(time_start=start, time_end=end)
    with pytest.raises(ValidationError, match="ordered time bounds"):
        EvidenceSource.model_validate_json(json.dumps(source))


def test_fixture_is_accepted_by_chat_capture_and_reused_on_retry():
    package = EvidencePackage.model_validate_json(FIXTURE.read_text(encoding="utf-8"))
    payload = package.model_dump(mode="json")
    evidence_used = []
    captures = []

    async def capture(question, history):
        captures.append(question)
        return payload

    async def draft(question, evidence_json, *_):
        evidence_used.append(evidence_json)
        payload["sources"][1]["checkpoint"] += 1
        return "Draft"

    decisions = iter((False, True))

    async def evaluate(question, evidence_json, draft):
        evidence_used.append(evidence_json)
        return SimpleNamespace(is_acceptable=next(decisions), feedback="Check the snapshot")

    service = ChatService(
        SimpleNamespace(draft=draft), SimpleNamespace(evaluate=evaluate), capture, lambda _: None
    )
    reply = asyncio.run(service.answer(package.question, []))

    assert reply.status == "approved"
    assert captures == [package.question]
    assert len(evidence_used) == 4
    assert len(set(evidence_used)) == 1
    assert json.loads(evidence_used[0])["sources"][1]["checkpoint"] == 17
    assert reply.sources[1]["window_state"] == "open"
