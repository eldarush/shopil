"""Reference/rendering guarantees; mocked models do not prove semantic selection."""
import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from chat.critic import Critic
from chat.model import ModelError
from chat.service import ChatService
from chat.structured import Selection, StructuredActor, catalog, render


def evidence():
    return {"snapshot_id": "fixed-snapshot", "captured_at": "2026-11-20T10:20:00Z", "sources": [{
        "table": "marts.mart_product_reviews", "metric": "full_history_product_metrics", "units": "ILS and ratings",
        "identifiers": ["product_id"], "time_start": "2026-01-01", "time_end": "2026-10-09", "time_bounds": "[)",
        "rows": [{"product_id": 3, "avg_rating": 3.95, "revenue": "100.00"},
                 {"product_id": 5, "avg_rating": 3.52, "revenue": "8925.00"}],
    }]}


def test_row_bound_values_and_provenance_cannot_be_rewritten_by_actor():
    data = evidence()
    answer = render(Selection(status="facts", facts=["s0:r1:avg_rating"]), data, catalog(data))
    assert "3.52 (product_id=5)" in answer
    assert "3.95" not in answer
    assert "מועד צילום הנתונים: 2026-11-20T10:20:00Z" in answer
    assert "מ־2026-01-01 (כולל) עד 2026-10-09 (לא כולל)" in answer


@pytest.mark.parametrize("raw", [
    '{"status":"facts","facts":["s0:r1:avg_rating"],"value":3.95}',
    '{"status":"facts","facts":[]}',
    '{"status":"unavailable","facts":["s0:r1:revenue"]}',
    '{"status":"facts","facts":["x","x"]}',
    '{"status":"facts","facts":[3]}',
])
def test_arbitrary_values_prose_inconsistent_or_duplicate_selections_fail_closed(raw):
    with pytest.raises(ValidationError):
        Selection.model_validate_json(raw)


def test_unknown_reference_fails_and_other_product_never_mislabeled():
    data = evidence()
    with pytest.raises(ModelError):
        render(Selection(status="facts", facts=["s0:r99:revenue"]), data, catalog(data))
    # Selecting the wrong product remains a semantic error; renderer makes it visible,
    # rather than laundering product3's value into a purported product5 claim.
    answer = render(Selection(status="facts", facts=["s0:r0:avg_rating"]), data, catalog(data))
    assert "3.95 (product_id=3)" in answer
    assert "product_id=5" not in answer


def test_unavailable_is_explicit_and_retains_actual_capture_metadata():
    data = evidence()
    answer = render(Selection(status="unavailable", facts=[]), data, catalog(data))
    assert "המידע המבוקש בשאלה אינו זמין" in answer
    assert "8925" not in answer
    assert "מועד צילום הנתונים: 2026-11-20T10:20:00Z" in answer


def test_real_service_retries_rendered_candidate_and_audits_raw_selection():
    data = evidence()
    captured = json.dumps(data)
    calls = []
    responses = iter([
        '{"status":"facts","facts":["s0:r0:avg_rating"]}',
        '{"feedback":"Wrong product; requested product5", "is_acceptable":false}',
        '{"status":"facts","facts":["s0:r1:avg_rating"]}',
        '{"feedback":"Correct product5 rating", "is_acceptable":true}',
    ])
    async def complete(messages, *, schema=None):
        calls.append((deepcopy(messages), schema))
        return next(responses)
    async def capture(question, history):
        return data
    audit = []
    model = SimpleNamespace(complete=complete)
    service = ChatService(StructuredActor(model), Critic(model), capture, audit.append)
    reply = asyncio.run(service.answer("מה דירוג מוצר 5?", []))
    assert reply.status == "approved"
    assert "3.52 (product_id=5)" in reply.answer
    assert len(reply.history) == 2
    attempts = [event for event in audit if event["event"] == "attempt"]
    assert len(attempts) == 2
    assert attempts[0]["rendering"] == "deterministic_fact_references"
    assert json.loads(attempts[0]["actor_output_json"])["facts"] == ["s0:r0:avg_rating"]
    assert "3.95 (product_id=3)" in attempts[0]["draft"]
    assert "3.52 (product_id=5)" in attempts[1]["draft"]
    # Critic reviews precisely what will be displayed, not the raw selection JSON.
    assert json.loads(calls[3][0][3]["content"])["candidate_answer"] == reply.answer
    assert calls[0][0][2]["content"] == calls[2][0][2]["content"]
