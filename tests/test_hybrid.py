"""Finite-grammar mechanics; not proof of question relevance or model accuracy."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from chat.critic import Critic
from chat.hybrid import HybridActor, Narrative, render_narrative
from chat.model import ModelError
from chat.service import ChatService
from chat.hybrid import semantic_catalog as catalog


def evidence():
    return {"snapshot_id": "s1", "captured_at": "2026-11-20T10:00:00Z", "sources": [{
        "table": "marts.mart_product_reviews", "metric": "full_history_product_metrics", "units": "ILS and ratings",
        "identifiers": ["product_id"], "time_start": "2026-01-01", "time_end": "2026-10-09", "time_bounds": "[)",
        "rows": [{"product_id": 3, "avg_rating": 3.95}, {"product_id": 5, "avg_rating": 3.52, "revenue": "8925.00"}],
    }]}


def plan(sentence, explanation="none"):
    return Narrative(status="answer", sentences=[sentence], explanation=explanation)


@pytest.mark.parametrize("sentence", [
    "הדירוג הממוצע הוא 3.95.",
    "הדירוג הממוצע הוא שלוש נקודה תשעים וחמש.",
    "מוצר 5 קיבל דירוג ממוצע {{s0:id_product_id_3:avg_rating}}.",
    "ההכנסה מהזמנות ששולמו היא {{s0:id_product_id_5:avg_rating}}.",
    "הדירוג הממוצע הוא {{s0:r99:avg_rating}}.",
    "הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}. זו הכנסה.",
])
def test_unproved_literal_values_entities_wrong_metrics_extra_prose_fail_closed(sentence):
    data = evidence()
    with pytest.raises(ModelError):
        render_narrative(plan(sentence), data, catalog(data))


def test_model_authored_supported_sentence_is_bound_to_original_row_and_metadata():
    data = evidence()
    answer = render_narrative(plan("לפי הנתונים, ממוצע הדירוגים הוא {{s0:id_product_id_5:avg_rating}}.", "rating_not_revenue"), data, catalog(data))
    assert "לפי הנתונים, ממוצע הדירוגים הוא 3.52 (product_id=5)." in answer
    assert "3.95" not in answer
    assert "מועד צילום הנתונים: 2026-11-20T10:00:00Z" in answer
    assert "עד 2026-10-09 (לא כולל)" in answer
    assert "לא הכנסה" in answer


def test_refusal_does_not_claim_warehouse_data_absence():
    data = evidence()
    answer = render_narrative(Narrative(status="cannot_verify", sentences=[], explanation="none"), data, catalog(data))
    assert "לא ניתן להפיק תשובה מאומתת" in answer
    assert "אפשר לנסח שאלה ממוקדת" in answer
    assert "אינו זמין" not in answer


def test_explanation_must_match_selected_facts():
    data = evidence()
    with pytest.raises(ModelError):
        render_narrative(plan("הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}.", "stream_not_paid"), data, catalog(data))


def test_actor_critic_retry_preserves_authored_json_rendered_answer_and_snapshot():
    replies = iter([
        '{"status":"answer","sentences":["הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}."],"explanation":"none"}',
        '{"feedback":"Explain that rating is not revenue", "is_acceptable":false}',
        '{"status":"answer","sentences":["לפי הנתונים, ממוצע הדירוגים הוא {{s0:id_product_id_5:avg_rating}}."],"explanation":"rating_not_revenue"}',
        '{"feedback":"Matches product5 rating", "is_acceptable":true}',
    ])
    calls, audit = [], []
    async def complete(messages, *, schema=None):
        calls.append(messages)
        return next(replies)
    async def capture(question, history):
        return evidence()
    model = SimpleNamespace(complete=complete)
    service = ChatService(HybridActor(model), Critic(model), capture, audit.append)
    reply = asyncio.run(service.answer("מה הדירוג הממוצע של מוצר 5?", []))
    assert reply.status == "approved"
    assert "3.52 (product_id=5)" in reply.answer
    attempts = [event for event in audit if event["event"] == "attempt"]
    assert all(event["rendering"] == "constrained_hebrew_narrative" for event in attempts)
    assert "ממוצע הדירוגים" in attempts[-1]["actor_output_json"]
    assert calls[0][2]["content"] == calls[2][2]["content"]
    assert json.loads(calls[3][3]["content"])["candidate_answer"] == reply.answer


def test_unsupported_prose_is_not_published_even_if_model_would_approve():
    calls, audit = [], []
    async def complete(messages, *, schema=None):
        calls.append(messages)
        return '{"status":"answer","sentences":["מוצר5 קיבל דירוג3.95"],"explanation":"none"}'
    async def capture(question, history):
        return evidence()
    model = SimpleNamespace(complete=complete)
    result = asyncio.run(ChatService(HybridActor(model), Critic(model), capture, audit.append).answer("מה הדירוג?", []))
    assert result.status == "error" and result.answer is None and result.history == []
    assert len(calls) == 1  # Invalid prose cannot reach publication through Critic approval.


def test_followup_context_reaches_actor_but_current_values_are_rendered():
    history = [{"role": "user", "content": "מה הדירוג?"}, {"role": "assistant", "content": "תשובה קודמת"}]
    calls = []
    async def complete(messages, *, schema=None):
        calls.append(messages)
        return '{"status":"answer","sentences":["ממוצע הדירוגים הוא {{s0:id_product_id_5:avg_rating}}."],"explanation":"none"}'
    answer = asyncio.run(HybridActor(SimpleNamespace(complete=complete)).draft("ומה לגבי מוצר5?", json.dumps(evidence()), history))
    assert json.loads(calls[0][1]["content"])["approved_history"] == history
    assert "3.52 (product_id=5)" in answer


def test_compact_reference_index_preserves_all_bindings_without_repeated_rows():
    data = evidence()
    # Representative multi-field rows with long source text: each original row
    # used to be copied once per selectable field, multiplying prompt size.
    row = data["sources"][0]["rows"][1]
    row.update(review_count=42, negative_reviews=5, paid_orders=56,
               product_name="שם מוצר ארוך " * 30)
    calls = []
    async def complete(messages, *, schema=None):
        calls.append(messages)
        return '{"status":"cannot_verify","sentences":[],"explanation":"none"}'
    frozen = json.dumps(data, ensure_ascii=False)
    asyncio.run(HybridActor(SimpleNamespace(complete=complete)).draft("מה הדירוג הממוצע של מוצר5?", frozen, []))
    messages = calls[0]
    index = json.loads(messages[3]["content"])
    entries = {key: value for key, value in catalog(data).items() if key == "s0:id_product_id_5:avg_rating"}
    assert index["available_references"] == list(entries)
    assert "available_facts" not in index
    assert all(isinstance(ref, str) for ref in index["available_references"])
    assert messages[2]["content"] is frozen
    assert sum(message["content"] == frozen for message in messages) == 1
    legacy = [{"reference": ref, "field": field, "row": source_row,
               "table": source["table"], "metric": source["metric"]}
              for ref, (source, source_row, field) in entries.items()]
    compact_bytes = len(json.dumps(index["available_references"], ensure_ascii=False).encode())
    legacy_bytes = len(json.dumps(legacy, ensure_ascii=False).encode())
    assert compact_bytes < legacy_bytes / 10
    # Byte savings are not tokenizer counts or a universal context-size guarantee.


def test_generation_schema_encodes_status_sentence_coupling():
    schema = Narrative.generation_schema(catalog(evidence()))
    answer, refusal = schema["anyOf"]
    assert answer["properties"]["status"]["enum"] == ["answer"]
    assert answer["properties"]["sentences"]["minItems"] == 1
    assert answer["properties"]["sentences"]["maxItems"] == 6
    assert refusal["properties"]["status"]["enum"] == ["cannot_verify"]
    assert refusal["properties"]["sentences"]["maxItems"] == 0
    assert refusal["properties"]["explanation"]["enum"] == ["none"]
    assert all(branch["additionalProperties"] is False for branch in schema["anyOf"])


def test_observed_invalid_refusal_is_rejected_without_repair():
    from pydantic import ValidationError
    raw = '{"explanation":"none","sentences":["status cannot_verify"],"status":"cannot_verify"}'
    with pytest.raises(ValidationError):
        Narrative.model_validate_json(raw)
    valid = Narrative.model_validate_json('{"status":"cannot_verify","sentences":[],"explanation":"none"}')
    assert valid.sentences == []


def test_actor_sends_branch_schema_and_explicit_refusal_example():
    calls = []
    async def complete(messages, *, schema=None):
        calls.append((messages, schema))
        return '{"status":"cannot_verify","sentences":[],"explanation":"none"}'
    asyncio.run(HybridActor(SimpleNamespace(complete=complete)).draft("שאלה", json.dumps(evidence()), []))
    messages, schema = calls[0]
    assert schema == Narrative.generation_schema({})
    assert '{"status":"cannot_verify","sentences":[],"explanation":"none"}' in messages[0]["content"]


def test_generation_schema_enumerates_only_renderer_accepted_row_bound_sentences():
    data = evidence()
    entries = catalog(data)
    schema = Narrative.generation_schema(entries)
    options = schema["anyOf"][0]["properties"]["sentences"]["items"]["enum"]
    assert options and len(options) == len(set(options))
    assert "הדירוג הממוצע הוא 3.52." not in options
    assert "הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}." in options
    assert "ההכנסה מהזמנות ששולמו היא {{s0:id_product_id_5:avg_rating}}." not in options
    for option in options:
        assert render_narrative(plan(option), data, entries)


def test_generation_enum_is_bounded_and_empty_evidence_can_only_refuse(monkeypatch):
    import chat.hybrid as hybrid
    schema = Narrative.generation_schema({})
    assert schema["properties"]["status"]["enum"] == ["cannot_verify"]
    assert schema["properties"]["sentences"]["maxItems"] == 0
    monkeypatch.setattr(hybrid, "MAX_SENTENCE_CHOICES", 1)
    with pytest.raises(ModelError):
        Narrative.generation_schema(catalog(evidence()))
    monkeypatch.setattr(hybrid, "MAX_SENTENCE_CHOICES", 1024)
    monkeypatch.setattr(hybrid, "MAX_SCHEMA_BYTES", 1)
    with pytest.raises(ModelError):
        Narrative.generation_schema(catalog(evidence()))


def test_backend_violation_is_preserved_in_audit_but_never_repaired_or_published():
    raw = '{"status":"answer","sentences":["הדירוג הממוצע הוא 3.52."],"explanation":"none"}'
    calls, audit = [], []
    async def complete(messages, *, schema=None):
        calls.append(schema)
        return raw
    async def capture(question, history):
        return evidence()
    model = SimpleNamespace(complete=complete)
    result = asyncio.run(ChatService(HybridActor(model), Critic(model), capture, audit.append).answer("דירוג?", []))
    assert result.status == "error" and result.answer is None and result.history == []
    assert len(calls) == 1
    attempt = next(event for event in audit if event["event"] == "attempt")
    assert attempt["actor_output_json"] == raw
    assert attempt["draft"] is None
    assert attempt["rendering"] == "constrained_hebrew_narrative"


def test_references_expose_real_identifiers_and_do_not_change_with_row_order():
    from copy import deepcopy
    data = evidence()
    changed = deepcopy(data)
    changed["sources"][0]["rows"].reverse()
    first, second = catalog(data), catalog(changed)
    ref = "s0:id_product_id_5:avg_rating"
    assert first[ref][1]["avg_rating"] == second[ref][1]["avg_rating"] == 3.52
    assert first["s0:id_product_id_3:avg_rating"][1]["avg_rating"] == 3.95
    changed["sources"][0]["rows"].append(changed["sources"][0]["rows"][0])
    with pytest.raises(ModelError):
        catalog(changed)


def test_same_reference_dedup_preserves_first_wording_raw_output_and_audit():
    raw = json.dumps({"status":"answer", "sentences":[
        "לפי הנתונים, ממוצע הדירוגים הוא {{s0:id_product_id_5:avg_rating}}.",
        "הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}.",
        "בנתונים הזמינים, הדירוג הממוצע הוא {{s0:id_product_id_5:avg_rating}}."], "explanation":"none"}, ensure_ascii=False)
    responses = iter([raw, '{"feedback":"Matches product5", "is_acceptable":true}'])
    async def complete(messages, *, schema=None):
        return next(responses)
    async def capture(question, history):
        return evidence()
    audit=[]
    model=SimpleNamespace(complete=complete)
    reply=asyncio.run(ChatService(HybridActor(model),Critic(model),capture,audit.append).answer("מה דירוג מוצר5?",[]))
    assert reply.status == "approved"
    assert reply.answer.count("3.52 (product_id=5)") == 1
    assert reply.answer.startswith("לפי הנתונים, ממוצע הדירוגים")
    attempt=next(event for event in audit if event["event"]=="attempt")
    assert attempt["actor_output_json"] == raw
    assert [item["index"] for item in attempt["normalization"]["removed_sentences"]] == [1,2]


def test_dedup_does_not_hide_invalid_clause_or_change_wrong_entity():
    from chat.hybrid import normalize_repeated_references
    data=evidence(); entries=catalog(data)
    bad=Narrative(status="answer",sentences=[
        "הדירוג הממוצע הוא {{s0:id_product_id_3:avg_rating}}.",
        "ההכנסה מהזמנות ששולמו היא {{s0:id_product_id_3:avg_rating}}."],explanation="none")
    with pytest.raises(ModelError):
        normalize_repeated_references(bad,entries)
    good=bad.model_copy(update={"sentences":[bad.sentences[0],bad.sentences[0]]})
    normalized,removed=normalize_repeated_references(good,entries)
    result=render_narrative(normalized,data,entries)
    assert len(removed)==1
    assert "3.95 (product_id=3)" in result
    assert "product_id=5" not in result
