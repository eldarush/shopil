import asyncio
import json

import pytest
from pydantic import ValidationError

from chat.critic import Critic, Evaluation


class FakeModel:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def complete(self, messages, *, schema=None):
        self.calls.append((messages, schema))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.mark.parametrize("approved", [True, False])
def test_critic_returns_strict_decision_with_original_evidence(approved):
    raw = json.dumps({"is_acceptable": approved, "feedback": "Matched sources"})
    model = FakeModel(raw)
    evidence = '{ "snapshot_id": "s1", "revenue": null, "orders": 0 }'
    result = asyncio.run(Critic(model).evaluate("מה ההכנסה?", evidence, "ההכנסה אינה זמינה"))
    assert result.is_acceptable is approved
    messages, schema = model.calls[0]
    assert messages[2]["content"] == evidence
    assert json.loads(messages[3]["content"])["candidate_answer"] == "ההכנסה אינה זמינה"
    assert schema["required"] == ["feedback", "is_acceptable"]
    assert list(schema["properties"]) == ["feedback", "is_acceptable"]
    assert schema["additionalProperties"] is False


@pytest.mark.parametrize("raw", [
    '{}', '{"is_acceptable":true}', '{"feedback":"OK"}',
    '{"is_acceptable":"false","feedback":"No"}',
    '{"is_acceptable":1,"feedback":"Yes"}',
    '{"is_acceptable":true,"feedback":null}',
    '{"is_acceptable":true,"feedback":" "}',
    '{"is_acceptable":true,"feedback":"OK","extra":1}',
    '```json\n{"is_acceptable":true,"feedback":"OK"}\n```',
    'not JSON', '[]', '', 'null',
])
def test_invalid_model_evaluation_fails_without_retry_or_coercion(raw):
    model = FakeModel(raw)
    with pytest.raises(ValidationError):
        asyncio.run(Critic(model).evaluate("שאלה", '{"snapshot_id":"s1"}', "תשובה"))
    assert len(model.calls) == 1


def test_transport_failure_propagates_without_fabricating_a_decision():
    model = FakeModel(TimeoutError("test timeout"))
    with pytest.raises(TimeoutError):
        asyncio.run(Critic(model).evaluate("שאלה", '{}', "תשובה"))
    assert len(model.calls) == 1


def test_time_index_keeps_independent_source_ranges_and_original_evidence():
    evidence = (
        '{"batch":{"time_end":"2027-01-01"},"sources":['
        '{"table":"marts.fct_orders","time_start":"2026-01-01","time_end":"2027-01-01","rows":[]},'
        '{"table":"marts.mart_product_reviews","time_start":"2026-01-01","time_end":"2026-10-09","rows":[]},'
        '{"table":"empty","time_start":null,"time_end":null,"time_coverage":"no_observed_history"}]}'
    )
    model = FakeModel('{"is_acceptable":false,"feedback":"Wrong product coverage"}')
    asyncio.run(Critic(model).evaluate("Product revenue?", evidence, "A candidate"))
    messages, _ = model.calls[0]
    assert messages[2]["content"] is evidence
    ranges = json.loads(messages[-2]["content"])["source_time_ranges"]
    assert messages[-1] == messages[3]
    assert [row["time_end"] for row in ranges] == ["2027-01-01", "2026-10-09", None]
    assert [row["table"] for row in ranges] == ["marts.fct_orders", "marts.mart_product_reviews", "empty"]
    assert ranges[-1]["time_coverage"] == "no_observed_history"
    assert all("rows" not in row for row in ranges)


@pytest.mark.parametrize("question,evidence,draft", [
    (" ", '{}', "answer"), ("question", '[]', "answer"),
    ("question", 'bad json', "answer"), ("question", '{}', ""),
    ("question", '{"sources":null}', "answer"),
    ("question", '{"sources":["invalid"]}', "answer"),
])
def test_invalid_input_does_not_call_model(question, evidence, draft):
    model = FakeModel('{"is_acceptable":true,"feedback":"OK"}')
    with pytest.raises(ValueError):
        asyncio.run(Critic(model).evaluate(question, evidence, draft))
    assert not model.calls


def test_evaluation_is_immutable():
    result = Evaluation(is_acceptable=False, feedback="Use the correct units")
    with pytest.raises(ValidationError):
        result.is_acceptable = True
