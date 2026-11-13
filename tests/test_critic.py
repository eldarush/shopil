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
    assert schema["required"] == ["is_acceptable", "feedback"]
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


@pytest.mark.parametrize("question,evidence,draft", [
    (" ", '{}', "answer"), ("question", '[]', "answer"),
    ("question", 'bad json', "answer"), ("question", '{}', ""),
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
