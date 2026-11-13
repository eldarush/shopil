"""Loop tests use explicit fake models; they do not validate LLM factual quality."""

import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from copy import deepcopy
import json
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace

import pytest

from chat.model import ModelError
from chat.service import ChatService
from chat.actor import Actor as GroundedActor
from chat.critic import Critic as StructuredCritic


def snapshot():
    return {"snapshot_id": "s1", "captured_at": "2026-11-13T10:00:00+00:00",
            "sources": [{"table": "marts.fct_orders", "units": "ILS"}],
            "rows": [{"paid_revenue": "0.00", "average_rating": None}]}


@pytest.mark.parametrize("accepted", [True, False])
def test_terminal_publication_and_shutdown_are_serialized(accepted):
    entered, release, stopping = Event(), Event(), Event()
    events, closing_during_commit = [], []

    def audit(event):
        if event["event"] == "terminal":
            entered.set()
            assert release.wait(5)
            closing_during_commit.append(instance._closing)
        events.append(event)

    instance, _, _, _, _ = service(critic=Critic([accepted] * 3), audit=audit)

    def stop():
        stopping.set()
        instance.shutdown()

    with ThreadPoolExecutor(2) as pool:
        answer = pool.submit(lambda: asyncio.run(instance.answer("question", [])))
        try:
            assert entered.wait(5)
            shutdown = pool.submit(stop)
            assert stopping.wait(5)
            with pytest.raises(FutureTimeout):
                shutdown.result(timeout=.05)
        finally:
            release.set()
        reply = answer.result(timeout=5)
        shutdown.result(timeout=5)
    assert closing_during_commit == [False]
    status = "approved" if accepted else "rejected"
    assert reply.status == status and len(reply.history) == (2 if accepted else 0)
    assert [event["status"] for event in events if event["event"] == "terminal"] == [status]


def test_shutdown_deadline_includes_slow_terminal_publication():
    entered, release = Event(), Event()

    def audit(event):
        if event["event"] == "terminal":
            entered.set()
            assert release.wait(5)

    instance, _, _, _, _ = service(audit=audit)
    with ThreadPoolExecutor(2) as pool:
        answer = pool.submit(lambda: asyncio.run(instance.answer("question", [])))
        try:
            assert entered.wait(5)
            shutdown = pool.submit(instance.shutdown, .02)
            with pytest.raises(RuntimeError, match="timed out.*terminal publication"):
                shutdown.result(timeout=1)
        finally:
            release.set()
        assert answer.result(timeout=5).status == "approved"
    instance.shutdown()


def test_shutdown_during_attempt_audit_prevents_terminal_approval():
    entered, release = Event(), Event()
    events = []

    def audit(event):
        if event["event"] == "attempt":
            entered.set()
            assert release.wait(5)
        events.append(event)

    instance, _, _, _, _ = service(audit=audit)
    with ThreadPoolExecutor(2) as pool:
        answer = pool.submit(lambda: asyncio.run(instance.answer("question", [])))
        try:
            assert entered.wait(5)
            shutdown = pool.submit(instance.shutdown)
            deadline = monotonic() + 2
            while not instance._closing and monotonic() < deadline:
                sleep(.001)
            assert instance._closing
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            answer.result(timeout=5)
        shutdown.result(timeout=5)
    assert [event["status"] for event in events if event["event"] == "terminal"] == ["error"]


@pytest.mark.parametrize("stage", ["capture", "actor", "critic"])
def test_shutdown_waits_for_cancelled_question_audit_and_closes_admission(stage):
    async def scenario():
        started = asyncio.Event()
        events = []

        async def wait_here():
            started.set()
            await asyncio.Event().wait()

        async def capture(question, history):
            if stage == "capture":
                await wait_here()
            return snapshot()

        class WaitingActor:
            async def draft(self, *args):
                if stage == "actor":
                    await wait_here()
                return "unapproved draft"

        class WaitingCritic:
            async def evaluate(self, *args):
                await wait_here()

        instance = ChatService(WaitingActor(), WaitingCritic(), capture, events.append)
        history = [{"role": "user", "content": "old question"},
                   {"role": "assistant", "content": "old approved answer"}]
        original = deepcopy(history)
        pending = asyncio.create_task(instance.answer("new question", history))
        await started.wait()
        await asyncio.to_thread(instance.shutdown)
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert events[-1]["event"] == "terminal" and events[-1]["status"] == "error"
        assert events[-1]["error_type"] == "CancelledError" and events[-1]["error_stage"] == stage
        assert sum(event["event"] == "terminal" for event in events) == 1
        if stage != "capture":
            assert events[-2]["event"] == "attempt"
            assert events[-2]["draft"] == ("unapproved draft" if stage == "critic" else None)
        count = len(events)
        refused = await instance.answer("after shutdown", history)
        assert refused.status == "error" and refused.answer is None
        assert refused.history == history == original
        assert len(events) == count  # No new question was admitted.

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [OSError("unavailable disk"), ValueError("invalid audit encoding")])
def test_shutdown_reports_failed_cancellation_audit(failure):
    async def scenario():
        started = asyncio.Event()

        class WaitingActor:
            async def draft(self, *args):
                started.set()
                await asyncio.Event().wait()

        def audit(event):
            if event["event"] == "terminal":
                raise failure

        instance, _, _, _, _ = service(actor=WaitingActor(), audit=audit)
        pending = asyncio.create_task(instance.answer("question", []))
        await started.wait()
        with pytest.raises(RuntimeError, match="audit"):
            await asyncio.to_thread(instance.shutdown)
        with pytest.raises(asyncio.CancelledError):
            await pending

    asyncio.run(scenario())


def test_shutdown_timeout_cannot_publish_answer_from_model_that_ignores_cancellation():
    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()

        class StubbornActor:
            async def draft(self, *args):
                started.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    await release.wait()
                return "must not be published after shutdown"

        instance, _, events, _, critic = service(actor=StubbornActor())
        pending = asyncio.create_task(instance.answer("question", []))
        await started.wait()
        try:
            with pytest.raises(RuntimeError, match="timed out"):
                await asyncio.to_thread(instance.shutdown, .02)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert not critic.calls
        assert events[-1]["status"] == "error"
        assert events[-2]["draft"] == "must not be published after shutdown"
        await asyncio.to_thread(instance.shutdown)

    asyncio.run(scenario())


def test_shutdown_finalizes_each_concurrent_question():
    async def scenario():
        both_started = asyncio.Event()
        calls = 0

        class WaitingActor:
            async def draft(self, *args):
                nonlocal calls
                calls += 1
                if calls == 2:
                    both_started.set()
                await asyncio.Event().wait()

        instance, _, events, _, _ = service(actor=WaitingActor())
        tasks = [asyncio.create_task(instance.answer(question, [])) for question in ("one", "two")]
        await both_started.wait()
        await asyncio.to_thread(instance.shutdown)
        for task in tasks:
            with pytest.raises(asyncio.CancelledError):
                await task
        terminals = [event for event in events if event["event"] == "terminal"]
        assert len({event["request_id"] for event in terminals}) == 2
        assert len(terminals) == 2 and all(event["status"] == "error" for event in terminals)

    asyncio.run(scenario())


class Actor:
    def __init__(self, responses=None, during=None):
        self.responses = iter(responses or ["תשובה מאושרת"] * 3)
        self.calls = []
        self.during = during

    async def draft(self, question, evidence_json, history, previous_draft=None, feedback=None):
        self.calls.append((question, evidence_json, deepcopy(history), previous_draft, feedback))
        if self.during:
            self.during()
        answer = next(self.responses)
        if isinstance(answer, Exception):
            raise answer
        return answer


class Critic:
    def __init__(self, decisions):
        self.decisions = iter(decisions)
        self.calls = []

    async def evaluate(self, question, evidence_json, draft):
        self.calls.append((question, evidence_json, draft))
        decision = next(self.decisions)
        if isinstance(decision, Exception):
            raise decision
        return SimpleNamespace(is_acceptable=decision, feedback="Use the current source")


def service(actor=None, critic=None, evidence=None, audit=None):
    calls, events = [], []
    evidence = snapshot() if evidence is None else evidence

    async def capture(question, history):
        calls.append((question, deepcopy(history)))
        return evidence

    actor = actor or Actor()
    critic = critic or Critic([True])
    instance = ChatService(actor, critic, capture, events.append if audit is None else audit)
    return instance, calls, events, actor, critic


def test_first_approval_appends_only_approved_pair_after_auditing():
    instance, captures, events, actor, critic = service()
    history = [{"role": "user", "content": "קודם"}, {"role": "assistant", "content": "ישן"}]
    original = deepcopy(history)
    result = asyncio.run(instance.answer("מה ההכנסה?", history))
    assert result.status == "approved"
    assert result.answer == "תשובה מאושרת"
    assert result.history == original + [
        {"role": "user", "content": "מה ההכנסה?"},
        {"role": "assistant", "content": result.answer},
    ]
    assert history == original
    assert len(captures) == len(actor.calls) == len(critic.calls) == 1
    assert [event["event"] for event in events] == ["snapshot", "attempt", "terminal"]
    assert events[-1]["status"] == "approved"
    assert {event["snapshot_id"] for event in events} == {"s1"}
    assert len({event["request_id"] for event in events}) == 1


def test_rejection_passes_previous_draft_and_feedback_then_approves():
    instance, captures, events, actor, critic = service(
        Actor(["טיוטה שגויה", "תיקון מאושר"]), Critic([False, True]))
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.answer == "תיקון מאושר"
    assert len(captures) == 1
    assert actor.calls[1][3:] == ("טיוטה שגויה", "Use the current source")
    assert actor.calls[0][1] is actor.calls[1][1] is critic.calls[0][1] is critic.calls[1][1]
    assert all(item["content"] != "טיוטה שגויה" for item in result.history)
    assert events[1]["draft"] == "טיוטה שגויה"


def test_three_rejections_stop_without_returning_any_draft():
    instance, captures, events, actor, critic = service(
        Actor(["bad1", "bad2", "bad3"]), Critic([False, False, False]))
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "rejected"
    assert result.answer is None and result.history == []
    assert "bad" not in result.message
    assert len(captures) == 1 and len(actor.calls) == len(critic.calls) == 3
    assert events[-1]["attempts"] == 3


def test_mutation_during_answer_does_not_change_snapshot_but_next_question_refreshes():
    evidence = snapshot()
    actor = Actor(during=lambda: evidence["rows"][0].update(paid_revenue="99.00"))
    instance, captures, events, actor, critic = service(actor, Critic([False, True, True]), evidence)
    first = asyncio.run(instance.answer("שאלה", []))
    second = asyncio.run(instance.answer("ועכשיו?", first.history))
    assert len(captures) == 2
    assert json.loads(actor.calls[0][1])["rows"][0]["paid_revenue"] == "0.00"
    assert actor.calls[0][1] == actor.calls[1][1]
    assert json.loads(actor.calls[2][1])["rows"][0]["paid_revenue"] == "99.00"
    assert second.status == "approved"
    assert actor.calls[2][2] == first.history


@pytest.mark.parametrize("failure", [ModelError("secret-body"), TimeoutError("secret-body"),
                                     ValueError("invalid evaluation secret-body")])
@pytest.mark.parametrize("stage", ["actor", "critic"])
def test_model_errors_are_terminal_and_never_leak_details_or_history(failure, stage):
    actor = Actor([failure] if stage == "actor" else ["unapproved"])
    critic = Critic([failure] if stage == "critic" else [True])
    instance, _, events, _, _ = service(actor, critic)
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "error" and result.answer is None and result.history == []
    assert len(actor.calls) == 1
    assert len(critic.calls) == (1 if stage == "critic" else 0)
    assert events[-1]["error_stage"] == stage
    assert events[-1]["status"] == "error"
    assert "secret-body" not in json.dumps(events) + result.message


@pytest.mark.parametrize("failed_event", ["snapshot", "attempt", "terminal"])
def test_audit_failure_never_publishes_an_answer(failed_event):
    def audit(event):
        if event["event"] == failed_event:
            raise OSError("disk unavailable")

    instance, _, _, _, _ = service(audit=audit)
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "error" and result.answer is None and result.history == []


@pytest.mark.parametrize("patch", [
    {"snapshot_id": ""}, {"captured_at": "2026-11-13T10:00:00"},
    {"captured_at": "bad-time"}, {"sources": None}, {"question": "different"},
    {"rows": [{"paid_revenue": float("nan")}]}, {"rows": [{"bad": object()}]},
])
def test_invalid_evidence_stops_before_models(patch):
    evidence = snapshot()
    evidence.update(patch)
    instance, _, events, actor, critic = service(evidence=evidence)
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "error"
    assert not actor.calls and not critic.calls
    assert result.snapshot_id is None
    assert events[-1]["error_stage"] == "capture"


def test_invalid_critic_boolean_is_not_coerced_into_approval():
    instance, _, _, _, _ = service(critic=Critic(["false"]))
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "error" and result.answer is None


def test_unexpected_programming_error_is_not_hidden():
    instance, _, _, _, _ = service(actor=Actor([AttributeError("programming bug")]))
    with pytest.raises(AttributeError):
        asyncio.run(instance.answer("שאלה", []))


def test_capture_and_actor_cannot_mutate_callers_approved_history():
    original = [{"role": "user", "content": "קודם"}, {"role": "assistant", "content": "תשובה"}]

    async def capture(question, history):
        history[0]["content"] = "changed"
        return snapshot()

    instance = ChatService(Actor(), Critic([True]), capture, lambda event: None)
    result = asyncio.run(instance.answer("המשך", original))
    result.history[0]["content"] = "also changed"
    assert original[0]["content"] == "קודם"


@pytest.mark.parametrize("history", [None, [{"role": "user", "content": "orphan"}],
                                     [{"role": "system", "content": "bad"},
                                      {"role": "assistant", "content": "bad"}]])
def test_invalid_history_is_rejected_before_capture(history):
    instance, captures, _, _, _ = service()
    with pytest.raises(ValueError):
        asyncio.run(instance.answer("שאלה", history))
    assert not captures


def test_concurrent_questions_keep_snapshots_histories_and_audit_requests_separate():
    events = []

    async def capture(question, history):
        evidence = snapshot()
        evidence["snapshot_id"] = question
        return evidence

    class InterleavedActor:
        async def draft(self, question, evidence_json, history, previous_draft=None, feedback=None):
            await asyncio.sleep(0)
            assert json.loads(evidence_json)["snapshot_id"] == question
            assert history == []
            return question

    instance = ChatService(InterleavedActor(), Critic([True, True]), capture, events.append)

    async def run():
        return await asyncio.gather(instance.answer("ראשון", []), instance.answer("שני", []))

    first, second = asyncio.run(run())
    assert first.snapshot_id == first.answer == first.history[-1]["content"] == "ראשון"
    assert second.snapshot_id == second.answer == second.history[-1]["content"] == "שני"
    requests = {event["request_id"] for event in events}
    assert len(requests) == 2
    for request_id in requests:
        request_events = [event for event in events if event["request_id"] == request_id]
        assert len({event["snapshot_id"] for event in request_events}) == 1
        assert [event["event"] for event in request_events] == ["snapshot", "attempt", "terminal"]


class ScriptedModel:
    """Explicit mock transport shared by the real Actor and Critic classes."""

    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    async def complete(self, messages, *, schema=None):
        self.calls.append((deepcopy(messages), deepcopy(schema)))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def test_real_actor_and_critic_classes_correct_using_one_snapshot_with_mock_transport():
    model = ScriptedModel([
        "ההכנסה היא 99 ש״ח",
        '{"is_acceptable": false, "feedback": "ההכנסה בראיות היא אפס, לא 99"}',
        "לפי marts.fct_orders ההכנסה היא 0.00 ש״ח בצילום המצב s1.",
        '{"is_acceptable": true, "feedback": "המספר והמקור תואמים לראיות"}',
    ])
    instance, captures, events, _, _ = service(GroundedActor(model), StructuredCritic(model))
    result = asyncio.run(instance.answer("מה ההכנסה?", []))
    assert result.status == "approved"
    assert result.answer == json.loads(model.calls[3][0][3]["content"])["candidate_answer"]
    assert "0.00" in result.answer
    assert len(captures) == 1 and len(model.calls) == 4
    evidence = model.calls[0][0][3]["content"]
    assert model.calls[1][0][2]["content"] == evidence
    assert model.calls[2][0][3]["content"] == evidence
    assert model.calls[3][0][2]["content"] == evidence
    correction = json.loads(model.calls[2][0][-1]["content"])
    assert correction["previous_draft"] == "ההכנסה היא 99 ש״ח"
    assert correction["feedback"] == "ההכנסה בראיות היא אפס, לא 99"
    assert model.calls[1][1]["required"] == ["is_acceptable", "feedback"]
    assert events[-1]["attempts"] == 2
    assert result.history[-1]["content"] == result.answer


@pytest.mark.parametrize("evaluation", [
    'not JSON', '{"is_acceptable":"true","feedback":"looks fine"}',
    '{"is_acceptable":true,"feedback":""}',
])
def test_real_critic_schema_errors_end_request_without_model_retry(evaluation):
    model = ScriptedModel(["טיוטה שלא אושרה", evaluation])
    instance, _, events, _, _ = service(GroundedActor(model), StructuredCritic(model))
    result = asyncio.run(instance.answer("שאלה", []))
    assert result.status == "error" and result.answer is None and result.history == []
    assert len(model.calls) == 2
    assert events[-2]["draft"] == "טיוטה שלא אושרה"
    assert events[-2]["is_acceptable"] is None
    assert events[-1]["error_type"] == "ValidationError"
    assert events[-1]["error_stage"] == "critic"
