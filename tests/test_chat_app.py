import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from chat.app import ChatPage, build_app


class FakeService:
    """UI-only fixture; these answers are not produced by a language model."""

    def __init__(self, status="approved"):
        self.status = status
        self.calls = []

    async def answer(self, question, history):
        self.calls.append((question, deepcopy(history)))
        answer = "נתון בדיקה בעברית"
        return SimpleNamespace(
            status=self.status, answer=answer if self.status == "approved" else None,
            message="אושר" if self.status == "approved" else "לא התקבלה תשובה מאושרת",
            history=history + [{"role": "user", "content": question},
                               {"role": "assistant", "content": answer}],
            snapshot_id="fixture-snapshot", captured_at="2026-11-13T12:00:00+00:00",
            sources=[{"table": "marts.fct_orders", "unit": "ILS"}],
        )


@pytest.mark.parametrize("status", ["rejected", "error"])
def test_unapproved_response_never_enters_ui_or_session_history(status):
    history = [{"role": "user", "content": "שאלה קודמת"},
               {"role": "assistant", "content": "תשובה קודמת"}]
    original = deepcopy(history)
    output = asyncio.run(ChatPage(FakeService(status)).ask("שאלה חדשה", history))
    assert output[0] == output[1] == original
    assert output[-1] == "שאלה חדשה"
    assert history == original


def test_blank_question_does_not_call_backend():
    service = FakeService()
    result = asyncio.run(ChatPage(service).ask(" ", []))
    assert service.calls == []
    assert result[0] == result[1] == []


def test_real_service_keeps_rejected_draft_in_audit_only(tmp_path):
    import json
    from chat.actor import Actor
    from chat.audit import JsonlAudit
    from chat.critic import Critic
    from chat.service import ChatService

    class ScriptedModel:
        def __init__(self, responses):
            self.responses = iter(responses)

        async def complete(self, messages, *, schema=None):
            return next(self.responses)

    async def capture(question, history):
        return {"snapshot_id": "fixture", "captured_at": "2026-11-13T12:00:00+00:00",
                "sources": [{"table": "marts.fct_orders", "unit": "ILS", "paid_revenue": "0.00"}]}

    service = ChatService(
        Actor(ScriptedModel(["טיוטה שגויה", "ההכנסה היא אפס"])),
        Critic(ScriptedModel(['{"is_acceptable":false,"feedback":"Correct the amount"}',
                              '{"is_acceptable":true,"feedback":"Matches the fixture"}'])),
        capture, JsonlAudit(tmp_path),
    )
    output = asyncio.run(ChatPage(service).ask("מה ההכנסה?", []))
    assert output[0] == [{"role": "user", "content": "מה ההכנסה?"},
                         {"role": "assistant", "content": "ההכנסה היא אפס"}]
    events = [json.loads(line) for line in next(tmp_path.glob("*.jsonl")).read_text(encoding="utf-8").splitlines()]
    assert [event["event"] for event in events] == ["snapshot", "attempt", "attempt", "terminal"]
    assert events[1]["draft"] == "טיוטה שגויה"
    assert events[1]["is_acceptable"] is False
    assert events[-1]["status"] == "approved"


def test_three_hebrew_questions_through_running_gradio_api():
    from gradio_client import Client

    service = FakeService()
    app = build_app(service)
    try:
        _, url, _ = app.launch(server_name="127.0.0.1", share=False,
                               prevent_thread_lock=True, quiet=True)
        client = Client(url, verbose=False)
        for index, question in enumerate(("מה ההכנסה?", "ומה לגבי מוצר 5?", "מהן עלויות המשלוח?")):
            result = client.predict(question, api_name="/ask")
            assert len(result[0]) == (index + 1) * 2
            assert result[1] == "אושר"
            assert result[2] == [{"table": "marts.fct_orders", "unit": "ILS"}]
            assert "fixture-snapshot" in result[3]
        assert [len(history) for _, history in service.calls] == [0, 2, 4]
        cleared = client.predict(api_name="/clear")
        assert cleared[0] == []
        client.predict("שאלה אחרי ניקוי", api_name="/ask")
        assert service.calls[-1][1] == []
        other_session = Client(url, verbose=False)
        other_session.predict("שאלה משיחה נפרדת", api_name="/ask")
        assert service.calls[-1][1] == []
    finally:
        app.close()
