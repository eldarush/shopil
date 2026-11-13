"""SHOPIL-36 conversation contracts with SQLite fixtures and a scripted model.

These tests verify actual UI/service/model message plumbing, fresh queries and
missing-value preservation. The fixture's tiny product-reference resolver is not
production retrieval, and the scripted model is not evidence of LLM quality.
Run with ``uv run pytest tests/test_conversation_cases.py``; no credentials needed.
"""

import asyncio
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import json
import re
import sqlite3
from uuid import uuid4

import pytest

from chat.actor import Actor
from chat.app import ChatPage
from chat.critic import Critic
from chat.service import ChatService


class SqlEvidenceFixture:
    def __init__(self, path):
        self.path = path
        self.calls = []
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("CREATE TABLE product_metrics (product_id INTEGER PRIMARY KEY, "
                               "paid_revenue_ils TEXT, review_count INTEGER, average_rating REAL)")
            connection.executemany("INSERT INTO product_metrics VALUES (?, ?, ?, ?)",
                                   [(5, "12.50", 2, 4.5), (8, "0.00", 0, None)])
            connection.commit()

    async def capture(self, question, history):
        self.calls.append((question, deepcopy(history)))
        questions = [question] + [item["content"] for item in reversed(history) if item["role"] == "user"]
        match = next((match for text in questions if (match := re.search(r"מוצר (\d+)", text))), None)
        if match is None:
            raise ValueError("This fixture needs a product reference in the question or approved history")
        product = int(match[1])
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM product_metrics WHERE product_id = ?", (product,)
            )]
        return {"snapshot_id": uuid4().hex, "captured_at": datetime.now(timezone.utc).isoformat(),
                "question": question, "product_id": product,
                "sources": [{"table": "product_metrics", "units": "ILS, count, rating 1-5",
                             "time_range": "test fixture snapshot"}], "rows": rows}


def fixture_answer(question, evidence):
    """Deterministic expected wording for this test fixture, not a real Actor."""
    source = f"לפי product_metrics, צילום מצב {evidence['snapshot_id']}"
    if not evidence["rows"]:
        return f"{source}: אין נתונים על מוצר {evidence['product_id']}."
    row = evidence["rows"][0]
    if "דירוג" in question:
        if row["average_rating"] is None:
            return f"{source}: ממוצע הדירוג של מוצר {row['product_id']} חסר; אין ביקורות, ולא ניתן להסיק דירוג אפס."
        return f"{source}: דירוג מוצר {row['product_id']} הוא {row['average_rating']} מתוך 5."
    return f"{source}: ההכנסה מהזמנות ששולמו עבור מוצר {row['product_id']} היא {row['paid_revenue_ils']} ש״ח."


class InspectingScriptedModel:
    def __init__(self, *, bad_missing_draft=False):
        self.bad_missing_draft = bad_missing_draft
        self.actor_calls = []
        self.critic_calls = []

    async def complete(self, messages, *, schema=None):
        if schema is None:
            question = messages[2]["content"]
            evidence_json = messages[3]["content"]
            history = json.loads(messages[1]["content"])["approved_history"]
            correction = json.loads(messages[4]["content"]) if len(messages) == 5 else None
            self.actor_calls.append((question, evidence_json, history, correction))
            if self.bad_missing_draft and len(self.actor_calls) == 1:
                return "ממוצע הדירוג הוא 0."
        else:
            assert schema["required"] == ["is_acceptable", "feedback"]
            question = json.loads(messages[1]["content"])["question"]
            evidence_json = messages[2]["content"]
            draft = json.loads(messages[3]["content"])["candidate_answer"]
            expected = fixture_answer(question, json.loads(evidence_json))
            self.critic_calls.append((question, evidence_json, draft))
            return json.dumps({"is_acceptable": draft == expected,
                               "feedback": "הממוצע חסר בראיות; אין להחליף אותו באפס."
                               if draft != expected else "התשובה תואמת לערך ולמקור בראיות."}, ensure_ascii=False)
        return fixture_answer(question, json.loads(evidence_json))


def test_supported_question_and_context_dependent_followup_refresh_database(tmp_path):
    evidence = SqlEvidenceFixture(tmp_path / "metrics.db")
    model = InspectingScriptedModel()
    audit = []
    page = ChatPage(ChatService(Actor(model), Critic(model), evidence.capture, audit.append))

    async def conversation():
        first = await page.ask("מה ההכנסה ממוצר 5?", [])
        with closing(sqlite3.connect(evidence.path)) as connection:
            connection.execute("UPDATE product_metrics SET paid_revenue_ils = '22.50' WHERE product_id = 5")
            connection.commit()
        return first, await page.ask("ומה ההכנסה שלו עכשיו?", first[1])

    first, second = asyncio.run(conversation())
    assert len(evidence.calls) == len(model.actor_calls) == len(model.critic_calls) == 2
    assert evidence.calls[0][1] == []
    assert evidence.calls[1][1] == first[1] == model.actor_calls[1][2]
    assert "מוצר 5" in first[1][0]["content"]
    assert "מוצר 5" not in evidence.calls[1][0]  # This turn depends on approved context.
    assert "12.50" in first[0][-1]["content"]
    assert "22.50" in second[0][-1]["content"]
    assert "12.50" not in second[0][-1]["content"]
    assert len(first[1]) == 2 and len(second[1]) == 4
    assert first[4] != second[4] and first[3] == second[3]
    snapshots = [json.loads(call[1]) for call in model.actor_calls]
    assert [snapshot["product_id"] for snapshot in snapshots] == [5, 5]
    assert [snapshot["rows"][0]["paid_revenue_ils"] for snapshot in snapshots] == ["12.50", "22.50"]
    assert [call[1] for call in model.actor_calls] == [call[1] for call in model.critic_calls]
    assert [event["evidence_json"] for event in audit if event["event"] == "snapshot"] == [
        call[1] for call in model.actor_calls
    ]


@pytest.mark.parametrize("question,expected_value,expected_phrase", [
    ("מה ההכנסה ממוצר 8?", "0.00", "0.00 ש״ח"),
    ("מה הדירוג של מוצר 8?", None, "ממוצע הדירוג של מוצר 8 חסר"),
    ("מה ההכנסה ממוצר 999?", "absent_row", "אין נתונים על מוצר 999"),
])
def test_zero_null_and_missing_product_remain_distinct(tmp_path, question, expected_value, expected_phrase):
    evidence = SqlEvidenceFixture(tmp_path / "metrics.db")
    model = InspectingScriptedModel(bad_missing_draft=expected_value is None)
    audit = []
    page = ChatPage(ChatService(Actor(model), Critic(model), evidence.capture, audit.append))
    result = asyncio.run(page.ask(question, []))
    assert expected_phrase in result[0][-1]["content"]
    assert len(evidence.calls) == 1
    snapshot = json.loads(model.actor_calls[0][1])
    if expected_value == "absent_row":
        assert snapshot["rows"] == []
        assert "0.00" not in result[0][-1]["content"]
    else:
        assert snapshot["rows"][0]["paid_revenue_ils"] == "0.00"
        assert snapshot["rows"][0]["review_count"] == 0
        assert snapshot["rows"][0]["average_rating"] is None
    assert all(call[1] == model.actor_calls[0][1] for call in model.actor_calls + model.critic_calls)
    assert result[3] == snapshot["sources"] and snapshot["snapshot_id"] in result[4]
    assert len(result[1]) == 2
    assert audit[-1]["status"] == "approved"
    if expected_value is None:
        attempts = [event for event in audit if event["event"] == "attempt"]
        assert [event["is_acceptable"] for event in attempts] == [False, True]
        assert model.actor_calls[1][3] == {
            "previous_draft": "ממוצע הדירוג הוא 0.",
            "feedback": "הממוצע חסר בראיות; אין להחליף אותו באפס.",
        }
        assert all(item["content"] != "ממוצע הדירוג הוא 0." for item in result[1])
