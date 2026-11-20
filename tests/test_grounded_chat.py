"""Regression tests for the default chat's intent, retrieval, and authored prose."""

import asyncio
from datetime import date
import json
from types import SimpleNamespace

import duckdb
import pytest

from chat.capture import build_capture
from chat.grounded import GroundedActor
from chat.model import ModelError
from chat.query_scope import QueryScope, resolve_query

from test_capture import database, warehouse


@pytest.mark.parametrize("question", [
    "מה ההכנסה מהזמנות ששולמו?", "כמה הכנסנו מהזמנות ששולמו?",
    "מהי ההכנסה מהזמנות ששולמו?", "כמה כסף נכנס מהזמנות ששולמו?",
])
def test_revenue_paraphrases(question):
    assert resolve_query(question, []) == QueryScope("paid_revenue")


def test_resolves_period_and_product_followup():
    question = "מהי ההכנסה מהזמנות ששולמו עבור מוצר 5 בין 2026-07-01 ל-2026-07-03?"
    old = resolve_query(question, [])
    assert old == QueryScope("paid_revenue", 5, start=date(2026, 7, 1), end=date(2026, 7, 3))
    history = [{"role": "user", "content": question},
               {"role": "assistant", "content": "התוצאה הישנה היתה 999."}]
    actual = resolve_query("ומה עם מוצר 6 באותה תקופה?", history)
    assert actual == QueryScope("paid_revenue", 6, start=old.start, end=old.end)


@pytest.fixture
def capture(tmp_path):
    historical = warehouse(tmp_path)
    with duckdb.connect(str(historical)) as db:
        db.execute("CREATE TABLE staging.stg_products AS SELECT product_id, product_name FROM marts.mart_product_reviews")
    return build_capture(historical, database(tmp_path), start=date(2026, 7, 1),
                         end=date(2026, 7, 4), question_scoped=True)


def test_scoped_history_excludes_boundary_other_products_and_refunds(capture):
    question = "מהי ההכנסה מהזמנות ששולמו עבור מוצר 5 בין 2026-07-01 ל-2026-07-03?"
    payload = asyncio.run(capture(question, []))
    source, = payload["sources"]
    assert source["rows"] == [{"product_id": 5, "product_name": "Product 5", "paid_revenue": "15.35"}]
    assert source["table"] == "marts.fct_orders"
    assert source["time_start"] == "2026-07-01" and source["time_end"] == "2026-07-03"
    assert "stream" not in payload


def test_scoped_capture_distinguishes_zero_null_and_missing_product(capture):
    zero = asyncio.run(capture("כמה הזמנות שולמו עבור מוצר 6?", []))
    null = asyncio.run(capture("מה הדירוג הממוצע של מוצר 6?", []))
    missing = asyncio.run(capture("כמה הזמנות שולמו עבור מוצר 99?", []))
    assert zero["query_scope"]["availability"] == "available"
    assert zero["sources"][0]["rows"][0]["paid_orders"] == 0
    assert null["query_scope"]["availability"] == "missing"
    assert null["sources"][0]["rows"][0]["avg_rating"] is None
    assert missing["query_scope"]["availability"] == "missing"
    assert missing["sources"][0]["rows"] == []


def test_scoped_stream_chooses_state_and_product_before_reading_evidence(capture):
    payload = asyncio.run(capture("כמה ביקורות שליליות התקבלו בזרם למוצר5 בחלון הפתוח האחרון?", []))
    assert "stream" in payload
    source, = payload["sources"]
    assert source["table"] == "review_state"
    row, = source["rows"]
    assert row["product_id"] == 5 and row["negative_reviews"] == 4
    assert source["checkpoint"] == 10 and payload["query_scope"]["window_state"] == "open"
    alert = asyncio.run(capture("כמה ביקורות שליליות היו בעת ההתראה למוצר5 בחלון הפתוח האחרון?", []))
    alert_source, = alert["sources"]
    assert alert_source["table"] == "review_alerts"
    assert alert_source["rows"][0]["negative_reviews_at_trigger"] == 3
    closed = asyncio.run(capture("מה סכום אירועי ההזמנה בחלון הסגור האחרון?", []))
    assert closed["sources"][0]["rows"][0]["revenue_ils"] == "123.45"


def test_actor_authors_prose_but_cannot_supply_its_own_number(capture):
    question = "כמה הזמנות שולמו?"
    snapshot = json.dumps(asyncio.run(capture(question, [])))
    async def complete(messages, *, schema):
        assert messages[2]["content"] == snapshot
        assert "enum" not in schema["properties"]["answer"]
        return json.dumps({"answer": "בסך הכול נספרו {{value}} הזמנות ששולמו בתקופה הזו."})

    answer = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(question, snapshot, []))
    assert answer.startswith("בסך הכול נספרו 3 הזמנות")
    assert "marts.fct_orders" in answer and "2026-07-04 (לא כולל)" in answer

    async def ungrounded(messages, *, schema):
        return json.dumps({"answer": "שולמו {{value}} הזמנות בסכום 123."})

    with pytest.raises(ModelError, match="grounded explanation"):
        asyncio.run(GroundedActor(SimpleNamespace(complete=ungrounded)).draft(question, snapshot, []))
