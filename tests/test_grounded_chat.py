"""Regression tests for the default chat's intent, retrieval, and authored prose."""

import asyncio
from datetime import date
import json
import re
import sqlite3
from types import SimpleNamespace

import duckdb
import pytest

from chat.capture import build_capture
from chat.grounded import GroundedActor, candidate_feedback, source_lines
from chat.model import ModelError
from chat.query_scope import QueryScope, resolve_query

from test_capture import database, warehouse


@pytest.mark.parametrize("question", [
    "מה ההכנסה מהזמנות ששולמו?", "כמה הכנסנו מהזמנות ששולמו?",
    "מהי ההכנסה מהזמנות ששולמו?", "כמה כסף נכנס מהזמנות ששולמו?",
    "מה ההכנסה מההזמנות ששולמו?", "אפשר לדעת מה ההכנסה מההזמנות ששולמו?",
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
    alert_source = next(source for source in alert["sources"] if source["table"] == "review_alerts")
    assert alert_source["table"] == "review_alerts"
    assert alert_source["rows"][0]["negative_reviews_at_trigger"] == 3
    closed = asyncio.run(capture("מה סכום אירועי ההזמנה בחלון הסגור האחרון?", []))
    assert closed["sources"][0]["rows"][0]["revenue_ils"] == "123.45"


def test_missing_alert_retains_the_matching_window(tmp_path):
    stream = database(tmp_path)
    with sqlite3.connect(stream) as conn:
        conn.execute("DELETE FROM review_alerts")
        conn.execute("UPDATE review_state SET reviews=1, negative_reviews=1 WHERE product_id=5")
    capture = build_capture(tmp_path / "unused.duckdb", stream, start=date(2026, 7, 1),
                            end=date(2026, 7, 4), question_scoped=True)
    q = "כמה ביקורות שליליות היו בעת ההתראה למוצר 5 בחלון הפתוח האחרון?"
    evidence = asyncio.run(capture(q, []))
    assert evidence["query_scope"]["availability"] == "missing"
    assert evidence["query_scope"]["reason"] == "no_alert_in_matching_window"
    [window] = evidence["sources"]
    assert window["table"] == "review_state" and window["rows"][0]["negative_reviews"] == 1
    assert window["time_start"] in source_lines(evidence)


def test_stream_amount_guard_rejects_payment_status_and_wrong_finality():
    from chat.grounded import evidence_error

    evidence = {"query_scope": {"availability": "available", "field": "revenue_ils", "window_state": "open"},
                "sources": [{"rows": [{"revenue_ils": "10.00"}]}], "stream": {"checkpoint": 4}}
    assert not evidence_error("סכום אירועי ההזמנה בחלון הפתוח הוא 10 ₪.", evidence)
    assert evidence_error("ההכנסה מהזמנות ששולמו בחלון הפתוח היא 10 ₪.", evidence)
    assert evidence_error("סכום אירועי ההזמנה בחלון הסגור הוא 10 ₪.", evidence)


def test_stream_count_revision_explains_the_correct_singular_grammar():
    from chat.grounded import evidence_error

    evidence = {"query_scope": {"availability": "available", "field": "orders", "window_state": "open"},
                "sources": [{"rows": [{"orders": 1}]}], "stream": {"checkpoint": 0}}
    assert "נקלט אירוע הזמנה אחד" in evidence_error("נקלט אחד אירועי הזמנה.", evidence)
    assert not evidence_error("בחלון נקלט אירוע אחד של הזמנה.", evidence)
    evidence["sources"][0]["rows"][0]["orders"] = 3
    assert evidence_error("בחלון הפתוח התקבלו 3 ביקורות.", evidence)
    assert not evidence_error("בחלון הפתוח נקלטו 3 אירועי הזמנה.", evidence)


def test_stream_actor_uses_the_frozen_count_as_digits(capture):
    q = "כמה אירועי הזמנה נקלטו בחלון הסגור האחרון?"
    evidence = asyncio.run(capture(q, []))
    evidence["sources"][0]["rows"][0]["orders"] = 2
    correct = "בחלון הסגור נקלטו 2 אירועי הזמנה."

    async def complete(messages, *, schema):
        assert all("enum" not in schema["properties"][name] for name in ("answer", "citation"))
        return json.dumps({"answer": correct, "citation": source_lines(evidence)})

    answer = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(q, json.dumps(evidence), []))
    assert not candidate_feedback(answer, evidence)


def test_actor_owns_numbers_and_numeric_guard_checks_only_authored_prose(capture):
    from chat.grounded import evidence_error

    question = "כמה הזמנות שולמו?"
    evidence = asyncio.run(capture(question, []))
    snapshot = json.dumps(evidence)
    async def complete(messages, *, schema):
        assert messages[2]["content"] == snapshot
        return json.dumps({"answer": "בתקופה זו שולמו 3 הזמנות.", "citation": source_lines(evidence)})

    answer = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(question, snapshot, []))
    assert answer.startswith("בתקופה זו שולמו 3 הזמנות.")
    assert "marts.fct_orders" in answer and "2026-07-04 (לא כולל)" in answer
    assert evidence_error(answer.split("\n\n")[0], evidence) == ""
    assert evidence_error("בתקופה זו שולמו 300 הזמנות.", evidence)


def test_known_coverage_and_profit_are_not_zero_or_revenue(capture):
    for question in ("כמה הרווחנו מהזמנות ששולמו?", "מהן עלויות המשלוח?"):
        result = asyncio.run(capture(question, []))
        assert result["query_scope"]["availability"] == "missing"
        assert result["query_scope"]["time_start"] is None
        assert result["query_scope"]["consulted_table"] == "marts.fct_orders"
        assert result["sources"] == []
    result = asyncio.run(capture("כמה הזמנות שולמו בשנת 1990?", []))
    assert result["query_scope"]["availability"] == "missing"
    assert not result["sources"][0]["rows"]


def test_ordinary_paraphrases():
    assert resolve_query("כמה שילמו לנו על ההזמנות?", []).metric == "paid_revenue"
    assert resolve_query("מה היו ההכנסות מהזמנות ששולמו?", []).metric == "paid_revenue"
    current = resolve_query("כמה הזמנות שולמו השנה?", [])
    assert current.metric == "paid_orders"
    assert current.start == date(date.today().year, 1, 1)

def test_publication_check_catches_wrong_prose_without_rewriting_critic_verdict(capture):
    from chat.critic import Critic

    question = "כמה הזמנות שולמו?"
    evidence = json.dumps(asyncio.run(capture(question, [])), ensure_ascii=False)
    async def approve(messages, *, schema):
        assert messages[2]["content"] == evidence
        return json.dumps({"is_acceptable": True, "feedback": "נבדק."})

    critic = Critic(SimpleNamespace(complete=approve))
    for wrong in ("שולמו 99 הזמנות.", "ההכנסה היא 3 ₪.", "שולמו 3 הזמנות הזמנות."):
        candidate = wrong + "\n\n" + source_lines(json.loads(evidence))
        checked = asyncio.run(critic.evaluate(question, evidence, candidate))
        assert checked.is_acceptable and checked.feedback == "נבדק."
        assert candidate_feedback(candidate, json.loads(evidence))

@pytest.mark.parametrize("wording", ["כמה כסף הכניסה החנות מההזמנות ששולמו?", "מה ההכנסה הכוללת מהזמנות ששולמו?"])
def test_business_income_wording(wording):
    assert resolve_query(wording, []) == QueryScope("paid_revenue")


def test_observed_source_bounds_protect_subranges_and_full_period(tmp_path):
    from chat.grounded import source_lines
    db = warehouse(tmp_path)
    capture = build_capture(db, database(tmp_path), start=date(2026, 1, 1), end=date(2027, 1, 1), question_scoped=True)
    missing = asyncio.run(capture("כמה הזמנות שולמו בין 2026-12-01 עד 2026-12-10?", []))
    assert missing["query_scope"]["availability"] == "missing" and not missing["sources"][0]["rows"]
    partial = asyncio.run(capture("כמה הזמנות שולמו בין 2026-07-02 עד 2026-07-20?", []))
    scope = partial["query_scope"]
    assert scope["availability"] == "available" and scope["partial_period"]
    [source] = partial["sources"]
    assert (source["time_start"], source["time_end"]) == ("2026-07-02", "2026-07-04")
    assert "2026-07-20" in source_lines(partial)
    year = asyncio.run(capture("כמה הזמנות שולמו בשנת 2026?", []))
    assert year["query_scope"]["availability"] == "available"
    assert "כיסוי הנתונים לתקופת הדיווח חלקי" in source_lines(year)
    assert year["sources"][0]["rows"][0]["paid_orders"] == 3
    with duckdb.connect(str(db)) as conn:
        conn.execute("DELETE FROM marts.fct_orders")
    empty = asyncio.run(capture("כמה הזמנות שולמו?", []))
    assert empty["query_scope"]["availability"] == "missing"
    assert empty["query_scope"]["observed_start"] is None
    assert empty["sources"][0]["rows"] == []

def test_profit_cannot_reuse_history_revenue_or_invent_shipping_specificity(capture):
    from chat.grounded import evidence_error

    evidence = asyncio.run(capture("כמה הרווחנו מהזמנות ששולמו?", []))
    assert evidence_error("ההכנסה הייתה 97210.10 ₪, אך אי אפשר לחשב רווח.", evidence)
    assert evidence_error("הרווח אינו ידוע כי נתוני עלויות המשלוח חסרים.", evidence)
    assert not evidence_error("אי אפשר לחשב את הרווח משום שנתוני העלויות חסרים.", evidence)
    year = asyncio.run(capture("כמה הזמנות שולמו בשנת 1990?", []))
    assert evidence_error("אין נתוני הזמנות לשנת 1990.", year)


def test_profit_retains_requested_scope_without_claiming_known_revenue(capture):
    from chat.grounded import evidence_error

    evidence = asyncio.run(capture("כמה הרווחנו עבור מוצר 99 בשנת 1990?", []))
    scope = evidence["query_scope"]
    assert scope["availability"] == "missing"
    assert scope["product_id"] == 99 and scope["requested_start"] == "1990-01-01"
    assert "הכנסה" not in scope["reason"]
    assert "המוצר שהתבקש: 99" in source_lines(evidence) and "1990-01-01" in source_lines(evidence)
    assert evidence_error("ההכנסה ידועה אבל חסרים נתוני עלויות לחישוב הרווח.", evidence)


def test_missing_profit_with_invalid_warehouse_returns_audited_error(tmp_path):
    from chat.service import ChatService

    corrupt = tmp_path / "warehouse.duckdb"
    corrupt.write_text("invalid warehouse")
    capture = build_capture(corrupt, tmp_path / "unused.db", start=date(2026, 1, 1),
                            end=date(2027, 1, 1), question_scoped=True)
    audit = []
    service = ChatService(None, None, capture, audit.append)
    result = asyncio.run(service.answer("כמה הרווחנו?", []))
    assert result.status == "error" and result.history == []
    assert audit[-1]["error_stage"] == "capture" and audit[-1]["error_type"] == "ModelError"


@pytest.mark.parametrize(("field", "valid", "wrong"), [
    ("avg_rating", "הדירוג הממוצע של מוצר 5 הוא 4.", "למוצר 5 יש 4 ביקורות."),
    ("review_count", "למוצר 5 יש 4 ביקורות.", "למוצר 5 יש 4 ביקורות שליליות."),
    ("negative_reviews", "למוצר 5 יש 4 ביקורות שליליות.", "הדירוג הממוצע של מוצר 5 הוא 4."),
    ("negative_reviews_at_trigger", "בעת ההתראה היו 4 ביקורות שליליות למוצר 5.", "למוצר 5 יש 4 ביקורות."),
])
def test_review_value_is_valid_only_for_the_requested_metric(field, valid, wrong):
    from chat.grounded import evidence_error

    evidence = {"query_scope": {"availability": "available", "field": field},
                "sources": [{"rows": [{"product_id": 5, field: 4}]}]}
    assert not evidence_error(valid, evidence)
    assert evidence_error(wrong, evidence)


def test_ranking_requires_all_tied_products(capture):
    from chat.grounded import evidence_error

    evidence = asyncio.run(capture("לאיזה מוצר יש הכי הרבה ביקורות שליליות?", []))
    evidence["sources"][0]["rows"] = [
        {"product_id": 5, "product_name": "בקבוק", "negative_reviews": 4},
        {"product_id": 6, "product_name": "ספל", "negative_reviews": 4},
    ]
    assert evidence_error("מוצר 5 קיבל 4 ביקורות שליליות.", evidence)
    assert not evidence_error("מוצר 5 ומוצר 6 קיבלו 4 ביקורות שליליות כל אחד.", evidence)
    assert not evidence_error("לבקבוק ולספל 4 ביקורות שליליות כל אחד.", evidence)

    assert not evidence_error("לבקבוק (מזהה 5) ולספל (מזהה 6) יש 4 ביקורות שליליות כל אחד.", evidence)


def test_actor_can_revise_its_citation(capture):
    from chat.grounded import source_lines
    question = "כמה הזמנות שולמו?"
    evidence = asyncio.run(capture(question, []))
    text = json.dumps(evidence)
    paragraph = "שולמו 3 הזמנות."
    for cited, good in [(source_lines(evidence), True), ("מקור: unrelated_source", False)]:
        async def complete(messages, *, schema):
            return json.dumps({"answer": paragraph, "citation": cited})
        draft = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(question, text, []))
        assert draft == paragraph + "\n\n" + cited
        assert bool(candidate_feedback(draft, evidence)) != good

def test_actor_authors_missing_metric_without_prepared_wording(capture):
    question = "כמה הרווחנו מהזמנות ששולמו?"
    evidence = json.dumps(asyncio.run(capture(question, [])), ensure_ascii=False)
    authored = "אין די נתונים על עלויות החנות לחישוב הרווח."

    async def complete(messages, *, schema):
        assert messages[2]["content"] is evidence
        assert "ספרות" in messages[0]["content"]
        assert json.loads(messages[-1]["content"]) == {
            "question": question, "previous_draft": None, "feedback": None
        }
        assert all("enum" not in schema["properties"][field] for field in ("answer", "citation"))
        assert authored not in " ".join(msg["content"] for msg in messages)
        return json.dumps({"answer": authored, "citation": source_lines(json.loads(evidence))})

    answer = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(question, evidence, []))
    assert answer.startswith(authored)
    assert "marts.fct_orders" in answer


def test_missing_period_shows_the_requested_dates_only_in_the_citation(capture):
    q = "כמה הזמנות שולמו בשנת 1990?"
    evidence = asyncio.run(capture(q, []))

    async def complete(messages, *, schema):
        assert all("enum" not in schema["properties"][field] for field in ("answer", "citation"))
        return json.dumps({"answer": "מספר ההזמנות ששולמו אינו זמין לתקופה המבוקשת.",
                           "citation": source_lines(evidence)})

    draft = asyncio.run(GroundedActor(SimpleNamespace(complete=complete)).draft(q, json.dumps(evidence), []))
    assert "1990" not in draft.split("\n\n")[0]
    assert "1990-01-01" in draft and not candidate_feedback(draft, evidence)


@pytest.mark.parametrize("garbled", ["קיבл", "ה忚נסה", "נגمר", "ההכנסa"])
def test_foreign_script_inside_hebrew_is_rejected(capture, garbled):
    from chat.grounded import evidence_error
    evidence = asyncio.run(capture("כמה הזמנות שולמו?", []))
    assert evidence_error(f"בתקופה זו שולמו 3 הזמנות {garbled}.", evidence)

def test_product_identifier_cannot_substitute_for_the_measured_value(capture):
    from chat.grounded import evidence_error
    q = "כמה הכנסנו מהזמנות ששולמו עבור מוצר 5?"
    evidence = asyncio.run(capture(q, []))
    assert evidence_error("ההכנסה ממוצר 115 הסתכמה ב־5 ₪.", evidence)
    assert not evidence_error("ההכנסה ממוצר 5 הסתכמה ב־115.35 ₪.", evidence)


def test_audit_detects_extra_currency_and_wrong_product_scope(capture):
    from chat.grounded import evidence_error

    stream = asyncio.run(capture("כמה אירועי הזמנה נקלטו בחלון הפתוח האחרון?", []))
    value = stream["sources"][0]["rows"][0]["orders"]
    assert evidence_error(f"בחלון הפתוח נקלטו {value} אירועי הזמנה בשווי {value} ₪.", stream)
    product = asyncio.run(capture("כמה הכנסנו מהזמנות ששולמו עבור מוצר 5?", []))
    assert evidence_error("ההכנסה מהזמנות ששולמו לכלל המוצרים הסתכמה ב־115.35 ₪.", product)
