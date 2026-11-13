import asyncio
from contextlib import closing
from datetime import date
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import duckdb
import pytest

from chat.service import ChatService
from data_access.capture import make_capture


def test_capture_freezes_evidence_during_retry_and_refreshes(tmp_path):
    batch = tmp_path / "batch.duckdb"
    with duckdb.connect(str(batch)) as db:
        db.execute("""
            CREATE SCHEMA marts; CREATE SCHEMA staging;
            CREATE TABLE marts.fct_orders (product_id INTEGER, order_date DATE, status VARCHAR, amount DECIMAL(10,2));
            INSERT INTO marts.fct_orders VALUES (5, '2026-01-01', 'paid', 10);
            CREATE TABLE staging.stg_reviews (review_id INTEGER, product_id INTEGER, rating INTEGER, review_text VARCHAR, created_at TIMESTAMPTZ);
            CREATE TABLE marts.mart_product_reviews (product_id INTEGER, product_name VARCHAR, review_count BIGINT, avg_rating DOUBLE, negative_reviews BIGINT, paid_orders BIGINT, revenue DECIMAL(10,2));
            INSERT INTO marts.mart_product_reviews VALUES (5, 'item', 0, NULL, 0, 1, 10);
        """)
    stream = tmp_path / "stream.db"
    with closing(sqlite3.connect(stream)) as db, db:
        db.executescript("""
            CREATE TABLE stream_meta(singleton, checkpoint, watermark_ms, config_json, finalized);
            INSERT INTO stream_meta VALUES(1, 0, NULL, '{"window_seconds":60}', 0);
            CREATE TABLE counters(name, value);
            CREATE TABLE order_state(window_start_ms, orders, revenue_cents);
            CREATE TABLE realtime_revenue(window_start_ms, window_end_ms, orders, revenue_cents, emitted_at_watermark_ms, closure_reason);
            CREATE TABLE review_state(window_start_ms, product_id, reviews, negative_reviews);
            CREATE TABLE review_windows(window_start_ms, window_end_ms, product_id, reviews, negative_reviews, emitted_at_watermark_ms, closure_reason);
            CREATE TABLE review_alerts(product_id, window_start_ms, negative_reviews, trigger_event_id, emitted_at_watermark_ms);
        """)
    capture = make_capture(batch, stream, start=date(2026, 1, 1), end=date(2027, 1, 1))
    observed = []

    async def draft(question, evidence, *args):
        observed.append(evidence)
        if len(observed) == 1:
            with closing(sqlite3.connect(stream)) as db, db:
                db.execute("UPDATE stream_meta SET checkpoint=1, watermark_ms=1000")
                db.execute("INSERT INTO order_state VALUES(0, 1, 500)")
        return "draft"

    decisions = iter([False, True, True])

    async def evaluate(question, evidence, draft):
        observed.append(evidence)
        return SimpleNamespace(is_acceptable=next(decisions), feedback="ok")

    service = ChatService(SimpleNamespace(draft=draft), SimpleNamespace(evaluate=evaluate), capture, lambda _: None)

    async def run():
        first = await service.answer("first", [])
        second = await service.answer("second", first.history)
        return first, second

    first, second = asyncio.run(run())
    assert first.status == second.status == "approved"
    assert len(set(observed[:4])) == 1
    assert first.snapshot_id != second.snapshot_id
    old, new = json.loads(observed[0]), json.loads(observed[4])
    assert next(s for s in old["sources"] if s["table"] == "stream_meta")["checkpoint"] == 0
    assert next(s for s in new["sources"] if s["table"] == "order_state")["checkpoint"] == 1
    assert next(s for s in new["sources"] if s["table"] == "marts.fct_orders")["rows"][0]["paid_revenue"] == "10.00"

    stream.unlink()
    result = asyncio.run(service.answer("missing", []))
    assert result.status == "error"
