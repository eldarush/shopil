import asyncio
from contextlib import closing
from datetime import UTC, date, datetime
import json
import sqlite3
from threading import get_ident

import duckdb
import pytest

from chat import capture
from chat.model import ModelError
from data_access.evidence import EvidencePackage


def database(tmp_path, *, empty=False):
    """Real SQLite fixture using the streaming engine's persisted columns."""
    path = tmp_path / "stream.db"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("""
            CREATE TABLE stream_meta (singleton, checkpoint, watermark_ms, config_json, finalized);
            CREATE TABLE counters (name, value);
            CREATE TABLE order_state (window_start_ms, orders, revenue_cents);
            CREATE TABLE realtime_revenue (window_start_ms, window_end_ms, orders, revenue_cents,
                                           emitted_at_watermark_ms, closure_reason);
            CREATE TABLE review_state (window_start_ms, product_id, reviews, negative_reviews);
            CREATE TABLE review_windows (window_start_ms, window_end_ms, product_id, reviews,
                                         negative_reviews, emitted_at_watermark_ms, closure_reason);
            CREATE TABLE review_alerts (product_id, window_start_ms, negative_reviews,
                                        trigger_event_id, emitted_at_watermark_ms);
        """)
        config = json.dumps({"window_seconds": 60, "grace_seconds": 10,
                             "out_of_order_seconds": 0, "alert_threshold": 3})
        connection.execute("INSERT INTO stream_meta VALUES (1, ?, ?, ?, 0)",
                           (-1 if empty else 10, None if empty else 125000, config))
        connection.execute("INSERT INTO counters VALUES ('accepted_reviews', ?)", (0 if empty else 9,))
        if not empty:
            connection.executescript("""
                INSERT INTO realtime_revenue VALUES (-60000, 0, 1, 99999, 70000, 'watermark');
                INSERT INTO realtime_revenue VALUES (0, 60000, 2, 12345, 70000, 'watermark');
                INSERT INTO order_state VALUES (60000, 1, 5), (120000, 1, 0);
                INSERT INTO review_windows VALUES (0, 60000, 5, 4, 4, 70000, 'watermark');
                INSERT INTO review_windows VALUES (0, 60000, 9, 1, 0, 70000, 'watermark');
                INSERT INTO review_state VALUES (60000, 5, 4, 4), (60000, 9, 1, 0);
                INSERT INTO review_alerts VALUES (5, 0, 3, 'review-third-closed', 20000);
                INSERT INTO review_alerts VALUES (5, 60000, 3, 'review-third-open', 80000);
            """)
        connection.commit()
    return path


def source(result, table, state):
    return next(item.model_dump(mode="json") for item in result["sources"]
                if item.table == table and item.window_state == state)


def test_exact_scoped_sources_units_and_trigger_semantics(tmp_path):
    result = asyncio.run(capture.capture_stream(database(tmp_path)))
    assert len(result["sources"]) == 6
    stream = result["stream"]
    assert stream["checkpoint"] == 10 and stream["watermark"] == "1970-01-01T00:02:05+00:00"
    assert stream["config"]["grace_seconds"] == 10 and stream["finalized"] is False
    assert stream["omitted_window_count"] == 1
    assert len(stream["selected_window_starts"]) == 3
    assert "all-time" in stream["counters_scope"] and stream["counters"] == {"accepted_reviews": 9}
    closed = source(result, "realtime_revenue", "closed")
    assert [row["revenue_cents"] for row in closed["rows"]] == [12345]
    assert closed["rows"][0]["revenue_ils"] == "123.45"
    assert closed["rows"][0]["closure_reason"] == "watermark"
    assert closed["time_start"] == "1970-01-01T00:00:00Z"
    assert closed["time_end"] == "1970-01-01T00:01:00Z"
    assert closed["identifiers"] == ["window_start_ms"]
    assert "no payment status" in closed["metric"] and "latest 3" in closed["metric"]
    assert "integer agorot" in closed["units"]
    open_orders = source(result, "order_state", "open")
    assert [row["revenue_ils"] for row in open_orders["rows"]] == ["0.05", "0.00"]
    assert open_orders["rows"][-1]["window_end_ms"] == 180000
    for state, table in (("open", "review_state"), ("closed", "review_windows")):
        reviews = source(result, table, state)
        assert [row["product_id"] for row in reviews["rows"]] == [5, 9]
        alert = source(result, "review_alerts", state)["rows"][0]
        assert alert["negative_reviews_at_trigger"] == 3
        assert "negative_reviews" not in alert
        assert reviews["rows"][0]["negative_reviews"] == 4
        assert alert["trigger_event_id"] == f"review-third-{state}"
    assert all(item.checkpoint == 10 and item.watermark.timestamp() == 125 for item in result["sources"])
    package = EvidencePackage(question="מה מצב הזרם?", sources=result["sources"])
    assert EvidencePackage.model_validate_json(package.model_dump_json()) == package


def test_real_engine_teaching_deliveries_interoperate_with_adapter(tmp_path):
    from experiments.chat_evaluation import teaching_evidence

    path = tmp_path / "teaching.db"
    # Runs the committed 17 deliveries, in source order, under isolated Python 3.13.
    expected = teaching_evidence(path)
    result = asyncio.run(capture.capture_stream(path, latest_windows=4))
    assert result["stream"]["checkpoint"] == 16
    assert result["stream"]["finalized"] is True
    assert result["stream"]["omitted_window_count"] == 0
    assert result["stream"]["watermark"] == expected["sources"][0]["watermark"]
    assert result["stream"]["config"] == expected["stream_config"]
    assert result["stream"]["counters"]["received"] == 17
    windows = source(result, "realtime_revenue", "closed")["rows"]
    assert [row["revenue_cents"] for row in windows] == [2500, 5000, 3500, 1200]
    assert sum(row["revenue_cents"] for row in windows) == 12200
    assert [row["closure_reason"] for row in windows] == ["watermark"] * 3 + ["demo_end_of_input"]
    assert windows[-1]["emitted_at_watermark"] == result["stream"]["watermark"]
    assert windows[-1]["window_end_ms"] > windows[-1]["emitted_at_watermark_ms"]
    assert all(item.window_state == "closed" for item in result["sources"])
    alerts = source(result, "review_alerts", "closed")["rows"]
    assert len(alerts) == 1
    alert = alerts[0]
    assert (alert["product_id"], alert["negative_reviews_at_trigger"], alert["trigger_event_id"]) == (5, 3, "review-3")
    reviews = source(result, "review_windows", "closed")["rows"]
    current = next(row for row in reviews if (row["product_id"], row["window_start_ms"]) ==
                   (alert["product_id"], alert["window_start_ms"]))
    assert current["negative_reviews"] == current["reviews"] == 4
    package = EvidencePackage(question="מה מצב חלונות הזרם?", sources=result["sources"])
    assert EvidencePackage.model_validate_json(package.model_dump_json()) == package


def test_each_capture_is_fresh_detached_and_reads_once_off_event_loop(tmp_path, monkeypatch):
    path = database(tmp_path)
    original = capture.read_stream_snapshot
    threads = []

    def observed(database):
        threads.append(get_ident())
        return original(database)

    monkeypatch.setattr(capture, "read_stream_snapshot", observed)

    async def run():
        before = await capture.capture_stream(path)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("UPDATE stream_meta SET checkpoint=11, watermark_ms=126000")
            connection.execute("UPDATE order_state SET revenue_cents=200 WHERE window_start_ms=120000")
            connection.commit()
        return before, await capture.capture_stream(path)

    before, after = asyncio.run(run())
    assert len(threads) == 2 and all(thread != get_ident() for thread in threads)
    assert before["stream"]["checkpoint"] == 10 and after["stream"]["checkpoint"] == 11
    assert source(before, "order_state", "open")["rows"][-1]["revenue_ils"] == "0.00"
    assert source(after, "order_state", "open")["rows"][-1]["revenue_ils"] == "2.00"


def test_empty_state_has_progress_but_no_fabricated_dates_or_zero_sources(tmp_path):
    result = asyncio.run(capture.capture_stream(database(tmp_path, empty=True)))
    assert result["sources"] == ()
    assert result["stream"]["checkpoint"] == -1
    assert result["stream"]["watermark"] is None
    assert result["stream"]["selected_window_starts"] == []
    assert result["stream"]["omitted_window_count"] == 0
    assert len(result["stream"]["empty_tables_in_scope"]) == 5


def test_missing_database_fails_closed_without_creation_or_path_leak(tmp_path):
    path = tmp_path / "sensitive-missing.db"
    with pytest.raises(ModelError, match="Unable to read valid streaming evidence") as caught:
        asyncio.run(capture.capture_stream(path))
    assert "sensitive" not in str(caught.value) and not path.exists()


def test_all_products_retained_or_entire_capture_fails_at_row_budget(tmp_path):
    path = database(tmp_path, empty=True)
    with closing(sqlite3.connect(path)) as connection:
        connection.executemany("INSERT INTO review_state VALUES (0, ?, 1, 0)",
                               [(product,) for product in range(1, capture.MAX_SOURCE_ROWS + 2)])
        connection.commit()
    with pytest.raises(ModelError, match="row limit"):
        asyncio.run(capture.capture_stream(path, latest_windows=1))


def test_finite_finalization_is_distinct_from_watermark_closure(tmp_path):
    path = database(tmp_path, empty=True)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE stream_meta SET checkpoint=0, watermark_ms=5000, finalized=1")
        connection.execute("INSERT INTO realtime_revenue VALUES (0, 60000, 1, 100, 5000, 'demo_end_of_input')")
        connection.commit()
    result = asyncio.run(capture.capture_stream(path))
    assert result["stream"]["finalized"] is True
    assert result["stream"]["watermark"] == "1970-01-01T00:00:05+00:00"
    row = source(result, "realtime_revenue", "closed")["rows"][0]
    assert row["closure_reason"] == "demo_end_of_input"
    assert row["emitted_at_watermark_ms"] == 5000
    assert row["window_end_ms"] == 60000


@pytest.mark.parametrize("corruption", [
    "DELETE FROM stream_meta",
    "UPDATE stream_meta SET config_json='private-invalid-json'",
    "DELETE FROM review_windows WHERE product_id=5",
])
def test_invalid_snapshot_fails_closed_with_safe_error(tmp_path, corruption):
    path = database(tmp_path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(corruption)
        connection.commit()
    with pytest.raises(ModelError) as caught:
        asyncio.run(capture.capture_stream(path))
    assert str(caught.value) == "Unable to read valid streaming evidence"


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_window_limit_rejected(limit, tmp_path):
    with pytest.raises(ValueError, match="positive integer"):
        asyncio.run(capture.capture_stream(tmp_path / "unused.db", latest_windows=limit))


def warehouse(tmp_path, *, empty=False):
    """Completed synthetic warehouse; every product remains in the full-load mart."""
    path = tmp_path / "historical.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("""
            CREATE SCHEMA marts;
            CREATE SCHEMA staging;
            CREATE TABLE marts.fct_orders (
                product_id INTEGER, order_date DATE, status VARCHAR, amount DECIMAL(10, 2)
            );
            CREATE TABLE staging.stg_reviews (
                review_id INTEGER, product_id INTEGER, rating INTEGER,
                review_text VARCHAR, created_at TIMESTAMPTZ
            );
            CREATE TABLE marts.mart_product_reviews (
                product_id INTEGER, product_name VARCHAR, review_count BIGINT,
                avg_rating DOUBLE, negative_reviews BIGINT, paid_orders BIGINT,
                revenue DECIMAL(10, 2)
            );
        """)
        connection.executemany("INSERT INTO marts.mart_product_reviews VALUES (?, ?, 0, NULL, 0, 0, 0)",
                               [(pid, f"Product {pid}") for pid in range(1, 9)])
        if not empty:
            connection.execute("""
                INSERT INTO marts.fct_orders VALUES
                    (5, '2026-07-01', 'paid', 10.10), (5, '2026-07-02', 'paid', 5.25),
                    (5, '2026-07-03', 'paid', 100), (5, '2026-07-01', 'refunded', 50);
                UPDATE marts.mart_product_reviews
                SET review_count=16, avg_rating=3, paid_orders=3, revenue=115.35 WHERE product_id=5;
            """)
            # Equal timestamps and reverse insertion also exercise the reader's review-ID ordering.
            connection.executemany("INSERT INTO staging.stg_reviews VALUES (?, 5, 3, ?, ?)",
                                   [(rid, f"Review {rid}", datetime(2026, 7, 1, tzinfo=UTC))
                                    for rid in range(16, 0, -1)])
    return path


def payload_source(payload, table):
    return next(source for source in payload["sources"] if source["table"] == table)


def test_combined_real_databases_freeze_retries_refresh_next_question_and_keep_scopes(tmp_path, monkeypatch):
    from chat.actor import Actor
    from chat.critic import Critic
    from chat.service import ChatService

    historical, stream = warehouse(tmp_path), database(tmp_path)
    read_batch, read_stream = capture.read_batch_sources, capture.read_stream_snapshot
    reads = []

    def observed_batch(path, **options):
        reads.append(("batch", get_ident(), options))
        return read_batch(path, **options)

    def observed_stream(path):
        reads.append(("stream", get_ident(), {}))
        return read_stream(path)

    monkeypatch.setattr(capture, "read_batch_sources", observed_batch)
    monkeypatch.setattr(capture, "read_stream_snapshot", observed_stream)
    callback = capture.build_capture(historical, stream, start=date(2026, 7, 1), end=date(2026, 7, 3))

    class ScriptedModel:
        def __init__(self):
            self.entered, self.resume = asyncio.Event(), asyncio.Event()
            self.evidence = []
            self.decisions = 0

        async def complete(self, messages, *, schema=None):
            self.evidence.append(messages[3 if schema is None else 2]["content"])
            if schema is None:
                if len(self.evidence) == 1:
                    self.entered.set()
                    await self.resume.wait()
                return "תשובת מודל מתוסרט לבדיקת ההרכבה"
            self.decisions += 1
            return json.dumps({"is_acceptable": self.decisions > 1, "feedback": "משוב בדיקה מתוסרט"})

    async def run():
        model, audit, original_history = ScriptedModel(), [], []
        service = ChatService(Actor(model), Critic(model), callback, audit.append)
        task = asyncio.create_task(service.answer("מה מצב הנתונים?", original_history))
        await asyncio.wait_for(model.entered.wait(), timeout=5)
        with closing(sqlite3.connect(stream)) as connection:
            connection.execute("UPDATE stream_meta SET checkpoint=11, watermark_ms=126000")
            connection.execute("UPDATE order_state SET revenue_cents=200 WHERE window_start_ms=120000")
            connection.commit()
        model.resume.set()
        first = await asyncio.wait_for(task, timeout=5)
        second = await service.answer("ומה לגבי מוצר 5?", first.history)
        assert original_history == []
        return model, audit, first, second

    model, audit, first, second = asyncio.run(run())
    assert first.status == second.status == "approved"
    assert first.snapshot_id != second.snapshot_id
    assert len(model.evidence) == 6 and len(set(model.evidence[:4])) == 1
    assert model.evidence[4] == model.evidence[5] != model.evidence[0]
    before, after = json.loads(model.evidence[0]), json.loads(model.evidence[4])
    assert before["question"] == "מה מצב הנתונים?" and after["question"] == "ומה לגבי מוצר 5?"
    assert before["stream"]["checkpoint"] == 10 and after["stream"]["checkpoint"] == 11
    assert payload_source(before, "order_state")["rows"][-1]["revenue_ils"] == "0.00"
    assert payload_source(after, "order_state")["rows"][-1]["revenue_ils"] == "2.00"
    paid, products = payload_source(before, "marts.fct_orders"), payload_source(before, "marts.mart_product_reviews")
    assert paid["rows"] == [{"paid_orders": 2, "paid_revenue": "15.35"}]
    assert paid["units"] == "ILS and order count" and paid["time_end"] == "2026-07-03"
    assert "agorot" in payload_source(before, "order_state")["units"]
    assert products["time_end"] == "2026-07-04" and products["metric"] == "full_history_product_metrics"
    assert products["rows"][4]["revenue"] == "115.35"
    for payload in (before, after):
        assert [row["product_id"] for row in payload_source(payload, "marts.mart_product_reviews")["rows"]] == list(range(1, 9))
        assert payload["batch"]["time_start"] == "2026-07-01" and payload["batch"]["time_end"] == "2026-07-03"
        assert payload["batch"]["cross_source_product_mapping"] == "unverified; sources remain separate"
    assert [kind for kind, _, _ in reads] == ["batch", "stream", "batch", "stream"]
    assert all(thread != get_ident() for _, thread, _ in reads)
    assert all(options == {"start": date(2026, 7, 1), "end": date(2026, 7, 3)}
               for kind, _, options in reads if kind == "batch")
    assert [event["snapshot_id"] for event in audit if event["event"] == "snapshot"] == [first.snapshot_id, second.snapshot_id]


def test_review_text_sample_is_latest_twelve_with_explicit_omissions(tmp_path):
    callback = capture.build_capture(warehouse(tmp_path), database(tmp_path, empty=True),
                                     start=date(2026, 7, 1), end=date(2026, 7, 3))
    history = [{"role": "user", "content": "product 8"}, {"role": "assistant", "content": "previous answer"}]
    payload = asyncio.run(callback("SELECT * FROM imaginary; product 5", history))
    reviews = payload_source(payload, "staging.stg_reviews")
    assert [row["review_id"] for row in reviews["rows"]] == list(range(5, 17))
    assert reviews["time_start"] == "2026-07-01T00:00:00Z"
    assert reviews["time_end"] == "2026-07-03T00:00:00Z"
    assert "sample" in reviews["metric"] and "omitted=4" in reviews["metric"]
    sample = payload["batch"]["review_text_sample"]
    assert (sample["available"], sample["selected"], sample["omitted"], sample["limit"]) == (16, 12, 4, 12)
    assert "created_at" in sample["selection"] and "review_id" in sample["selection"]
    assert len(payload_source(payload, "marts.mart_product_reviews")["rows"]) == 8
    assert history == [{"role": "user", "content": "product 8"}, {"role": "assistant", "content": "previous answer"}]


@pytest.mark.parametrize("empty_warehouse", [False, True])
def test_combined_empty_results_preserve_observed_zero_and_full_mart_scope(tmp_path, empty_warehouse):
    callback = capture.build_capture(warehouse(tmp_path, empty=empty_warehouse), database(tmp_path, empty=True),
                                     start=date(2027, 1, 1), end=date(2027, 1, 2))
    payload = asyncio.run(callback("האם קיימים נתונים?", []))
    assert payload_source(payload, "marts.fct_orders")["rows"] == [{"paid_orders": 0, "paid_revenue": "0.00"}]
    assert payload_source(payload, "staging.stg_reviews")["rows"] == []
    sample = payload["batch"]["review_text_sample"]
    assert (sample["available"], sample["selected"], sample["omitted"]) == (0, 0, 0)
    assert payload["stream"]["checkpoint"] == -1 and payload["stream"]["watermark"] is None
    assert all(source["table"].startswith(("marts.", "staging.")) for source in payload["sources"])
    if empty_warehouse:
        assert len(payload["sources"]) == 2  # No invented full-mart date coverage.
    else:
        products = payload_source(payload, "marts.mart_product_reviews")
        assert len(products["rows"]) == 8 and products["time_end"] == "2026-07-04"
        assert products["rows"][-1]["review_count"] == 0 and products["rows"][-1]["avg_rating"] is None


@pytest.mark.parametrize("failure", ["missing_batch", "batch_schema", "missing_stream"])
def test_combined_read_failure_is_audited_without_answer_or_private_path(tmp_path, failure):
    from chat.actor import Actor
    from chat.critic import Critic
    from chat.service import ChatService

    historical, stream = warehouse(tmp_path), database(tmp_path)
    if failure == "missing_batch":
        historical = tmp_path / "private-missing.duckdb"
    elif failure == "batch_schema":
        with duckdb.connect(str(historical)) as connection:
            connection.execute("DROP TABLE marts.mart_product_reviews")
    else:
        stream = tmp_path / "private-missing.db"

    class NeverCalled:
        async def complete(self, *_args, **_kwargs):
            raise AssertionError("Model must not see partial evidence")

    callback = capture.build_capture(historical, stream, start=date(2026, 7, 1), end=date(2026, 7, 3))
    history = [{"role": "user", "content": "old question"}, {"role": "assistant", "content": "approved answer"}]
    audit = []
    model = NeverCalled()
    reply = asyncio.run(ChatService(Actor(model), Critic(model), callback, audit.append).answer("new question", history))
    assert reply.status == "error" and reply.answer is None and reply.history == history
    assert reply.snapshot_id is None and reply.sources == []
    assert len(audit) == 1 and audit[0]["event"] == "terminal" and audit[0]["error_stage"] == "capture"
    assert audit[0]["error_type"] == "ModelError" and "private" not in json.dumps(audit)
    assert not (tmp_path / "private-missing.duckdb").exists() and not (tmp_path / "private-missing.db").exists()


@pytest.mark.parametrize("start,end,windows", [
    (date(2026, 7, 1), date(2026, 7, 1), 3),
    (date(2026, 7, 2), date(2026, 7, 1), 3),
    (datetime(2026, 7, 1, tzinfo=UTC), date(2026, 7, 2), 3),
    ("2026-07-01", date(2026, 7, 2), 3),
    (date(2026, 7, 1), date(2026, 7, 2), 0),
    (date(2026, 7, 1), date(2026, 7, 2), True),
])
def test_combined_factory_validates_scope_before_any_question(tmp_path, start, end, windows):
    with pytest.raises(ValueError):
        capture.build_capture(tmp_path / "unused.duckdb", tmp_path / "unused.db",
                              start=start, end=end, latest_windows=windows)
