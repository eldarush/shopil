from datetime import date
from pathlib import Path

import duckdb
import pytest

from data_access.batch import read_batch_sources
from data_access.evidence import EvidencePackage


def test_batch_evidence_uses_paid_rows_and_half_open_ranges(tmp_path):
    database = tmp_path / "historical.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("""
            CREATE SCHEMA marts;
            CREATE SCHEMA staging;
            CREATE TABLE marts.fct_orders (
                product_id INTEGER, order_date DATE, status VARCHAR, amount DECIMAL(10, 2)
            );
            INSERT INTO marts.fct_orders VALUES
                (5, '2026-07-01', 'paid', 10.10), (5, '2026-07-02', 'paid', 5.25),
                (5, '2026-07-03', 'paid', 100), (5, '2026-07-01', 'refunded', 50);
            CREATE TABLE staging.stg_reviews (
                review_id INTEGER, product_id INTEGER, rating INTEGER,
                review_text VARCHAR, created_at TIMESTAMPTZ
            );
            INSERT INTO staging.stg_reviews VALUES
                (1, 5, 2, 'Slow', '2026-07-01T00:00:00Z'),
                (2, 5, 5, 'Great', '2026-07-03T00:00:00Z');
            CREATE TABLE marts.mart_product_reviews (
                product_id INTEGER, product_name VARCHAR, review_count BIGINT,
                avg_rating DOUBLE, negative_reviews BIGINT, paid_orders BIGINT,
                revenue DECIMAL(10, 2)
            );
            INSERT INTO marts.mart_product_reviews VALUES
                (5, 'Charger', 2, 3.5, 1, 3, 115.35),
                (6, 'Cup', 0, NULL, 0, 0, 0);
        """)
    start, end = date(2026, 7, 1), date(2026, 7, 3)
    paid, reviews, metrics = read_batch_sources(database, start=start, end=end, product_id=5)
    assert paid.model_dump(mode="json")["rows"] == [
        {"paid_orders": 2, "paid_revenue": "15.35", "product_id": 5}
    ]
    assert [row["review_id"] for row in reviews.model_dump(mode="json")["rows"]] == [1]
    assert metrics.time_end == date(2026, 7, 4)
    assert metrics.model_dump(mode="json")["rows"][0]["revenue"] == "115.35"

    empty_paid, _, empty_metrics = read_batch_sources(database, start=start, end=end, product_id=6)
    assert empty_paid.model_dump(mode="json")["rows"][0]["paid_orders"] == 0
    assert empty_metrics.model_dump(mode="json")["rows"][0]["avg_rating"] is None
    assert all(not source.rows for source in read_batch_sources(
        database, start=start, end=end, product_id=7
    ))


def test_reader_rejects_invalid_inputs_and_does_not_create_database(tmp_path):
    database = tmp_path / "missing.duckdb"
    start, end = date(2026, 7, 1), date(2026, 7, 3)
    with pytest.raises(ValueError, match="nonempty range"):
        read_batch_sources(database, start=end, end=start)
    with pytest.raises(ValueError, match="positive integer"):
        read_batch_sources(database, start=start, end=end, product_id=True)
    with pytest.raises(duckdb.IOException):
        read_batch_sources(database, start=start, end=end)
    assert not database.exists()


@pytest.mark.parametrize("product_id", [None, 6, 7])
def test_known_zero_product_metrics_survive_without_observed_history(tmp_path, product_id):
    database = tmp_path / "empty_history.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("""
            CREATE SCHEMA staging;
            CREATE SCHEMA marts;
            CREATE TABLE staging.stg_products (
                product_id INTEGER, product_name VARCHAR, category VARCHAR
            );
            INSERT INTO staging.stg_products VALUES (6, 'Cup', 'Home');
            CREATE TABLE staging.stg_reviews (
                review_id INTEGER, product_id INTEGER, rating INTEGER,
                review_text VARCHAR, created_at TIMESTAMPTZ
            );
            CREATE TABLE marts.fct_orders (
                product_id INTEGER, order_date DATE, status VARCHAR, amount DECIMAL(10, 2)
            );
            INSERT INTO marts.fct_orders VALUES (6, '2026-07-01', 'refunded', 10.00);
        """)
        model = (Path(__file__).parents[1] / "etl/dbt/models/marts/mart_product_reviews.sql").read_text(
            encoding="utf-8"
        )
        for name, schema in (("stg_reviews", "staging"), ("stg_products", "staging"), ("fct_orders", "marts")):
            model = model.replace("{{ ref('" + name + "') }}", schema + "." + name)
        connection.execute("CREATE TABLE marts.mart_product_reviews AS " + model)

    paid, reviews, metrics = read_batch_sources(
        database, start=date(2026, 1, 1), end=date(2027, 1, 1), product_id=product_id
    )
    assert metrics.time_start is None and metrics.time_end is None
    assert metrics.time_coverage == "no_observed_history"
    assert metrics.model_dump(mode="json")["rows"] == ([] if product_id == 7 else [{
        "product_id": 6, "product_name": "Cup", "review_count": 0, "avg_rating": None,
        "negative_reviews": 0, "paid_orders": 0, "revenue": "0.00",
    }])
    assert not reviews.rows
    assert paid.time_coverage == "bounded"
    assert (paid.time_start, paid.time_end) == (date(2026, 1, 1), date(2027, 1, 1))
    assert bool(paid.rows) == (product_id != 7)
    package = EvidencePackage(question="How many reviews?", sources=(paid, reviews, metrics))
    assert EvidencePackage.model_validate_json(package.model_dump_json()) == package
