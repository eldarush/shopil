"""Fixed read-only queries over the completed historical DuckDB warehouse."""

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import duckdb

from .evidence import EvidenceSource


def read_batch_sources(
    database: Path, *, start: date, end: date, product_id: int | None = None
) -> tuple[EvidenceSource, ...]:
    """Read historical data for the half-open calendar range [start, end)."""
    if type(start) is not date or type(end) is not date or start >= end:
        raise ValueError("Expected a nonempty range of calendar dates")
    if product_id is not None and (type(product_id) is not int or product_id < 1):
        raise ValueError("Expected a positive integer product ID")

    review_start = datetime.combine(start, time.min, UTC)
    review_end = datetime.combine(end, time.min, UTC)
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.begin()
        paid = connection.execute("""
            SELECT count(*), coalesce(sum(amount), 0)
            FROM marts.fct_orders
            WHERE status = 'paid' AND order_date >= ? AND order_date < ?
              AND (? IS NULL OR product_id = ?)
        """, [start, end, product_id, product_id]).fetchone()
        reviews = connection.execute("""
            SELECT review_id, product_id, rating, review_text, created_at
            FROM staging.stg_reviews
            WHERE created_at >= ? AND created_at < ?
              AND (? IS NULL OR product_id = ?)
            ORDER BY created_at, review_id
        """, [review_start, review_end, product_id, product_id]).fetchall()
        products = connection.execute("""
            SELECT product_id, product_name, review_count, avg_rating,
                   negative_reviews, paid_orders, revenue
            FROM marts.mart_product_reviews
            WHERE (? IS NULL OR product_id = ?)
            ORDER BY product_id
        """, [product_id, product_id]).fetchall()
        # The materialized mart covers the full load, independently of the question's range.
        coverage = connection.execute("""
            SELECT min(day), max(day) FROM (
                SELECT order_date AS day FROM marts.fct_orders WHERE status = 'paid'
                UNION ALL
                SELECT cast(timezone('UTC', created_at) AS date) AS day
                FROM staging.stg_reviews
            )
        """).fetchone()

    count, revenue = paid
    paid_row = {"paid_orders": count, "paid_revenue": str(revenue)}
    if product_id is not None:
        paid_row["product_id"] = product_id
        # A missing catalog product has no historical metric.
        paid_rows = [paid_row] if products else []
    else:
        paid_rows = [paid_row]

    sources = [
        EvidenceSource(
            table="marts.fct_orders", metric="historical_paid_revenue",
            units="ILS and order count", identifiers=("product_id",) if product_id is not None else (),
            time_start=start, time_end=end, rows=paid_rows,
        ),
        EvidenceSource(
            table="staging.stg_reviews", metric="cleaned_reviews", units="rating (1–5) and text",
            identifiers=("review_id", "product_id"), time_start=review_start, time_end=review_end,
            rows=[
                {"review_id": review_id, "product_id": pid, "rating": rating,
                 "review_text": review_text, "created_at": created_at.astimezone(UTC).isoformat()}
                for review_id, pid, rating, review_text, created_at in reviews
            ],
        ),
    ]
    first_day, last_day = coverage
    if first_day is not None:
        sources.append(EvidenceSource(
            table="marts.mart_product_reviews", metric="full_history_product_metrics",
            units="review and order counts, rating (1–5), revenue in ILS",
            identifiers=("product_id",), time_start=first_day, time_end=last_day + timedelta(days=1),
            rows=[
                {"product_id": pid, "product_name": name, "review_count": review_count,
                 "avg_rating": avg_rating, "negative_reviews": negative_reviews,
                 "paid_orders": paid_orders, "revenue": str(revenue)}
                for pid, name, review_count, avg_rating, negative_reviews, paid_orders, revenue in products
            ],
        ))
    return tuple(sources)
