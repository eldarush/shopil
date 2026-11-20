"""Read only the requested historical measure using parameterized SQL."""

from datetime import UTC, datetime, time
from pathlib import Path

import duckdb

from .evidence import EvidenceSource


ORDER_SQL = {
    "paid_revenue": ("coalesce(sum(amount), 0)", "ILS"),
    "paid_orders": ("count(*)", "orders"),
}
REVIEW_SQL = {
    "avg_rating": ("round(avg(rating), 2)", "rating (1–5)"),
    "review_count": ("count(*)", "reviews"),
    "negative_reviews": ("count(*) filter (where rating <= 2)", "reviews"),
}


def read_scoped_batch(database: Path, metric, start, end, product_id):
    if metric in ORDER_SQL:
        expression, units = ORDER_SQL[metric]
        table, time_column, condition = "marts.fct_orders", "order_date", "status = 'paid'"
    else:
        table, time_column, condition = "staging.stg_reviews", "created_at", "true"
        start, end = (datetime.combine(day, time.min, UTC) for day in (start, end))
        expression, units = REVIEW_SQL["negative_reviews" if metric == "top_negative_reviews" else metric]

    with duckdb.connect(str(database), read_only=True) as db:
        db.begin()
        if metric == "top_negative_reviews":
            # Report ties; the database determines which rows have the highest count.
            results = db.execute("""
                SELECT p.product_id, p.product_name, count(r.review_id) AS negative_reviews
                FROM staging.stg_products p LEFT JOIN staging.stg_reviews r
                  ON p.product_id = r.product_id AND r.rating <= 2
                 AND r.created_at >= ? AND r.created_at < ?
                GROUP BY p.product_id, p.product_name
                QUALIFY rank() OVER (ORDER BY count(r.review_id) DESC) = 1
                ORDER BY p.product_id
            """, [start, end]).fetchall()
            rows = [{"product_id": pid, "product_name": name, "negative_reviews": count}
                    for pid, name, count in results]
            identifiers = ("product_id",)
        else:
            product = None if product_id is None else db.execute(
                "SELECT product_name FROM staging.stg_products WHERE product_id = ?", [product_id]
            ).fetchone()
            identifiers = ("product_id",) if product_id is not None else ()
            rows = []
            if product_id is None or product is not None:
                value, = db.execute(f"""
                    SELECT {expression} FROM {table}
                     WHERE {condition} AND {time_column} >= ? AND {time_column} < ?
                       AND (? IS NULL OR product_id = ?)
                """, [start, end, product_id, product_id]).fetchone()
                row = {metric: str(value) if metric == "paid_revenue" else value}
                if product is not None:
                    row.update(product_id=product_id, product_name=product[0])
                rows.append(row)

    return EvidenceSource(table=table, metric=metric, units=units, identifiers=identifiers,
                          time_start=start, time_end=end, rows=rows)
