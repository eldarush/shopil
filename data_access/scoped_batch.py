"""Read only the requested historical measure using parameterized SQL."""

from datetime import UTC, datetime, time, timedelta
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


def read_scoped_batch(database: Path, metric, start, end, product_id, *, reporting_period):
    requested_start, requested_end = start, end
    if metric in ORDER_SQL:
        expression, units = ORDER_SQL[metric]
        table, time_column, condition = "marts.fct_orders", "order_date", "status = 'paid'"
    else:
        table, time_column, condition = "staging.stg_reviews", "created_at", "true"
        expression, units = REVIEW_SQL["negative_reviews" if metric == "top_negative_reviews" else metric]

    with duckdb.connect(str(database), read_only=True) as db:
        db.execute("SET TimeZone = 'UTC'")
        db.begin()
        first, last = db.execute(
            f"SELECT CAST(min({time_column}) AS DATE), CAST(max({time_column}) AS DATE) FROM {table}"
        ).fetchone()
        observed_end = last + timedelta(days=1) if last else None
        coverage = {
            "observed_start": first.isoformat() if first else None,
            "observed_end": observed_end.isoformat() if observed_end else None,
            "requested_start": requested_start.isoformat(), "requested_end": requested_end.isoformat(),
        }
        unavailable = None
        if first is None:
            unavailable = "אין רשומות במקור ולכן אין כיסוי לתקופה המבוקשת."
        else:
            start = max(requested_start, first, reporting_period[0])
            end = min(requested_end, observed_end, reporting_period[1])
            if start >= end:
                unavailable = "התקופה המבוקשת חורגת מכיסוי הנתונים."
                start, end = first, observed_end
            elif (start, end) != (requested_start, requested_end):
                coverage["partial_period"] = True
        if first is None:
            start, end = None, None
        elif metric not in ORDER_SQL:
            start, end = (datetime.combine(day, time.min, UTC) for day in (start, end))
        if unavailable:
            coverage["missing_reason"] = unavailable
            rows, identifiers = [], ()
        elif metric == "top_negative_reviews":
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
                          time_start=start, time_end=end, rows=rows,
                          time_coverage="bounded" if start else "no_observed_history"), coverage
