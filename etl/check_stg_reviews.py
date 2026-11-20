"""Verify the course fixture's cleaning counts and every retained row."""
import argparse
from collections import Counter
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb


def source_time(value):
    stamp = datetime.fromisoformat(value)
    return stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)


def verify_review_load(db, raw):
    """Compare multisets so two copies of a duplicate must survive the load."""
    source = Counter((r["review_id"], r["order_id"], r["product"]["id"], r["rating"], r["text"],
                      source_time(r["created_at"]), tuple(r.get("tags") or [])) for r in raw)
    flattened = db.execute("""
        SELECT r.review_id, r.order_id, r.product__id, r.rating, r.text, r.created_at,
               (SELECT list(t.value ORDER BY t._dlt_list_idx)
                FROM raw.reviews__tags t WHERE t._dlt_parent_id=r._dlt_id)
        FROM raw.reviews r
    """).fetchall()
    actual = Counter((*row[:-1], tuple(row[-1] or [])) for row in flattened)
    expected_tags = sum(len(r.get("tags") or []) for r in raw)
    actual_tags = db.execute("SELECT count(*) FROM raw.reviews__tags").fetchone()[0]
    if actual != source or actual_tags != expected_tags:
        raise SystemExit("Raw reviews, normalized product IDs, or ordered parent/child tags differ from source")
    print(f"Verified {len(raw)} source review rows including duplicates, product links, and {actual_tags} tags")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab_dir", type=Path, help="Path to the ETL course checkout")
    lab_dir = parser.parse_args().lab_dir
    raw = json.loads((lab_dir / "data/raw/reviews.json").read_text(encoding="utf-8"))
    valid_ratings = [row for row in raw if row["rating"] in (1, 2, 3, 4, 5)]
    valid_text = [row for row in valid_ratings if row["text"].strip()]
    candidates = [
        (
            row["review_id"], row["order_id"], row["product"]["id"],
            row["rating"], row["text"].strip(),
            source_time(row["created_at"]),
        )
        for row in valid_text
    ]
    expected_by_id = {}
    for row in sorted(candidates, key=lambda row: (row[5], row[1:5])):
        expected_by_id.setdefault(row[0], row)

    with duckdb.connect(str(lab_dir / "warehouse.duckdb"), read_only=True) as db:
        verify_review_load(db, raw)
        raw_count, rating_count, text_count = db.execute(
            r"""
            select
                count(*),
                count(*) filter (where rating in (1, 2, 3, 4, 5)),
                count(*) filter (
                    where rating in (1, 2, 3, 4, 5)
                      and regexp_matches(text, '[^[:space:]\p{Z}]')
                )
            from raw.reviews
            """
        ).fetchone()
        staged = db.execute(
            """
            select review_id, order_id, product_id, rating, review_text, created_at
            from staging.stg_reviews
            order by review_id
            """
        ).fetchall()

    expected_counts = (len(raw), len(valid_ratings), len(valid_text), len(expected_by_id))
    actual_counts = (raw_count, rating_count, text_count, len(staged))
    if expected_counts != (310, 304, 298, 289) or actual_counts != expected_counts:
        raise SystemExit(f"Reference counts (310, 304, 298, 289), JSON {expected_counts}, warehouse {actual_counts}")
    actual_by_id = {row[0]: row for row in staged}
    mismatched = sorted(
        review_id for review_id in expected_by_id.keys() | actual_by_id.keys()
        if expected_by_id.get(review_id) != actual_by_id.get(review_id)
    )
    if mismatched:
        raise SystemExit(f"Retained rows differ from the reference for review IDs: {mismatched}")

    print("Raw → valid rating → nonempty text → earliest valid per ID:", *actual_counts, sep=" ")
    print("Verified retained review IDs and row values against the source:")
    print(", ".join(str(review_id) for review_id in sorted(actual_by_id)))


if __name__ == "__main__":
    main()
