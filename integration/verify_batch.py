"""Verify two full Batch runs from the pinned course inputs."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import duckdb

from .batch import COURSE_REVISION, ROOT


QUERIES = {
    "orders": "SELECT * EXCLUDE (_dlt_id, _dlt_load_id) FROM raw.orders ORDER BY ALL",
    "reviews": "SELECT * EXCLUDE (_dlt_id, _dlt_load_id) FROM raw.reviews ORDER BY ALL",
    "review_tags": """
        SELECT r.* EXCLUDE (_dlt_id, _dlt_load_id), t._dlt_list_idx, t.value
        FROM raw.reviews__tags t JOIN raw.reviews r ON t._dlt_parent_id = r._dlt_id
        ORDER BY ALL
    """,
    "product_mart": "SELECT * FROM marts.mart_product_reviews ORDER BY ALL",
}


def snapshot(warehouse: Path) -> dict:
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        tables = {}
        for name, query in QUERIES.items():
            rows = connection.execute(query).fetchall()
            encoded = json.dumps(rows, default=str, ensure_ascii=False).encode("utf-8")
            tables[name] = {"count": len(rows), "sha256": sha256(encoded).hexdigest()}
        unique_orders = connection.execute("SELECT count(DISTINCT order_id) FROM raw.orders").fetchone()[0]
        unique_reviews = connection.execute("SELECT count(DISTINCT review_id) FROM raw.reviews").fetchone()[0]
        tags = connection.execute("SELECT count(*) FROM raw.reviews__tags").fetchone()[0]
    return {
        "tables": tables, "unique_order_ids": unique_orders,
        "unique_review_ids": unique_reviews, "tag_rows": tags,
    }


def verify(lab_dir: Path, etl_python: Path, output: Path) -> None:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    with TemporaryDirectory(prefix="shopil-reference-") as temp:
        checkout = Path(temp) / "lab"
        subprocess.run(["git", "clone", "--quiet", "--no-checkout", str(lab_dir.resolve()), str(checkout)],
                       check=True)
        subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", "--detach", COURSE_REVISION],
                       check=True)
        snapshots = []
        for name in ("first", "second"):
            with (output / f"{name}.log").open("wb") as log:
                subprocess.run(
                    [str(etl_python.absolute()), "-m", "integration.batch", "--lab-dir", str(checkout),
                     "--etl-python", str(etl_python.absolute()), "--run-dir", str(output / name)],
                    cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True,
                )
            snapshots.append(snapshot(checkout / "warehouse.duckdb"))

    first, second = snapshots
    differences = [name for name in first if first[name] != second[name]]
    expected = {
        "orders": 600, "reviews": 310, "review_tags": 284, "product_mart": 8,
    }
    counts_match = all(first["tables"][name]["count"] == count for name, count in expected.items())
    keys_match = first["unique_order_ids"] == 600 and first["unique_review_ids"] == 300 and first["tag_rows"] == 284
    passed = counts_match and keys_match and not differences
    report = {"course_revision": COURSE_REVISION, "first": first, "second": second,
              "differences": differences, "passed": passed}
    (output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if not passed:
        raise SystemExit(f"Batch verification failed. See {output / 'comparison.json'}")
    print(f"Batch reference verification passed. See {output / 'comparison.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-dir", type=Path, required=True, help="Local Git checkout of the course lab")
    parser.add_argument("--etl-python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory")
    args = parser.parse_args()
    verify(args.lab_dir, args.etl_python, args.output)


if __name__ == "__main__":
    main()
