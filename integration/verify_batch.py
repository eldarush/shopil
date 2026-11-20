"""Verify two full Batch runs from the pinned course inputs."""

import argparse
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory

import duckdb

from .batch import COURSE_REVISION, ROOT, bundled_files


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
        cursor = connection.execute("SELECT * FROM marts.mart_product_reviews ORDER BY product_id")
        columns = [column[0] for column in cursor.description]
        products = [dict(zip(columns, row)) for row in cursor.fetchall()]
        retained_ids = [r[0] for r in connection.execute(
            "SELECT review_id FROM staging.stg_reviews ORDER BY review_id"
        ).fetchall()]
    return {
        "tables": tables, "unique_order_ids": unique_orders,
        "unique_review_ids": unique_reviews, "tag_rows": tags,
        "product_metrics": products, "retained_review_ids": retained_ids,
    }


def changed_order_experiment(checkout, etl_python, output):
    """Edit one order in the disposable source, then deliver that same edit twice."""
    source = checkout / "data/raw/orders.csv"
    with source.open(encoding="utf-8", newline="") as stream:
        orders = list(csv.DictReader(stream))
    warehouse = checkout / "warehouse.duckdb"
    with duckdb.connect(str(warehouse), read_only=True) as db:
        order_id, old_amount = db.execute("""
            SELECT order_id, amount FROM marts.fct_orders
            WHERE status='paid' ORDER BY order_id LIMIT 1
        """).fetchone()
    order = next(row for row in orders if int(row["order_id"]) == order_id)
    original = {key: order[key] for key in ("amount", "updated_at")}
    newest = max(datetime.fromisoformat(row["updated_at"]) for row in orders)
    order.update(amount=format(old_amount + Decimal("1.00"), ".2f"),
                 updated_at=(newest + timedelta(days=1)).isoformat())
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, list(order))
        writer.writeheader()
        writer.writerows(orders)
    observed = []
    for name, written in (("change", 1), ("replay", 0)):
        result = subprocess.run([str(etl_python), str(checkout / "pipelines/02_dlt_pipeline.py"), "--offline"],
                                cwd=checkout, check=True, capture_output=True, text=True, encoding="utf-8",
                                env=dict(os.environ, PYTHONUTF8="1", RUNTIME__DLTHUB_TELEMETRY="false"))
        with duckdb.connect(str(warehouse), read_only=True) as db:
            keys = db.execute("SELECT count(*), count(DISTINCT order_id) FROM raw.orders").fetchone()
            matches = db.execute("SELECT amount, updated_at FROM raw.orders WHERE order_id=?", [str(order_id)]).fetchall()
        if keys != (600, 600) or matches != [(order["amount"], order["updated_at"])]:
            raise AssertionError(f"Merge produced unexpected keys or row: {keys}, {matches}")
        if f"orders written in this run: {written}" not in result.stdout.splitlines():
            raise AssertionError(f"Unexpected normalized row count on {name}: {result.stdout}")
        observed.append(dict(load=name, orders_written=written, table_rows=keys[0],
                             unique_ids=keys[1], matching_rows=len(matches), amount=matches[0][0],
                             updated_at=matches[0][1]))
    details = dict(order_id=order_id, original=original, original_max_cursor=newest.isoformat(),
                   expected="Single updated order, then zero rows written. Total and unique keys stay 600.",
                   observed=observed, passed=True)
    (output / "changed-order.json").write_text(json.dumps(details, indent=2), encoding="utf-8")


def check_fanout_detection(checkout, project, etl_python, output):
    """Substitute the erroneous many-to-many total for one product on the temporary DB."""
    warehouse = checkout / "warehouse.duckdb"
    with duckdb.connect(str(warehouse)) as db:
        product_id, correct, fanout = db.execute("""
            SELECT m.product_id, m.revenue, sum(o.amount) FROM marts.mart_product_reviews m
            JOIN marts.fct_orders o USING (product_id)
            JOIN staging.stg_reviews r USING (product_id)
            WHERE o.status='paid' GROUP BY m.product_id, m.revenue
            HAVING sum(o.amount) > m.revenue ORDER BY m.product_id LIMIT 1
        """).fetchone()
        db.execute("UPDATE marts.mart_product_reviews SET revenue=? WHERE product_id=?", [fanout, product_id])
    dbt = etl_python.parent / ("dbt.exe" if os.name == "nt" else "dbt")
    with (output / "fanout-test.log").open("wb") as log:
        result = subprocess.run([str(dbt), "test", "--select", "assert_product_mart_reconciles",
                                 "--profiles-dir", ".", "--target-path", str(output / "fanout-target")],
                                cwd=project, stdout=log, stderr=subprocess.STDOUT,
                                env=dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
                                         WAREHOUSE_PATH=str(warehouse), PYTHONUTF8="1"))
    [test] = json.loads((output / "fanout-target/run_results.json").read_text(encoding="utf-8"))["results"]
    if result.returncode != 1 or test["status"] != "fail" or test["failures"] != 1:
        raise AssertionError(f"Expected one failing product: {test['status']}, {test['failures']}")
    report = dict(product_id=product_id, correct_revenue=str(correct), direct_join_revenue=str(fanout),
                  dbt_test=test["unique_id"], expected_failing_rows=1, observed_failing_rows=test["failures"],
                  passed=True)
    (output / "fanout-control.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


def check_unreviewed_product(checkout, project, etl_python, output):
    """Deliver a ninth catalog product through dlt and dbt in the disposable lab."""
    source = checkout / "data/raw/products.csv"
    with source.open("a", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerow(["9001", "Course control", "Control", "1.00"])
    warehouse = checkout / "warehouse.duckdb"
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               RUNTIME__DLTHUB_TELEMETRY="false", WAREHOUSE_PATH=str(warehouse), PYTHONUTF8="1")
    dbt = etl_python.parent / ("dbt.exe" if os.name == "nt" else "dbt")
    with (output / "unreviewed-product.log").open("wb") as log:
        subprocess.run([str(etl_python), str(checkout / "pipelines/02_dlt_pipeline.py"), "--offline"],
                       cwd=checkout, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        subprocess.run([str(dbt), "build", "--profiles-dir", ".", "--target-path", str(output / "catalog-target")],
                       cwd=project, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    with duckdb.connect(str(warehouse), read_only=True) as db:
        result = db.execute(f"""
            SELECT count(*) over () AS products, product_id, review_count, avg_rating,
                   negative_reviews, paid_orders, revenue
            FROM marts.mart_product_reviews
            QUALIFY product_id=9001
        """).fetchall()
        normalized = db.execute("SELECT product_id FROM staging.stg_products WHERE product_id=9001").fetchall()
    if normalized != [(9001,)] or result != [(9, 9001, 0, None, 0, 0, Decimal("0.00"))]:
        raise AssertionError(f"Product with no observations lost or misrepresented: {result}")
    keys = ("products", "product_id", "review_count", "avg_rating", "negative_reviews", "paid_orders", "revenue")
    report = dict(input=["9001", "Course control", "Control", "1.00"],
                  expected="9 products; zero counts and revenue for 9001; null average",
                  observed=dict(zip(keys, result[0])), persisted_in="marts.mart_product_reviews", passed=True)
    (output / "unreviewed-product.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")


def verify(lab_dir: Path, etl_python: Path, output: Path) -> None:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    with TemporaryDirectory(prefix="shopil-reference-") as temp:
        checkout = Path(temp) / "lab"
        lab_dir = lab_dir.resolve()
        files = bundled_files(lab_dir)
        if files is None:
            subprocess.run(["git", "clone", "--quiet", "--no-checkout", str(lab_dir), str(checkout)], check=True)
            subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", "--detach", COURSE_REVISION], check=True)
        else:
            for path in [*files, lab_dir / "course-manifest.json"]:
                target = checkout / path.relative_to(lab_dir)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
        snapshots = []
        for name in ("first", "second"):
            with (output / f"{name}.log").open("wb") as log:
                subprocess.run(
                    [str(etl_python.absolute()), "-m", "integration.batch", "--lab-dir", str(checkout),
                     "--etl-python", str(etl_python.absolute()), "--run-dir", str(output / name)],
                    cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True,
                )
            snapshots.append(snapshot(checkout / "warehouse.duckdb"))
        changed_order_experiment(checkout, etl_python.absolute(), output)
        check_fanout_detection(checkout, output / "second/dbt_project", etl_python.absolute(), output)
        check_unreviewed_product(checkout, output / "second/dbt_project", etl_python.absolute(), output)

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
    (output / "comparison.json").write_text(json.dumps(report, indent=2, default=str, ensure_ascii=False) + "\n", encoding="utf-8")
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
