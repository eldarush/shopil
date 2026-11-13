"""Check the Assignment 1 reference dataset after running dlt and dbt."""

import argparse
from decimal import Decimal
from pathlib import Path

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("warehouse", type=Path, help="Path to the course warehouse.duckdb")
    args = parser.parse_args()

    checks = [
        ("Raw orders", "select count(*) from raw.orders", 600),
        ("Staged orders", "select count(*) from staging.stg_orders", 600),
        ("Valid orders", "select count(*) from marts.fct_orders", 549),
        ("Products", "select count(*) from staging.stg_products", 8),
        ("Paid orders", "select count(*) from marts.fct_orders where status = 'paid'", 402),
        (
            "Paid revenue (ILS)",
            "select sum(amount) from marts.fct_orders where status = 'paid'",
            Decimal("97210.10"),
        ),
    ]

    failed = False
    with duckdb.connect(str(args.warehouse), read_only=True) as warehouse:
        for label, query, expected in checks:
            result = warehouse.execute(query).fetchone()
            actual = result[0]
            passed = actual == expected
            print(f"{'PASS' if passed else 'FAIL'} {label}: {actual} (expected {expected})")
            failed |= not passed
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
