"""Load the ShopIL reviews snapshot into the course DuckDB warehouse.

Adapted from exercises/starter/04_reviews_pipeline.py in the ShopIL ETL course lab.
"""

import argparse
import json
from pathlib import Path

import dlt


@dlt.resource(name="reviews", write_disposition="replace")
def reviews(source: Path):
    with source.open(encoding="utf-8") as snapshot:
        records = json.load(snapshot)
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise ValueError(f"Expected a JSON array of review objects in {source}")
    yield records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lab_dir", type=Path, help="Path to the ETL course checkout")
    args = parser.parse_args()
    lab_dir = args.lab_dir.resolve()

    pipeline = dlt.pipeline(
        pipeline_name="shop_il_reviews",
        pipelines_dir=str(lab_dir / ".dlt" / "pipelines"),
        destination=dlt.destinations.duckdb(str(lab_dir / "warehouse.duckdb")),
        dataset_name="raw",
    )
    load_info = pipeline.run(reviews(lab_dir / "data" / "raw" / "reviews.json"))
    load_info.raise_on_failed_jobs()
    print(load_info)


if __name__ == "__main__":
    main()
