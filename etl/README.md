# ShopIL Batch ETL

[SHOPIL-11](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-11) loads the original ShopIL customers, orders and products into DuckDB with dlt and builds the course dbt models. Run the commands below from the ShopIL repository root in a shell on macOS or Linux. This run uses Python 3.14.10 and uv. Keep the ETL environment separate from the application environment. Let the batch process release DuckDB before chat opens it read-only.

```sh
git clone https://github.com/paulavrahami/etl-course-demo.git ../etl-course-demo
git -C ../etl-course-demo checkout --detach 6788526695bbb20ac40f46a947413fff3545b86a
uv venv --python 3.14.10 ../.venv-shopil-etl
uv pip install --python ../.venv-shopil-etl/bin/python -r etl/requirements.txt

shopil_python="$PWD/../.venv-shopil-etl/bin/python"
shopil_dbt="$PWD/../.venv-shopil-etl/bin/dbt"
(
  cd ../etl-course-demo &&
  "$shopil_python" pipelines/02_dlt_pipeline.py --offline &&
  cd dbt_project &&
  "$shopil_dbt" build --profiles-dir .
) &&
"$shopil_python" etl/check_base.py ../etl-course-demo/warehouse.duckdb
```

If the course checkout already exists, use it in place of the clone after verifying its revision. Run against the original course CSVs in `data/raw/`. The dlt run in offline mode also loads the course's local GitHub issues sample. The dbt profile's default warehouse path is relative to the working directory, so run dbt from `dbt_project/` as shown.

The pipeline, inputs and dbt models come from the [course ETL lab](https://github.com/paulavrahami/etl-course-demo/tree/6788526695bbb20ac40f46a947413fff3545b86a). ShopIL adds the reference check and keeps the generated warehouse and dlt state in the course checkout.

## Reference run

The original dataset produced 600 raw and staged orders, 549 valid orders, eight products, and 402 paid orders totaling ILS 97,210.10. dbt built six models and ran 18 tests. Seventeen tests passed. The course relationship test reported 19 staged orders with no matching customer, which the course fact model excludes through its inner join. dbt recorded one warning and zero errors.

Re-running the same orders input with the same dlt state wrote zero orders. All six reference checks passed again.

## Load the reviews snapshot

[SHOPIL-13](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-13) uses the same course checkout, warehouse and virtual environment as the baseline. From the ShopIL repository root, run:

```sh
shopil_root="$PWD"
(
  cd ../etl-course-demo &&
  "$shopil_root/../.venv-shopil-etl/bin/python" "$shopil_root/etl/load_reviews.py" . &&
  "$shopil_root/../.venv-shopil-etl/bin/python" exercises/check_exercise.py 1
)
```

The loader adapts the course's [reviews starter](https://github.com/paulavrahami/etl-course-demo/blob/6788526695bbb20ac40f46a947413fff3545b86a/exercises/starter/04_reviews_pipeline.py). The source is `data/raw/reviews.json` in the pinned course checkout. dlt replaces the snapshot in `raw.reviews`, flattens `product.id` to `product__id`, and writes tags to `raw.reviews__tags`. The raw load preserves duplicates and invalid review values for the staging model to handle.

On Python 3.14.10 with DuckDB 1.5.6, the course checker passed all three step 1 checks on both loads. Each load produced 310 review rows with 300 distinct review IDs and 284 tag rows. All six baseline checks passed after the second load.

## Build the cleaned reviews view

[SHOPIL-14](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-14) extends the course dbt project. After running the base models and the raw reviews load, run these commands from the ShopIL repository root:

```sh
shopil_root="$PWD"
cp etl/dbt/models/staging/stg_reviews.sql etl/dbt/models/staging/_stg_reviews.yml \
  ../etl-course-demo/dbt_project/models/staging/
(
  cd ../etl-course-demo/dbt_project &&
  "$shopil_root/../.venv-shopil-etl/bin/dbt" build --select stg_reviews --profiles-dir .
) &&
(
  cd ../etl-course-demo &&
  "$shopil_root/../.venv-shopil-etl/bin/python" exercises/check_exercise.py 2
) &&
"$shopil_root/../.venv-shopil-etl/bin/python" etl/check_stg_reviews.py ../etl-course-demo
```

The SQL and schema tests adapt the [course reviews starter](https://github.com/paulavrahami/etl-course-demo/tree/6788526695bbb20ac40f46a947413fff3545b86a/exercises/starter). The view preserves `created_at` as a timestamp with time zone. It first filters ratings outside 1–5, then strips leading and trailing whitespace and drops empty text. It keeps the earliest valid timestamp per `review_id`. Ties use order ID, product ID, rating, and cleaned text in ascending order.

The original fixture produced **310 raw → 304 with valid ratings → 298 with nonempty text → 289 after deduplication**. The project checker independently computes the winning rows from the JSON and prints all retained IDs after comparing every projected value. On Python 3.14.10 and DuckDB 1.5.6, all six dbt tests and all five course checker checks passed.
