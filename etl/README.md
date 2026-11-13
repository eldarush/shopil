# ShopIL Batch ETL

[SHOPIL-11](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-11) loads the original ShopIL customers, orders and products into DuckDB with dlt and builds the course dbt models. Run the commands below from the ShopIL repository root in a shell on macOS or Linux. Python 3.12 and uv are required.

```sh
git clone https://github.com/paulavrahami/etl-course-demo.git ../etl-course-demo
git -C ../etl-course-demo checkout --detach 6788526695bbb20ac40f46a947413fff3545b86a
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r etl/requirements.txt

shopil_python="$PWD/.venv/bin/python"
shopil_dbt="$PWD/.venv/bin/dbt"
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
