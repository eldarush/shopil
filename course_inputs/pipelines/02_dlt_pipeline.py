"""
מודול 2 - Extract & Load עם dlt
================================
מה רואים כאן:
  * resource מותאם אישית מקובץ CSV (customers, products)    -> write_disposition="replace"
  * טעינה אינקרמנטלית עם merge לפי מפתח ראשי (orders)        -> רק שורות חדשות/מעודכנות
  * מקור REST API (GitHub issues) עם JSON מקונן               -> dlt מנרמל לטבלאות-בת אוטומטית
  * Schema inference + schema evolution                     -> בלי לכתוב CREATE TABLE

הרצה:
    python pipelines/02_dlt_pipeline.py              # כולל קריאה ל-GitHub API
    python pipelines/02_dlt_pipeline.py --offline    # בלי אינטרנט: issues מקובץ דוגמה
    export GITHUB_TOKEN=...                          # מומלץ בכיתה (מגבלת 60 קריאות/שעה לכל IP בלי טוקן)

תוצר: warehouse.duckdb, סכמה raw
"""
import argparse
import csv
import json
import os
from pathlib import Path

import dlt

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "warehouse.duckdb"


def read_csv(name: str):
    with open(RAW / name, encoding="utf-8") as f:
        yield from csv.DictReader(f)


# --- Snapshot מלא: בכל ריצה מחליפים את הטבלה (קבצי ייצוא יומיים) ---
# column hints: מכריחים טקסט בשכבת raw. בלי זה dlt עלול לזהות "2026-03-01" כ-timestamp,
# ואז שורה עם "01/03/2026" תיפול לעמודת variant (order_date__v_text). את הניקוי עושים ב-dbt.
@dlt.resource(name="customers", write_disposition="replace",
              columns={name: {"data_type": "text"} for name in ("signup_date", "updated_at")})
def customers():
    yield from read_csv("customers.csv")


@dlt.resource(name="products", write_disposition="replace")
def products():
    yield from read_csv("products.csv")


# --- אינקרמנטלי: dlt זוכר את ה-updated_at המקסימלי שראה (state) ---
@dlt.resource(name="orders", primary_key="order_id", write_disposition="merge",
              columns={name: {"data_type": "text"} for name in ("order_date", "amount", "updated_at")})
def orders(updated_at=dlt.sources.incremental("updated_at", initial_value="2026-01-01T00:00:00")):
    # dlt מסנן לבד שורות שכבר נטענו. בריצה שנייה - 0 שורות חדשות.
    yield from read_csv("orders.csv")


# --- מקור API ---
def github_issues_source(repo: str, max_pages: int = 2):
    from dlt.sources.rest_api import rest_api_source

    client: dict = {"base_url": f"https://api.github.com/repos/{repo}/"}
    if token := os.getenv("GITHUB_TOKEN"):
        client["auth"] = {"type": "bearer", "token": token}

    source = rest_api_source({
        "client": client,
        "resource_defaults": {"primary_key": "id", "write_disposition": "merge"},
        "resources": [{
            "name": "github_issues",
            "endpoint": {
                "path": "issues",
                "params": {
                    "state": "all", "per_page": 50, "sort": "updated", "direction": "asc",
                    "since": {"type": "incremental", "cursor_path": "updated_at",
                              "initial_value": "2026-01-01T00:00:00Z"},
                },
            },
        }],
    })
    return source.add_limit(max_pages)  # מגבילים מספר עמודים כדי לא לשרוף rate limit


@dlt.resource(name="github_issues", primary_key="id", write_disposition="merge")
def github_issues_offline():
    with open(RAW / "github_issues_sample.json", encoding="utf-8") as f:
        yield json.load(f)


def make_pipeline():
    return dlt.pipeline(
        pipeline_name="shop_il",
        pipelines_dir=str(ROOT / ".dlt" / "pipelines"),
        destination=dlt.destinations.duckdb(str(WAREHOUSE)),
        dataset_name="raw",
    )


def run_pipeline(offline: bool = True, repo: str = "dlt-hub/dlt"):
    """נקודת כניסה אחת - גם מה-CLI וגם מ-Dagster."""
    pipeline = make_pipeline()
    issues = github_issues_offline() if offline else github_issues_source(repo)
    return pipeline, pipeline.run([customers(), products(), orders(), issues])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true", help="GitHub issues מקובץ מקומי")
    ap.add_argument("--repo", default="dlt-hub/dlt")
    args = ap.parse_args()

    pipeline, load_info = run_pipeline(offline=args.offline, repo=args.repo)
    print(load_info)
    trace = pipeline.last_trace
    if trace and trace.last_normalize_info:
        counts = trace.last_normalize_info.row_counts
        print(f"orders written in this run: {counts.get('orders', 0)}")

    # מה נטען? (כולל טבלאות-בת שנוצרו מנרמול JSON, למשל github_issues__labels)
    with pipeline.sql_client() as client:
        tables = client.execute_sql(
            "select table_name from information_schema.tables "
            "where table_schema = 'raw' and table_name not like '_dlt%' order by 1")
        print("\nטבלאות ב-raw:")
        for (t,) in tables:
            n = client.execute_sql(f'select count(*) from raw."{t}"')[0][0]
            print(f"  {t:<28} {n:>6} rows")


if __name__ == "__main__":
    main()
