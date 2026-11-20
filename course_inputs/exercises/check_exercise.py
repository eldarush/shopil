"""
בודק אוטומטי לתרגיל הכיתה.

    python exercises/check_exercise.py        # כל השלבים
    python exercises/check_exercise.py 2      # שלב ספציפי

הערכים הצפויים מחושבים ישירות מקובץ המקור (data/raw/reviews.json) - לא מקודדים מראש.
"""
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "warehouse.duckdb"
RAW_REVIEWS = json.loads((ROOT / "data" / "raw" / "reviews.json").read_text(encoding="utf-8"))

OK, FAIL = "✅", "❌"
results: list[bool] = []


def check(cond: bool, msg: str, hint: str = "") -> None:
    results.append(bool(cond))
    print(f"  {OK if cond else FAIL} {msg}" + (f"\n      💡 {hint}" if not cond and hint else ""))


def expected_clean_reviews() -> dict[int, dict]:
    clean = {}
    for r in RAW_REVIEWS:
        if r["rating"] in (1, 2, 3, 4, 5) and r["text"].strip():
            clean.setdefault(r["review_id"], r)
    return clean


def con():
    import duckdb
    return duckdb.connect(str(WAREHOUSE), read_only=True)


def table_exists(c, schema: str, table: str) -> bool:
    return c.execute("select count(*) from information_schema.tables where table_schema=? and table_name=?",
                     [schema, table]).fetchone()[0] > 0


def step1() -> None:
    print("\nשלב 1 - dlt: raw.reviews")
    with con() as c:
        if not table_exists(c, "raw", "reviews"):
            return check(False, "הטבלה raw.reviews קיימת", "הריצו: python pipelines/04_reviews_pipeline.py")
        n = c.execute("select count(*) from raw.reviews").fetchone()[0]
        check(n == len(RAW_REVIEWS), f"raw.reviews מכילה {len(RAW_REVIEWS)} שורות (נמצאו {n})",
              "raw צריך להכיל הכל כולל כפילויות. האם השתמשתם ב-replace? הרצתם פעמיים עם append?")
        check(table_exists(c, "raw", "reviews__tags"), "dlt יצר טבלת-בת raw.reviews__tags מהרשימה tags")
        cols = {r[0] for r in c.execute("select column_name from information_schema.columns "
                                        "where table_schema='raw' and table_name='reviews'").fetchall()}
        check("product__id" in cols, "השדה המקונן product.id נורמל לעמודה product__id")


def step2() -> None:
    print("\nשלב 2 - dbt: staging.stg_reviews")
    exp = expected_clean_reviews()
    with con() as c:
        if not table_exists(c, "staging", "stg_reviews"):
            return check(False, "המודל staging.stg_reviews קיים",
                         "cd dbt_project && dbt build --select stg_reviews --profiles-dir .")
        n, distinct, bad_rating, empty = c.execute("""
            select count(*), count(distinct review_id),
                   count(*) filter (where rating not between 1 and 5 or rating is null),
                   count(*) filter (where review_text is null or trim(review_text) = '')
            from staging.stg_reviews""").fetchone()
        check(n == distinct, f"אין כפילויות review_id ({n} שורות, {distinct} ייחודיות)", "qualify row_number() ...")
        check(bad_rating == 0, "כל הדירוגים בין 1 ל-5", "where rating between 1 and 5")
        check(empty == 0, "אין טקסטים ריקים", "nullif(trim(text), '')")
        check(n == len(exp), f"מספר השורות הוא {len(exp)} (נמצאו {n})")
        ids = {r[0] for r in c.execute("select review_id from staging.stg_reviews").fetchall()}
        check(ids == set(exp), "נשמרו בדיוק מזהי הביקורות החוקיות מהמקור")


def step3() -> None:
    print("\nשלב 3 - dbt: marts.mart_product_reviews")
    exp = expected_clean_reviews()
    agg = defaultdict(list)
    for r in exp.values():
        agg[r["product"]["id"]].append(r["rating"])
    with con() as c:
        if not table_exists(c, "marts", "mart_product_reviews"):
            return check(False, "המודל marts.mart_product_reviews קיים")
        rows = c.execute("select product_id, review_count, avg_rating, negative_reviews, revenue, paid_orders "
                         "from marts.mart_product_reviews").fetchall()
        check(len(rows) == 8, f"שורה לכל אחד מ-8 המוצרים (נמצאו {len(rows)})", "left join מ-stg_products")
        by_id = {r[0]: r for r in rows}
        ok_avg = all(pid in by_id and abs(float(by_id[pid][2]) - sum(v) / len(v)) < 0.01 for pid, v in agg.items())
        check(ok_avg, "avg_rating נכון לכל מוצר")
        ok_count = all(pid in by_id and by_id[pid][1] == len(v) for pid, v in agg.items())
        check(ok_count, "review_count נכון לכל מוצר")
        ok_neg = all(pid in by_id and by_id[pid][3] == sum(x <= 2 for x in v) for pid, v in agg.items())
        check(ok_neg, "negative_reviews נכון לכל מוצר")
        fct = dict(c.execute("select product_id, sum(amount) from marts.fct_orders "
                             "where status='paid' group by 1").fetchall())
        ok_rev = all(abs(float(by_id[p][4]) - float(v)) < 0.01 for p, v in fct.items() if p in by_id)
        check(ok_rev, "revenue תואם ל-fct_orders (רק paid)")
        paid = dict(c.execute("select product_id, count(*) from marts.fct_orders "
                              "where status='paid' group by 1").fetchall())
        check(all(p in by_id and by_id[p][5] == n for p, n in paid.items()), "paid_orders תואם למספר ההזמנות ששולמו")


def step4() -> None:
    print("\nשלב 4 - LanceDB: טבלת reviews")
    import lancedb
    try:
        table = lancedb.connect(str(ROOT / "lancedb")).open_table("reviews")
    except Exception:
        return check(False, "טבלת reviews קיימת ב-LanceDB", "python pipelines/05_reviews_rag.py")
    exp_n = len(expected_clean_reviews())
    check(table.count_rows() == exp_n, f"נטענו {exp_n} ביקורות (נמצאו {table.count_rows()})",
          "צריך לקרוא מ-staging.stg_reviews, לא מ-raw")
    names = set(table.schema.names)
    need = {"review_id", "product_id", "product_name", "rating", "review_text", "vector"}
    check(need <= names, "כל העמודות הנדרשות קיימות", f"חסרות: {need - names}")
    if {"review_id", "vector"} <= names:
        rows = table.to_arrow().select(["review_id", "vector"]).to_pylist()
        ids = [r["review_id"] for r in rows]
        check(len(ids) == len(set(ids)) and set(ids) == set(expected_clean_reviews()),
              "באינדקס יש רשומה אחת לכל ביקורת נקייה")
        dims = {len(r["vector"]) for r in rows if r["vector"] is not None}
        good_vectors = len(dims) == 1 and min(dims, default=0) > 0 and all(
            r["vector"] is not None and all(math.isfinite(v) for v in r["vector"]) for r in rows)
        check(good_vectors, "כל הווקטורים מלאים, סופיים ובאותו ממד")


STEPS = {"1": step1, "2": step2, "3": step3, "4": step4}

if __name__ == "__main__":
    for key in (sys.argv[1:] or STEPS):
        try:
            STEPS[key]()
        except Exception as e:  # noqa: BLE001
            check(False, f"שלב {key} נכשל עם שגיאה: {e}")
    passed = sum(results)
    print(f"\n{'🎉' if passed == len(results) else '📋'} {passed}/{len(results)} בדיקות עברו")
    sys.exit(0 if passed == len(results) else 1)
