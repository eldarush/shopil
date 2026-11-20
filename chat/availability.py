"""Bounded capability manifest for this application's fixed evidence readers.

This describes the supplied snapshot, never the entire warehouse or business.
It is not a natural-language question classifier.
"""

CONTRACT = "shopil-fixed-readers-v1"
TABLES = {"marts.fct_orders", "marts.mart_product_reviews", "staging.stg_reviews",
          "order_state", "realtime_revenue", "review_state", "review_windows", "review_alerts"}
REQUIRED = {"marts.fct_orders", "marts.mart_product_reviews", "staging.stg_reviews"}
METRICS = {
    "historical_paid_revenue": {"label": "הכנסה מהזמנות ששולמו", "fields": {"paid_revenue", "revenue"},
                                "tables": {"marts.fct_orders", "marts.mart_product_reviews"}},
    "historical_average_rating": {"label": "דירוג ממוצע של ביקורות היסטוריות", "fields": {"avg_rating"},
                                  "tables": {"marts.mart_product_reviews"}},
    "shipping_cost": {"label": "עלויות משלוח", "fields": {"shipping_cost", "shipping_fee", "delivery_fee", "shipping_cost_ils"},
                      "tables": TABLES},
}
# The fixed readers explicitly do not query shipping charges. Other absent field
# names do not automatically authorize a claim that their metric is unavailable.
DECLARED_NOT_QUERIED = {"shipping_cost"}


def build_metric_manifest(sources):
    present = {source["table"] for source in sources}
    if not REQUIRED <= present or not present <= TABLES:
        return None  # Unknown or partial capture: do not infer metric absence.
    supplied, alternatives = [], []
    for metric, definition in METRICS.items():
        values = [row[field] for source in sources if source["table"] in definition["tables"]
                  for row in source["rows"] for field in definition["fields"] if field in row]
        if values:  # Presence of NULL or zero is still a supplied metric.
            supplied.append(metric)
        if any(value is not None for value in values):
            alternatives.append(metric)
    return {"contract": CONTRACT, "scope": "current_snapshot_only",
            "source_tables": sorted(present), "supplied_metrics": sorted(supplied),
            "not_supplied_metrics": sorted(DECLARED_NOT_QUERIED - set(supplied)),
            "alternative_metrics": sorted(alternatives)}


def verified_manifest(evidence):
    actual = evidence.get("metric_availability")
    expected = build_metric_manifest(evidence["sources"])
    return actual if actual is not None and actual == expected else None


def missing_explanation(metric, evidence):
    manifest = verified_manifest(evidence)
    if manifest is None or metric not in manifest["not_supplied_metrics"]:
        raise ValueError("No capture-supported missing-metric declaration")
    label = METRICS[metric]["label"]
    text = [f"בצילום הנתונים הנוכחי לא סופק מדד {label}.",
            "זהו מידע חסר בראיות שסופקו, ולא ערך אפס; אין להסיק שהמידע אינו קיים במקורות אחרים.",
            "השאילתות המוגדרות מספקות מדדי הזמנות וביקורות, אך אינן מספקות כאן חיובי משלוח.",
            "מקורות שנבדקו בצילום זה: " + ", ".join(manifest["source_tables"])]
    alternatives = [name for name in manifest["alternative_metrics"] if name != metric]
    if alternatives:
        # Prefer a supplied metric whose value is actually present, including zero.
        choice = "historical_paid_revenue" if "historical_paid_revenue" in alternatives else alternatives[0]
        text.append(f"אפשר לשאול במקום זאת על {METRICS[choice]['label']}, שסופקה במקורות אלה.")
    text += [f"מזהה צילום הנתונים: {evidence['snapshot_id']}",
             f"מועד צילום הנתונים: {evidence['captured_at']}"]
    return "\n".join(text)
