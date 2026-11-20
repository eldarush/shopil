"""Documented full-match Hebrew metric grammar, not general language understanding."""
from dataclasses import dataclass
import re
import unicodedata

from .availability import METRICS

ALIASES = {
    "historical_paid_revenue": ("ההכנסה מהזמנות ששולמו", "הכנסה מהזמנות ששולמו", "סך ההכנסות מהזמנות ששולמו"),
    "historical_average_rating": ("הדירוג הממוצע", "דירוג ממוצע", "ממוצע הדירוגים", "הדירוג", "דירוג"),
    "shipping_cost": ("עלויות המשלוח", "עלויות משלוח", "עלות המשלוח", "עלות משלוח"),
}


STREAM_ALIASES = {
    "stream_order_amount": ("סכום אירועי ההזמנה",),
    "stream_order_count": ("מספר אירועי ההזמנה",),
    "stream_negative_reviews": ("מספר הביקורות השליליות בזרם",),
    "stream_alert_trigger_count": ("מספר הביקורות השליליות בעת ההתראה בזרם",),
}
STREAM_METRICS = {
    "stream_order_amount": {"fields": {"revenue_ils"}, "tables": {"order_state", "realtime_revenue"}},
    "stream_order_count": {"fields": {"orders"}, "tables": {"order_state", "realtime_revenue"}},
    "stream_negative_reviews": {"fields": {"negative_reviews"}, "tables": {"review_state", "review_windows"}},
    "stream_alert_trigger_count": {"fields": {"negative_reviews_at_trigger"}, "tables": {"review_alerts"}},
}
QUERY_METRICS = {**METRICS, **STREAM_METRICS}


@dataclass(frozen=True)
class QuestionScope:
    metric: str
    product_id: int | None = None
    window_state: str | None = None


def _text(question):
    if not isinstance(question, str):
        return ""
    value = " ".join(unicodedata.normalize("NFKC", question).split())
    return value[:-1].rstrip() if value.endswith("?") else value


def _one(question, previous=None):
    text = _text(question)
    followup = re.fullmatch(r"(?:ומה לגבי|ומה עם|מה לגבי) מוצר\s*([1-9][0-9]*)", text)
    if followup:
        product = int(followup[1])
        return QuestionScope(previous.metric, product, previous.window_state) if previous and product <= 2**63-1 else None
    for metric, aliases in STREAM_ALIASES.items():
        alias = "(?:" + "|".join(re.escape(item) for item in aliases) + ")"
        match = re.fullmatch(r"(?:מה|מהו) " + alias +
                            r"(?: (?:של |עבור |ל)?מוצר\s*([1-9][0-9]*))? בחלון (הפתוח|הסגור) האחרון(?: שמופיע בנתונים)?", text)
        if match:
            product = int(match[1]) if match[1] else None
            if product is not None and product > 2**63-1:
                return None
            if metric in {"stream_negative_reviews", "stream_alert_trigger_count"} and product is None:
                return None  # No invented across-product sum.
            if metric in {"stream_order_amount", "stream_order_count"} and product is not None:
                return None  # Order windows do not have product-level amounts.
            return QuestionScope(metric, product, "open" if match[2] == "הפתוח" else "closed")
    for metric, aliases in ALIASES.items():
        alias = "(?:" + "|".join(re.escape(item) for item in aliases) + ")"
        match = re.fullmatch(r"(?:מה|מהי|מהו|מהן|מהם) " + alias + r"(?: (?:של |עבור )?מוצר\s*([1-9][0-9]*))?", text)
        if match:
            product = int(match[1]) if match[1] else None
            return QuestionScope(metric, product) if product is None or product <= 2**63-1 else None
    return None


def recognize_question(question, history):
    previous = None
    # Unrecognized intervening user turns clear context. Assistant prose never
    # becomes an instruction or authoritative entity/metric mapping.
    for message in history:
        if message.get("role") == "user":
            previous = _one(message.get("content"), previous)
    return _one(question, previous)


def scoped_entries(entries, scope):
    if scope is None:
        return {}
    definition = QUERY_METRICS[scope.metric]
    if scope.metric in STREAM_METRICS:
        # Establish the latest appropriate window across ALL products before
        # filtering the requested product. Never fall back to an older product row.
        eligible = {ref: item for ref, item in entries.items()
                    for source, row, field in [item]
                    if source["table"] in definition["tables"] and field in definition["fields"]
                    and source.get("window_state") == scope.window_state
                    and type(row.get("window_start_ms")) is int}
        if not eligible:
            return {}
        observed = [row["window_start_ms"] for source, row, _ in entries.values()
                    if source["table"] in {"order_state", "realtime_revenue", "review_state", "review_windows", "review_alerts"}
                    and source.get("window_state") == scope.window_state
                    and type(row.get("window_start_ms")) is int]
        latest = max(observed)
        return {ref: item for ref, item in eligible.items() for _, row, _ in [item]
                if row["window_start_ms"] == latest
                and (scope.product_id is None or row.get("product_id") == scope.product_id)}
    result = {}
    for ref, (source, row, field) in entries.items():
        if source["table"] not in definition["tables"] or field not in definition["fields"]:
            continue
        if scope.product_id is not None:
            if row.get("product_id") != scope.product_id:
                continue
        elif scope.metric == "historical_paid_revenue":
            if source["table"] != "marts.fct_orders" or "product_id" in row:
                continue  # No invented total assembled from per-product rows.
        elif scope.metric == "historical_average_rating":
            continue  # No overall average supplied by the current fixed readers.
        result[ref] = (source, row, field)
    return result
