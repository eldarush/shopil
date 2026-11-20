"""Resolve supported Hebrew intents before opening the evidence stores."""

from dataclasses import dataclass, replace
from datetime import date
import re
import unicodedata

@dataclass(frozen=True)
class QueryScope:
    metric: str
    product_id: int | None = None
    window_state: str | None = None
    start: date | None = None
    end: date | None = None


HISTORICAL = {
    "paid_revenue": r"(?:מהי? |מהו |מה היו |מה הייתה )?(?:סך )?ה?הכנס(?:ה|ות)(?: הכוללת)? מה?הזמנות ששולמו|כמה (?:הכנסנו|התקבל) (?:מה?הזמנות|עבור ה?זמנות) ששולמו|כמה כסף נכנס מה?הזמנות ששולמו|כמה שילמו לנו על ההזמנות|כמה כסף הכניסה החנות מה?הזמנות ששולמו",
    "profit": r"כמה הרווחנו(?: מהזמנות ששולמו)?|מה(?:ו)? הרווח(?: הנקי)?|ומה לגבי הרווח, האם אפשר להשתמש בהכנסה מההזמנות ששולמו בתור רווח",
    "paid_orders": r"כמה הזמנות (?:כבר )?(?:שולמו|ששולמו|שולמו בפועל)|(?:מהו? )?מספר ההזמנות ששולמו",
    "avg_rating": r"(?:מהו? |מה )?(?:ה?דירוג הממוצע|ממוצע הדירוגים|ה?דירוג)",
    "review_count": r"כמה ביקורות (?:התקבלו|יש)|(?:מהו? )?מספר הביקורות",
    "negative_reviews": r"כמה ביקורות שליליות (?:התקבלו|יש)|(?:מהו? )?מספר הביקורות השליליות",
    "top_negative_reviews": r"לאיזה מוצר (?:יש|היו) הכי הרבה ביקורות שליליות",
    "shipping_cost": r"(?:מהן? )?עלויו?ת (?:ה)?משלוח|(?:מהי? )?עלות (?:ה)?משלוח|כמה (?:עלה|עלו) (?:המשלוח|המשלוחים)",
}

STREAM = {
    "stream_order_amount": r"(?:מהו? )?סכום אירועי ההזמנה|כמה כסף נקלט (?:בזרם )?מאירועי הזמנה",
    "stream_order_count": r"(?:מהו? )?מספר אירועי ההזמנה|כמה אירועי הזמנה (?:נקלטו|התקבלו)",
    "stream_negative_reviews": r"(?:מהו? )?מספר הביקורות השליליות בזרם|כמה ביקורות שליליות (?:נקלטו|התקבלו) בזרם",
    "stream_alert_trigger_count": r"(?:מהו? )?מספר הביקורות השליליות בעת ההתראה בזרם|כמה ביקורות שליליות היו בעת ההתראה",
}

# The historical review/order sources and stream IDs remain separate.
STREAM_PRODUCT_METRICS = {"stream_negative_reviews", "stream_alert_trigger_count"}
PRODUCT = r"(?: של | עבור | ל| )(?:מוצר|מוצר מספר)\s*([1-9][0-9]{0,18})"
PERIOD = r"(?: בין (\d{4}-\d\d-\d\d) (?:לבין |ל[-־]?|עד )(\d{4}-\d\d-\d\d)| בשנת (\d{4}))"
WINDOW = r" בחלון (הפתוח|הסגור) האחרון(?: שמופיע בנתונים)?"
FOLLOW_UP = r"(?:ומה לגבי|ומה עם|מה לגבי|מה עם|ועבור|ואצל) "


def _bounds(first, end, year):
    if year is not None:
        return date(int(year), 1, 1), date(int(year) + 1, 1, 1)
    if first is not None:
        start, stop = date.fromisoformat(first), date.fromisoformat(end)
        if start >= stop:
            raise ValueError("Empty range")
        return start, stop
    return None, None


def _one(question, previous):
    value = " ".join(unicodedata.normalize("NFKC", question).split()).rstrip(".?!").rstrip()
    value = re.sub(r"^(?:אפשר לדעת |אפשר בבקשה לדעת |תגיד לי |תוכל לומר לי )", "", value)
    followup = re.fullmatch(FOLLOW_UP + r"(?:מוצר|מוצר מספר)\s*([1-9][0-9]{0,18})(?: באותה תקופה)?", value)
    if followup and previous and (previous.metric in STREAM_PRODUCT_METRICS or
                                  previous.metric in HISTORICAL and previous.metric != "top_negative_reviews"):
        product = int(followup[1])
        return replace(previous, product_id=product) if product < 2**63 else None
    if value.endswith(" השנה"):
        value = value.removesuffix(" השנה") + f" בשנת {date.today().year}"
    window = re.search(WINDOW + "$", value)
    if window:
        prefix = value[:window.start()]
        for metric, pattern in STREAM.items():
            match = re.fullmatch(f"(?:{pattern})(?:{PRODUCT})?", prefix)
            if match:
                product = int(match[1]) if match[1] else None
                if (product is not None) != (metric in STREAM_PRODUCT_METRICS) or (product or 0) >= 2**63:
                    return None
                return QueryScope(metric, product, "open" if window[1] == "הפתוח" else "closed")
        return None
    for metric, pattern in HISTORICAL.items():
        match = re.fullmatch(f"(?:{pattern})(?:{PRODUCT})?(?:{PERIOD})?", value)
        if match:
            product = int(match[1]) if match[1] else None
            if (product or 0) >= 2**63 or product is not None and metric == "top_negative_reviews":
                return None
            try:
                start, end = _bounds(*match.groups()[1:])
            except ValueError:
                return None
            return QueryScope(metric, product, start=start, end=end)
    return None


def resolve_query(question, history):
    scope = None
    for message in history:
        if message["role"] == "user":
            scope = _one(message["content"], scope)
    return _one(question, scope)
