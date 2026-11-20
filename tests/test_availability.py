"""Capture-scoped absence is distinct from NULL, zero and unsupported questions."""
import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from chat.availability import build_metric_manifest, missing_explanation, verified_manifest
from chat.hybrid import HybridActor, Narrative, semantic_catalog


def snapshot():
    data = {"snapshot_id": "verified-capture", "captured_at": "2026-11-20T10:00:00Z", "sources": [
        {"table": "marts.fct_orders", "metric": "historical_paid_revenue", "units": "ILS", "identifiers": [],
         "time_start": "2026-01-01", "time_end": "2027-01-01", "rows": [{"paid_revenue": "0.00", "paid_orders": 0}]},
        {"table": "marts.mart_product_reviews", "metric": "full_history_product_metrics", "units": "ratings", "identifiers": ["product_id"],
         "time_start": "2026-01-01", "time_end": "2027-01-01", "rows": [{"product_id": 5, "avg_rating": None}]},
        {"table": "staging.stg_reviews", "metric": "cleaned_reviews", "units": "text", "identifiers": ["review_id"],
         "time_start": "2026-01-01", "time_end": "2027-01-01", "rows": []},
    ]}
    data["metric_availability"] = build_metric_manifest(data["sources"])
    return data


def test_zero_and_null_are_supplied_metrics_not_absence():
    data = snapshot(); manifest = verified_manifest(data)
    assert manifest["supplied_metrics"] == ["historical_average_rating", "historical_paid_revenue"]
    assert manifest["not_supplied_metrics"] == ["shipping_cost"]
    assert manifest["alternative_metrics"] == ["historical_paid_revenue"]
    text = missing_explanation("shipping_cost", data)
    assert "בצילום הנתונים הנוכחי" in text
    assert "ולא ערך אפס" in text
    assert "במקורות אחרים" in text
    assert "אפשר לשאול במקום זאת על הכנסה מהזמנות ששולמו" in text
    with pytest.raises(ValueError):
        missing_explanation("historical_average_rating", data)
    with pytest.raises(ValueError):
        missing_explanation("historical_paid_revenue", data)


@pytest.mark.parametrize("shipping", [None, 0, "0.00", "12.00"])
def test_present_shipping_field_never_authorizes_missing_even_when_null_or_zero(shipping):
    data = snapshot()
    data["sources"][0]["rows"][0]["shipping_cost"] = shipping
    # The original manifest is now stale; no claim can use it.
    assert verified_manifest(data) is None
    data["metric_availability"] = build_metric_manifest(data["sources"])
    assert "shipping_cost" in data["metric_availability"]["supplied_metrics"]
    with pytest.raises(ValueError):
        missing_explanation("shipping_cost", data)
    schema = Narrative.generation_schema(semantic_catalog(data), data)
    assert all(branch["properties"]["status"]["enum"] != ["missing_metric"] for branch in schema["anyOf"])


def test_absence_is_not_inferred_for_unknown_partial_or_unmarked_evidence():
    data = snapshot(); del data["metric_availability"]
    assert verified_manifest(data) is None
    partial = snapshot(); partial["sources"].pop()
    assert build_metric_manifest(partial["sources"]) is None
    unknown = snapshot(); unknown["sources"].append({"table": "unknown", "rows": []})
    assert build_metric_manifest(unknown["sources"]) is None
    forged = snapshot(); forged["metric_availability"]["not_supplied_metrics"].append("historical_paid_revenue")
    assert verified_manifest(forged) is None
    for invalid in (data, partial, unknown, forged):
        with pytest.raises(ValueError):
            missing_explanation("shipping_cost", invalid)


def test_missing_state_requires_named_declared_metric_not_generic_refusal():
    with pytest.raises(ValidationError):
        Narrative(status="missing_metric", sentences=[], explanation="none")
    with pytest.raises(ValidationError):
        Narrative(status="cannot_verify", missing_metric="shipping_cost", sentences=[], explanation="none")
    with pytest.raises(ValidationError):
        Narrative(status="missing_metric", missing_metric="historical_paid_revenue", sentences=[], explanation="none")


def test_actor_missing_output_is_schema_gated_and_rendered_with_snapshot_scope():
    data = snapshot(); calls=[]
    raw='{"status":"missing_metric","missing_metric":"shipping_cost","sentences":[],"explanation":"none"}'
    async def complete(messages, *, schema=None):
        calls.append(schema); return raw
    answer=asyncio.run(HybridActor(SimpleNamespace(complete=complete)).draft("מהן עלויות המשלוח?",json.dumps(data),[]))
    assert answer.actor_output_json == raw
    assert "לא סופק מדד עלויות משלוח" in answer
    assert "מזהה צילום הנתונים: verified-capture" in answer
    branch=calls[0]
    assert branch["properties"]["missing_metric"]["enum"] == ["shipping_cost"]
    assert "missing_metric" in branch["required"]
    # This proves protocol/rendering, not model understanding of the question.
