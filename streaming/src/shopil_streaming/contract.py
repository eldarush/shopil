"""SHOPIL-17: the streaming event contract, as enforced by the course engine.

`streaming_lab.events.Event` checks the event schema, including representable
integers and Unicode. This module exposes that stateless precheck. Ingestion
additionally validates configured window bounds and the current window's revenue
capacity; a schema-valid event can still be rejected there. All ingestion
rejections go to `dead_letters(source_offset, payload_json, reason)` in the SAME
transaction that advances the checkpoint. Read that recorded reason for the
authoritative ingestion outcome.

Identity: `event_id` is the business identity of an event. `offset` is only the
position in ONE ordered source partition; a producer retry may legitimately
deliver the same event_id again at a new offset (deduplicated, never re-counted).
"""
from __future__ import annotations

from typing import Any, Mapping

from streaming_lab.events import Event, InvalidEvent

CONTRACT: list[dict[str, str]] = [
    {"field": "event_id", "applies_to": "all", "type": "non-empty string",
     "rule": "Business identity. Schema-valid duplicates are ignored at new offsets; schema-invalid retries remain invalid."},
    {"field": "kind", "applies_to": "all", "type": "'order' | 'review'", "rule": "Any other value is invalid."},
    {"field": "event_time", "applies_to": "all", "type": "ISO-8601 with timezone (or tz-aware datetime)",
     "rule": "Naive timestamps are invalid. Normalised to UTC; windows are computed in integer milliseconds."},
    {"field": "product_id", "applies_to": "all", "type": "integer >= 1",
     "rule": "Strings, floats and booleans are invalid. Cross-source product mapping must be verified before joining Batch data."},
    {"field": "amount_cents", "applies_to": "order", "type": "integer >= 0 (agorot)",
     "rule": "Never a float. 12200 agorot = 122.00 ILS. Orders must not carry a rating."},
    {"field": "order_id", "applies_to": "order (optional)", "type": "integer >= 1", "rule": "Informational only."},
    {"field": "rating", "applies_to": "review", "type": "integer 1..5",
     "rule": "Reviews must not carry amount_cents. rating <= 2 counts as negative."},
    {"field": "text", "applies_to": "review", "type": "string", "rule": "Defaults to ''. Must be a string."},
]

# Stream order events have NO payment-status field. They are not the same metric
# as Batch "paid" revenue in fct_orders and must never be summed with it.
PAYMENT_STATUS_NOTE = ("Streaming order events carry amount_cents but no payment status; "
                       "they are not equivalent to Batch paid revenue (ILS, status='paid').")


def check(payload: Event | Mapping[str, Any] | Any) -> str | None:
    """Return a schema error or None; this does not predict state-dependent acceptance."""
    if isinstance(payload, Event):
        return None
    try:
        Event.from_dict(payload)
    except (InvalidEvent, ValueError, TypeError, OverflowError) as exc:
        return str(exc)
    return None


def dead_letter_report(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Dead letters in source order with input, offset and reason (SHOPIL-17/22)."""
    return [{"offset": row["source_offset"], "reason": row["reason"], "input": row["payload"]}
            for row in snapshot["dead_letters"]]
