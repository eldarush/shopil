"""SHOPIL-18: an endless, deterministic ShopIL order/review source.

The record at offset k is a pure function of k, so a restarted producer that
resumes from engine.next_offset re-creates exactly the deliveries it would have
sent (needed for replay/restart). Event time advances `step_s` per offset from a
fixed base, so a few real seconds already span several 60-second windows.

The source deliberately includes the realities the engine must handle:
  * every 13th delivery is a producer RETRY of the previous event_id (new offset);
  * every 19th event is stamped 100 s earlier (out of order -> late after grace);
  * every 4th event is a review, the rest are orders priced like the ShopIL catalog.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from streaming_lab.events import T0, Delivery, Event

# ShopIL catalog (etl-course-demo data/raw/products.csv), list price in agorot.
CATALOG_CENTS: dict[int, int] = {1: 34900, 2: 45900, 3: 8990, 4: 22900, 5: 11900, 6: 4990, 7: 19900, 8: 5900}
PRODUCT_IDS = tuple(CATALOG_CENTS)
NEGATIVE_TEXTS = ("הגיע שבור", "לא עובד בכלל", "איכות ירודה", "הסוללה נגמרת מהר")
POSITIVE_TEXTS = ("מוצר מעולה", "משלוח מהיר", "שווה כל שקל")


def shopil_event(sequence: int, *, base: datetime = T0, step_s: int = 2) -> Event:
    """The business event for source position `sequence` (before retry handling)."""
    when = base + timedelta(seconds=sequence * step_s - (100 if sequence % 19 == 18 else 0))
    product = PRODUCT_IDS[(sequence * 5) % len(PRODUCT_IDS)]
    if sequence % 4 == 3:
        negative = sequence % 5 != 0
        texts = NEGATIVE_TEXTS if negative else POSITIVE_TEXTS
        return Event(f"shopil-ev-{sequence:07d}", "review", when, product,
                     rating=1 + sequence % 2 if negative else 4 + sequence % 2,
                     text=texts[sequence % len(texts)])
    quantity = 2 if sequence % 7 == 0 else 1
    return Event(f"shopil-ev-{sequence:07d}", "order", when, product,
                 amount_cents=CATALOG_CENTS[product] * quantity, order_id=900_000 + sequence)


def delivery_at(offset: int, *, base: datetime = T0, step_s: int = 2) -> Delivery:
    # A retry re-sends the previous offset's event: same event_id, new offset.
    source = offset - 1 if offset % 13 == 12 else offset
    return Delivery(offset, shopil_event(source, base=base, step_s=step_s))


def shopil_source(start_offset: int = 0, *, limit: int | None = None,
                  base: datetime = T0, step_s: int = 2) -> Iterator[Delivery]:
    """Yield deliveries from `start_offset` forever (or `limit` records)."""
    offset = start_offset
    while limit is None or offset < start_offset + limit:
        yield delivery_at(offset, base=base, step_s=step_s)
        offset += 1
