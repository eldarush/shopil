"""SHOPIL-21: an independent, pure-Python model of the assignment's time policy.

    watermark    = max(previous_watermark, max_seen_event_time - out_of_order)
    close_window = watermark >= window_end + grace          (window = [start, end))

Only valid, unique and accepted events move max_seen_event_time. Invalid input,
duplicate event_ids and late events do not. The model never reads the engine's
database; experiments compare its predictions with the engine record by record.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from streaming_lab.events import Event, InvalidEvent, epoch_ms, window_start_ms


@dataclass
class WindowModel:
    window_seconds: int = 60
    grace_seconds: int = 10
    out_of_order_seconds: int = 0
    max_event_ms: int | None = None
    watermark_ms: int | None = None
    seen: set[str] = field(default_factory=set)
    open_windows: set[int] = field(default_factory=set)
    closed_windows: dict[int, int] = field(default_factory=dict)  # start -> watermark at close

    @property
    def width_ms(self) -> int:
        return self.window_seconds * 1000

    def window_end(self, start_ms: int) -> int:
        return start_ms + self.width_ms

    def is_closed(self, start_ms: int, watermark_ms: int | None) -> bool:
        return watermark_ms is not None and watermark_ms >= self.window_end(start_ms) + self.grace_seconds * 1000

    def step(self, payload: Event | dict[str, Any]) -> dict[str, Any]:
        """Predict the disposition of one delivery and the resulting time state."""
        try:
            event = payload if isinstance(payload, Event) else Event.from_dict(payload)
        except (InvalidEvent, ValueError, TypeError, OverflowError):
            return self._state("invalid", [])
        if event.event_id in self.seen:
            return self._state("duplicate", [])
        self.seen.add(event.event_id)
        t = epoch_ms(event.event_time)
        start = window_start_ms(t, self.window_seconds)
        if self.is_closed(start, self.watermark_ms):
            return self._state("late", [], start)
        self.max_event_ms = t if self.max_event_ms is None else max(self.max_event_ms, t)
        candidate = self.max_event_ms - self.out_of_order_seconds * 1000
        self.watermark_ms = candidate if self.watermark_ms is None else max(self.watermark_ms, candidate)
        if event.kind == "order":
            self.open_windows.add(start)
        closing = sorted(s for s in self.open_windows if self.is_closed(s, self.watermark_ms))
        for s in closing:
            self.open_windows.discard(s)
            self.closed_windows[s] = self.watermark_ms
        return self._state("accepted", closing, start)

    def _state(self, status: str, closing: list[int], start: int | None = None) -> dict[str, Any]:
        return {"status": status, "window_start_ms": start, "max_event_time_ms": self.max_event_ms,
                "watermark_ms": self.watermark_ms, "closed_now": closing}
