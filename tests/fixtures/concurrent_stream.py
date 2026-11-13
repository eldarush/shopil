"""Finite IPC-controlled producer for the concurrent-snapshot integration test."""

import asyncio
from datetime import timedelta
import json
import sys

from streaming_lab import Delivery, Event, StreamEngine, run_live, run_microbatch
from streaming_lab.events import T0


def review(number, second):
    return Event(f"review-{number}", "review", T0 + timedelta(seconds=second), 5,
                 rating=1, text=f"Negative review {number}")


def main():
    database, mode = sys.argv[1:]
    initial = [
        Event("order-1", "order", T0 + timedelta(seconds=5), 5, amount_cents=1000),
        review(1, 10), review(2, 12),
    ]
    incoming = [
        review(3, 15), review(4, 18),
        Event("order-2", "order", T0 + timedelta(seconds=75), 5, amount_cents=2000),
    ]
    with StreamEngine(database, window_seconds=60, grace_seconds=10,
                      out_of_order_seconds=0, alert_threshold=3) as engine:
        for offset, event in enumerate(initial):
            engine.ingest(event, offset=offset)
        print(json.dumps({"status": "ready", "checkpoint": engine.checkpoint,
                          "python": list(sys.version_info[:2])}), flush=True)
        if sys.stdin.readline().strip() != "produce":
            return
        deliveries = [Delivery(offset, event) for offset, event in enumerate(incoming, start=3)]
        options = {"duration_s": None, "interval_s": 0.01, "queue_maxsize": 1,
                   "processing_delay_s": 0.02, "finalize": False}
        if mode == "microbatch":
            runner = run_microbatch(engine, deliveries, max_batch_size=2, max_wait_s=0.1, **options)
        elif mode == "live":
            runner = run_live(engine, deliveries, **options)
        else:
            raise ValueError("Unknown test mode")
        result = asyncio.run(runner)
        print(json.dumps({"status": "updated", "checkpoint": engine.checkpoint,
                          "produced": result["produced"], "consumed": result["consumed"],
                          "finalized": result["snapshot"]["finalized"]}), flush=True)
        # Stay alive while the parent asks the second question; EOF closes cleanly.
        sys.stdin.readline()


if __name__ == "__main__":
    main()
