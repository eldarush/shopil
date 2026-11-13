"""SHOPIL-18: run the continuous ShopIL stream with live progress.

    uv run python -m shopil_streaming.service --mode live --seconds 10
    uv run python -m shopil_streaming.service --mode microbatch --batch-size 5 --max-wait 0.2
    uv run python -m shopil_streaming.service --db runs/chat/stream.db --forever   # alongside the chat

A producer task keeps admitting deliveries into a bounded asyncio.Queue while the
consumer processes them through the course StreamEngine, so consumption progress
is visible while production is still active. Progress lines show produced,
consumed, queue depth, checkpoint, watermark and cumulative results.

Restart: re-running with the same --db resumes from the committed checkpoint
(engine.next_offset) and the deterministic source re-creates the same records.
Ctrl+C cancels both tasks; every committed event stays durable. Windows stay
open unless --finalize is passed (an explicit end-of-input declaration).
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable

from streaming_lab import StreamEngine, run_live, run_microbatch

from .runs import fresh_db, shekels
from .source import shopil_source

DEFAULT_CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def progress_line(snapshot: dict[str, Any]) -> str:
    q, s, t = snapshot["queue"], snapshot["stats"], snapshot["totals"]
    line = (f"produced={q['produced']:>5} consumed={q['consumed']:>5} depth={q['depth']}/{q['maxsize']} "
            f"waits={q['backpressure_waits']} | checkpoint={snapshot['checkpoint']} wm={snapshot['watermark']} | "
            f"orders={t['orders']} revenue=₪{shekels(t['revenue_cents'])} reviews={t['reviews']} "
            f"dup={s['duplicates']} late={s['late']} alerts={len(snapshot['alerts'])} "
            f"open={len(snapshot['open_windows'])} closed={len(snapshot['revenue_windows'])}")
    if "batch" in snapshot:
        b = snapshot["batch"]
        line += f" | batch#{b['index']} size={b['size']} reason={b['flush_reason']}"
    return line


def throttled_printer(every_s: float, stream=sys.stdout) -> Callable[[dict[str, Any]], None]:
    last = [0.0]

    def callback(snapshot: dict[str, Any]) -> None:
        now = time.perf_counter()
        if now - last[0] >= every_s:
            last[0] = now
            print(progress_line(snapshot), file=stream, flush=True)
    return callback


async def run(engine: StreamEngine, *, mode: str, seconds: float | None, interval_s: float,
              queue_size: int, batch_size: int, max_wait_s: float, processing_delay_s: float,
              finalize: bool, on_snapshot=None) -> dict[str, Any]:
    source = shopil_source(engine.next_offset)
    common = dict(duration_s=seconds, interval_s=interval_s, queue_maxsize=queue_size,
                  processing_delay_s=processing_delay_s, finalize=finalize, on_snapshot=on_snapshot)
    if mode == "live":
        return await run_live(engine, source, **common)
    return await run_microbatch(engine, source, max_batch_size=batch_size, max_wait_s=max_wait_s, **common)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=("live", "microbatch"), default="live")
    p.add_argument("--db", type=Path, help="Resume this DB (default: a fresh DB under runs/)")
    p.add_argument("--seconds", type=float, default=10.0, help="Producer admission period")
    p.add_argument("--forever", action="store_true", help="Run until Ctrl+C (for use alongside the chat)")
    p.add_argument("--interval", type=float, default=0.05, help="Seconds between produced records")
    p.add_argument("--queue-size", type=int, default=8)
    p.add_argument("--batch-size", type=int, default=5)
    p.add_argument("--max-wait", type=float, default=0.2)
    p.add_argument("--processing-delay", type=float, default=0.0, help="Per-record sink delay (backpressure demo)")
    p.add_argument("--print-every", type=float, default=0.5)
    p.add_argument("--finalize", action="store_true")
    args = p.parse_args(argv)
    db = args.db or fresh_db(f"service-{args.mode}")
    print(f"DB: {db.resolve()}  mode={args.mode}  config={DEFAULT_CONFIG}", flush=True)
    with StreamEngine(db, **DEFAULT_CONFIG) as engine:
        print(f"resuming at offset {engine.next_offset}", flush=True)
        try:
            state = asyncio.run(run(engine, mode=args.mode, seconds=None if args.forever else args.seconds,
                                    interval_s=args.interval, queue_size=args.queue_size,
                                    batch_size=args.batch_size, max_wait_s=args.max_wait,
                                    processing_delay_s=args.processing_delay, finalize=args.finalize,
                                    on_snapshot=throttled_printer(args.print_every)))
        except KeyboardInterrupt:
            print(f"stopped by user; committed checkpoint={engine.checkpoint}", flush=True)
            return 130
    summary = {k: v for k, v in state.items() if k not in ("snapshot", "batch_details", "batch_wait_s")}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
