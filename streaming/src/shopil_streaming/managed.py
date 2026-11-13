"""Stream until stdin requests shutdown, then drain every admitted delivery."""

import argparse
import asyncio
from collections.abc import Callable
import json
from pathlib import Path
import signal
import sys
from threading import Event, Thread

from streaming_lab import StreamEngine, run_live, run_microbatch

from .service import DEFAULT_CONFIG, throttled_printer
from .source import shopil_source


async def run_managed(
    engine: StreamEngine,
    stop_requested: Callable[[], bool],
    *,
    mode: str = "live",
    interval_s: float = 0.05,
    queue_size: int = 8,
    batch_size: int = 5,
    max_wait_s: float = 0.2,
    processing_delay_s: float = 0,
    on_snapshot=None,
) -> dict:
    if mode not in {"live", "microbatch"}:
        raise ValueError("mode must be live or microbatch")

    def deliveries():
        source = shopil_source(engine.next_offset)
        while not stop_requested():
            yield next(source)

    options = dict(duration_s=None, interval_s=interval_s, queue_maxsize=queue_size,
                   processing_delay_s=processing_delay_s, finalize=False,
                   on_snapshot=on_snapshot)
    if mode == "live":
        state = await run_live(engine, deliveries(), **options)
    else:
        state = await run_microbatch(engine, deliveries(), max_batch_size=batch_size,
                                     max_wait_s=max_wait_s, **options)
    if state["produced"] != state["consumed"]:
        raise RuntimeError("Stream stopped without draining admitted deliveries")
    return state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--mode", choices=("live", "microbatch"), default="live")
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--queue-size", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-wait", type=float, default=0.2)
    args = parser.parse_args(argv)
    stdin_stopped = Event()
    signal_stopped = False

    def request_stop(signum, frame):
        nonlocal signal_stopped
        # Signal handlers only set a flag; they must not acquire thread locks.
        signal_stopped = True

    def read_control():
        sys.stdin.readline()
        stdin_stopped.set()

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)
    previous = {item: signal.signal(item, request_stop) for item in signals}
    # A daemon reader cannot hold up interpreter shutdown after a stream failure.
    Thread(target=read_control, name="stream-control", daemon=True).start()
    try:
        with StreamEngine(args.db, **DEFAULT_CONFIG) as engine:
            state = asyncio.run(run_managed(
                engine, lambda: signal_stopped or stdin_stopped.is_set(), mode=args.mode,
                interval_s=args.interval, queue_size=args.queue_size,
                batch_size=args.batch_size, max_wait_s=args.max_wait,
                on_snapshot=throttled_printer(0.5),
            ))
        print(json.dumps({key: state[key] for key in ("produced", "consumed", "stop_reason")}),
              flush=True)
        return 0
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)


if __name__ == "__main__":
    raise SystemExit(main())
