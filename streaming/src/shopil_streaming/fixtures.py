"""SHOPIL-37: versioned experiment inputs and independent databases.

Fixture files are JSON Lines in SOURCE order: {"offset", "event", "arrival_delay_s"}
per line. They are generated once from the course generators
(`python experiments/shopil_37_fixtures.py --write`) and committed, together with
`fixtures/streaming/manifest.json` (sha256, counts, settings, expected results).

Loading never sorts. Offsets must be exactly 0..n-1 in file order, otherwise the
loader refuses the file: sorting by event_time would change which events are
late, so it would silently change the experiment.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
from typing import Iterator

from streaming_lab import Delivery, Event, StreamEngine, generate_order_fixture, generate_review_fixture, teaching_fixture
from streaming_lab.events import InvalidEvent

from .runs import STREAMING_ROOT, fresh_db

FIXTURE_DIR = STREAMING_ROOT / "fixtures" / "streaming"
GENERATORS = {
    "teaching": ("streaming_lab.teaching_fixture()", teaching_fixture),
    "reviews": ("streaming_lab.generate_review_fixture()", generate_review_fixture),
    "orders300": ("streaming_lab.generate_order_fixture(n=300, seed=7)", lambda: generate_order_fixture(300, seed=7)),
}
TEACHING_CONFIG = {"window_seconds": 60, "grace_seconds": 10, "out_of_order_seconds": 0, "alert_threshold": 3}


def to_jsonl(deliveries: list[Delivery]) -> str:
    return "".join(json.dumps(d.to_dict(), ensure_ascii=False, sort_keys=True) + "\n" for d in deliveries)


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fixture_path(name: str, directory: Path = FIXTURE_DIR) -> Path:
    return directory / f"{name}.jsonl"


def load_fixture(name: str, directory: Path = FIXTURE_DIR) -> list[Delivery]:
    """Load a committed fixture in file (= source) order. Never sorts."""
    deliveries = []
    with fixture_path(name, directory).open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle):
            record = json.loads(line)
            if record["offset"] != line_no:
                raise ValueError(f"{name}: line {line_no} has offset {record['offset']}; offsets must be 0..n-1 in file order")
            try:
                event: Event | dict = Event.from_dict(record["event"])
            except InvalidEvent:
                event = record["event"]          # deliberately invalid input stays raw (-> dead_letters)
            deliveries.append(Delivery(record["offset"], event, record.get("arrival_delay_s", 0.0)))
    return deliveries


def manifest() -> dict:
    return json.loads((FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8"))


@contextmanager
def fresh_engine(tag: str, **config) -> Iterator[StreamEngine]:
    """A StreamEngine on a brand-new DB file; refuses to reuse existing state."""
    path = fresh_db(tag)
    if path.exists():
        raise FileExistsError(path)
    with StreamEngine(path, **(config or TEACHING_CONFIG)) as engine:
        yield engine
