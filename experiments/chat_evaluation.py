"""SHOPIL-42: a real rejection/correction run and a separately mocked limit check.

The model is supplied by the caller; no model or expected decision is fabricated.
This finite teaching fixture is an experiment, not the production retrieval layer.
"""

from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
from uuid import uuid4

from chat.actor import Actor
from chat.audit import JsonlAudit
from chat.critic import Critic, Evaluation
from chat.service import ChatService

ROOT = Path(__file__).resolve().parents[1]
QUESTION = "מה סכום אירועי ההזמנה שהתקבלו לצבירה בזרם, ללא האירועים המאוחרים?"
BAD_DRAFT = "ההכנסה שהתקבלה לצבירה בזרם היא 135.00 ש״ח."


def teaching_evidence(database: Path) -> dict:
    """Build a fresh course fixture, then read all evidence in one SQL transaction."""
    database = database.resolve()
    if database.exists():
        raise FileExistsError(database)
    fixture_dir = ROOT / "streaming/fixtures/streaming"
    manifest = json.loads((fixture_dir / "manifest.json").read_text(encoding="utf-8"))
    fixture_hash = hashlib.sha256((fixture_dir / "teaching.jsonl").read_text(encoding="utf-8").encode()).hexdigest()
    if fixture_hash != manifest["fixtures"]["teaching"]["sha256"]:
        raise ValueError("Teaching fixture does not match its committed manifest")
    executable = ROOT / "streaming/.venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run([str(executable), "-X", "utf8", "-c", """
import sys
from pathlib import Path
from shopil_streaming.fixtures import load_fixture, TEACHING_CONFIG
from streaming_lab import StreamEngine
path = Path(sys.argv[1])
if path.exists():
    raise FileExistsError(path)
with StreamEngine(path, **TEACHING_CONFIG) as engine:
    for delivery in load_fixture('teaching'):
        engine.ingest(delivery.event, offset=delivery.offset)
    engine.finalize_demo()
""", str(database)], check=True, cwd=ROOT, timeout=30)
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")
        meta = dict(connection.execute("SELECT * FROM stream_meta WHERE singleton=1").fetchone())
        windows = [dict(row) for row in connection.execute(
            "SELECT * FROM realtime_revenue ORDER BY window_start_ms")]
        late = [dict(row) for row in connection.execute(
            "SELECT event_id,event_time_ms,amount_cents FROM late_events WHERE kind='order' ORDER BY source_offset")]
        accepted = [dict(row) for row in connection.execute(
            "SELECT event_id,json_extract(payload_json,'$.product_id') AS product_id "
            "FROM seen_events WHERE kind='order' AND disposition='accepted' ORDER BY source_offset")]
        accepted_cents = connection.execute("SELECT sum(revenue_cents) FROM realtime_revenue").fetchone()[0]
        late_cents = connection.execute("SELECT sum(amount_cents) FROM late_events WHERE kind='order'").fetchone()[0]
    if (accepted_cents, late_cents, meta["checkpoint"]) != (12200, 1300, 16):
        raise AssertionError("Teaching fixture differs from the assignment reference")

    def iso(milliseconds):
        return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()

    for row in windows:
        for field in ("window_start_ms", "window_end_ms", "emitted_at_watermark_ms"):
            row[field.removesuffix("_ms")] = iso(row.pop(field))
    return {
        "snapshot_id": uuid4().hex,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "fixture": {"path": "streaming/fixtures/streaming/teaching.jsonl", "sha256_utf8_lf": fixture_hash},
        "stream_config": json.loads(meta["config_json"]),
        "sources": [{
            "table": "realtime_revenue; late_events; seen_events; stream_meta",
            "metric": "stream order events (no payment-status field); accepted and late kept separate",
            "units": "integer cents and precomputed ILS strings",
            "time_start": windows[0]["window_start"],
            "time_end": windows[-1]["window_end"],
            "checkpoint": meta["checkpoint"], "watermark": iso(meta["watermark_ms"]),
            "window_state": "closed",
            "finalization": "Finite fixture: remaining windows closed by demo_end_of_input; watermark unchanged",
            "rows": [{"accepted_cents": accepted_cents, "accepted_ils": str(Decimal(accepted_cents) / 100),
                      "late_cents": late_cents, "late_ils": str(Decimal(late_cents) / 100)}],
            "windows": windows, "accepted_event_ids_and_products": accepted,
            "late_event_ids": [row["event_id"] for row in late],
        }],
    }


class FaultyFirstDraft:
    """Assignment-permitted injection; every correction calls the actual Actor."""

    def __init__(self, actor):
        self.actor = actor

    async def draft(self, question, evidence_json, history, previous_draft=None, feedback=None):
        if previous_draft is None:
            return BAD_DRAFT
        return await self.actor.draft(question, evidence_json, history, previous_draft, feedback)


async def rejection_experiment(model, snapshot: dict, directory: Path) -> dict:
    """Return evidence, including failure, without treating schema validity as truth."""
    events = []
    durable = JsonlAudit(directory)

    def audit(event):
        durable(event)
        events.append(event)

    async def capture(question, history):
        return snapshot

    service = ChatService(FaultyFirstDraft(Actor(model)), Critic(model), capture, audit)
    reply = await service.answer(QUESTION, [])
    attempts = [event for event in events if event["event"] == "attempt"]
    passed = (reply.status == "approved" and len(attempts) in (2, 3)
              and attempts[0]["is_acceptable"] is False
              and attempts[-1]["is_acceptable"] is True
              and reply.answer != BAD_DRAFT)
    return {"case": "injected bad draft, real model correction and evaluation",
            "cycle_passed": passed, "manual_semantic_review_required": True,
            "status": reply.status, "answer": reply.answer,
            "snapshot_id": reply.snapshot_id, "events": events}


async def mock_three_rejections(snapshot: dict) -> dict:
    """Control-flow test only: both model roles below are explicitly mocked."""
    class MockActor:
        async def draft(self, *args):
            return BAD_DRAFT

    class MockCritic:
        async def evaluate(self, *args):
            return Evaluation(is_acceptable=False, feedback="Mock rejection: do not publish")

    async def capture(question, history):
        return snapshot

    events = []
    reply = await ChatService(MockActor(), MockCritic(), capture, events.append).answer(QUESTION, [])
    attempts = [event for event in events if event["event"] == "attempt"]
    return {"case": "MOCKED Actor and Critic: three rejections", "events": events,
            "passed": reply.status == "rejected" and len(attempts) == 3
            and reply.answer is None and reply.history == []}
