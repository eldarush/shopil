"""SHOPIL-43: actual SQLite streaming during an Actor/Critic answer and retry.

Run after both uv environments are synced: ``uv run pytest tests/test_concurrent_snapshot.py``.
SHOPIL_STREAMING_PYTHON may select an existing isolated streaming interpreter for a
worktree. Both runner modes use identical ordered events and fresh databases.

The model is explicitly scripted: this checks concurrency, snapshots, audit and
history, not language-model quality. The chat fixture wraps the read-only SQLite snapshot reader. Expected update:
checkpoint 2 -> 5; accepted reviews 2 -> 4; one alert; the first revenue window
closes at event-time watermark 75s (end 60s + grace 10s), without finalization.
"""

import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from chat.actor import Actor
from chat.audit import JsonlAudit
from chat.critic import Critic
from chat.service import ChatService
from data_access.streaming import WINDOW_TABLES, read_stream_snapshot


ROOT = Path(__file__).resolve().parents[1]

def capture_fixture(database, question):
    return {
        "snapshot_id": uuid4().hex,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "question": question,
        "sources": [{"table": table, "units": "integer cents or counts"}
                    for table, _ in WINDOW_TABLES],
        "stream": read_stream_snapshot(database),
    }


class BarrierModel:
    """First Actor call waits while a separate process commits the new events."""

    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.evidence = []
        self.histories = []
        self.corrections = []
        self.evaluations = 0

    async def complete(self, messages, *, schema=None):
        if schema is None:
            self.evidence.append(messages[3]["content"])
            self.histories.append(json.loads(messages[1]["content"])["approved_history"])
            if len(self.evidence) == 1:
                self.entered.set()
                await self.release.wait()
                return "טיוטה ראשונה לבדיקה"
            if len(messages) == 5:
                self.corrections.append(json.loads(messages[4]["content"]))
            snapshot = json.loads(messages[3]["content"])
            count = snapshot["stream"]["counters"]["accepted_reviews"]
            checkpoint = snapshot["stream"]["checkpoint"]
            return f"לפי review_state ו-review_windows, בצילום המצב בנקודת המשך {checkpoint} התקבלו {count} ביקורות."
        self.evidence.append(messages[2]["content"])
        self.evaluations += 1
        return json.dumps({"is_acceptable": self.evaluations > 1,
                           "feedback": "יש לציין את המונה והמקור." if self.evaluations == 1
                           else "המונה ונקודת ההמשך תואמים לצילום המצב."}, ensure_ascii=False)


async def read_worker(process):
    line = await asyncio.wait_for(process.stdout.readline(), timeout=15)
    if not line:
        error = await asyncio.wait_for(process.stderr.read(), timeout=5)
        raise AssertionError(f"Streaming fixture exited unexpectedly: {error.decode(errors='replace')}")
    return json.loads(line)


@pytest.mark.parametrize("mode", ["live", "microbatch"])
def test_incoming_events_do_not_change_active_answer_but_next_question_refreshes(tmp_path, mode):
    async def scenario():
        default = ROOT / "streaming/.venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        python = Path(os.environ.get("SHOPIL_STREAMING_PYTHON", str(default)))
        assert python.is_file(), "Run uv sync --project streaming --locked before this integration test"
        database = tmp_path / "stream.db"
        environment = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
        environment["PYTHONPATH"] = str(ROOT / "streaming/src")
        process = await asyncio.create_subprocess_exec(
            str(python), "-X", "utf8", "-u", str(ROOT / "tests/fixtures/concurrent_stream.py"),
            str(database), mode, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=environment,
        )
        answer_task = None
        try:
            assert await read_worker(process) == {"status": "ready", "checkpoint": 2, "python": [3, 13]}
            model = BarrierModel()
            captures = []

            async def capture(question, history):
                snapshot = capture_fixture(database, question)
                captures.append(snapshot)
                return snapshot

            service = ChatService(Actor(model), Critic(model), capture, JsonlAudit(tmp_path / "audit"))
            answer_task = asyncio.create_task(service.answer("כמה ביקורות התקבלו?", []))
            await asyncio.wait_for(model.entered.wait(), timeout=5)
            process.stdin.write(b"produce\n")
            await process.stdin.drain()
            assert await read_worker(process) == {
                "status": "updated", "checkpoint": 5, "produced": 3, "consumed": 3, "finalized": False,
            }
            assert not answer_task.done(), "Stream must advance while the first answer remains in progress"
            assert process.returncode is None
            live = capture_fixture(database, "test observation")["stream"]
            assert live["checkpoint"] == 5
            model.release.set()
            first = await asyncio.wait_for(answer_task, timeout=5)
            second = await asyncio.wait_for(service.answer("ומה המונה כעת?", first.history), timeout=5)

            assert first.status == second.status == "approved"
            assert len(captures) == 2
            assert first.snapshot_id != second.snapshot_id
            assert len(model.evidence) == 6  # two Actor/Critic pairs, then one fresh pair
            assert len(set(model.evidence[:4])) == 1
            assert model.evidence[4] == model.evidence[5] != model.evidence[0]
            old = json.loads(model.evidence[0])["stream"]
            new = json.loads(model.evidence[4])["stream"]
            assert old["checkpoint"] == 2 and new["checkpoint"] == 5
            assert old["counters"]["accepted_reviews"] == 2
            assert new["counters"]["accepted_reviews"] == 4
            assert old["rows"]["realtime_revenue"] == old["rows"]["review_alerts"] == []
            assert old["rows"]["order_state"][0]["revenue_cents"] == 1000
            closed = new["rows"]["realtime_revenue"]
            assert len(closed) == 1 and closed[0]["revenue_cents"] == 1000
            assert closed[0]["closure_reason"] == "watermark"
            assert new["watermark_ms"] == closed[0]["window_end_ms"] + 15_000
            assert new["rows"]["order_state"][0]["revenue_cents"] == 2000
            assert new["rows"]["review_windows"][0]["negative_reviews"] == 4
            alerts = new["rows"]["review_alerts"]
            assert len(alerts) == 1 and alerts[0]["trigger_event_id"] == "review-3"
            assert not new["finalized"]
            assert model.corrections == [{"previous_draft": "טיוטה ראשונה לבדיקה",
                                          "feedback": "יש לציין את המונה והמקור."}]
            assert model.histories == [[], [], first.history]
            assert "2 ביקורות" in first.answer and "4 ביקורות" in second.answer
            assert all(row["content"] != "טיוטה ראשונה לבדיקה" for row in second.history)

            audit = [[json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                     for path in (tmp_path / "audit").glob("*.jsonl")]
            assert len(audit) == 2
            for events in audit:
                frozen = events[0]["evidence_json"]
                assert events[0]["event"] == "snapshot"
                assert frozen in (model.evidence[0], model.evidence[4])
                assert {event["snapshot_id"] for event in events} == {json.loads(frozen)["snapshot_id"]}
                assert events[-1]["status"] == "approved"
            old_events = next(events for events in audit if events[0]["snapshot_id"] == first.snapshot_id)
            assert [event["is_acceptable"] for event in old_events if event["event"] == "attempt"] == [False, True]
            assert old_events[-1]["attempts"] == 2
        finally:
            if answer_task is not None and not answer_task.done():
                answer_task.cancel()
                await asyncio.gather(answer_task, return_exceptions=True)
            process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.wait()
        assert process.returncode == 0

    asyncio.run(scenario())
