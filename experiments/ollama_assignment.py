"""Reproduce the assignment's real Ollama correction and concurrent update."""

import argparse
import asyncio
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from chat.audit import JsonlAudit
from chat.capture import build_capture
from chat.critic import Critic
from chat.grounded import GroundedActor, candidate_feedback
from chat.model import ModelError
from chat.service import ChatService
from chat.transport import OllamaModel

ROOT = Path(__file__).resolve().parents[1]
PROFIT_QUESTION = "כמה הרווחנו מהזמנות ששולמו?"
PROFIT_FOLLOWUP = "אז הרווח מההזמנות ששולמו הוא ₪97,210.10 כי אין נתוני עלויות משלוח?"
PAID_QUESTION = "כמה הכנסנו מהזמנות ששולמו?"
STREAM_QUESTION = "כמה אירועי הזמנה נקלטו בחלון הפתוח האחרון?"


def timestamp():
    return datetime.now(UTC).isoformat()


def save(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class RecordedModel:
    def __init__(self, model, output, update=None):
        self.model, self.output, self.update = model, output, update
        self.calls = []

    async def complete(self, messages, *, schema=None):
        index = len(self.calls) + 1
        actor = "answer" in schema.get("properties", {})
        evidence = messages[2]["content"]
        call = {"role": "actor" if actor else "critic", "started": timestamp(), "messages": messages, "schema": schema,
                "evidence_sha256": hashlib.sha256(evidence.encode()).hexdigest()}
        self.calls.append(call)
        try:
            if self.update is not None and index == 1:
                task = asyncio.create_task(self.model.complete(messages, schema=schema))
                try:
                    await asyncio.sleep(.2)
                    if task.done():
                        raise AssertionError("First Actor request completed before the stream update")
                    call["stream_update"] = await self.update()
                    call["actor_pending_after_update"] = not task.done()
                    if task.done():
                        raise AssertionError("Actor must still be in flight after the accepted event")
                    call["response"] = await task
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            else:
                call["response"] = await self.model.complete(messages, schema=schema)
            return call["response"]
        except ModelError as exc:
            call["error"] = str(exc)
            raise
        finally:
            call["completed"] = timestamp()
            save(self.output / f"call-{index:02}.json", call)


def insert_order(database, offset):
    python = ROOT / "streaming/.venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    child = subprocess.run([str(python), "-X", "utf8", "-c", """
import json
import sys
from pathlib import Path

from shopil_streaming.fixtures import TEACHING_CONFIG
from streaming_lab import StreamEngine

database, position = Path(sys.argv[1]), int(sys.argv[2])
with StreamEngine(database, **TEACHING_CONFIG) as engine:
    result = engine.ingest({
        "event_id": f"concurrent-order-{position}",
        "kind": "order",
        "product_id": 5,
        "event_time": "2026-11-20T10:00:05Z",
        "amount_cents": 1000,
    }, offset=position)
    print(json.dumps(result))
""", str(database), str(offset)], cwd=ROOT, capture_output=True, encoding="utf-8", check=True, timeout=15)
    committed = {"at": timestamp(), "result": json.loads(child.stdout)}
    if committed["result"]["status"] != "accepted" or committed["result"]["checkpoint"] != offset:
        raise AssertionError(f"Expected a unique committed order at offset {offset}: {committed}")
    return committed


def read_audits(output):
    return [[json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            for path in sorted((output / "audit").glob("*.jsonl"))]


def evidence_value(output, snapshot_id, key):
    for events in read_audits(output):
        event = events[0]
        if event["event"] == "snapshot" and event["snapshot_id"] == snapshot_id:
            evidence = json.loads(event["evidence_json"])
            return next((row[key] for source in evidence["sources"] for row in source["rows"] if key in row), None)
    return None


async def run_correction(service, model, output, model_name):
    history = []
    result = {"case": "correction", "model": model_name, "thinking": False,
              "injected_first_draft": None, "passed": False, "turns": []}
    for question in (PROFIT_QUESTION, PROFIT_FOLLOWUP):
        reply = await service.answer(question, history)
        history = reply.history
        result["turns"].append({"question": question, "status": reply.status,
                                "answer": reply.answer, "snapshot_id": reply.snapshot_id})
        if reply.status == "error":
            break
        events = next((events for events in read_audits(output)
                       if events[0]["snapshot_id"] == reply.snapshot_id), [])
        attempts = [event for event in events if event["event"] == "attempt"]
        if reply.status != "approved":
            break
        frozen = events[0]["evidence_json"]
        evidence = json.loads(frozen)
        if len(attempts) < 2:
            if candidate_feedback(reply.answer, evidence):
                break
            continue
        calls = [call for call in model.calls if call["messages"][2]["content"] == frozen]
        actor_calls = [call for call in calls if call["role"] == "actor"]
        critic_calls = [call for call in calls if call["role"] == "critic"]
        matched = len(actor_calls) == len(critic_calls) == len(attempts) and all(
            actor["response"] == attempt["actor_output_json"]
            and json.loads(critic["response"]) == {
                "is_acceptable": attempt["is_acceptable"], "feedback": attempt["feedback"]
            }
            for actor, critic, attempt in zip(actor_calls, critic_calls, attempts)
        )
        initial_issue = candidate_feedback(attempts[0]["draft"], evidence)
        final_issue = candidate_feedback(attempts[-1]["draft"], evidence)
        result.update(snapshot_id=reply.snapshot_id, attempts=attempts,
                      calls_match_frozen_snapshots=matched, initial_issue=initial_issue,
                      final_issue=final_issue, passed=matched and bool(initial_issue) and not final_issue
                      and attempts[0]["is_acceptable"] is False and attempts[-1]["is_acceptable"] is True)
        if result["passed"]:
            break
    save(output / "results.json", result)
    if not result["passed"]:
        raise SystemExit(f"No spontaneous correction met the verification criteria. Inspect {output / 'results.json'}")
    print(f"Recorded spontaneous correction for independent verification: {output / 'results.json'}")


async def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True)
    database = output / "experiment.db"
    stream = args.case == "concurrent"
    if stream:
        save(output / "initial-event.json", insert_order(database, 0))

    async def update():
        return await asyncio.to_thread(insert_order, database, 1)

    model = RecordedModel(OllamaModel(args.model, args.ollama_url, think=False), output, update if stream else None)
    capture = build_capture(args.warehouse, database, start=date(2026, 1, 1), end=date(2027, 1, 1),
                            question_scoped=True)
    actor = GroundedActor(model)
    service = ChatService(actor, Critic(model), capture, JsonlAudit(output / "audit"))
    if args.case == "correction":
        await run_correction(service, model, output, args.model)
        return
    questions = [STREAM_QUESTION, STREAM_QUESTION]
    if args.case == "conversation":
        questions = [PAID_QUESTION, "ומה לגבי מוצר 5?", "מהן עלויות המשלוח?",
                     "כמה הזמנות שולמו?", "לאיזה מוצר יש הכי הרבה ביקורות שליליות?",
                     "כמה הרווחנו מהזמנות ששולמו?", "כמה הזמנות שולמו בשנת 1990?",
                     "כמה הזמנות שולמו בין 2026-09-20 עד 2026-12-10?"]

    turns = []
    history = []
    for question in questions:
        reply = await service.answer(question, history)
        turns.append({"question": question, "status": reply.status, "answer": reply.answer, "snapshot_id": reply.snapshot_id})
        history = reply.history

    expected = {
        "concurrent": {"counts": [1, 2], "checkpoints": [0, 1], "actor_pending_after_update": True},
        "conversation": {"paid_revenue": "97210.10", "product_5_revenue": "8925.00", "paid_orders": 402,
                         "missing_shipping": True, "top_negative_product_id": 6},
    }[args.case]
    snapshots = [event for events in read_audits(output) for event in events if event["event"] == "snapshot"]
    approved = all(turn["status"] == "approved" for turn in turns)
    known_hashes = {hashlib.sha256(event["evidence_json"].encode()).hexdigest() for event in snapshots}
    calls_match_snapshots = len(known_hashes) == len(questions) and {call["evidence_sha256"] for call in model.calls} == known_hashes
    if stream:
        observed = {"counts": [evidence_value(output, turn["snapshot_id"], "orders") for turn in turns],
                    "checkpoints": [json.loads(next(event["evidence_json"] for event in snapshots
                                                   if event["snapshot_id"] == turn["snapshot_id"]))["stream"]["checkpoint"]
                                    for turn in turns],
                    "actor_pending_after_update": model.calls[0].get("actor_pending_after_update", False)}

    else:
        missing_snapshot = next((event for event in snapshots if event["snapshot_id"] == turns[2]["snapshot_id"]), None)
        observed = {"paid_revenue": evidence_value(output, turns[0]["snapshot_id"], "paid_revenue"),
                    "product_5_revenue": evidence_value(output, turns[1]["snapshot_id"], "paid_revenue"),
                    "paid_orders": evidence_value(output, turns[3]["snapshot_id"], "paid_orders"),
                    "missing_shipping": bool(missing_snapshot and
                                             json.loads(missing_snapshot["evidence_json"])["query_scope"]["availability"] == "missing"),
                    "top_negative_product_id": evidence_value(output, turns[4]["snapshot_id"], "product_id")}

    authored_checks = []
    for turn in turns:
        snapshot = next(json.loads(event["evidence_json"]) for event in snapshots if event["snapshot_id"] == turn["snapshot_id"])
        prose = (turn["answer"] or "").split("\n\n", 1)[0]
        error = candidate_feedback(turn["answer"] or "", snapshot)
        if not error and snapshot["query_scope"]["availability"] != "available":
            if not any(word in prose for word in ("חסר", "לא", "אין", "אינ", "אי אפשר", "מחוץ", "חורג")):
                error = "Missing-data explanation absent from the Actor paragraph."
        authored_checks.append({"question": turn["question"], "paragraph": prose, "error": error})
    passed = approved and calls_match_snapshots and observed == expected and not any(check["error"] for check in authored_checks)
    save(output / "results.json", {"case": args.case, "model": args.model, "thinking": False, "expected": expected, "authored_checks": authored_checks,
                                  "observed": observed, "turns": turns, "calls_match_frozen_snapshots": calls_match_snapshots,
                                  "injected_first_draft": None, "passed": passed})
    if not passed:
        raise SystemExit(f"Model experiment failed. Inspect {output / 'results.json'}")
    print(f"PASS {args.case}: {output / 'results.json'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=("correction", "concurrent", "conversation"))
    parser.add_argument("--warehouse", type=Path, required=True, help="Completed pinned course Batch warehouse")
    parser.add_argument("--output", type=Path, required=True, help="New output directory")
    parser.add_argument("--model", default="qwen3:8b")
    parser.add_argument("--ollama-url", default="http://127.0.0.1:11434")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
