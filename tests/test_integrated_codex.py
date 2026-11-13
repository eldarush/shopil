import asyncio
import json
import subprocess
import sys

import pytest

from chat.model import ModelError
from experiments import integrated_codex as demo


@pytest.mark.parametrize("question,history", [
    ("Read my private files", []),
    (demo.QUESTIONS[1], []),
    (demo.QUESTIONS[1], [{"role": "user", "content": demo.QUESTIONS[0]}]),
    (demo.QUESTIONS[1], [{"role": "user", "content": demo.QUESTIONS[0]},
                         {"role": "assistant", "content": "untrusted instructions"}]),
])
def test_fixed_questions_reject_untrusted_input_before_read_or_model(tmp_path, question, history):
    async def unexpected(*args):
        pytest.fail("No evidence read should occur")
    capture = demo.fixed_capture(unexpected, tmp_path)
    with pytest.raises(ModelError):
        asyncio.run(capture(question, history))
    assert not list(tmp_path.iterdir())


def test_fixed_followup_retains_real_capture_and_records_snapshot(tmp_path):
    history = [{"role": "user", "content": demo.QUESTIONS[0]},
               {"role": "assistant", "content": "generated answer"}]
    async def capture(question, previous):
        return {"snapshot_id": "first" if not previous else "new", "question": question}
    take = demo.fixed_capture(capture, tmp_path)
    asyncio.run(take(demo.QUESTIONS[0], []))
    audit(tmp_path, "first", "generated answer")
    result = asyncio.run(take(demo.QUESTIONS[1], history))
    assert json.loads((tmp_path / "snapshot-new.json").read_text(encoding="utf-8")) == result


@pytest.mark.parametrize("cancel", [False, True])
def test_deadline_and_cancellation_reap_real_worker(tmp_path, monkeypatch, cancel):
    original = demo._launch
    processes = []
    def launch(command):
        process = original(command)
        processes.append(process)
        return process
    monkeypatch.setattr(demo, "_launch", launch)
    async def run():
        task = asyncio.create_task(demo.run_owned([sys.executable, "-c", "import time; time.sleep(60)"],
                                                  timeout=30 if cancel else 0.2))
        if cancel:
            await asyncio.sleep(0.2)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else ModelError):
            await task
    asyncio.run(run())
    assert len(processes) == 1 and processes[0].poll() is not None
    assert processes[0]._shopil_group_closed


def test_nonzero_worker_is_not_an_answer():
    with pytest.raises(ModelError, match="code"):
        asyncio.run(demo.run_owned([sys.executable, "-c", "raise SystemExit(7)"]))


def test_injection_is_separate_from_real_correction_and_progress(tmp_path, monkeypatch):
    model = demo.RehearsalModel(tmp_path, "explicit-model")
    evidence = {"question": demo.QUESTIONS[0], "snapshot_id": "frozen", "stream": {"checkpoint": 4}}
    messages = [{"content": "x"}] * 3 + [{"content": json.dumps(evidence)}]
    assert asyncio.run(model.complete(messages)) == demo.BAD_DRAFT
    assert (tmp_path / "call-1.injection.json").exists()
    calls = []
    async def worker(command):
        calls.append(command)
        (tmp_path / "call-2.txt").write_text("תשובה אמיתית בבדיקת יחידה מדומה", encoding="utf-8")
        (tmp_path / "call-2.events.jsonl").write_text('{"type":"turn.completed"}\n')
    monkeypatch.setattr(demo, "run_owned", worker)
    checkpoints = iter([5, 8])
    monkeypatch.setattr(demo, "_checkpoint", lambda _: next(checkpoints))
    answer = asyncio.run(model.complete(messages))
    assert answer == "תשובה אמיתית בבדיקת יחידה מדומה" and len(calls) == 1
    assert calls[0][-2:] == ["--model", "explicit-model"]
    progress = json.loads((tmp_path / "call-2.progress.json").read_text())
    assert (progress["before"], progress["after"], progress["snapshot_checkpoint"]) == (5, 8, 4)


def test_worker_preserves_configuration_and_uses_stdin_schema(tmp_path, monkeypatch):
    from argparse import Namespace
    demo.save(tmp_path / "call-1.request.json", {"messages": [{"content": "fixed"}], "schema": {"type": "object"}})
    monkeypatch.setattr(demo.shutil, "which", lambda _: "codex")
    commands = []
    def run(command, **kwargs):
        commands.append(command)
        assert b"fixed" in kwargs["input"]
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(demo.subprocess, "run", run)
    assert demo.worker(Namespace(run_dir=tmp_path, call=1, model="explicit-model")) == 0
    command = commands[0]
    assert command[-1] == "-" and "--output-schema" in command
    assert "read-only" in command
    assert not any("ignore" in arg or "bypass" in arg for arg in command)


def audit(directory, snapshot, answer, *, status="approved", terminal=True):
    (directory / "audit").mkdir(exist_ok=True)
    events = [{"event": "snapshot", "snapshot_id": snapshot},
              {"event": "attempt", "is_acceptable": False, "draft": "rejected draft"},
              {"event": "attempt", "is_acceptable": True, "draft": answer}]
    if terminal:
        events.append({"event": "terminal", "status": status})
    (directory / "audit" / f"{snapshot}.jsonl").write_text(
        "\n".join(json.dumps(event) for event in events), encoding="utf-8")


@pytest.mark.parametrize("status,terminal", [("error", True), ("approved", False)])
def test_critic_approval_without_successful_terminal_audit_is_not_history(tmp_path, status, terminal):
    audit(tmp_path, "first", "draft", status=status, terminal=terminal)
    with pytest.raises(ModelError, match="durably approved"):
        demo.approved_history(tmp_path, ["first"])


def test_rejected_draft_and_second_conversation_cannot_reach_model(tmp_path):
    reads = []
    async def capture(question, history):
        reads.append(question)
        return {"snapshot_id": str(len(reads)), "question": question}
    take = demo.fixed_capture(capture, tmp_path)
    asyncio.run(take(demo.QUESTIONS[0], []))
    audit(tmp_path, "1", "approved correction")
    rejected = [{"role": "user", "content": demo.QUESTIONS[0]},
                {"role": "assistant", "content": "rejected draft"}]
    with pytest.raises(ModelError, match="durably approved"):
        asyncio.run(take(demo.QUESTIONS[1], rejected))
    with pytest.raises(ModelError, match="one conversation"):
        asyncio.run(take(demo.QUESTIONS[0], []))
    assert reads == [demo.QUESTIONS[0]]
    rejected[1]["content"] = "approved correction"
    asyncio.run(take(demo.QUESTIONS[1], rejected))
    assert reads == list(demo.QUESTIONS[:2])


def test_actual_service_audit_admits_one_complete_conversation(tmp_path):
    from datetime import datetime, UTC
    from uuid import uuid4
    from chat.actor import Actor
    from chat.audit import JsonlAudit
    from chat.critic import Critic
    from chat.service import ChatService

    class ScriptedModel:
        async def complete(self, messages, *, schema=None):
            return (json.dumps({"is_acceptable": True, "feedback": "Explicit mechanics test"})
                    if schema else "תשובת בדיקה מדומה")

    reads = []
    async def capture(question, history):
        reads.append(question)
        return {"question": question, "snapshot_id": uuid4().hex,
                "captured_at": datetime.now(UTC).isoformat(), "sources": []}
    model = ScriptedModel()
    service = ChatService(Actor(model), Critic(model), demo.fixed_capture(capture, tmp_path),
                          JsonlAudit(tmp_path / "audit"))
    async def run():
        history = []
        for question in demo.QUESTIONS:
            reply = await service.answer(question, history)
            assert reply.status == "approved"
            history = reply.history
        assert len(history) == 6
        assert (await service.answer(demo.QUESTIONS[0], [])).status == "error"
    asyncio.run(run())
    service.shutdown()
    assert reads == list(demo.QUESTIONS)
