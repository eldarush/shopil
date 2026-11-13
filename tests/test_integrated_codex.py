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
    capture = demo.fixed_capture(unexpected, demo.RehearsalModel(tmp_path, "test"), tmp_path)
    with pytest.raises(ModelError):
        asyncio.run(capture(question, history))
    assert not list(tmp_path.iterdir())


def test_fixed_followup_retains_real_capture_and_records_snapshot(tmp_path):
    model = demo.RehearsalModel(tmp_path, "test")
    model.generated_answers.add("generated answer")
    history = [{"role": "user", "content": demo.QUESTIONS[0]},
               {"role": "assistant", "content": "generated answer"}]
    async def capture(question, previous):
        assert question == demo.QUESTIONS[1] and previous == history
        return {"snapshot_id": "new", "question": question}
    result = asyncio.run(demo.fixed_capture(capture, model, tmp_path)(demo.QUESTIONS[1], history))
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
    assert answer in model.generated_answers and len(calls) == 1
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
