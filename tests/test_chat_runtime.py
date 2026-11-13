"""Real loopback server lifecycle with an explicitly scripted model/test capture.

Run with ``uv run pytest tests/test_chat_runtime.py``. The child uses the same
interpreter as pytest; neither a model service nor production data is required.
"""

import asyncio
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import httpx
import pytest
from gradio_client import Client

from chat import runtime


class ScriptedModel:
    def __init__(self, wait_stage="complete", marker=None):
        self.wait_stage, self.marker = wait_stage, marker

    async def complete(self, messages, *, schema=None):
        if self.wait_stage == ("actor" if schema is None else "critic"):
            self.marker.touch()
            await asyncio.Event().wait()
        if schema is None:
            evidence = json.loads(messages[3]["content"])
            return f"לפי test_metrics: ההכנסה היא {evidence['paid_revenue_ils']} ש״ח."
        evidence = json.loads(messages[2]["content"])
        candidate = json.loads(messages[3]["content"])["candidate_answer"]
        return json.dumps({"is_acceptable": candidate ==
                           f"לפי test_metrics: ההכנסה היא {evidence['paid_revenue_ils']} ש״ח.",
                           "feedback": "נבדק מול נתון הבדיקה."}, ensure_ascii=False)


async def fixture_capture(question, history):
    return {"question": question, "snapshot_id": "server-fixture",
            "captured_at": "2026-11-13T12:00:00+00:00", "paid_revenue_ils": "12.50",
            "sources": [{"table": "test_metrics", "unit": "ILS"}]}


@pytest.mark.parametrize("shutdown,stage", [
    ("eof", "complete"), ("newline", "complete"), ("eof", "actor"), ("newline", "critic"),
])
def test_real_server_audits_completed_or_interrupted_question_before_shutdown(tmp_path, shutdown, stage):
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONPATH=str(root), PYTHONUTF8="1",
                       PYTHONDONTWRITEBYTECODE="1", GRADIO_ANALYTICS_ENABLED="False")
    log_path = tmp_path / "server.log"
    with log_path.open("w", encoding="utf-8") as log:
        child = subprocess.Popen(
            [sys.executable, "-X", "utf8", "-u", str(Path(__file__).resolve()),
             str(port), str(tmp_path / "audit"), stage, str(tmp_path / "model-started")], cwd=root, env=environment,
            stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True,
        )
        client = None
        try:
            deadline = time.monotonic() + 30
            with httpx.Client(trust_env=False, timeout=0.5) as probe:
                while True:
                    assert child.poll() is None, log_path.read_text(encoding="utf-8")
                    try:
                        response = probe.get(url + "/config")
                        if response.status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    assert time.monotonic() < deadline, "Gradio failed to become ready"
                    time.sleep(0.05)
            client = Client(url, verbose=False)
            job = client.submit("מה ההכנסה?", api_name="/ask")
            if stage == "complete":
                result = job.result(timeout=20)
                assert "12.50" in str(result[0])
                assert result[2] == [{"table": "test_metrics", "unit": "ILS"}]
                assert "server-fixture" in result[3]
            else:
                deadline = time.monotonic() + 15
                while not (tmp_path / "model-started").exists():
                    assert child.poll() is None
                    assert time.monotonic() < deadline, "Scripted model did not start"
                    time.sleep(.05)
            assert child.poll() is None
            if shutdown == "newline":
                child.stdin.write("\n")
                child.stdin.flush()
            else:
                child.stdin.close()
            assert child.wait(timeout=15) == 0, log_path.read_text(encoding="utf-8")
            events = [json.loads(line) for line in next((tmp_path / "audit").glob("*.jsonl"))
                      .read_text(encoding="utf-8").splitlines()]
            assert [event["event"] for event in events] == ["snapshot", "attempt", "terminal"]
            assert events[-1]["status"] == ("approved" if stage == "complete" else "error")
            if stage != "complete":
                assert events[-1]["error_stage"] == stage
                assert events[-1]["error_type"] == "CancelledError"
                assert events[1]["is_acceptable"] is None
                assert (events[1]["draft"] is None) == (stage == "actor")
            with socket.socket() as probe:
                assert probe.connect_ex(("127.0.0.1", port)) != 0
        finally:
            if client:
                client.close()
            if child.stdin and not child.stdin.closed:
                child.stdin.close()
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)


def test_standalone_interrupt_closes_server(monkeypatch, tmp_path):
    class App:
        closed = False

        def launch(self, **options):
            assert options["server_name"] == "127.0.0.1"
            assert options["share"] is False

        def close(self):
            self.closed = True

    class InterruptedWait:
        def wait(self, timeout):
            raise KeyboardInterrupt

    app = App()
    monkeypatch.setattr(runtime, "build_app", lambda service: app)
    monkeypatch.setattr(runtime, "Event", InterruptedWait)
    runtime.run_app(fixture_capture, ScriptedModel(), tmp_path)
    assert app.closed


def test_shutdown_failure_closes_server_and_propagates_unsuccessful_exit(monkeypatch, tmp_path):
    calls = []

    class App:
        def launch(self, **kwargs):
            pass

        def close(self):
            calls.append("closed")

    class Service:
        def shutdown(self):
            calls.append("shutdown")
            raise RuntimeError("cancellation audit failed")

    monkeypatch.setattr(runtime, "ChatService", lambda *args: Service())
    monkeypatch.setattr(runtime, "build_app", lambda service: App())
    monkeypatch.setattr(runtime.sys, "stdin", io.StringIO(""))
    with pytest.raises(RuntimeError, match="audit failed"):
        runtime.run_app(fixture_capture, ScriptedModel(), tmp_path, managed=True)
    assert calls == ["shutdown", "closed"]


@pytest.mark.parametrize("port", [True, 0, 65536, "7860"])
def test_invalid_port_fails_before_launch(port, tmp_path):
    with pytest.raises(ValueError, match="port"):
        runtime.run_app(fixture_capture, ScriptedModel(), tmp_path, port=port)


if __name__ == "__main__":
    runtime.run_app(fixture_capture, ScriptedModel(sys.argv[3], Path(sys.argv[4])), Path(sys.argv[2]),
                    managed=True, port=int(sys.argv[1]))
