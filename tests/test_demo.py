import sys
from pathlib import Path

from integration import demo


def test_demo_chooses_separate_locked_runtime_environments(tmp_path, monkeypatch):
    (tmp_path / "course_inputs").mkdir()
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["demo", "--runs", str(tmp_path / "runs")])
    monkeypatch.setenv("UV_PROJECT_ENVIRONMENT", str(tmp_path / "chat"))
    calls = []
    commands = []
    monkeypatch.setattr(demo.subprocess, "run", lambda argv, **kw: calls.append((argv, kw)))
    monkeypatch.setattr(demo, "launch", commands.append)

    demo.main()

    streaming, etl = calls
    assert str(tmp_path / "streaming") in streaming[0]
    assert "UV_PROJECT_ENVIRONMENT" not in streaming[1]["env"]
    assert etl[0][-3:] == ["--locked", "--only-group", "batch"]
    command, = commands
    interpreter = Path(command[command.index("--etl-python") + 1])
    assert interpreter.parent.parent == Path(etl[1]["env"]["UV_PROJECT_ENVIRONMENT"])
    assert interpreter.parent.parent.name == ".venv-etl"
    assert interpreter != Path(sys.executable)
