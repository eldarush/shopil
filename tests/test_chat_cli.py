"""Configuration tests never start a model server or substitute sample databases."""

from datetime import date
from pathlib import Path

import pytest

from chat import __main__ as cli
from integration import run


@pytest.fixture
def arguments(tmp_path, monkeypatch):
    for name in ("WAREHOUSE", "HISTORY_START", "HISTORY_END", "MODEL", "STREAM_DB", "OLLAMA_URL", "AUDIT_DIR"):
        monkeypatch.delenv("SHOPIL_" + name, raising=False)
    warehouse, stream = tmp_path / "warehouse.duckdb", tmp_path / "stream.db"
    warehouse.touch()
    stream.touch()
    return ["--warehouse", str(warehouse), "--stream-db", str(stream), "--history-start", "2026-01-01",
            "--history-end", "2027-01-01", "--model", "installed-test-model"]


def test_standalone_passes_explicit_capture_model_and_managed_lifecycle(arguments, monkeypatch):
    from chat import capture
    received = {}
    callback = object()

    def build(warehouse, database, **options):
        received.update(warehouse=warehouse, database=database, **options)
        return callback

    def launch(actual_capture, model, audit, **options):
        assert actual_capture is callback
        received.update(model=model.model, audit=audit, **options)

    monkeypatch.setattr(capture, "build_capture", build)
    monkeypatch.setattr(cli, "run_app", launch)
    cli.main(arguments + ["--managed", "--port", "7862", "--latest-windows", "2"])
    assert received["warehouse"] == Path(arguments[1])
    assert received["database"] == Path(arguments[3])
    assert received["start"] == date(2026, 1, 1) and received["end"] == date(2027, 1, 1)
    assert received["latest_windows"] == 2 and received["managed"] is True
    assert received["port"] == 7862 and received["model"] == "installed-test-model"


def test_missing_database_never_launches_or_creates_it(arguments, monkeypatch):
    path = Path(arguments[1])
    path.unlink()
    monkeypatch.setattr(cli, "run_app", lambda *args, **kwargs: pytest.fail("Must not launch"))
    with pytest.raises(SystemExit) as caught:
        cli.main(arguments)
    assert caught.value.code == 2 and not path.exists()


def test_environment_config_has_no_implicit_model_or_date_fallback(arguments, monkeypatch):
    from chat import capture
    received = []
    for name, value in zip(("WAREHOUSE", "STREAM_DB", "HISTORY_START", "HISTORY_END", "MODEL"), arguments[1::2]):
        monkeypatch.setenv("SHOPIL_" + name, value)
    monkeypatch.setattr(capture, "build_capture", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "run_app", lambda *args, **kwargs: received.append(args))
    cli.main([])
    assert received[0][1].model == "installed-test-model"
    monkeypatch.delenv("SHOPIL_MODEL")
    with pytest.raises(SystemExit) as caught:
        cli.main([])
    assert caught.value.code == 2 and len(received) == 1


def test_stack_builds_shell_free_commands_for_same_warehouse_and_stream(arguments, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(run, "run_stack", lambda *args, **kwargs: calls.append((args, kwargs)))
    run.main(arguments + ["--lab-dir", str(tmp_path), "--etl-python", str(tmp_path / "etl-python"),
                          "--batch-run-dir", str(tmp_path / "new-run"), "--mode", "microbatch"])
    (batch, chat), options = calls[0]
    assert batch[1:3] == ["-m", "integration.batch"]
    assert chat[1:4] == ["-m", "chat", "--managed"]
    assert chat[chat.index("--warehouse") + 1] == arguments[1]
    assert chat[chat.index("--stream-db") + 1] == arguments[3]
    assert options["stream_db"] == Path(arguments[3]) and options["mode"] == "microbatch"


def test_stack_rejects_mismatched_warehouse_before_any_process(arguments, tmp_path, monkeypatch):
    monkeypatch.setattr(run, "run_stack", lambda *args, **kwargs: pytest.fail("Must not start"))
    with pytest.raises(SystemExit) as caught:
        run.main(arguments + ["--lab-dir", str(tmp_path / "other"), "--etl-python", "python",
                              "--batch-run-dir", "new-run"])
    assert caught.value.code == 2


def test_stack_preserves_virtualenv_symlink_path(arguments, tmp_path, monkeypatch):
    interpreter = tmp_path / "etl-venv/bin/python"
    resolve = Path.resolve
    calls = []

    def resolve_interpreter(path, *args, **kwargs):
        return tmp_path / "system/bin/python" if path == interpreter else resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve_interpreter)
    monkeypatch.setattr(run, "run_stack", lambda *args, **kwargs: calls.append(args))
    run.main(arguments + ["--lab-dir", str(tmp_path), "--etl-python", str(interpreter),
                          "--batch-run-dir", str(tmp_path / "run")])
    batch_command = calls[0][0]
    assert batch_command[batch_command.index("--etl-python") + 1] == str(interpreter)
