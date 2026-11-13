"""Real subprocess and SQLite lifecycle tests; chat and batch are explicit stubs."""

from contextlib import closing
import os
import sqlite3
import sys
import time

import pytest

from integration import run


def python(code, *args):
    return [sys.executable, "-X", "utf8", "-c", code, *map(str, args)]


def track_children(monkeypatch):
    children = []
    launch = run._launch

    def track(*args, **kwargs):
        process = launch(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(run, "_launch", track)
    return children


def test_failed_batch_never_starts_stream_or_chat(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    marker = tmp_path / "chat-started"
    with pytest.raises(run.StackError, match="Batch failed"):
        run.run_stack(python("raise SystemExit(7)"),
                      python("from pathlib import Path;import sys;Path(sys.argv[1]).touch()", marker),
                      stream_db=tmp_path / "stream.db")
    assert len(children) == 1 and all(child.poll() is not None for child in children)
    assert not marker.exists() and not (tmp_path / "stream.db").exists()


@pytest.mark.parametrize("mode", ["live", "microbatch"])
def test_batch_finishes_then_stream_advances_while_chat_runs(tmp_path, monkeypatch, mode):
    children = track_children(monkeypatch)
    database, batch_marker = tmp_path / "stream.db", tmp_path / "batch-finished"
    batch = python("from pathlib import Path;import sys;Path(sys.argv[1]).touch()", batch_marker)
    chat = python("""
from pathlib import Path
import sqlite3, sys, time
assert Path(sys.argv[1]).is_file(), 'batch was not completed'
with sqlite3.connect(sys.argv[2]) as db:
    start = db.execute('SELECT checkpoint FROM stream_meta').fetchone()[0]
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        current = db.execute('SELECT checkpoint FROM stream_meta').fetchone()[0]
        if current > start:
            break
        time.sleep(.05)
    else:
        raise AssertionError('stream stopped while chat was active')
""", batch_marker, database)
    run.run_stack(batch, chat, stream_db=database, mode=mode)
    assert len(children) == 3 and all(child.poll() == 0 for child in children)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT checkpoint FROM stream_meta").fetchone()[0] > 0
        assert connection.execute("SELECT finalized FROM stream_meta").fetchone()[0] == 0


def test_stream_failure_stops_chat_cooperatively_and_leaves_no_children(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    database, stopped = tmp_path / "stream.db", tmp_path / "chat-stopped"
    chat = python("""
from pathlib import Path
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    db.execute('DROP TABLE stream_meta')
sys.stdin.readline()
Path(sys.argv[2]).touch()
""", database, stopped)
    with pytest.raises(run.StackError, match="Streaming"):
        run.run_stack(python("pass"), chat, stream_db=database, shutdown_timeout=3)
    assert stopped.exists()
    assert all(child.poll() is not None for child in children)


def test_chat_that_ignores_shutdown_is_force_stopped_and_reported(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    database = tmp_path / "stream.db"
    chat = python("""
import sqlite3, sys, time
with sqlite3.connect(sys.argv[1]) as db:
    db.execute('DROP TABLE stream_meta')
time.sleep(120)
""", database)
    with pytest.raises(run.StackError, match="forced termination"):
        run.run_stack(python("pass"), chat, stream_db=database, shutdown_timeout=0.2)
    assert all(child.poll() is not None for child in children)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_invalid_timeouts_do_not_start_any_process(tmp_path, monkeypatch, timeout):
    children = track_children(monkeypatch)
    with pytest.raises(ValueError, match="timeouts"):
        run.run_stack(python("pass"), python("pass"), stream_db=tmp_path / "stream.db",
                      startup_timeout=timeout)
    assert not children


@pytest.mark.parametrize("command", ["python -c pass", [], [""], ["python", None], ["python", "bad\0arg"]])
def test_invalid_chat_command_is_rejected_before_batch_starts(tmp_path, monkeypatch, command):
    children = track_children(monkeypatch)
    with pytest.raises(ValueError, match="argument lists"):
        run.run_stack(python("pass"), command, stream_db=tmp_path / "stream.db")
    assert not children


def test_startup_timeout_stops_stream_without_starting_chat(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    monkeypatch.setattr(run, "_checkpoint", lambda database: -1)
    with pytest.raises(run.StackError, match="startup deadline"):
        run.run_stack(python("pass"), python("raise AssertionError('chat started')"),
                      stream_db=tmp_path / "stream.db", startup_timeout=0.15)
    assert len(children) == 2
    assert all(child.poll() == 0 for child in children)


def test_managed_stream_accepts_newline_as_a_graceful_stop(tmp_path):
    database = tmp_path / "stream.db"
    interpreter = run.ROOT / "streaming/.venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    child = run._launch([str(interpreter), "-X", "utf8", "-m", "shopil_streaming.managed",
                         "--db", str(database)], controlled=True)
    try:
        deadline = time.monotonic() + 5
        while run._checkpoint(database) in (-1, None):
            assert child.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        child.stdin.write(b"\n")
        child.stdin.flush()
        assert child.wait(timeout=5) == 0
    finally:
        run._stop(child, "Streaming", 5)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT finalized FROM stream_meta").fetchone()[0] == 0
