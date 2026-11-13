"""Real subprocess and SQLite lifecycle tests; chat and batch are explicit stubs."""

from contextlib import closing
import io
import os
import socket
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace

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


def test_stop_file_drains_stream_and_stops_chat_without_signals(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    stop_file, stopped = tmp_path / "stop", tmp_path / "chat-stopped"
    database = tmp_path / "stream.db"
    chat = python("""
from pathlib import Path
import sys
Path(sys.argv[1]).touch()
sys.stdin.readline()
Path(sys.argv[2]).touch()
""", stop_file, stopped)
    run.run_stack(python("pass"), chat, stream_db=database, stop_file=stop_file)
    assert stopped.is_file()
    assert len(children) == 3 and all(child.poll() == 0 for child in children)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT checkpoint FROM stream_meta").fetchone()[0] >= 0
        assert connection.execute("SELECT finalized FROM stream_meta").fetchone()[0] == 0


def test_existing_stop_file_does_not_start_any_process(tmp_path, monkeypatch):
    children = track_children(monkeypatch)
    stop_file = tmp_path / "stop"
    stop_file.touch()
    with pytest.raises(ValueError, match="Stop file already exists"):
        run.run_stack(python("pass"), python("pass"), stream_db=tmp_path / "stream.db", stop_file=stop_file)
    assert not children


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


@pytest.mark.parametrize("failure", [
    subprocess.TimeoutExpired("taskkill", 5), OSError("taskkill unavailable"), None,
])
def test_windows_tree_cleanup_failure_is_bounded_and_falls_back_to_child_kill(monkeypatch, failure):
    class UncooperativeProcess:
        pid = 1234

        def __init__(self):
            self.stdin = io.BytesIO()
            self.killed = False
            self.waits = []

        def poll(self):
            return 1 if self.killed else None

        def kill(self):
            self.killed = True

        def wait(self, timeout):
            self.waits.append(timeout)
            if not self.killed:
                raise subprocess.TimeoutExpired("child", timeout)
            return 1

    calls = []

    def taskkill(command, **kwargs):
        calls.append((command, kwargs))
        if failure:
            raise failure
        return subprocess.CompletedProcess(command, returncode=1)

    monkeypatch.setattr(run, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(run.subprocess, "run", taskkill)
    child = UncooperativeProcess()
    with pytest.raises(run.StackError, match="forced termination"):
        run._stop(child, "Chat", 0.1)
    assert calls[0][0] == ["taskkill", "/PID", "1234", "/T", "/F"]
    assert calls[0][1]["timeout"] == 5
    assert child.stdin.closed and child.killed
    assert child.waits == [0.1, 5]


@pytest.mark.parametrize("component", ["batch", "chat"])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_exited_leader_cannot_leave_a_descendant_running(tmp_path, component, exit_code):
    # A socket held by the grandchild proves process exit, including on POSIX
    # where an orphan can remain as an already-dead zombie until init reaps it.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(10)
        marker = tmp_path / "descendant-ready"
        descendant = """
import pathlib, socket, sys
with socket.create_connection(('127.0.0.1', int(sys.argv[1]))) as connection:
    pathlib.Path(sys.argv[2]).touch()
    connection.recv(1)
"""
        command = python("""
import pathlib, subprocess, sys, time
# Keep a second generation alive too; all descendants must belong to the group.
middle = "import subprocess,sys; raise SystemExit(subprocess.call(sys.argv[1:]))"
subprocess.Popen([sys.executable, '-c', middle, sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])
deadline = time.monotonic() + 8
while not pathlib.Path(sys.argv[3]).exists():
    if time.monotonic() >= deadline: raise TimeoutError('descendant not ready')
    time.sleep(.01)
raise SystemExit(int(sys.argv[4]))
""", descendant, listener.getsockname()[1], marker, exit_code)
        def execute():
            run.run_stack(command if component == "batch" else python("pass"),
                          command if component == "chat" else python("pass"),
                          stream_db=tmp_path / "stream.db")
        if exit_code:
            with pytest.raises(run.StackError):
                execute()
        else:
            try:
                execute()
            except run.StackError as error:
                assert "descendants" in str(error)
        assert marker.exists()
        with listener.accept()[0] as connection:
            connection.settimeout(5)
            try:
                assert connection.recv(1) == b"", "descendant survived supervisor cleanup"
            except ConnectionResetError:
                pass  # Windows forced process exit resets rather than closes the socket.


def test_completed_group_cleanup_never_reuses_the_process_id(monkeypatch):
    calls = []
    child = SimpleNamespace(pid=1234, poll=lambda: 0, wait=lambda timeout: 0)
    monkeypatch.setattr(run, "os", SimpleNamespace(name="posix", killpg=lambda *args: calls.append(args)))
    monkeypatch.setattr(run, "signal", SimpleNamespace(SIGKILL=9))
    assert run._kill_group(child) is True
    assert run._kill_group(child) is False
    assert calls == [(1234, run.signal.SIGKILL)]
