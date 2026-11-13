"""Run batch first, then supervised streaming and chat in isolated environments."""

from collections.abc import Sequence
from contextlib import closing
import math
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


class StackError(RuntimeError):
    """A component failed or could not shut down cleanly."""


def _arguments(command: Sequence[str]) -> list[str]:
    if (isinstance(command, (str, bytes)) or not command or not command[0]
            or not all(isinstance(arg, str) and "\0" not in arg for arg in command)):
        raise ValueError("Commands must be nonempty argument lists")
    return list(command)


def _launch(command: Sequence[str], *, controlled: bool = False) -> subprocess.Popen:
    environment = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    environment.update(PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    return subprocess.Popen(
        _arguments(command), cwd=ROOT, env=environment,
        stdin=subprocess.PIPE if controlled else subprocess.DEVNULL,
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )


def _checkpoint(database: Path) -> int | None:
    if not database.exists():
        return -1
    try:
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=0.2)) as connection:
            row = connection.execute("SELECT checkpoint FROM stream_meta WHERE singleton=1").fetchone()
            return int(row[0]) if row else -1
    except sqlite3.OperationalError as exc:
        if "no such table: stream_meta" in str(exc):
            return -1
        if exc.sqlite_errorcode == sqlite3.SQLITE_BUSY:
            return None
        raise


def _kill_group(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        result = subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode and process.poll() is None:
            process.kill()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=5)


def _stop(process: subprocess.Popen, label: str, timeout: float) -> None:
    if process.stdin is not None:
        try:
            process.stdin.close()
        except BrokenPipeError:
            pass
    try:
        code = process.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        _kill_group(process)
        raise StackError(f"{label} required forced termination; shutdown was not clean") from exc
    if code != 0:
        raise StackError(f"{label} exited with code {code}")


def run_stack(
    batch_command: Sequence[str],
    chat_command: Sequence[str],
    *,
    stream_db: Path,
    mode: str = "live",
    startup_timeout: float = 20,
    shutdown_timeout: float = 15,
) -> None:
    """The chat command must honor stdin EOF as a graceful shutdown request.

    Commands run without a shell and inherit visible stdout/stderr. The caller
    supplies the real batch command and a chat command using its managed mode.
    Databases, including finalized ones, are never reset or silently replaced.
    """
    if mode not in {"live", "microbatch"}:
        raise ValueError("mode must be live or microbatch")
    batch_command, chat_command = _arguments(batch_command), _arguments(chat_command)
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) or value <= 0
           for value in (startup_timeout, shutdown_timeout)):
        raise ValueError("Lifecycle timeouts must be positive finite seconds")
    stream_python = ROOT / "streaming/.venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not stream_python.is_file():
        raise StackError("Run uv sync --project streaming --locked before starting the stack")
    database = Path(stream_db).resolve()
    batch = stream = chat = None
    try:
        batch = _launch(batch_command)
        if batch.wait() != 0:
            raise StackError(f"Batch failed with code {batch.returncode}; streaming and chat were not started")
        initial_checkpoint = _checkpoint(database)
        if initial_checkpoint is None:
            raise StackError("Stream database is locked; cannot establish the starting checkpoint")
        stream = _launch([str(stream_python), "-X", "utf8", "-m", "shopil_streaming.managed",
                          "--db", str(database), "--mode", mode], controlled=True)
        deadline = time.monotonic() + startup_timeout
        while True:
            if stream.poll() is not None:
                raise StackError(f"Streaming exited before readiness with code {stream.returncode}")
            checkpoint = _checkpoint(database)
            if checkpoint is not None and checkpoint > initial_checkpoint:
                break
            if time.monotonic() >= deadline:
                raise StackError("Streaming did not commit a new event before the startup deadline")
            time.sleep(0.05)
        chat = _launch(chat_command, controlled=True)
        while chat.poll() is None:
            if stream.poll() is not None:
                raise StackError(f"Streaming stopped while chat was running (code {stream.returncode})")
            time.sleep(0.05)
        if stream.poll() is not None:
            raise StackError(f"Streaming stopped unexpectedly (code {stream.returncode})")
        if chat.returncode != 0:
            raise StackError(f"Chat failed with code {chat.returncode}")
    finally:
        errors = []
        for process, label in ((chat, "Chat"), (stream, "Streaming")):
            if process is not None:
                try:
                    _stop(process, label, shutdown_timeout)
                except (StackError, OSError, subprocess.SubprocessError) as exc:
                    errors.append(f"{label}: {exc}")
        if batch is not None and batch.poll() is None:
            _kill_group(batch)
            errors.append("Batch was interrupted and required termination")
        if errors:
            raise StackError("; ".join(errors))
