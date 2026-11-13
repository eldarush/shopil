"""Run batch first, then supervised streaming and chat in isolated environments."""

from collections.abc import Sequence
from contextlib import closing
import argparse
import math
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
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
    arguments = _arguments(command)
    options = dict(cwd=ROOT, env=environment,
                   stdin=subprocess.PIPE if controlled else subprocess.DEVNULL)
    if os.name != "nt":
        return subprocess.Popen(arguments, start_new_session=True, **options)
    from .windows_job import Job
    job = Job()
    try:
        os.set_handle_inheritable(job.handle, True)
        startup = subprocess.STARTUPINFO(lpAttributeList={"handle_list": [job.handle]})
        startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startup.wShowWindow = subprocess.SW_HIDE
        process = subprocess.Popen(
            [sys.executable, str(ROOT / "integration/windows_job.py"), str(job.handle), *arguments],
            startupinfo=startup, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            close_fds=True, **options)
        process._shopil_job = job
        return process
    except BaseException:
        job.close()
        raise
    finally:
        if job.handle is not None:
            os.set_handle_inheritable(job.handle, False)


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


def _kill_group(process: subprocess.Popen) -> bool:
    if getattr(process, "_shopil_group_closed", False):
        return False
    job = getattr(process, "_shopil_job", None)
    if job is not None:
        remaining = job.close()
        process.wait(timeout=5)
        process._shopil_group_closed = True
        return remaining
    remaining = process.poll() is None
    if os.name == "nt":
        if not remaining:
            return False
        try:
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            # Tree cleanup can be unavailable or stall; still reap the direct child.
            pass
        if process.poll() is None:
            process.kill()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
            remaining = True
        except ProcessLookupError:
            remaining = False
    process.wait(timeout=5)
    process._shopil_group_closed = True
    return remaining


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
    descendants = _kill_group(process)
    if code != 0:
        raise StackError(f"{label} exited with code {code}")
    if descendants:
        raise StackError(f"{label} left descendants requiring forced termination")


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
        if _kill_group(batch):
            raise StackError("Batch left descendants requiring forced termination")
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
        if batch is not None:
            interrupted = batch.poll() is None
            try:
                remaining = _kill_group(batch)
                if interrupted or remaining:
                    errors.append("Batch failed or left descendants requiring forced termination")
            except (OSError, subprocess.SubprocessError) as exc:
                errors.append(f"Batch cleanup: {exc}")
        if errors:
            raise StackError("; ".join(errors))


def main(argv=None):
    from chat.__main__ import add_arguments
    from chat.transport import OllamaModel

    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    parser.add_argument("--lab-dir", type=Path, required=True)
    parser.add_argument("--etl-python", type=Path, required=True)
    parser.add_argument("--batch-run-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=("live", "microbatch"), default="live")
    args = parser.parse_args(argv)
    if args.warehouse.resolve() != (args.lab_dir / "warehouse.duckdb").resolve():
        parser.error("--warehouse must name warehouse.duckdb in --lab-dir")
    if args.history_start >= args.history_end or args.latest_windows < 1 or not 1 <= args.port <= 65535:
        parser.error("Use ordered history dates, positive latest-windows and port 1–65535")
    try:
        OllamaModel(args.model, args.ollama_url)
    except ValueError as error:
        parser.error(str(error))
    batch = [sys.executable, "-m", "integration.batch", "--lab-dir", str(args.lab_dir.resolve()),
             "--etl-python", os.path.abspath(args.etl_python), "--run-dir", str(args.batch_run_dir.resolve())]
    chat = [sys.executable, "-m", "chat", "--managed", "--warehouse", str(args.warehouse.resolve()),
            "--stream-db", str(args.stream_db.resolve()), "--history-start", args.history_start.isoformat(),
            "--history-end", args.history_end.isoformat(), "--model", args.model,
            "--ollama-url", args.ollama_url, "--audit-dir", str(args.audit_dir.resolve()),
            "--latest-windows", str(args.latest_windows), "--port", str(args.port)]
    try:
        run_stack(batch, chat, stream_db=args.stream_db, mode=args.mode)
    except KeyboardInterrupt:
        pass  # run_stack closes both children before propagating the interrupt.
    except (StackError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Stack failed: {error}\n")


if __name__ == "__main__":
    main()
