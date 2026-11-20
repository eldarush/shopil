"""Start the complete assignment from the bundled, pinned course inputs."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import subprocess
import sys

from .run import ROOT, main as launch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=os.environ.get("SHOPIL_MODEL", "qwen3:8b"))
    parser.add_argument("--mode", choices=("live", "microbatch"), default="live")
    parser.add_argument("--port", type=int, default=7860)
    parser.add_argument("--demonstrate-backpressure", action="store_true")
    parser.add_argument("--runs", type=Path, default=ROOT / "runs")
    parser.add_argument("--stop-file", type=Path)
    args = parser.parse_args()
    environment = {key: value for key, value in os.environ.items()
                   if key not in {"UV_PROJECT_ENVIRONMENT", "VIRTUAL_ENV"}}
    subprocess.run(["uv", "sync", "--project", str(ROOT / "streaming"), "--locked"],
                   env=environment, check=True)
    session = args.runs.resolve() / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    session.mkdir(parents=True)
    etl_environment = session / ".venv-etl"
    subprocess.run(["uv", "sync", "--project", str(ROOT), "--locked", "--only-group", "batch"],
                   env=dict(environment, UV_PROJECT_ENVIRONMENT=str(etl_environment)), check=True)
    etl_python = etl_environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    lab = session / "course"
    shutil.copytree(ROOT / "course_inputs", lab)
    command = ["--lab-dir", str(lab), "--etl-python", str(etl_python),
               "--batch-run-dir", str(session / "batch"), "--warehouse", str(lab / "warehouse.duckdb"),
               "--stream-db", str(session / "stream.db"), "--audit-dir", str(session / "audit"),
               "--history-start", "2026-01-01", "--history-end", "2027-01-01",
               "--model", args.model, "--no-thinking", "--mode", args.mode, "--port", str(args.port)]
    if args.stop_file:
        command.extend(["--stop-file", str(args.stop_file)])
    if args.demonstrate_backpressure:
        command.append("--demonstrate-backpressure")
    print(f"Session: {session}\nChat will open at http://127.0.0.1:{args.port}", flush=True)
    launch(command)


if __name__ == "__main__":
    main()
