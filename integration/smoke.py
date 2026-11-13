"""Exercise both streaming modes without credentials or persistent demo files."""

from pathlib import Path
from contextlib import closing
import os
import shutil
import sqlite3
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def smoke() -> None:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("Install uv before running the smoke test.")
    with tempfile.TemporaryDirectory(prefix="shopil-smoke-") as temporary:
        for mode in ("live", "microbatch"):
            database = Path(temporary) / f"{mode}.db"
            subprocess.run(
                [uv, "run", "--project", str(ROOT / "streaming"), "--locked",
                 "python", "-X", "utf8", "-m", "shopil_streaming.service", "--mode", mode,
                 "--seconds", "0.5", "--interval", "0.01", "--db", str(database)],
                check=True, timeout=60,
                env={key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"},
            )
            with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
                checkpoint = connection.execute(
                    "SELECT checkpoint FROM stream_meta WHERE singleton = 1"
                ).fetchone()
                if checkpoint is None or int(checkpoint[0]) < 0:
                    raise RuntimeError(f"{mode}: no events committed")
            print(f"PASS {mode}: committed checkpoint {checkpoint[0]}", flush=True)


if __name__ == "__main__":
    smoke()
