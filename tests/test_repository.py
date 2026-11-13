"""Check the repository's data/secret boundary using Git's actual ignore rules."""

from pathlib import Path
import subprocess


def test_generated_data_and_secrets_are_ignored():
    root = Path(__file__).resolve().parents[1]
    paths = [".env", "chat/.env.local", "runs/chat/audit.jsonl",
             "etl/warehouse.duckdb", "streaming/state.db-wal", ".venv/test.py"]
    result = subprocess.run(
        ["git", "check-ignore", "-z", "--stdin"], cwd=root,
        input="\0".join(paths).encode(), capture_output=True, check=True,
    )
    assert set(result.stdout.decode().rstrip("\0").split("\0")) == set(paths)
    example = subprocess.run(["git", "check-ignore", ".env.example"], cwd=root)
    assert example.returncode == 1
