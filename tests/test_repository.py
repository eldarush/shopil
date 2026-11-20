"""Check the repository's data/secret boundary using Git's actual ignore rules."""

from pathlib import Path
import shutil
import subprocess


def test_generated_data_and_secrets_are_ignored(tmp_path):
    root = Path(__file__).resolve().parents[1]
    # Exercise the delivered rules without requiring Git metadata in a source export.
    for relative in (".gitignore", "streaming/.gitignore"):
        target = tmp_path / relative
        target.parent.mkdir(exist_ok=True)
        shutil.copyfile(root / relative, target)
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    paths = [".env", "chat/.env.local", "runs/chat/audit.jsonl",
             "etl/warehouse.duckdb", "streaming/state.db-wal", ".venv/test.py"]
    result = subprocess.run(
        ["git", "check-ignore", "-z", "--stdin"], cwd=tmp_path,
        input="\0".join(paths).encode(), capture_output=True, check=True,
    )
    assert set(result.stdout.decode().rstrip("\0").split("\0")) == set(paths)
    example = subprocess.run(["git", "check-ignore", ".env.example"], cwd=tmp_path)
    assert example.returncode == 1
