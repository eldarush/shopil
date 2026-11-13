"""Sequence/error tests use stub processes; real ETL validation is a separate run."""

from io import BytesIO
import os
from pathlib import Path
import subprocess
from zipfile import ZipFile

import pytest

from integration import batch


@pytest.fixture
def setup(tmp_path, monkeypatch):
    repo, lab, runtime = (tmp_path / name for name in ("repo", "lab", "venv"))
    python = runtime / ("python.exe" if os.name == "nt" else "python")
    dbt = runtime / ("dbt.exe" if os.name == "nt" else "dbt")
    for path in (python, dbt, lab / "pipelines/02_dlt_pipeline.py", lab / "exercises/check_exercise.py",
                 *(repo / "etl/dbt/models" / name for name in batch.MODELS)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture component", encoding="utf-8")
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("dbt_project/profiles.yml", "fixture profile")
        archive.writestr("dbt_project/models/base.sql", "fixture base")
    commands = []

    def read(command, **kwargs):
        return batch.COURSE_REVISION if "rev-parse" in command else output.getvalue()

    def run(command, **kwargs):
        commands.append((command, kwargs))

    monkeypatch.setattr(batch, "ROOT", repo)
    monkeypatch.setattr(subprocess, "check_output", read)
    monkeypatch.setattr(subprocess, "run", run)
    return lab, python, tmp_path / "run", commands


def test_complete_sequence_uses_one_warehouse_and_preserves_course_files(setup, monkeypatch):
    lab, python, run_dir, commands = setup
    monkeypatch.setenv("DBT_PROJECT_DIR", str(lab / "unrelated-project"))
    original = lab / "dbt_project/models/staging/stg_reviews.sql"
    original.parent.mkdir(parents=True)
    original.write_text("local work must remain unchanged")
    warehouse = batch.prepare_batch(lab, python, run_dir)
    assert warehouse == lab / "warehouse.duckdb"
    assert len(commands) == 8
    assert commands[0][0][-1] == "--offline"
    assert commands[1][0][1].endswith("load_reviews.py")
    assert commands[2][0][1] == "build"
    assert commands[2][0][2:] == ["--project-dir", str(run_dir / "dbt_project"),
                                  "--profiles-dir", str(run_dir / "dbt_project")]
    assert commands[2][1]["cwd"] == run_dir / "dbt_project"
    assert commands[3][0][1].endswith("check_base.py")
    assert [command[-1] for command, _ in commands[4:7]] == ["1", "2", "3"]
    assert commands[7][0][1].endswith("check_stg_reviews.py")
    assert all(options["check"] is True for _, options in commands)
    assert all(options["env"]["WAREHOUSE_PATH"] == str(warehouse) for _, options in commands)
    assert original.read_text() == "local work must remain unchanged"
    assert (run_dir / "dbt_project/models/staging/stg_reviews.sql").read_text() == "fixture component"
    with pytest.raises(FileExistsError):
        batch.prepare_batch(lab, python, run_dir)
    assert len(commands) == 8


def test_failure_stops_following_components_and_retains_run_directory(setup, monkeypatch):
    lab, python, run_dir, commands = setup

    def fail(command, **kwargs):
        commands.append(command)
        raise subprocess.CalledProcessError(3, command)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        batch.prepare_batch(lab, python, run_dir)
    assert len(commands) == 1
    assert run_dir.is_dir()


def test_wrong_course_revision_fails_before_creating_run(setup, monkeypatch):
    lab, python, run_dir, commands = setup
    monkeypatch.setattr(subprocess, "check_output", lambda *args, **kwargs: "different revision")
    with pytest.raises(ValueError, match="Course checkout"):
        batch.prepare_batch(lab, python, run_dir)
    assert not run_dir.exists() and not commands


def test_missing_required_component_does_not_run_partial_pipeline(setup):
    lab, python, run_dir, commands = setup
    (batch.ROOT / "etl/dbt/models" / batch.MODELS[-1]).unlink()
    with pytest.raises(FileNotFoundError):
        batch.prepare_batch(lab, python, run_dir)
    assert not run_dir.exists() and not commands


def test_virtualenv_interpreter_symlink_keeps_its_environment(setup, monkeypatch):
    lab, python, run_dir, commands = setup
    resolve = Path.resolve
    base_python = python.parent.parent / "system/bin/python"

    def resolve_interpreter(path, *args, **kwargs):
        # Model the POSIX venv symlink on Windows, which need not permit creating links.
        return base_python if path == python else resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve_interpreter)
    batch.prepare_batch(lab, python, run_dir)
    assert commands[0][0][0] == str(python)
    assert Path(commands[2][0][0]).parent == python.parent
