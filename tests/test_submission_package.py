"""Packaging checks use synthetic evidence; they do not claim the assignment passed."""

import hashlib
import json
from pathlib import Path
import subprocess
from zipfile import ZipFile

import pytest

from tools.package_submission import package_submission


@pytest.fixture
def inputs(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    def git(*args):
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)
    git("init", "-b", "master")
    git("config", "user.name", "Packaging Test")
    git("config", "user.email", "packaging@example.invalid")
    (repo / "README.md").write_text("Source attribution and run instructions", encoding="utf-8")
    git("add", "README.md")
    git("commit", "-m", "test: prepare packaging fixture")
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    selection = {"etl": ["etl.json"], "streaming": ["stream.json"], "chat": ["audit.jsonl"],
                 "screenshots": ["stream.png", "answer.png"], "report": ["report.md"]}
    for names in selection.values():
        for name in names:
            (evidence / name).write_text("SYNTHETIC test content", encoding="utf-8")
    (evidence / "selection.json").write_text(json.dumps(selection), encoding="utf-8")
    return repo, evidence, tmp_path / "submission.zip", selection


def test_exact_committed_source_selected_evidence_hashes_and_reproducibility(inputs):
    repo, evidence, output, selection = inputs
    (evidence / "unselected-private.txt").write_text("must not be included")
    manifest = package_submission(repo, evidence, output)
    with ZipFile(output) as archive:
        assert archive.testzip() is None
        assert archive.read("source/README.md") == (repo / "README.md").read_bytes()
        assert not any("unselected" in name or ".git/" in name for name in archive.namelist())
        assert json.loads(archive.read("manifest.json")) == manifest
        for name, expected in manifest["sha256"].items():
            assert hashlib.sha256(archive.read(name)).hexdigest() == expected
    second = output.with_name("second.zip")
    package_submission(repo, evidence, second)
    assert output.read_bytes() == second.read_bytes()
    with pytest.raises(FileExistsError):
        package_submission(repo, evidence, output)


@pytest.mark.parametrize("name", ["../outside.txt", "/outside.txt", "C:/outside.txt", "sub\\file.txt",
                                  "missing.json", ".env", "warehouse.duckdb"])
def test_invalid_or_sensitive_selection_creates_no_archive(inputs, name):
    repo, evidence, output, selection = inputs
    selection["etl"] = [name]
    if name in {".env", "warehouse.duckdb"}:
        (evidence / name).write_text("do not package")
    (evidence / "selection.json").write_text(json.dumps(selection))
    with pytest.raises(ValueError):
        package_submission(repo, evidence, output)
    assert not output.exists()


@pytest.mark.parametrize("problem", ["missing_section", "empty_file", "one_screenshot", "dirty_source"])
def test_incomplete_or_uncommitted_inputs_fail_before_zip_creation(inputs, problem):
    repo, evidence, output, selection = inputs
    if problem == "missing_section":
        del selection["report"]
    elif problem == "empty_file":
        (evidence / "etl.json").write_bytes(b"")
    elif problem == "one_screenshot":
        selection["screenshots"] = ["stream.png", "stream.png"]
    else:
        (repo / "README.md").write_text("uncommitted")
    (evidence / "selection.json").write_text(json.dumps(selection))
    with pytest.raises(ValueError):
        package_submission(repo, evidence, output)
    assert not output.exists()


def test_archive_uses_captured_commit_if_head_changes(inputs, monkeypatch):
    repo, evidence, output, _ = inputs
    original_source = (repo / "README.md").read_bytes()
    check_output = subprocess.check_output
    revision = check_output(["git", "-C", str(repo), "rev-parse", "HEAD"]).decode().strip()

    def advance_after_revision(command, *args, **kwargs):
        result = check_output(command, *args, **kwargs)
        if command[3:] == ["rev-parse", "HEAD"]:
            # Only this test's disposable repository changes, between the two Git reads.
            (repo / "README.md").write_text("A later commit, not the captured source")
            subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-m", "test: advance head"],
                           check=True, capture_output=True)
        return result

    monkeypatch.setattr(subprocess, "check_output", advance_after_revision)
    manifest = package_submission(repo, evidence, output)
    assert check_output(["git", "-C", str(repo), "rev-parse", "HEAD"]).decode().strip() != revision
    assert manifest["source_commit"] == revision
    with ZipFile(output) as archive:
        assert archive.read("source/README.md") == original_source


@pytest.mark.parametrize("section,names", [
    ("screenshots", ["stream.png", "./stream.png"]),
    ("report", ["./etl.json"]),
])
def test_normalized_duplicate_selection_fails_before_archive(inputs, section, names):
    repo, evidence, output, selection = inputs
    selection[section] = names
    (evidence / "selection.json").write_text(json.dumps(selection))
    with pytest.raises(ValueError, match="Duplicate evidence"):
        package_submission(repo, evidence, output)
    assert not output.exists()


def test_resolved_screenshot_alias_is_not_a_second_file(inputs, monkeypatch):
    repo, evidence, output, _ = inputs
    resolve = Path.resolve

    def resolve_alias(path, *args, **kwargs):
        # Model symlink resolution without requiring Windows symlink privileges.
        return resolve(evidence / "stream.png" if path == evidence / "answer.png" else path,
                       *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve_alias)
    with pytest.raises(ValueError, match="Duplicate evidence"):
        package_submission(repo, evidence, output)
    assert not output.exists()


@pytest.mark.parametrize("target_name", [".env", "warehouse.duckdb"])
def test_resolved_alias_cannot_hide_prohibited_filename(inputs, monkeypatch, target_name):
    repo, evidence, output, _ = inputs
    target = evidence / target_name
    target.write_text("SYNTHETIC prohibited content")
    resolve = Path.resolve

    def resolve_alias(path, *args, **kwargs):
        # Only the resolver is simulated; the real filename guard must reject its target.
        return resolve(target if path == evidence / "etl.json" else path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve_alias)
    with pytest.raises(ValueError, match="Blocked runtime/secret paths"):
        package_submission(repo, evidence, output)
    assert not output.exists()
