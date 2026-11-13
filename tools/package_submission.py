"""Package committed source and explicitly selected assignment evidence, without submitting it."""

import argparse
import hashlib
from io import BytesIO
import json
from pathlib import Path, PurePosixPath
import subprocess
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from tools.workflow_guard import check_paths

SECTIONS = {"etl", "streaming", "chat", "screenshots", "report"}


def package_submission(repo: Path, evidence: Path, output: Path) -> dict:
    """Validate presence and hashes, not experimental correctness or screenshot content."""
    repo, evidence, output = repo.resolve(), evidence.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError(output)

    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args])

    if git("status", "--porcelain").strip():
        raise ValueError("Commit the reviewed source before packaging; working tree is not clean")
    revision = git("rev-parse", "HEAD").decode().strip()
    sections = json.loads((evidence / "selection.json").read_text(encoding="utf-8"))
    if not isinstance(sections, dict) or set(sections) != SECTIONS:
        raise ValueError("selection.json must list etl, streaming, chat, screenshots and report")
    files = {}
    selected_paths = set()
    for section, names in sections.items():
        if not isinstance(names, list) or not names:
            raise ValueError(f"{section} needs at least one selected evidence file")
        for name in names:
            if not isinstance(name, str) or not name or "\\" in name:
                raise ValueError("Evidence paths must be relative forward-slash paths")
            relative = PurePosixPath(name)
            if relative.is_absolute() or ".." in relative.parts or ":" in name:
                raise ValueError("Evidence paths must stay inside the evidence directory")
            path = (evidence / relative).resolve()
            if not path.is_relative_to(evidence):
                raise ValueError("Evidence link points outside the evidence directory")
            if not path.is_file():
                raise ValueError(f"Missing evidence file: {name}")
            check_paths([relative.as_posix(), path.relative_to(evidence).as_posix()])
            archive_path = "evidence/" + relative.as_posix()
            if path in selected_paths or archive_path in files:
                raise ValueError(f"Duplicate evidence file: {name}")
            selected_paths.add(path)
            content = path.read_bytes()
            if not content:
                raise ValueError(f"Empty evidence file: {name}")
            files[archive_path] = content
    screenshots = sections["screenshots"]
    if len(set(screenshots)) < 2 or any(Path(name).suffix.lower() not in {".png", ".jpg", ".jpeg"}
                                        for name in screenshots):
        raise ValueError("Select separate image files for the active stream and sourced answer")

    with ZipFile(BytesIO(git("archive", "--format=zip", revision))) as source:
        for item in source.infolist():
            if not item.is_dir():
                check_paths([item.filename])
                files["source/" + item.filename] = source.read(item)
    manifest = {
        "source_commit": revision,
        "evidence_selection": sections,
        "verification_scope": "File presence and SHA-256 only; review experimental correctness separately",
        "sha256": {name: hashlib.sha256(content).hexdigest() for name, content in sorted(files.items())},
    }
    files["manifest.json"] = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation protects previous submission archives from accidental replacement.
    with ZipFile(output, "x", compression=ZIP_DEFLATED) as archive:
        for name, content in sorted(files.items()):
            entry = ZipInfo(name, date_time=(2000, 1, 1, 0, 0, 0))
            entry.compress_type = ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, content)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True, help="Directory containing selection.json")
    parser.add_argument("--output", type=Path, required=True, help="New ZIP path; existing files are preserved")
    args = parser.parse_args()
    package_submission(Path(__file__).resolve().parents[1], args.evidence, args.output)
    print(args.output.resolve())


if __name__ == "__main__":
    main()
