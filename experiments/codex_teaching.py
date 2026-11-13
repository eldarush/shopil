"""Run ONLY the fixed public teaching experiment through an authenticated Codex CLI.

This is a reproducible language experiment, not a backend for the chat app.
There are no arguments for user questions, review text, databases or prompts.
Normal Codex configuration, authentication, rules and trusted hooks remain active.
"""

import asyncio
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4

from chat.model import ModelError
from .chat_evaluation import ROOT, teaching_evidence, rejection_experiment, mock_three_rejections


def verify_text_only_turn(events):
    if not any(event.get("type") == "turn.completed" for event in events):
        raise ModelError("The live model turn did not complete")
    for event in events:
        if event.get("type", "").startswith("item."):
            # Host configuration warnings are preserved, not counted as tool calls.
            if event.get("item", {}).get("type") not in {"agent_message", "reasoning", "error"}:
                raise ModelError("The controlled experiment invoked a tool; inspect its event log")


class _TeachingModel:
    def __init__(self, directory):
        self.directory, self.calls = directory, 0

    async def complete(self, messages, *, schema=None):
        self.calls += 1
        stem = self.directory / f"call-{self.calls}"
        prompt = ("Controlled read-only language evaluation using only fixed public numeric teaching data. "
                  "Answer directly, without tools, browsing or file access. Apply the supplied system-role "
                  "text as the evaluation task. Return only the requested output.\n"
                  + json.dumps(messages, ensure_ascii=False))
        command = [shutil.which("codex"), "exec", "--ephemeral", "--sandbox", "read-only",
                   "--skip-git-repo-check", "-C", str(self.directory), "--json",
                   "--output-last-message", str(stem.with_suffix(".txt"))]
        if command[0] is None:
            raise ModelError("Install and authenticate Codex CLI before this optional experiment")
        if schema:
            schema_path = stem.with_suffix(".schema.json")
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            command += ["--output-schema", str(schema_path)]
        try:
            process = await asyncio.to_thread(
                subprocess.run, [*command, "-"], input=prompt, text=True, encoding="utf-8",
                capture_output=True, timeout=180,
            )
        except subprocess.TimeoutExpired as exc:
            for suffix, content in ((".events.jsonl", exc.stdout), (".stderr.txt", exc.stderr)):
                if isinstance(content, bytes):
                    stem.with_suffix(suffix).write_bytes(content)
                elif content is not None:
                    stem.with_suffix(suffix).write_text(content, encoding="utf-8")
            raise ModelError("Live model call exceeded its 180-second deadline") from exc
        stem.with_suffix(".events.jsonl").write_text(process.stdout, encoding="utf-8")
        stem.with_suffix(".stderr.txt").write_text(process.stderr, encoding="utf-8")
        if process.returncode:
            raise ModelError(f"Live model call failed with code {process.returncode}")
        events = [json.loads(line) for line in process.stdout.splitlines() if line.strip()]
        verify_text_only_turn(events)
        return stem.with_suffix(".txt").read_text(encoding="utf-8")


async def main():
    executable = shutil.which("codex")
    if executable is None:
        raise SystemExit("Install and authenticate Codex CLI before this optional experiment")
    version = subprocess.run([executable, "--version"], check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                              capture_output=True, text=True, timeout=10).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, check=True,
                           capture_output=True, text=True, timeout=10).stdout.strip()
    directory = (Path("runs") / ("chat-evaluation-" + uuid4().hex)).resolve()
    directory.mkdir(parents=True)
    print(directory, flush=True)
    snapshot = teaching_evidence(directory / "teaching.db")
    (directory / "snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    real = await rejection_experiment(_TeachingModel(directory), snapshot, directory / "audit")
    mock = await mock_three_rejections(snapshot)
    result = {"runtime": "authenticated Codex CLI; fixed teaching experiment only",
              "cli_version": version, "resolved_model_id": None,
              "model_selection": "CLI configured default; resolved model ID not exposed in observed events",
              "source_revision": revision, "working_tree_dirty": bool(dirty),
              "experiment_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                    for path in Path(__file__).parent.glob("*.py")},
              "real": real, "mock": mock, "manual_semantic_review_required": True}
    (directory / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"real_cycle_passed": real["cycle_passed"], "mock_passed": mock["passed"]}), flush=True)
    return 0 if real["cycle_passed"] and mock["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
