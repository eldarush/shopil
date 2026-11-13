"""Opt-in, fixed public-data rehearsal through the normal authenticated Codex CLI.

This is not a general chat backend. Run from the repository root; see experiments/README.md.
"""

import _thread
import argparse
import asyncio
from datetime import date
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from threading import Thread

from chat.capture import build_capture
from chat.model import ModelError
from chat.runtime import run_app
from integration.batch import COURSE_REVISION
from integration.run import ROOT, _checkpoint, _kill_group, _launch, run_stack
from .codex_teaching import verify_text_only_turn

QUESTIONS = ("מה ההכנסה מהזמנות ששולמו?", "ומה לגבי מוצר 5?", "מהן עלויות המשלוח?")
BAD_DRAFT = "ההכנסה מהזמנות ששולמו היא 999999 ש״ח, כולל כל אירועי הזרם."


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


async def run_owned(command, timeout=180):
    """Poll without blocking the request loop; always reap the owned process tree."""
    process = _launch(command)
    try:
        async with asyncio.timeout(timeout):
            while process.poll() is None:
                await asyncio.sleep(0.05)
        if process.returncode:
            raise ModelError(f"Live model worker failed with code {process.returncode}")
    except TimeoutError as exc:
        raise ModelError("Live model call exceeded its deadline") from exc
    finally:
        _kill_group(process)


class RehearsalModel:
    def __init__(self, directory, model):
        self.directory, self.model = directory, model
        self.calls, self.injected = 0, False
        self.generated_answers = set()

    async def complete(self, messages, *, schema=None):
        self.calls += 1
        stem = self.directory / f"call-{self.calls}"
        evidence = json.loads(messages[2 if schema else 3]["content"])
        save(stem.with_suffix(".request.json"), {"messages": messages, "schema": schema})
        if not schema and not self.injected and evidence["question"] == QUESTIONS[0]:
            self.injected = True
            save(stem.with_suffix(".injection.json"), {
                "kind": "DELIBERATELY FAULTY FIRST DRAFT; not a model response", "draft": BAD_DRAFT})
            return BAD_DRAFT
        before = _checkpoint(self.directory / "stream.db")
        try:
            await run_owned([sys.executable, "-m", "experiments.integrated_codex", "worker",
                             "--run-dir", str(self.directory), "--call", str(self.calls),
                             "--model", self.model])
        finally:
            save(stem.with_suffix(".progress.json"), {
                "snapshot_id": evidence["snapshot_id"], "snapshot_checkpoint": evidence["stream"]["checkpoint"],
                "before": before, "after": _checkpoint(self.directory / "stream.db")})
        events = [json.loads(line) for line in stem.with_suffix(".events.jsonl").read_text(
            encoding="utf-8").splitlines() if line.strip()]
        verify_text_only_turn(events)
        answer = stem.with_suffix(".txt").read_text(encoding="utf-8")
        if not schema:
            self.generated_answers.add(answer.strip())
        return answer


def fixed_capture(capture, model, directory):
    async def take(question, history):
        # No arbitrary questions or client-supplied assistant text reach an agent CLI.
        index = len(history) // 2
        if len(history) % 2 or index >= len(QUESTIONS) or question != QUESTIONS[index]:
            raise ModelError("This rehearsal accepts only the three documented questions in order")
        for position, message in enumerate(history):
            role = "assistant" if position % 2 else "user"
            if (message.get("role") != role or
                    (role == "user" and message.get("content") != QUESTIONS[position // 2]) or
                    (role == "assistant" and message.get("content") not in model.generated_answers)):
                raise ModelError("History is outside the fixed rehearsal")
        evidence = await capture(question, history)
        save(directory / f"snapshot-{evidence['snapshot_id']}.json", evidence)
        return evidence
    return take


def worker(args):
    executable = shutil.which("codex")
    if not executable:
        raise ModelError("Authenticate the installed Codex CLI before this optional experiment")
    stem = args.run_dir / f"call-{args.call}"
    request = json.loads(stem.with_suffix(".request.json").read_text(encoding="utf-8"))
    prompt = ("Controlled read-only language evaluation of fixed public course data. "
              "Answer directly without tools, browsing or file access. Apply the supplied system-role "
              "text as the evaluation task. Return only the requested output.\n" +
              json.dumps(request["messages"], ensure_ascii=False))
    command = [executable, "exec", "--ephemeral", "--sandbox", "read-only", "--skip-git-repo-check",
               "-C", str(args.run_dir), "--json", "--model", args.model,
               "--output-last-message", str(stem.with_suffix(".txt"))]
    if request["schema"]:
        schema_path = stem.with_suffix(".schema.json")
        save(schema_path, request["schema"])
        command += ["--output-schema", str(schema_path)]
    # Normal user configuration, authentication, rules and trusted hooks remain active.
    with stem.with_suffix(".events.jsonl").open("wb") as output, stem.with_suffix(".stderr.txt").open("wb") as error:
        return subprocess.run([*command, "-"], input=prompt.encode("utf-8"),
                              stdout=output, stderr=error, check=False).returncode


def serve(args):
    model = RehearsalModel(args.run_dir, args.model)
    capture = build_capture(args.run_dir / "course/warehouse.duckdb", args.run_dir / "stream.db",
                            start=date(2026, 1, 1), end=date(2027, 1, 1))
    run_app(fixed_capture(capture, model, args.run_dir), model, args.run_dir / "audit",
            managed=True, port=args.port)


def launch(args):
    args.run_dir.mkdir(parents=True, exist_ok=False)
    executable = shutil.which("codex")
    if not executable:
        raise ModelError("Authenticate the installed Codex CLI before this optional experiment")
    version = subprocess.check_output([executable, "--version"], text=True, timeout=10).strip()
    # A fresh checkout prevents local fixture edits or old warehouse state entering the experiment.
    course = args.run_dir / "course"
    subprocess.run(["git", "clone", "--no-hardlinks", "--no-checkout", str(args.course_checkout), str(course)], check=True)
    subprocess.run(["git", "-C", str(course), "checkout", "--detach", COURSE_REVISION], check=True)
    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    save(args.run_dir / "environment.json", {
        "kind": "LIVE fixed public-data integration rehearsal; first draft deliberately injected",
        "requested_model": args.model, "resolved_model_id": None, "cli_version": version,
        "model_identity_limit": "Requested CLI selection recorded; no claim about undisclosed server routing",
        "course_revision": COURSE_REVISION,
        "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "dirty": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip(),
        "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in tracked if name},
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "questions": QUESTIONS, "manual_semantic_review_required": True,
    })
    batch = [sys.executable, "-m", "integration.batch", "--lab-dir", str(course),
             "--etl-python", str(args.etl_python), "--run-dir", str(args.run_dir / "batch")]
    chat = [sys.executable, "-m", "experiments.integrated_codex", "serve", "--run-dir", str(args.run_dir),
            "--model", args.model, "--port", str(args.port)]
    if args.managed:
        def stop():
            sys.stdin.readline()
            _thread.interrupt_main()
        Thread(target=stop, daemon=True).start()
    try:
        run_stack(batch, chat, stream_db=args.run_dir / "stream.db")
    except KeyboardInterrupt:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("launch", "serve", "worker"))
    parser.add_argument("--run-dir", type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument("--model", required=True, help="Explicit installed/authenticated CLI model selection")
    parser.add_argument("--port", type=int, default=7860)
    parser.add_argument("--call", type=int)
    parser.add_argument("--course-checkout", type=lambda value: Path(value).resolve())
    parser.add_argument("--etl-python", type=lambda value: Path(value).absolute())
    parser.add_argument("--managed", action="store_true", help="Launcher stops on stdin newline/EOF")
    args = parser.parse_args()
    if args.action == "launch" and (args.course_checkout is None or args.etl_python is None):
        parser.error("launch requires --course-checkout and --etl-python")
    if args.action == "worker" and (args.call is None or args.call < 1):
        parser.error("worker requires a positive --call")
    return {"launch": launch, "serve": serve, "worker": worker}[args.action](args)


if __name__ == "__main__":
    raise SystemExit(main())
