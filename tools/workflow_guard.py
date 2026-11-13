#!/usr/bin/env python3
"""Dependency-free local and CI workflow checks. Never calls Jira or mutates Git history."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

CONFIG = ".workflow-guard.json"
PYTHON_VERSION = (3, 14, 10)
TYPES = ("feat", "fix", "docs", "style", "refactor", "perf", "test", "build", "ci", "chore", "revert")
BRANCH_TYPES = ("feature", "fix", "hotfix", "docs", "refactor", "test", "build", "ci", "chore", "perf")


class Violation(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Violation(message)


def git(repo, *args):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, encoding="utf-8", errors="replace")
    if result.returncode:
        raise Violation(f"git {args[0]} failed: {result.stderr.strip()}")
    return result.stdout


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    require(isinstance(config, dict), "Configuration must be an object")
    require(set(config) <= {"project_keys", "main_branch"}, "Unknown configuration field")
    keys = config.get("project_keys")
    require(isinstance(keys, list) and bool(keys), "project_keys must be a nonempty list")
    require(all(isinstance(k, str) and re.fullmatch(r"[A-Z][A-Z0-9]+", k) for k in keys), "Invalid Jira project key")
    require(isinstance(config.get("main_branch"), str) and bool(re.fullmatch(r"[A-Za-z0-9._/-]+", config["main_branch"])), "Invalid main_branch")
    return config


def branch_ticket(branch, config):
    require(branch != config["main_branch"], "Work on a ticket feature branch, not the main branch")
    match = re.fullmatch(r"([a-z]+)/([A-Z][A-Z0-9]+-[1-9][0-9]*)-([a-z0-9]+(?:-[a-z0-9]+)*)", branch)
    require(match is not None and match[1] in BRANCH_TYPES, "Branch must look like feature/SHOPIL-7-short-description")
    ticket = match[2]
    require(ticket.rsplit("-", 1)[0] in config["project_keys"], "Branch Jira project is not configured")
    return ticket


def conventional(message, ticket):
    lines = message.splitlines()
    require(bool(lines), "Commit/PR title is empty")
    match = re.fullmatch(r"([a-z]+)(?:\([a-zA-Z0-9_./-]+\))?!?: (\S.*\S|\S)", lines[0])
    require(match is not None and match[1] in TYPES, "Use Conventional Commits, e.g. feat(etl): add reviews [SHOPIL-7]")
    require(len(lines) < 2 or lines[1] == "", "Commit body must start after a blank line")
    require(re.search(r"(?<![A-Za-z0-9-])" + re.escape(ticket) + r"(?![A-Za-z0-9-])", message) is not None, "Commit/PR must reference the branch's exact Jira ticket")


def forbidden(path):
    name = PurePosixPath(path).name.lower()
    parts = [p.lower() for p in PurePosixPath(path).parts]
    env = (name == ".env" or name.startswith(".env.")) and not name.endswith(".example")
    database = re.search(r"\.(?:db|sqlite3?|duckdb)(?:$|[.-])", name) is not None
    return bool(env or database or name in {"secrets.toml", "credentials.json", "id_rsa", "id_ed25519"}
                or name.endswith((".pem", ".key")) or any(p in {".venv", "__pycache__", "node_modules"} for p in parts))


def check_paths(paths):
    bad = [p for p in paths if forbidden(p)]
    require(not bad, "Blocked runtime/secret paths: " + ", ".join(bad))


def tree_paths(repo, revision):
    return [p for p in git(repo, "ls-tree", "-r", "--name-only", "-z", revision).split("\0") if p]


def verify_sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", value) is not None, "Expected a full hexadecimal commit SHA")
    return value


def check_pr(repo, event, config):
    pr = event.get("pull_request")
    require(isinstance(pr, dict), "Expected a pull_request event")
    require(pr["base"]["ref"] == config["main_branch"], "PR must target the configured main branch")
    ticket = branch_ticket(pr["head"]["ref"], config)
    conventional(pr["title"], ticket)
    body = pr.get("body") or ""
    require(re.search(r"https://[^\s<>]+/browse/" + re.escape(ticket) + r"(?=[\s)\]>]|$)", body) is not None, "PR body must link its Jira ticket")
    for heading in ("Summary", "Validation"):
        section = re.search(r"(?im)^## " + heading + r"\s*\n(.*?)(?=^## |\Z)", body, re.S)
        require(section is not None and bool(section[1].strip()), f"PR needs a nonempty {heading} section")
    base = verify_sha(pr["base"]["sha"])
    head = verify_sha(pr["head"]["sha"])
    commits = git(repo, "rev-list", f"{base}..{head}").splitlines()
    require(bool(commits), "PR has no commits beyond its base")
    for commit in commits:
        parents = git(repo, "show", "-s", "--format=%P", commit).split()
        require(len(parents) <= 1, "Merge commits are not allowed on feature branches; rebase only when authorized")
        conventional(git(repo, "show", "-s", "--format=%B", commit).rstrip(), ticket)
    check_paths(tree_paths(repo, head))
    return ticket


def check_jira(issue, identity, ticket, config, phase="work"):
    require(re.fullmatch(r"[A-Z][A-Z0-9]+-[1-9][0-9]*", ticket) is not None, "Invalid ticket key")
    require(ticket.rsplit("-", 1)[0] in config["project_keys"], "Jira project is not configured")
    require(issue.get("key") == ticket, "Jira response does not match requested ticket")
    fields = issue.get("fields", {})
    account_id = identity.get("accountId")
    require(isinstance(account_id, str) and bool(account_id.strip()), "Authenticated Jira accountId is required; display names are not identity")
    assignee = fields.get("assignee") or {}
    require(assignee.get("accountId") == account_id, "Ticket is unassigned or belongs to another account; read-only access only")
    require(bool(fields.get("summary")), "Jira issue is missing its summary")
    category = fields.get("status", {}).get("statusCategory", {})
    allowed = {"new", "indeterminate"} if phase == "start" else {"indeterminate"}
    category_key = category.get("key") or {"To Do": "new", "In Progress": "indeterminate", "Done": "done"}.get(category.get("name"))
    require(category_key in allowed, "Ticket status is not valid for this workflow phase")


def install_hooks(repo, config_path):
    require(Path(git(repo, "rev-parse", "--show-toplevel").strip()).resolve() == repo.resolve(), "Run hook installation at the repository root")
    require(config_path.resolve() == (repo / CONFIG).resolve(), "Hook installation requires the repository-root configuration")
    result = subprocess.run(["git", "-C", str(repo), "config", "--get", "core.hooksPath"], capture_output=True, text=True)
    require(result.returncode == 1, "Existing core.hooksPath must be integrated manually; it will not be replaced")
    hooks = Path(git(repo, "rev-parse", "--git-path", "hooks").strip())
    if not hooks.is_absolute():
        hooks = repo / hooks
    hooks = hooks.resolve()
    names = {"pre-commit": "pre-commit", "commit-msg": 'commit-msg --message-file "$1"', "pre-push": "pre-push"}
    require(not any((hooks / n).exists() or (hooks / n).is_symlink() for n in names), "Existing hooks will not be overwritten; integrate manually")
    hooks.mkdir(parents=True, exist_ok=True)
    import shlex
    command = " ".join(shlex.quote(x.replace("\\", "/")) for x in (sys.executable, str(Path(__file__).resolve())))
    for name, args in names.items():
        path = hooks / name
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(f'#!/bin/sh\nexec {command} {args}\n')
        path.chmod(0o755)


def pre_push(stream, config):
    for line in stream.splitlines():
        fields = line.split()
        require(len(fields) == 4, "Malformed Git pre-push input")
        local_ref, local_sha, remote_ref, _ = fields
        if remote_ref.startswith("refs/tags/"):
            continue
        require(remote_ref.startswith("refs/heads/"), "Unsupported push destination")
        branch_ticket(remote_ref.removeprefix("refs/heads/"), config)
        if set(local_sha) != {"0"}:
            require(local_ref == remote_ref, "Push a feature branch under its own name")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["branch", "commit-msg", "pre-commit", "pre-push", "pr", "jira", "install-hooks"])
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path)
    parser.add_argument("--branch")
    parser.add_argument("--message-file", type=Path)
    parser.add_argument("--event", type=Path)
    parser.add_argument("--issue-file", type=Path)
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--phase", choices=["start", "work", "finish"], default="work")
    parser.add_argument("--ticket")
    args = parser.parse_args(argv)
    try:
        require(sys.version_info[:3] == PYTHON_VERSION, "Python 3.14.10 is required. Use py -3.14 on Windows or a Python 3.14.10 virtual environment.")
        config_path = args.config or args.repo / CONFIG
        config = load_config(config_path)
        if args.command == "install-hooks":
            install_hooks(args.repo, config_path)
        elif args.command == "pr":
            event = args.event or Path(os.environ["GITHUB_EVENT_PATH"])
            check_pr(args.repo, json.loads(event.read_text(encoding="utf-8")), config)
        elif args.command == "jira":
            require(args.issue_file is not None and args.identity_file is not None and args.ticket is not None, "jira requires --issue-file, --identity-file, and --ticket")
            check_jira(json.loads(args.issue_file.read_text(encoding="utf-8")), json.loads(args.identity_file.read_text(encoding="utf-8")), args.ticket, config, args.phase)
        elif args.command == "pre-push":
            pre_push(sys.stdin.read(), config)
        else:
            branch = args.branch or git(args.repo, "branch", "--show-current").strip()
            ticket = branch_ticket(branch, config)
            if args.command == "commit-msg":
                require(args.message_file is not None, "commit-msg requires --message-file")
                conventional(args.message_file.read_text(encoding="utf-8").rstrip(), ticket)
            elif args.command == "pre-commit":
                check_paths([p for p in git(args.repo, "diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z").split("\0") if p])
        print(f"PASS: {args.command}")
        return 0
    except (Violation, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
