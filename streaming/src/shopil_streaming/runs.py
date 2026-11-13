"""Fresh run directories and committed evidence files shared by all experiments.

Every experiment opens a NEW SQLite database (the engine refuses to reuse a DB
with different settings, and comparisons must not share state). Databases go to
streaming/runs/, which is git-ignored. Small, deterministic outputs go to
streaming/evidence/<TICKET>/ and are committed as proof for the Jira task.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from streaming_lab.reporting import fresh_run_directory

STREAMING_ROOT = Path(__file__).resolve().parents[2]
RUNS_DIR = STREAMING_ROOT / "runs"
EVIDENCE_DIR = STREAMING_ROOT / "evidence"


def fresh_db(tag: str) -> Path:
    """Return a path to a database inside a brand-new unique directory."""
    return fresh_run_directory(RUNS_DIR, prefix=f"{tag}-") / "stream.db"


def evidence_path(ticket: str, name: str) -> Path:
    path = EVIDENCE_DIR / ticket / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_json(ticket: str, name: str, data: Any) -> Path:
    path = evidence_path(ticket, name)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
                    encoding="utf-8")
    return path


def write_text(ticket: str, name: str, text: str) -> Path:
    path = evidence_path(ticket, name)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")
    return path


def shekels(cents: int) -> str:
    """Integer agorot -> '122.00' without floats (assignment reports in ILS)."""
    if isinstance(cents, bool) or not isinstance(cents, int):
        raise TypeError("expected integer cents")
    sign = "-" if cents < 0 else ""
    whole, frac = divmod(abs(cents), 100)
    return f"{sign}{whole:,}.{frac:02d}"


def md_table(rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    if not rows:
        return "_(no rows)_"
    columns = columns or list(rows[0])
    def cell(value: Any) -> str:
        text = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        return text.replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(cell(row.get(col, "")) for col in columns) + " |" for row in rows]
    return "\n".join(lines)
