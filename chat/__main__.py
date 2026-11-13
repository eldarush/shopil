"""Launch the Hebrew chat against completed Batch data and a running stream."""

import argparse
from datetime import date
import os
from pathlib import Path

from .runtime import run_app
from .transport import OllamaModel


def add_arguments(parser):
    """Shared explicit configuration for standalone chat and the stack launcher."""
    for name, kind in (("warehouse", Path), ("history-start", date.fromisoformat),
                       ("history-end", date.fromisoformat), ("model", str)):
        value = os.environ.get("SHOPIL_" + name.upper().replace("-", "_"))
        parser.add_argument("--" + name, type=kind, default=value, required=not value)
    parser.add_argument("--stream-db", type=Path, default=os.environ.get("SHOPIL_STREAM_DB", "runs/stream.db"))
    parser.add_argument("--ollama-url", default=os.environ.get("SHOPIL_OLLAMA_URL", "http://127.0.0.1:11434"))
    parser.add_argument("--thinking", action=argparse.BooleanOptionalAction, default=None,
                        help="Override thinking for models that support it; otherwise keep the model default")
    parser.add_argument("--audit-dir", type=Path, default=os.environ.get("SHOPIL_AUDIT_DIR", "runs/audit"))
    parser.add_argument("--latest-windows", type=int, default=3)
    parser.add_argument("--port", type=int, default=7860)


def main(argv=None):
    from .capture import build_capture

    parser = argparse.ArgumentParser(description=__doc__)
    add_arguments(parser)
    parser.add_argument("--managed", action="store_true", help="Stop on stdin newline or EOF")
    args = parser.parse_args(argv)
    for path in (args.warehouse, args.stream_db):
        if not path.is_file():
            parser.error(f"Database does not exist: {path}")
    try:
        model = OllamaModel(args.model, args.ollama_url, think=args.thinking)
        capture = build_capture(args.warehouse, args.stream_db, start=args.history_start,
                                end=args.history_end, latest_windows=args.latest_windows)
        if not 1 <= args.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
    except ValueError as error:
        parser.error(str(error))
    run_app(capture, model, args.audit_dir, managed=args.managed, port=args.port)


if __name__ == "__main__":
    main()
