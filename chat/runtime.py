"""Local server composition; the caller supplies the real evidence reader."""

from collections.abc import Awaitable, Callable
from pathlib import Path
import sys
from threading import Event

from .actor import Actor
from .app import build_app
from .audit import JsonlAudit
from .critic import Critic
from .model import ChatModel
from .service import ChatService


def run_app(
    capture: Callable[[str, list[dict[str, str]]], Awaitable[dict]],
    model: ChatModel,
    audit_directory: Path,
    *,
    managed: bool = False,
    port: int = 7860,
) -> None:
    """Serve on loopback until Ctrl+C, or stdin newline/EOF in managed mode.

    Call from the process's main thread. Managed mode matches the integration
    supervisor's stdin-close protocol. This function does not load sample data,
    download a model, or choose a production database on the caller's behalf.
    """
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("port must be an integer between 1 and 65535")
    if type(managed) is not bool:
        raise ValueError("managed must be a boolean")
    service = ChatService(Actor(model), Critic(model), capture, JsonlAudit(audit_directory))
    app = build_app(service)
    try:
        app.launch(server_name="127.0.0.1", server_port=port, share=False,
                   inbrowser=False, prevent_thread_lock=True, ssr_mode=False,
                   mcp_server=False, run_history=False)
        if managed:
            sys.stdin.readline()
        else:
            wait = Event()
            while True:
                wait.wait(1)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            service.shutdown()
        finally:
            app.close()
