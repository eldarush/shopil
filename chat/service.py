"""One frozen snapshot and at most three Actor/Critic attempts per question."""

from collections.abc import Awaitable, Callable
import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from threading import Condition
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from .model import ModelError

if TYPE_CHECKING:
    from .actor import Actor
    from .critic import Critic


@dataclass(frozen=True)
class Reply:
    status: Literal["approved", "rejected", "error"]
    answer: str | None
    message: str
    snapshot_id: str | None
    captured_at: str | None
    sources: list
    history: list[dict[str, str]]


def _approved_history(history: list[dict[str, str]]) -> list[dict[str, str]]:
    if not isinstance(history, list) or len(history) % 2:
        raise ValueError("History must contain complete approved question/answer pairs")
    for index, item in enumerate(history):
        if (not isinstance(item, dict) or set(item) != {"role", "content"}
                or item["role"] != ("user" if index % 2 == 0 else "assistant")
                or not isinstance(item["content"], str)):
            raise ValueError("History must contain approved user/assistant text pairs")
    return deepcopy(history)


def _freeze(snapshot: dict, question: str) -> tuple[str, dict]:
    if not isinstance(snapshot, dict):
        raise ValueError("Capture must return an evidence object")
    snapshot = deepcopy(snapshot)
    for field in ("snapshot_id", "captured_at"):
        if not isinstance(snapshot.get(field), str) or not snapshot[field].strip():
            raise ValueError(f"Evidence requires {field}")
    timestamp = datetime.fromisoformat(snapshot["captured_at"])
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Evidence time requires a timezone")
    if not isinstance(snapshot.get("sources"), list):
        raise ValueError("Evidence requires a sources list")
    if snapshot.get("question", question) != question:
        raise ValueError("Evidence belongs to a different question")
    snapshot["question"] = question
    try:
        serialized = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("Evidence must contain only finite JSON values") from exc
    return serialized, snapshot


class ChatService:
    def __init__(
        self,
        actor: "Actor",
        critic: "Critic",
        capture: Callable[[str, list[dict[str, str]]], Awaitable[dict]],
        audit: Callable[[dict], None],
    ):
        self.actor, self.critic = actor, critic
        self.capture, self.audit = capture, audit
        self._condition = Condition()
        self._active = {}
        self._closing = False
        self._shutdown_audit_failed = False

    def shutdown(self, timeout: float = 5) -> None:
        """Stop admission and await cancellation audits from the server's main thread.

        Call outside the event loops handling answers, before closing the server.
        A timeout or unavailable audit sink must produce an unsuccessful shutdown.
        """
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Shutdown timeout must be positive and finite")
        with self._condition:
            self._closing = True
            active = list(self._active.items())
        for task, loop in active:
            loop.call_soon_threadsafe(task.cancel)
        with self._condition:
            if not self._condition.wait_for(lambda: not self._active, timeout=timeout):
                raise RuntimeError("Chat shutdown timed out waiting for question audits")
            if self._shutdown_audit_failed:
                raise RuntimeError("Chat shutdown could not persist a cancellation audit")

    async def answer(self, question: str, history: list[dict[str, str]]) -> Reply:
        task = asyncio.current_task()
        with self._condition:
            if self._closing:
                return Reply("error", None, "המערכת נסגרת; לא התקבלה שאלה חדשה.",
                             None, None, [], _approved_history(history))
            self._active[task] = asyncio.get_running_loop()
        try:
            return await self._answer(question, history)
        finally:
            with self._condition:
                del self._active[task]
                self._condition.notify_all()

    async def _answer(self, question: str, history: list[dict[str, str]]) -> Reply:
        if not isinstance(question, str) or not question.strip():
            raise ValueError("Question must be nonempty text")
        approved = _approved_history(history)
        request_id = uuid4().hex
        snapshot_id = captured_at = None
        sources = []
        attempt = 0
        draft = feedback = None
        stage = "capture"

        def record(event: str, **fields) -> None:
            self.audit({"event": event, "request_id": request_id,
                        "snapshot_id": snapshot_id,
                        "recorded_at": datetime.now(timezone.utc).isoformat(), **fields})

        def reply(status, answer, message, result_history=approved):
            return Reply(status, answer, message, snapshot_id, captured_at,
                         deepcopy(sources), deepcopy(result_history))

        try:
            captured = await self.capture(question, deepcopy(approved))
            if self._closing:
                raise asyncio.CancelledError
            evidence_json, snapshot = _freeze(captured, question)
            snapshot_id, captured_at = snapshot["snapshot_id"], snapshot["captured_at"]
            sources = snapshot["sources"]
            stage = "audit_snapshot"
            record("snapshot", evidence_json=evidence_json)
            for attempt in range(1, 4):
                previous_draft = draft
                draft = None
                stage = "actor"
                draft = await self.actor.draft(
                    question, evidence_json, deepcopy(approved), previous_draft, feedback
                )
                if self._closing:
                    raise asyncio.CancelledError
                if not isinstance(draft, str) or not draft.strip():
                    raise ModelError("Actor returned empty or nontext output")
                stage = "critic"
                evaluation = await self.critic.evaluate(question, evidence_json, draft)
                if self._closing:
                    raise asyncio.CancelledError
                if (type(evaluation.is_acceptable) is not bool
                        or not isinstance(evaluation.feedback, str)
                        or not evaluation.feedback.strip()):
                    raise ValueError("Critic returned an invalid evaluation")
                feedback = evaluation.feedback
                stage = "audit_attempt"
                record("attempt", attempt=attempt, draft=draft,
                       is_acceptable=evaluation.is_acceptable, feedback=feedback)
                if evaluation.is_acceptable:
                    stage = "audit_terminal"
                    record("terminal", status="approved", attempts=attempt)
                    updated = approved + [{"role": "user", "content": question},
                                          {"role": "assistant", "content": draft}]
                    return reply("approved", draft, "התשובה אושרה על סמך הנתונים.", updated)
            stage = "audit_terminal"
            record("terminal", status="rejected", attempts=attempt)
            return reply("rejected", None, "לא התקבלה תשובה מאושרת לאחר שלושה ניסיונות.")
        except asyncio.CancelledError:
            failed = False
            if stage in ("actor", "critic"):
                try:
                    record("attempt", attempt=attempt, draft=draft,
                           is_acceptable=None, feedback=None,
                           error_type="CancelledError", error_stage=stage)
                except (OSError, ValueError):
                    failed = True
            try:
                record("terminal", status="error", attempts=attempt,
                       error_type="CancelledError", error_stage=stage)
            except (OSError, ValueError):
                failed = True
            if failed:
                with self._condition:
                    self._shutdown_audit_failed = True
            raise
        except (ModelError, TimeoutError, ValueError, OSError) as exc:
            try:
                if stage in ("actor", "critic"):
                    record("attempt", attempt=attempt, draft=draft,
                           is_acceptable=None, feedback=None,
                           error_type=type(exc).__name__, error_stage=stage)
                record("terminal", status="error", attempts=attempt,
                       error_type=type(exc).__name__, error_stage=stage)
            except OSError:
                # An unavailable audit sink must never turn an answer into success.
                pass
            return reply("error", None, "אירעה שגיאה בעיבוד השאלה. לא הוצגה תשובה לא מאושרת.")
