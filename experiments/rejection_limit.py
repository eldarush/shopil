"""Explicitly mocked three-rejection experiment on the same service used by Gradio."""

import argparse
import asyncio
from datetime import date
import json
from pathlib import Path

from chat.capture import build_capture
from chat.critic import Evaluation
from chat.service import ChatService


class FaultyActor:
    async def draft(self, question, evidence_json, history, previous_draft=None, feedback=None):
        return "ההכנסה מהזמנות ששולמו הסתכמה בשקל אחד."


class RejectingCritic:
    async def evaluate(self, question, evidence_json, draft):
        return Evaluation(is_acceptable=False, feedback="בדיקה מדומה. הסכום שגוי ויש לבדוק אותו שוב מול השאילתה.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture = build_capture(args.warehouse, Path("unused.sqlite"), start=date(2026, 1, 1),
                            end=date(2027, 1, 1), question_scoped=True)
    events = []
    service = ChatService(FaultyActor(), RejectingCritic(), capture, events.append)
    result = asyncio.run(service.answer("כמה הכנסנו מהזמנות ששולמו?", []))
    assert result.status == "rejected" and result.history == [] and result.answer is None
    assert [event["is_acceptable"] for event in events if event["event"] == "attempt"] == [False] * 3
    assert events[-1]["attempts"] == 3
    with args.output.open("x", encoding="utf-8") as out:
        json.dump({"case": "MOCKED Actor and Critic: three rejections", "passed": True, "events": events,
                   "visible_history": result.history, "user_message": result.message}, out, ensure_ascii=False, indent=2)
        out.write("\n")


if __name__ == "__main__":
    main()
