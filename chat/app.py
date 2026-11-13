"""Hebrew Gradio interface with per-session, approved-only conversation history."""

from copy import deepcopy
from typing import TYPE_CHECKING

import gradio as gr

if TYPE_CHECKING:
    from .service import ChatService


class ChatPage:
    def __init__(self, service: "ChatService"):
        self.service = service

    async def ask(self, question: str, history: list[dict[str, str]]):
        if not question.strip():
            return history, history, "נא להזין שאלה.", [], "", question
        result = await self.service.answer(question, deepcopy(history))
        approved = result.history if result.status == "approved" else history
        snapshot = " · ".join(value for value in (result.snapshot_id, result.captured_at) if value)
        return (deepcopy(approved), deepcopy(approved), result.message,
                result.sources, snapshot, "" if result.status == "approved" else question)


def build_app(service: "ChatService") -> gr.Blocks:
    page = ChatPage(service)
    with gr.Blocks(title="ShopIL", analytics_enabled=False) as app:
        gr.Markdown("# ShopIL — העוזר העסקי\nשאלו בעברית על נתוני החנות. רק תשובות שאושרו יוצגו בשיחה.")
        history = gr.State([])
        conversation = gr.Chatbot(label="השיחה", rtl=True, render_markdown=False,
                                  sanitize_html=True, allow_file_downloads=False)
        question = gr.Textbox(label="השאלה שלך", rtl=True, lines=2,
                              placeholder="מה ההכנסה מהזמנות ששולמו?")
        send = gr.Button("שליחה", variant="primary")
        status = gr.Textbox(label="סטטוס", interactive=False, rtl=True)
        snapshot = gr.Textbox(label="מזהה ומועד צילום הנתונים", interactive=False)
        sources = gr.JSON(label="מקורות הנתונים")
        gr.Examples(
            [["מה ההכנסה מהזמנות ששולמו?"], ["ומה לגבי מוצר 5?"], ["מהן עלויות המשלוח?"]],
            inputs=question, label="שאלות לדוגמה",
        )
        outputs = [conversation, history, status, sources, snapshot, question]
        send.click(page.ask, [question, history], outputs, api_name="ask",
                   concurrency_id="chat-submit", concurrency_limit=1)
        question.submit(page.ask, [question, history], outputs, api_name=False,
                        concurrency_id="chat-submit", concurrency_limit=1)
        conversation.clear(lambda: ([], [], "", [], "", ""), outputs=outputs,
                           api_name="clear", concurrency_id="chat-submit", concurrency_limit=1)
    return app.queue(default_concurrency_limit=1)
