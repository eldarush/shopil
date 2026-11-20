"""The model boundary shared by answer generation and evaluation."""

from typing import Protocol


class ModelError(RuntimeError):
    """A model request failed or returned an unusable response."""


class RenderedDraft(str):
    """Keep the original model output alongside the displayed answer for audit."""
    def __new__(cls, text, raw):
        value = super().__new__(cls, text)
        value.actor_output_json = raw
        return value

    def __getnewargs__(self):
        return str(self), self.actor_output_json


class ChatModel(Protocol):
    async def complete(
        self, messages: list[dict[str, str]], *, schema: dict | None = None
    ) -> str:
        """Return completed text, or raise ModelError; never substitute an answer."""
        ...
