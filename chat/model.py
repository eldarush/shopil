"""The model boundary shared by answer generation and evaluation."""

from typing import Protocol


class ModelError(RuntimeError):
    """A model request failed or returned an unusable response."""


class ChatModel(Protocol):
    async def complete(
        self, messages: list[dict[str, str]], *, schema: dict | None = None
    ) -> str:
        """Return completed text, or raise ModelError; never substitute an answer."""
        ...
