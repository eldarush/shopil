"""Text-only requests to an explicitly configured local Ollama server."""

import asyncio
import math
import re
from urllib.parse import urlsplit

import httpx

from .model import ModelError

MIN_OLLAMA_VERSION = (0, 13, 0)


class OllamaModel:
    """Require stable Ollama >=0.13.0 and fail instead of dropping model context.

    That released API and both runners support disabling truncation and shifting:
    https://github.com/ollama/ollama/tree/v0.13.0
    Check /api/version before every model request; older/unknown servers may
    silently ignore these fields. No tools, redirects, proxy inheritance,
    downloads, or automatic retries are used.
    """

    def __init__(
        self,
        model: str,
        base_url: str = "http://127.0.0.1:11434",
        timeout: float = 120,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not isinstance(model, str) or not model.strip():
            raise ValueError("An installed Ollama model name is required")
        try:
            if not isinstance(base_url, str) or any(c.isspace() for c in base_url):
                raise ValueError
            parsed = urlsplit(base_url)
            if (
                parsed.scheme not in ("http", "https")
                or parsed.hostname not in ("localhost", "127.0.0.1", "::1")
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in ("", "/")
                or "?" in base_url
                or "#" in base_url
                or parsed.port == 0
            ):
                raise ValueError
        except ValueError:
            raise ValueError("Ollama URL must be a loopback HTTP(S) origin without credentials") from None
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("Model timeout must be positive and finite")
        self.model = model.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Injection exists for HTTP contract tests, not an alternate model provider.
        self._transport = transport

    async def complete(
        self, messages: list[dict[str, str]], *, schema: dict | None = None
    ) -> str:
        if not isinstance(messages, list) or not messages or any(
            not isinstance(message, dict)
            or set(message) != {"role", "content"}
            or message["role"] not in ("system", "user", "assistant")
            or not isinstance(message["content"], str)
            for message in messages
        ):
            raise ModelError("Model input must contain text messages only")
        if schema is not None and not isinstance(schema, dict):
            raise ModelError("Model output schema must be an object")
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "truncate": False,
            "shift": False,
            "options": {"temperature": 0, "num_predict": 1024, "num_ctx": 8192},
        }
        if schema is not None:
            payload["format"] = schema
        try:
            async with asyncio.timeout(self.timeout):
                async with httpx.AsyncClient(
                    timeout=self.timeout,
                    transport=self._transport,
                    trust_env=False,
                    follow_redirects=False,
                ) as client:
                    version_response = await client.get(f"{self.base_url}/api/version")
                    version_response.raise_for_status()
                    try:
                        version_payload = version_response.json()
                    except ValueError:
                        raise ModelError("Unable to verify the local Ollama version") from None
                    version = version_payload.get("version") if isinstance(version_payload, dict) else None
                    match = (re.fullmatch(r"([0-9]+)\.([0-9]+)\.([0-9]+)", version)
                             if isinstance(version, str) else None)
                    if match is None or tuple(map(int, match.groups())) < MIN_OLLAMA_VERSION:
                        raise ModelError("Stable Ollama 0.13.0 or newer is required to preserve evidence context")
                    response = await client.post(f"{self.base_url}/api/chat", json=payload)
                    response.raise_for_status()
        except (TimeoutError, httpx.TimeoutException):
            raise ModelError("Local model request timed out") from None
        except httpx.HTTPStatusError:
            raise ModelError("Local model returned an HTTP error") from None
        except httpx.RequestError:
            raise ModelError("Local model could not be reached") from None
        except (TypeError, ValueError):
            raise ModelError("Model request could not be encoded") from None
        try:
            result = response.json()
        except ValueError:
            raise ModelError("Local model returned invalid JSON") from None
        if not isinstance(result, dict) or "error" in result:
            raise ModelError("Local model returned an invalid response")
        if result.get("done") is not True or result.get("done_reason") != "stop":
            raise ModelError("Local model response was incomplete or truncated")
        message = result.get("message")
        if not isinstance(message, dict) or message.get("role") != "assistant":
            raise ModelError("Local model returned an invalid assistant message")
        if any(
            item.get("refusal") is not None
            or item.get("tool_calls") not in (None, [])
            for item in (result, message)
        ):
            raise ModelError("Local model refused the request or requested a tool")
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ModelError("Local model returned empty or nontext output")
        return content.strip()
