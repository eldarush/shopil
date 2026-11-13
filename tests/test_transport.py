"""HTTP contract tests; MockTransport never contacts a real model server."""

import asyncio
from copy import deepcopy
import inspect
import json

import httpx
import pytest

from chat.model import ModelError
from chat.transport import OllamaModel


MESSAGES = [{"role": "user", "content": "מה ההכנסה?"}]
VALID = {
    "done": True,
    "done_reason": "stop",
    "message": {"role": "assistant", "content": "ההכנסה בצילום המצב היא אפס."},
}


def model_transport(handler):
    """Provide a supported version while individual tests control chat responses."""
    async def route(request):
        if request.url.path == "/api/version":
            assert request.method == "GET"
            return httpx.Response(200, json={"version": "0.13.0"})
        assert request.method == "POST" and request.url.path == "/api/chat"
        result = handler(request)
        return await result if inspect.isawaitable(result) else result

    return httpx.MockTransport(route)


def test_actor_request_is_local_text_only_and_bounded(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=VALID)

    monkeypatch.setenv("HTTP_PROXY", "http://unwanted-proxy.example")
    original = deepcopy(MESSAGES)
    model = OllamaModel("qwen3:4b-instruct", transport=model_transport(handler))
    assert asyncio.run(model.complete(MESSAGES)) == VALID["message"]["content"]
    assert len(requests) == 1
    assert str(requests[0].url) == "http://127.0.0.1:11434/api/chat"
    payload = json.loads(requests[0].content)
    assert payload == {
        "model": "qwen3:4b-instruct", "messages": original, "stream": False,
        "truncate": False, "shift": False,
        "options": {"num_predict": 4096, "num_ctx": 16384},
    }
    assert "authorization" not in requests[0].headers
    assert MESSAGES == original


def test_critic_schema_is_sent_as_format_and_output_is_not_reinterpreted():
    schema = {"type": "object", "properties": {"is_acceptable": {"type": "boolean"}},
              "required": ["is_acceptable"]}
    original = deepcopy(schema)
    content = '{"is_acceptable":false,"feedback":"המספר אינו תואם"}'

    def handler(request):
        assert json.loads(request.content)["format"] == original
        payload = json.loads(request.content)
        assert "think" not in payload
        assert "temperature" not in payload["options"]
        return httpx.Response(200, json={**VALID, "message": {"role": "assistant", "content": content}})

    model = OllamaModel("qwen3:4b-instruct", transport=model_transport(handler))
    assert asyncio.run(model.complete(MESSAGES, schema=schema)) == content
    assert schema == original


@pytest.mark.parametrize("think", [True, False])
def test_explicit_thinking_setting_preserves_sampling_defaults(think):
    def handler(request):
        payload = json.loads(request.content)
        assert payload["think"] is think
        assert "temperature" not in payload["options"]
        return httpx.Response(200, json=VALID)

    model = OllamaModel("qwen3:1.7b", think=think, transport=model_transport(handler))
    assert asyncio.run(model.complete(MESSAGES)) == VALID["message"]["content"]


@pytest.mark.parametrize("think", [0, 1, "false"])
def test_invalid_thinking_override_is_rejected(think):
    with pytest.raises(ValueError, match="think"):
        OllamaModel("qwen3", think=think)


@pytest.mark.parametrize("url", [
    "https://example.com", "http://0.0.0.0:11434", "http://192.168.1.1:11434",
    "ftp://localhost", "http://user:secret@localhost", "http://@localhost",
    "http://localhost/api", "http://localhost?token=secret", "http://localhost#secret",
    "http://localhost?", "http://localhost#", "http://localhost:0", "http://localhost:99999",
    "http://localhost:bad", "http://[::1", "http://localhost\n", None,
])
def test_nonlocal_or_credential_bearing_urls_are_rejected_without_echoing_url(url):
    with pytest.raises(ValueError, match="loopback") as caught:
        OllamaModel("qwen3", base_url=url)
    assert "secret" not in str(caught.value)
    assert "example.com" not in str(caught.value)


@pytest.mark.parametrize("url", ["http://localhost:11435/", "http://[::1]:11434", "https://127.0.0.1"])
def test_loopback_origins_are_accepted(url):
    model = OllamaModel("qwen3", base_url=url)
    assert model.base_url == url.rstrip("/")


@pytest.mark.parametrize("timeout", [0, -1, True, float("inf"), float("nan"), "120"])
def test_invalid_timeouts_are_rejected(timeout):
    with pytest.raises(ValueError, match="timeout"):
        OllamaModel("qwen3", timeout=timeout)


@pytest.mark.parametrize("model", ["", "  ", None])
def test_model_is_explicit(model):
    with pytest.raises(ValueError, match="model name"):
        OllamaModel(model)


@pytest.mark.parametrize("status", [302, 401, 404, 429, 500])
def test_http_errors_and_redirects_do_not_retry_or_expose_body(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="secret response detail", headers={"Location": "https://example.com"})

    model = OllamaModel("qwen3", transport=model_transport(handler))
    with pytest.raises(ModelError, match="HTTP error") as caught:
        asyncio.run(model.complete(MESSAGES))
    assert "secret" not in str(caught.value)
    assert len(calls) == 1


@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.ConnectError])
def test_request_errors_are_sanitized_and_not_retried(error):
    calls = []

    def handler(request):
        calls.append(request)
        raise error("credential and private URL", request=request)

    model = OllamaModel("qwen3", transport=model_transport(handler))
    with pytest.raises(ModelError) as caught:
        asyncio.run(model.complete(MESSAGES))
    assert "credential" not in str(caught.value)
    assert len(calls) == 1


def test_total_deadline_cancels_slow_request():
    calls = []

    async def handler(request):
        calls.append(request)
        await asyncio.sleep(1)
        return httpx.Response(200, json=VALID)

    model = OllamaModel("qwen3", timeout=0.02, transport=model_transport(handler))
    with pytest.raises(ModelError, match="timed out"):
        asyncio.run(model.complete(MESSAGES))
    assert len(calls) == 1


def test_invalid_response_json_is_sanitized():
    model = OllamaModel("qwen3", transport=model_transport(
        lambda request: httpx.Response(200, text="not JSON: secret detail")
    ))
    with pytest.raises(ModelError, match="invalid JSON"):
        asyncio.run(model.complete(MESSAGES))


@pytest.mark.parametrize("messages,schema", [
    ([], None), (None, None),
    ([{"role": "tool", "content": "run a command"}], None),
    ([{"role": "user", "content": "text", "tool_calls": [{}]}], None),
    ([{"role": "user", "content": 42}], None),
    (MESSAGES, "json"),
])
def test_nontext_or_tool_inputs_fail_before_any_request(messages, schema):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=VALID)

    model = OllamaModel("qwen3", transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError):
        asyncio.run(model.complete(messages, schema=schema))
    assert calls == []


@pytest.mark.parametrize("result", [
    [], None, {"error": "secret detail"}, {},
    {**VALID, "done": False}, {**VALID, "done": 1},
    {**VALID, "done_reason": "length"}, {**VALID, "done_reason": "load"},
    {**VALID, "message": None}, {**VALID, "message": {"role": "user", "content": "text"}},
    {**VALID, "message": {"role": "assistant", "content": ""}},
    {**VALID, "message": {"role": "assistant", "content": " \n"}},
    {**VALID, "message": {"role": "assistant", "content": {"text": "text"}}},
    {**VALID, "message": {"role": "assistant", "content": "text", "tool_calls": [{"function": "run"}]}},
    {**VALID, "message": {"role": "assistant", "content": "text", "refusal": "refused"}},
    {**VALID, "refusal": "refused"}, {**VALID, "tool_calls": [{}]},
])
def test_incomplete_refused_or_unexpected_responses_are_never_answers(result):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=result)

    model = OllamaModel("qwen3", transport=model_transport(handler))
    with pytest.raises(ModelError):
        asyncio.run(model.complete(MESSAGES))
    assert len(calls) == 1


def test_concurrent_calls_do_not_share_messages_or_block_event_loop():
    async def run():
        arrived = 0
        both_arrived = asyncio.Event()

        async def handler(request):
            nonlocal arrived
            arrived += 1
            if arrived == 2:
                both_arrived.set()
            await asyncio.wait_for(both_arrived.wait(), timeout=1)
            content = json.loads(request.content)["messages"][0]["content"]
            return httpx.Response(200, json={**VALID, "message": {"role": "assistant", "content": content}})

        model = OllamaModel("qwen3", transport=model_transport(handler))
        return await asyncio.gather(
            model.complete([{"role": "user", "content": "ראשון"}]),
            model.complete([{"role": "user", "content": "שני"}]),
        )

    assert asyncio.run(run()) == ["ראשון", "שני"]


@pytest.mark.parametrize("version", ["0.13.0", "0.40.16", "1.0.0"])
def test_version_verified_before_sending_any_model_messages(version):
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path == "/api/version":
            assert request.content == b""
            return httpx.Response(200, json={"version": version})
        return httpx.Response(200, json=VALID)

    model = OllamaModel("qwen3", transport=httpx.MockTransport(handler))
    assert asyncio.run(model.complete(MESSAGES)) == VALID["message"]["content"]
    assert calls == [("GET", "/api/version"), ("POST", "/api/chat")]


@pytest.mark.parametrize("payload", [
    {"version": "0.12.5"}, {"version": "0.13.0-rc1"}, {"version": "0.13.0+custom"},
    {"version": "private-unknown-version"}, {"version": None}, {"version": 13},
    {}, [], "0.13.0",
])
def test_old_or_unknown_version_fails_before_chat(payload):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=payload)

    model = OllamaModel("qwen3", transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError, match="Ollama 0.13.0") as caught:
        asyncio.run(model.complete(MESSAGES))
    assert calls == ["/api/version"]
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("status", [200, 302, 404, 500])
def test_unreadable_version_never_sends_chat_or_leaks_response(status):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(status, text="private invalid version response",
                              headers={"Location": "https://example.com"})

    model = OllamaModel("qwen3", transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError) as caught:
        asyncio.run(model.complete(MESSAGES))
    assert calls == ["/api/version"]
    assert "private" not in str(caught.value)


def test_version_check_is_inside_total_request_deadline():
    calls = []

    async def handler(request):
        calls.append(request.url.path)
        await asyncio.sleep(1)
        return httpx.Response(200, json={"version": "0.13.0"})

    model = OllamaModel("qwen3", timeout=0.02, transport=httpx.MockTransport(handler))
    with pytest.raises(ModelError, match="timed out"):
        asyncio.run(model.complete(MESSAGES))
    assert calls == ["/api/version"]


def test_restarted_older_server_is_not_trusted_from_previous_call():
    versions = iter(("0.13.0", "0.12.5"))
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/version":
            return httpx.Response(200, json={"version": next(versions)})
        return httpx.Response(200, json=VALID)

    model = OllamaModel("qwen3", transport=httpx.MockTransport(handler))
    assert asyncio.run(model.complete(MESSAGES)) == VALID["message"]["content"]
    with pytest.raises(ModelError, match="Ollama 0.13.0"):
        asyncio.run(model.complete(MESSAGES))
    assert calls == ["/api/version", "/api/chat", "/api/version"]


@pytest.mark.parametrize("error_stage", ["actor", "critic"])
def test_context_overflow_cannot_publish_an_answer_or_change_history(error_stage):
    from chat.actor import Actor
    from chat.critic import Critic
    from chat.service import ChatService

    calls = []

    def handler(request):
        payload = json.loads(request.content)
        assert payload["truncate"] is False and payload["shift"] is False
        calls.append(payload)
        if error_stage == "critic" and len(calls) == 1:
            return httpx.Response(200, json=VALID)
        return httpx.Response(413, json={"error": "input exceeds context: private response details"})

    async def capture(question, history):
        return {"question": question, "snapshot_id": "overflow-test",
                "captured_at": "2026-11-13T12:00:00+00:00", "sources": []}

    history = [{"role": "user", "content": "previous question"},
               {"role": "assistant", "content": "previous approved answer"}]
    audit = []
    model = OllamaModel("qwen3", transport=model_transport(handler))
    service = ChatService(Actor(model), Critic(model), capture, audit.append)
    reply = asyncio.run(service.answer("new question", history))
    assert reply.status == "error" and reply.answer is None and reply.history == history
    assert len(calls) == (1 if error_stage == "actor" else 2)
    assert audit[-1]["status"] == "error" and audit[-1]["error_stage"] == error_stage
    assert "private response details" not in json.dumps(audit)
