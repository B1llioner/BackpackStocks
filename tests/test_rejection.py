import gzip
import time
from pathlib import Path
from runpy import run_path

import httpx
import pytest

from backpack_stocks.rejection import classify_rejection
from backpack_stocks.transport import RemoteError, Transport, UnknownOutcome


@pytest.mark.parametrize(
    "status,message,kind,reason",
    [
        (401, "fixture-secret", "authentication", None),
        (
            403,
            "API key does not have access to this endpoint",
            "permissions_or_firewall",
            "missing_permission",
        ),
        (403, "Request blocked by firewall", "permissions_or_firewall", "firewall_rule"),
        (403, "fixture-secret", "permissions_or_firewall", "unspecified"),
        (429, "[API Gateway] Too many requests", "rate_limit", "plan_quota"),
        (429, "Too many requests", "rate_limit", "firewall_rule"),
        (429, "fixture-secret", "rate_limit", "unspecified"),
        (403, "[TURNSTILE] fixture-secret", "interactive_check", None),
    ],
)
def test_access_diagnostics_have_only_fixed_labels(status, message, kind, reason):
    result = classify_rejection(httpx.Response(status, json={"message": message, "token": "fixture-secret"}))
    assert result is not None and result["kind"] == kind
    assert result.get("reason") == reason
    assert "fixture-secret" not in str(result)


@pytest.mark.parametrize(
    "content,provider",
    [
        (b"<html>Vercel Security Checkpoint</html>", "vercel"),
        (b"<script src='/cdn-cgi/challenge-platform/'></script>", "cloudflare"),
        (b"<html>captcha</html>", "unknown"),
    ],
)
def test_html_checkpoint_has_a_diagnostic_without_disclosing_body(content, provider):
    response = httpx.Response(200, headers={"content-type": "text/html; charset=utf-8"}, content=content)
    assert classify_rejection(response) == {
        "status": 200,
        "kind": "interactive_check",
        "provider": provider,
    }


def test_successful_json_with_captcha_text_is_not_an_interactive_check():
    assert classify_rejection(httpx.Response(200, json={"name": "captcha"})) is None


@pytest.mark.parametrize("content", [b"not-json", b"[]", b'{"message": 3}', b"x" * 8193])
def test_unrecognised_or_large_error_bodies_do_not_escape(content):
    result = classify_rejection(httpx.Response(429, content=content))
    assert result == {"status": 429, "kind": "rate_limit", "provider": "api", "reason": "unspecified"}


@pytest.mark.parametrize("status", [200, 403, 429])
@pytest.mark.parametrize("method,error", [("GET", RemoteError), ("POST", UnknownOutcome)])
async def test_vercel_checkpoint_never_retries_or_consumes_api_reset(status, method, error):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(
            status,
            headers={
                "x-vercel-mitigated": "challenge",
                "x-vercel-challenge-token": "fixture-private-token",
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(time.time() + 120),
                "Retry-After": "120",
            },
            json={"message": "[API Gateway] Too many requests", "token": "fixture-private-token"},
        )

    transport = Transport(
        "",
        client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler)),
        rate_headers=True,
    )
    try:
        with pytest.raises(error, match="verification required") as raised:
            await transport.request(method, "/route")
        assert len(seen) == 1
        state = transport.status()
        assert state["last_rejection"] == {
            "status": status,
            "kind": "interactive_check",
            "provider": "vercel",
        }
        assert state["responses_429_all_buckets"] == 0
        assert state["adaptive_backoffs"] == 0 and state["cooldown_s"] == 0
        assert state["remote_budgets"] == {}
        assert "fixture-private-token" not in str(state) + str(raised.value)
    finally:
        await transport.close()


@pytest.mark.parametrize("status", [403, 429])
@pytest.mark.parametrize("method,error", [("GET", RemoteError), ("POST", UnknownOutcome)])
async def test_vercel_deny_stops_without_replaying_or_changing_route(status, method, error):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(status, headers={"x-vercel-mitigated": "deny"}, content=b"fixture-secret")

    transport = Transport(
        "", client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler))
    )
    try:
        with pytest.raises(error, match="firewall denied"):
            await transport.request(method, "/route")
        assert len(seen) == 1
        assert transport.status()["last_rejection"]["kind"] == "firewall_deny"
        assert transport.responses_429 == 0
    finally:
        await transport.close()


async def test_actual_vercel_rate_limit_retains_backoff_and_retry():
    seen = []

    def handler(request):
        seen.append(time.monotonic())
        if len(seen) == 1:
            return httpx.Response(429, headers={"x-vercel-mitigated": "rate_limit", "Retry-After": "0.04"})
        return httpx.Response(200, json={"ok": True})

    transport = Transport(
        "", client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler))
    )
    try:
        assert await transport.request("GET", "/route") == {"ok": True}
        assert len(seen) == 2 and seen[1] - seen[0] >= 0.035
        assert transport.responses_429 == 1 and transport.limiter.adaptive_backoffs == 1
        assert transport.status()["last_rejection"] == {
            "status": 429,
            "kind": "rate_limit",
            "provider": "vercel",
            "reason": "firewall_rule",
        }
    finally:
        await transport.close()


def test_research_classifier_handles_already_decoded_gzip_bytes():
    classify = run_path(str(Path(__file__).resolve().parents[1] / "scripts/research_jupiter.py"))["classify"]
    response = httpx.Response(
        429,
        headers={"content-encoding": "gzip", "content-type": "application/json"},
        content=gzip.compress(b'{"message":"[API Gateway] Too many requests"}'),
    )
    assert classify(response, response.content) == "rate_limit"
