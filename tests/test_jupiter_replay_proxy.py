from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from solders.keypair import Keypair

from backpack_stocks.jupiter_proxy import replay_proxy
from backpack_stocks.jupiter_replay import SOL, USDC, JupiterReplay, ReplayError, SwapInput

PROXY = "http://fixture-user:fixture-proxy-secret@proxy.example:8080"


def test_proxy_precedence_and_project_environment_names():
    assert replay_proxy({"OUTBOUND_PROXY_URL": PROXY}) == PROXY
    assert replay_proxy({"JUPITER_PROXY_URL": PROXY, "OUTBOUND_PROXY_URL": "http://other:80"}) == PROXY
    assert replay_proxy({"JUPITER_REPLAY_PROXY": PROXY, "JUPITER_PROXY_URL": "http://other:80"}) == PROXY
    assert replay_proxy({"MY_PROXY": PROXY}, env_name="MY_PROXY") == PROXY


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"JUPITER_REPLAY_PROXY": "DIRECT", "OUTBOUND_PROXY_URL": PROXY},
        {"JUPITER_PROXY_URL": "http://u:secret@host:99999"},
        {"JUPITER_PROXY_URL": "socks5://u:secret@host:1080"},
    ],
)
def test_no_missing_invalid_or_direct_proxy_fallback(values):
    with pytest.raises(ValueError) as raised:
        replay_proxy(values)
    assert "secret" not in str(raised.value)


def test_default_client_requires_proxy_and_injected_client_cannot_ignore_it():
    with pytest.raises(ReplayError, match="requires"):
        JupiterReplay()
    with pytest.raises(ValueError, match="would ignore"):
        JupiterReplay(proxy=PROXY, client=object())


async def test_single_proxy_transport_covers_order_holdings_auth_lifecycle_and_execute(monkeypatch, tmp_path):
    real_client = httpx.AsyncClient
    constructions = []
    paths = []
    wallet = Keypair()
    address = str(wallet.pubkey())
    expires = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()

    def handler(request):
        paths.append((request.url.host, request.url.path))
        assert "proxy-authorization" not in request.headers  # Transport sends it only to the proxy.
        if request.url.path.endswith("/order"):
            return httpx.Response(
                200,
                json={
                    "inputMint": SOL,
                    "outputMint": USDC,
                    "swapMode": "ExactIn",
                    "inAmount": "10000000",
                    "requestId": "fake",
                },
            )
        if request.url.path.endswith("/challenge"):
            return httpx.Response(
                200, json={"type": "message", "challenge": f"Authenticate Jupiter {address}"}
            )
        if request.url.path.endswith(("/verify", "/refresh")):
            return httpx.Response(
                200,
                json={
                    "authMode": "access_refresh",
                    "accessToken": "fake-access",
                    "refreshToken": "fake-refresh",
                    "expiresAt": expires,
                    "tokenType": "Bearer",
                },
            )
        if request.url.path.endswith("/vault"):
            return httpx.Response(404, json={"error": "Vault not found"})
        return httpx.Response(200, json={"success": True})

    def factory(**kwargs):
        constructions.append(kwargs)
        return real_client(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    client = JupiterReplay(proxy=PROXY, audit=tmp_path / "audit.jsonl", interval=0)
    try:
        await client.order(SwapInput(SOL, USDC, "0.01", 9))
        await client.holdings(address)
        await client.login(wallet)
        await client.trigger_vault()
        await client.refresh()
        await client.logout()
        # Execute byte gates are covered separately; here verify the shared HTTP submission transport.
        await client.request("POST", client.base + "/execute", payload={"signedTransaction": "fake"})
    finally:
        await client.close()
    assert len(constructions) == 1
    assert constructions[0]["proxy"] == PROXY and constructions[0]["trust_env"] is False
    assert {host for host, _ in paths} == {"api.jup.ag", "ultra-api.jup.ag", "trigger-api.jup.ag"}
    records = [json.loads(line) for line in (tmp_path / "audit.jsonl").read_text().splitlines()]
    assert len(records) == 8 and all(r["proxied"] for r in records)


async def test_proxy_error_does_not_retry_or_expose_credentials(monkeypatch, caplog):
    real_client = httpx.AsyncClient
    constructions = []
    calls = []

    def handler(request):
        calls.append(request)
        logging.getLogger("httpcore.proxy").debug("Proxy %s", PROXY)
        raise httpx.ProxyError(PROXY)

    def factory(**kwargs):
        constructions.append(kwargs)
        return real_client(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    client = JupiterReplay(proxy=PROXY, interval=0)
    try:
        with caplog.at_level(logging.DEBUG), pytest.raises(ReplayError) as raised:
            await client.order(SwapInput(SOL, USDC, "0.01", 9))
        assert raised.value.__cause__ is None and raised.value.__suppress_context__
        assert "fixture-proxy-secret" not in str(raised.value) + caplog.text
        assert len(calls) == len(constructions) == 1
    finally:
        await client.close()
