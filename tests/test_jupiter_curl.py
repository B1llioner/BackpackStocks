from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from backpack_stocks.jupiter_curl import CurlClient
from backpack_stocks.jupiter_replay import (
    SOL,
    USDC,
    JupiterReplay,
    ReplayError,
    SwapInput,
    frontend_key_from_source,
)
from backpack_stocks.jupiter_wallet import decode_swap_transaction, sign_swap_transaction

curl = pytest.importorskip("curl_cffi")
from curl_cffi.requests.exceptions import RequestException  # noqa: E402

PROXY = "http://fixture%40user:fixture%3Apassword@proxy.example:8080"


class StubSession:
    def __init__(self, **kwargs):
        self.options = kwargs
        self.calls = []
        self.closed = False
        self.fail = False
        self.challenge = None
        self.wallet = None

    async def request(self, method, url, *, params=None, json=None, headers=None, timeout=None):
        self.last_timeout = timeout
        self.calls.append((method, url, params, json, headers))
        assert "proxy-authorization" not in headers
        if self.fail:
            raise RequestException(PROXY)
        status = 200
        if url.endswith("/order"):
            data = {
                "inputMint": params["inputMint"],
                "outputMint": params["outputMint"],
                "swapMode": params["swapMode"],
                "inAmount": params["amount"],
                "outAmount": "1000000",
                "requestId": "fixture-request",
            }
        elif url.endswith("/challenge"):
            self.wallet = json["walletPubkey"]
            self.challenge = f"Jupiter authenticate {self.wallet}: fixture nonce"
            data = {"type": "message", "challenge": self.challenge}
        elif url.endswith(("/verify", "/refresh")):
            if url.endswith("/verify"):
                assert json["walletPubkey"] == self.wallet
                assert Signature.from_string(json["signature"]).verify(
                    Pubkey.from_string(self.wallet), self.challenge.encode("utf-8")
                )
            data = {
                "authMode": "access_refresh",
                "tokenType": "Bearer",
                "accessToken": "fixture-access",
                "refreshToken": "fixture-refresh",
                "expiresAt": (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
            }
        elif url.endswith("/vault"):
            assert headers["authorization"] == "Bearer fixture-access"
            status, data = 404, {"error": "Vault not found"}
        else:
            data = {"success": True}
        return SimpleNamespace(status_code=status, content=b"", json=lambda: data)

    async def close(self):
        self.closed = True


async def test_curl_single_session_proxy_auth_signature_and_no_proxy_bypass(monkeypatch, tmp_path):
    monkeypatch.setattr(curl, "AsyncSession", StubSession)
    monkeypatch.setenv("NO_PROXY", "*")
    client = JupiterReplay(
        proxy=PROXY,
        transport="curl_cffi",
        website_key="fixture-key",
        interval=0,
        audit=tmp_path / "audit.jsonl",
    )
    session = client.http.session
    try:
        await client.order(SwapInput(SOL, USDC, "0.01", 9))
        await client.holdings(str(Keypair().pubkey()))
        await client.login(Keypair())
        assert (await client.trigger_vault())["phase"] == "not_registered"
        await client.refresh()
        await client.logout()
        # Validated byte/signature gates are independently tested before this shared POST transport.
        await client.request("POST", client.base + "/execute", payload={"signedTransaction": "fixture"})
    finally:
        await client.close()
    assert session.closed and len(session.calls) == 8
    assert session.options["proxy"] == "http://proxy.example:8080"
    assert session.options["proxy_auth"] == ("fixture@user", "fixture:password")
    assert session.options["trust_env"] is False and session.options["verify"] is True
    assert session.options["allow_redirects"] is False and session.options["retry"] == 0
    assert session.options["debug"] is False and session.options["impersonate"] == "chrome"
    assert session.options["curl_options"][curl.CurlOpt.NOPROXY] == ""
    assert session.options["http_version"] == curl.CurlHttpVersion.V2TLS
    records = [json.loads(row) for row in (tmp_path / "audit.jsonl").read_text().splitlines()]
    assert len(records) == 8 and all(r["proxied"] and r["transport"] == "curl_cffi" for r in records)
    audit = (tmp_path / "audit.jsonl").read_text()
    assert not any(secret in audit for secret in ("fixture-access", "fixture-refresh", "fixture-key", PROXY))


async def test_curl_failure_is_not_retried_or_exposed(monkeypatch):
    monkeypatch.setattr(curl, "AsyncSession", StubSession)
    client = JupiterReplay(proxy=PROXY, transport="curl_cffi", interval=0)
    session = client.http.session
    session.fail = True
    try:
        with pytest.raises(ReplayError, match="no automatic retry") as raised:
            await client.order(SwapInput(SOL, USDC, "0.01", 9))
        assert raised.value.__suppress_context__ and raised.value.__cause__ is None
        assert "fixture" not in str(raised.value) and len(session.calls) == 1
    finally:
        await client.close()


@pytest.mark.parametrize("proxy", ["DIRECT", "", "socks5://proxy.example:1080"])
def test_curl_cannot_be_created_without_an_explicit_supported_proxy(proxy):
    with pytest.raises(ValueError):
        CurlClient(proxy)


def test_public_key_extraction_requires_exact_unambiguous_config():
    assert frontend_key_from_source(b'({apiKey: t.swapApiKey ?? "fixture"})') == "fixture"
    with pytest.raises(ReplayError):
        frontend_key_from_source(b'({apiKey: "unrelated"})')
    with pytest.raises(ReplayError):
        frontend_key_from_source(b'({apiKey: t.swapApiKey ?? "a"});({apiKey: t.swapApiKey ?? "b"})')


async def test_curl_execute_uses_wallet_signature_and_frontend_landing_timeout(monkeypatch):
    import base64

    monkeypatch.setattr(curl, "AsyncSession", StubSession)
    wallet = Keypair()
    message = MessageV0.try_compile(wallet.pubkey(), [], [], Hash.new_unique())
    tx = VersionedTransaction.populate(message, [Signature.default()])
    prepared = {
        "backend": "website",
        "receivedAt": time.time(),
        "quote": {
            "transaction": base64.b64encode(bytes(tx)).decode(),
            "taker": str(wallet.pubkey()),
            "requestId": "fixture",
            "router": "metis",
        },
    }
    signed = sign_swap_transaction(prepared, wallet)
    client = JupiterReplay(proxy=PROXY, transport="curl_cffi", interval=0)
    session = client.http.session
    try:
        await client.execute(prepared, signed, allow_execute=True)
        assert len(session.calls) == 1 and session.last_timeout == 92
        assert decode_swap_transaction(session.calls[0][3]["signedTransaction"]).verify_with_results() == [
            True
        ]
    finally:
        await client.close()
