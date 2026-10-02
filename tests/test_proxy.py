import base64
import logging
from unittest.mock import AsyncMock

import httpx
import pytest

from backpack_stocks.backpack import Backpack
from backpack_stocks.models import Settings
from backpack_stocks.proxy import PROXY_ENV_NAMES, ProxyLogRedactor, checked_proxy, resolve_proxy
from backpack_stocks.runtime import public_clients
from backpack_stocks.transport import RemoteError, Transport

PROXY = "http://fixture-user:fixture-secret@proxy.example:8080"


@pytest.fixture
def clean_routes(monkeypatch):
    for name in PROXY_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_provider_override_empty_inherits_and_direct_is_explicit(clean_routes):
    clean_routes.setenv("OUTBOUND_PROXY_URL", PROXY)
    clean_routes.setenv("JUPITER_PROXY_URL", "")
    assert resolve_proxy("JUPITER") == PROXY
    clean_routes.setenv("JUPITER_PROXY_URL", "DIRECT")
    assert resolve_proxy("JUPITER") is None
    assert resolve_proxy("SOLANA") == PROXY
    clean_routes.setenv("BACKPACK_PROXY_URL", "http://bp.example:8080")
    assert resolve_proxy("BACKPACK_WS") == "http://bp.example:8080"
    clean_routes.setenv("BACKPACK_WS_PROXY_URL", "DIRECT")
    assert resolve_proxy("BACKPACK_WS") is None


@pytest.mark.parametrize(
    "value",
    [
        "socks5://proxy.example:1080",
        "http://",
        "http://u:secret@host:99999",
        "http://u:secret@host/x",
        "http://host?secret=1",
        "http://host/#secret",
        "http://u:secret@host\n",
    ],
)
def test_invalid_proxy_errors_never_echo_credentials(value):
    with pytest.raises(ValueError) as raised:
        checked_proxy(value)
    assert "secret" not in str(raised.value) and value not in str(raised.value)


def test_proxy_credentials_are_redacted_from_http_and_websocket_debug_logs(caplog):
    guard = ProxyLogRedactor(PROXY)
    encoded = base64.b64encode(b"fixture-user:fixture-secret").decode()
    try:
        with caplog.at_level(logging.DEBUG):
            logging.getLogger("httpcore.proxy").debug("CONNECT %s", PROXY)
            logging.getLogger("websockets.client").debug("Proxy-Authorization: Basic %s", encoded)
            logging.getLogger("httpx").debug("password=%s", "fixture-secret")
        assert "fixture-secret" not in caplog.text and encoded not in caplog.text
        assert "<redacted>" in caplog.text
    finally:
        guard.close()


async def test_proxy_failure_is_not_retried_directly(monkeypatch):
    real_client = httpx.AsyncClient
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ProxyError("fixture proxy unavailable")

    options = []

    def client_factory(**kwargs):
        options.append(kwargs)
        return real_client(
            base_url=kwargs["base_url"], transport=httpx.MockTransport(handler), trust_env=False
        )

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    t = Transport("https://api.example", proxy=PROXY)
    try:
        with pytest.raises(RemoteError, match="Read request failed"):
            await t.request("GET", "/price")
        assert len(calls) == 3 and len(options) == 1
        assert options[0]["proxy"] == PROXY and options[0]["trust_env"] is False
        assert t.status()["proxied"] is True
        assert PROXY not in str(t.status()) and "fixture-secret" not in str(t.status())
    finally:
        await t.close()


async def test_runtime_wires_http_ws_and_rpc_without_loading_signing_keys(clean_routes, monkeypatch):
    clean_routes.setenv("OUTBOUND_PROXY_URL", PROXY)
    clean_routes.setenv("BACKPACK_WS_PROXY_URL", "DIRECT")
    clean_routes.setenv("SOLANA_PROXY_URL", "DIRECT")
    monkeypatch.setattr(
        "backpack_stocks.runtime.required_env", AsyncMock(side_effect=AssertionError("no keys"))
    )
    bp, jup, rpc = public_clients(Settings(source="public"))
    try:
        assert bp.http.status()["proxied"] and jup.status()["proxied"]
        assert bp.stock_feed.proxy is None and not rpc.http.status()["proxied"]
    finally:
        for client in (bp, jup, rpc):
            await client.close()


async def test_backpack_proxy_covers_websocket_unless_explicitly_overridden():
    bp = Backpack(proxy=PROXY)
    direct_ws = Backpack(proxy=PROXY, ws_proxy="DIRECT")
    try:
        assert bp.stock_feed.proxy == PROXY and direct_ws.stock_feed.proxy is None
    finally:
        await bp.close()
        await direct_ws.close()
