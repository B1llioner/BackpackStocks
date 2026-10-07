import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from backpack_stocks.models import Settings
from backpack_stocks.proxy import PROXY_ENV_NAMES
from backpack_stocks.runtime import public_clients
from backpack_stocks.wallet_watch import WalletJournal, WalletWatcher

SHARED_PROXY = "http://fixture-user:fixture-password@shared.example:8080"
RPC_PROXY = "http://rpc-fixture:rpc-password@rpc-proxy.example:8080"


@pytest.mark.parametrize("override", [None, RPC_PROXY])
async def test_resolved_solana_proxy_reaches_http_and_wallet_websocket(monkeypatch, tmp_path, override):
    for name in PROXY_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OUTBOUND_PROXY_URL", SHARED_PROXY)
    if override is not None:
        monkeypatch.setenv("SOLANA_PROXY_URL", override)
    expected = override or SHARED_PROXY
    factories, packets, sockets = [], [], []
    real_client = httpx.AsyncClient

    def respond(request):
        body = json.loads(request.content)
        packets.append(body)
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": body["id"], "result": {"context": {"slot": 2}, "value": [None]}},
        )

    def client_factory(**options):
        factories.append(options)
        return real_client(
            base_url=options["base_url"], transport=httpx.MockTransport(respond), trust_env=False
        )

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    subscribed = asyncio.Event()
    sent = []

    class Socket:
        async def send(self, payload):
            sent.append(json.loads(payload))
            subscribed.set()

        async def recv(self):
            await asyncio.Future()

    @asynccontextmanager
    async def connect(url, **options):
        sockets.append((url, options))
        yield Socket()

    monkeypatch.setattr("backpack_stocks.wallet_stream.websockets.connect", connect)
    settings = Settings(source="public", rpc_url="https://rpc.example.test/rpc")
    backpack, dex, rpc = public_clients(settings)
    watcher = WalletWatcher(WalletJournal(tmp_path / "wallet.sqlite3"), rpc, None)
    try:
        assert watcher.stream is not None
        watcher.stream.update({"public-account": "fixture-wallet"})
        watcher.stream_task = asyncio.create_task(watcher.stream.run())
        await asyncio.wait_for(subscribed.wait(), 1)
        response = await rpc.accounts(["public-account"], commitment="confirmed")

        rpc_options = next(option for option in factories if option["base_url"] == settings.rpc_url)
        assert rpc_options["proxy"] == expected and rpc_options["trust_env"] is False
        assert rpc.http.status()["proxied"] is True
        assert rpc.proxy == watcher.stream.proxy == expected
        assert sockets[0][0] == "wss://rpc.example.test/rpc"
        assert sockets[0][1]["proxy"] == expected
        assert len(sockets) == 1 and len(packets) == 1
        assert sent[0]["method"] == "logsSubscribe"
        assert packets[0]["method"] == "getMultipleAccounts"
        assert response == {"context": {"slot": 2}, "value": [None]}
        assert expected not in str(rpc.http.status())
    finally:
        await watcher.close()
        for client in (backpack, dex, rpc):
            await client.close()
