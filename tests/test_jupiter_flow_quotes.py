import asyncio
import time
from unittest.mock import AsyncMock

import httpx
import pytest
from solders.pubkey import Pubkey

from backpack_stocks.jupiter_flow_quotes import JupiterFlowQuotes
from backpack_stocks.jupiter_replay import JupiterReplay, ReplayError
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings


@pytest.mark.parametrize("reverse", [False, True])
async def test_monitor_uses_same_manual_order_as_script_and_cannot_execute(reverse):
    settings = Settings()
    settings.wallet = str(Pubkey.new_unique())
    source, target = (USDC_MINT, STOCK_MINT) if reverse else (STOCK_MINT, USDC_MINT)
    amount = 10_000_000 if reverse else 111_940_000
    calls = []

    def response(request):
        calls.append(request)
        assert request.method == "GET" and request.url.path == "/swap/v2a/order"
        p = request.url.params
        assert p["amount"] == str(amount) and p["inputMint"] == source and p["outputMint"] == target
        assert p["clientPlatform"] == "jupiter.web.swap_page" and p["apiKey"] == "fixture-public-key"
        assert p["slippageBps"] == "50" and p["priorityFeeLamports"] == "1000000"
        assert p["swapMode"] == "ExactIn" and p["taker"] == settings.wallet
        return httpx.Response(
            200,
            json={
                "inputMint": source,
                "outputMint": target,
                "inAmount": str(amount),
                "outAmount": "1007522204",
                "otherAmountThreshold": "1002484593",
                "swapMode": "ExactIn",
                "slippageBps": 50,
                "priceImpact": "0.6628",
                "feeBps": 10,
                "router": "jupiter",
                "taker": settings.wallet,
                "requestId": "mock",
                "transaction": "unsigned-not-for-monitor",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        replay = JupiterReplay(client=client, website_key="fixture-public-key", interval=0)
        replay.execute = AsyncMock(side_effect=AssertionError("no execution"))
        reader = JupiterFlowQuotes(settings, replay=replay)
        q = await reader.quote_pair(source, target, amount, priority=10)
        assert q["outAmount"] == "1007522204" and q["feeBps"] == 10
        assert q["quote_source"] == "jupiter_flow" and q["quote_profile"] == "manual"
        assert "transaction" not in q and "requestId" not in q
        assert "execute" not in dir(reader)
        assert len(calls) == 1 and reader.status()["wallet_context"]
        replay.execute.assert_not_called()
        await reader.close()


async def test_preview_quote_omits_wallet_and_rejects_bad_minimum_or_impact():
    settings = Settings()

    def response(request):
        assert "taker" not in request.url.params
        return httpx.Response(
            200,
            json={
                "inputMint": STOCK_MINT,
                "outputMint": USDC_MINT,
                "inAmount": "1",
                "outAmount": "1",
                "otherAmountThreshold": "2",
                "swapMode": "ExactIn",
                "requestId": "mock",
                "priceImpact": "0.1",
                "slippageBps": 50,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        reader = JupiterFlowQuotes(settings, replay=JupiterReplay(client=client, interval=0))
        with pytest.raises(ValueError, match="minimum"):
            await reader.quote_pair(STOCK_MINT, USDC_MINT, 1)


async def test_frontend_429_defers_future_requests_without_retrying():
    calls = []

    def response(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "30"}, json={"error": "limited"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        replay = JupiterReplay(client=client, interval=0)
        with pytest.raises(ReplayError, match="HTTP 429"):
            await replay.request("GET", replay.base + "/order")
        assert replay._defer_until > time.monotonic() + 29
        assert replay.responses_429 == len(calls) == 1


def test_adapter_constructor_does_not_load_wallet_keys(monkeypatch):
    import os

    class PublicOnlyEnvironment(dict):
        def get(self, name, default=None):
            assert name not in {"SOLANA_WALLET_KEY", "SOLANA_KEYPAIR_BASE64", "JUPITER_REPLAY_KEYPAIR"}
            return super().get(name, default)

    monkeypatch.setattr(os, "environ", PublicOnlyEnvironment())
    replay = type("Replay", (), {"interval": 2.1})()
    reader = JupiterFlowQuotes(Settings(), replay=replay)
    assert reader.wallet is None


async def test_slow_responses_overlap_but_request_starts_remain_paced():
    entered, release = asyncio.Event(), asyncio.Event()
    starts = []

    async def response(request):
        starts.append(time.monotonic())
        if len(starts) == 1:
            entered.set()
            await release.wait()
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        replay = JupiterReplay(client=client, interval=0.03)
        slow = asyncio.create_task(replay.request("GET", replay.base + "/first"))
        try:
            await asyncio.wait_for(entered.wait(), 0.5)
            assert await asyncio.wait_for(replay.request("GET", replay.base + "/second"), 0.5) == {"ok": True}
            assert not slow.done() and starts[1] - starts[0] >= 0.025
            release.set()
            await slow
            assert replay.queued == 0
        finally:
            slow.cancel()
            await asyncio.gather(slow, return_exceptions=True)


async def test_a_429_received_during_pacing_delays_the_waiting_request():
    starts = []

    async def response(request):
        starts.append(time.monotonic())
        if len(starts) == 1:
            await asyncio.sleep(0.01)
            return httpx.Response(429, headers={"Retry-After": "0.08"}, json={"error": "limited"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        replay = JupiterReplay(client=client, interval=0.04)
        results = await asyncio.gather(
            *(replay.request("GET", replay.base + "/order") for _ in range(2)), return_exceptions=True
        )
        assert isinstance(results[0], ReplayError) and results[1] == {"ok": True}
        assert starts[1] - starts[0] >= 0.08
        assert replay.queued == 0 and replay.responses_429 == 1


async def test_cancelled_admission_does_not_leak_capacity_or_queue_count():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"ok": True}))
    ) as client:
        replay = JupiterReplay(client=client, interval=0)
        replay._defer_until = time.monotonic() + 30
        tasks = [asyncio.create_task(replay.request("GET", replay.base + "/order")) for _ in range(8)]
        await asyncio.sleep(0)
        assert replay.queued == 8
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert replay.queued == 0
        replay._defer_until = 0
        assert await asyncio.wait_for(replay.request("GET", replay.base + "/order"), 0.5) == {"ok": True}


async def test_quote_age_includes_http_latency_and_limits_status_is_complete(monkeypatch):
    now = [10000.0]
    monkeypatch.setattr("backpack_stocks.jupiter_replay.time.time", lambda: now[0])

    async def response(request):
        now[0] += 4
        return httpx.Response(
            200,
            json={
                "inputMint": STOCK_MINT,
                "outputMint": USDC_MINT,
                "inAmount": "1",
                "outAmount": "100",
                "otherAmountThreshold": "99",
                "swapMode": "ExactIn",
                "slippageBps": 50,
                "priceImpact": "0.1",
                "requestId": "fixture",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        reader = JupiterFlowQuotes(Settings(), replay=JupiterReplay(client=client, interval=0))
        quote = await reader.quote_pair(STOCK_MINT, USDC_MINT, 1)
        assert now[0] - quote["_observed_at"] == 4
        assert reader.status()["queued"] == reader.status()["cooldown_s"] == 0
