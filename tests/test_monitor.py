import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from backpack_stocks.feed import StockFeed
from backpack_stocks.jupiter import Jupiter
from backpack_stocks.limiter import PriorityLimiter
from backpack_stocks.models import (
    STOCK_MINT,
    TOKEN_2022,
    USDC_MINT,
    MonitorPolicy,
    Observation,
    Policy,
    Settings,
)
from backpack_stocks.monitor import FORWARD, REVERSE, Asset, SpreadMonitor, discover
from backpack_stocks.transport import Transport


class Clock:
    def __init__(self):
        self.now = 10000.0

    def __call__(self):
        return self.now


def asset(symbol="WEN.US", mint=STOCK_MINT, *, deposit=True):
    return Asset(
        symbol,
        mint,
        6,
        Decimal("0.08"),
        Decimal("0.16"),
        Decimal("0.08"),
        deposit,
        (
            {
                "name": "US_EQUITIES_REGULAR",
                "stepSize": "0.00001",
                "minQuantity": "0.01",
                "maxQuantity": "50000",
            },
        ),
    )


def observed(clock, ask="6", bid="5.9"):
    return Observation(
        ask=ask, bid=bid, timestamp=clock(), session="US_EQUITIES_REGULAR", source="fixture WS"
    )


@pytest.fixture
def monitor():
    clock = Clock()
    settings = Settings(
        source="public",
        policy=Policy(withdrawal_semantics="deduct"),
        monitor=MonitorPolicy(rfq_validation=False, jupiter_flow=False),
    )
    feed = StockFeed()
    feed.tickers.add("WEN")
    feed.latest["WEN"] = observed(clock)
    feed.subscribe = AsyncMock()
    bp = SimpleNamespace(
        stock_feed=feed,
        assets=AsyncMock(),
        securities=AsyncMock(),
        sessions=AsyncMock(return_value=[]),
        holidays=AsyncMock(return_value=[]),
        tickers=AsyncMock(),
        http=SimpleNamespace(status=lambda: {}),
    )
    jup = SimpleNamespace(
        interval=1.05,
        quote_pair=AsyncMock(),
        prices=AsyncMock(),
        http=SimpleNamespace(status=lambda: {}),
        status=lambda: {},
    )
    rpc = SimpleNamespace(accounts=AsyncMock(), http=SimpleNamespace(status=lambda: {}))
    mon = SpreadMonitor(settings, bp, jup, rpc, clock=clock)
    mon.assets = {"WEN.US": asset()}
    mon.chain = {"WEN.US": "verified_unit_mapping"}
    mon.metadata_at = clock()
    return mon, clock


async def test_forward_sizing_uses_fee_adjusted_token_amount_and_net_profit(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "priceImpact": "0.1",
        "router": "fixture",
    }
    await mon.quote(("WEN.US", FORWARD))
    mon.jupiter.quote_pair.assert_awaited_once_with(STOCK_MINT, USDC_MINT, 16586660, priority=10)
    row = mon.snapshot()["rows"][0]
    assert row["acquired_raw"] == 16666660
    assert row["cost_raw"] == 99999960
    assert row["minimum_output_raw"] == 103973478
    assert row["profit_raw"] == 3943518
    assert row["candidate"] and row["indicative"] and row["trading_supported"]


async def test_reverse_rfq_session_rejection_is_visible_and_not_a_runtime_error(monitor):
    mon, _ = monitor
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], sessions=())
    mon.jupiter.quote_pair.return_value = {"outAmount": "1000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    assert mon.blocked["rfq_session_unavailable"] == 1
    assert mon.errors["quote"] == 0
    assert ("WEN.US", REVERSE) not in mon.results


async def test_jupiter_cooldown_does_not_delay_backpack_external_price_updates(monitor):
    mon, clock = monitor
    entered = asyncio.Event()

    async def held_price(mints):
        entered.set()
        await asyncio.Event().wait()

    mon.jupiter.prices.side_effect = held_price
    mon.backpack.tickers.return_value = [{"symbol": "WEN.US_USDC", "lastPrice": "6"}]
    task = asyncio.create_task(mon.refresh_prices())
    try:
        await asyncio.wait_for(entered.wait(), 0.2)
        assert mon.external["WEN.US"] == "6"
        clock.now += 10
        mon.backpack.stock_feed.latest.clear()
        mon.backpack.tickers.return_value = [{"symbol": "WEN.US_USDC", "lastPrice": "5"}]
        await mon.refresh_external_prices()
        assert mon.observation(mon.assets["WEN.US"]).ask == "5"
        assert mon.external_at == clock() and not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_batch_screening_reserves_quote_capacity_under_keyless_budget(monitor):
    mon, _ = monitor
    mon.assets = {f"S{i}.US": asset(f"S{i}.US") for i in range(68)}
    mon.jupiter.interval = 2.1
    assert mon.price_batch_period() == pytest.approx(16.8)
    mon.jupiter.interval = 0.105
    assert mon.price_batch_period() == 10


async def test_cached_forward_quote_is_rejected_after_rfqs_session_step_changes(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "priceImpact": "0.1",
        "router": "fixture",
    }
    await mon.quote(("WEN.US", FORWARD))
    assert mon.snapshot()["rows"][0]["fresh"]
    mon.assets["WEN.US"] = replace(
        mon.assets["WEN.US"],
        sessions=(
            {"name": "US_EQUITIES_REGULAR", "stepSize": "1", "minQuantity": "1", "maxQuantity": "50000"},
        ),
    )
    assert not mon.snapshot()["rows"][0]["fresh"]


async def test_reverse_uses_exact_usdc_notional_and_conservative_stock_sale(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "16300000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    mon.jupiter.quote_pair.assert_awaited_once_with(USDC_MINT, STOCK_MINT, 100000000, priority=10)
    row = mon.snapshot(include_hidden=True)["rows"][0]
    assert row["profit_raw"] == -4340850
    assert not any("profit_raw" in r for r in mon.snapshot()["rows"])
    assert not row["candidate"] and not row["trading_supported"]


def test_reverse_sale_rounds_to_session_step_and_does_not_credit_unsellable_dust(monitor):
    mon, clock = monitor
    overnight = replace(
        mon.assets["WEN.US"],
        sessions=({"name": "OVERNIGHT", "stepSize": "1", "minQuantity": "1", "maxQuantity": "1000"},),
    )
    quote = observed(clock).model_copy(update={"session": "OVERNIGHT"})
    assert mon.sale_quantity(overnight, quote, 16218500) == 16000000
    with pytest.raises(ValueError, match="session minimum"):
        mon.sale_quantity(overnight, quote, 500000)


async def test_cached_route_reprices_immediately_on_provider_tick_without_new_jupiter_request(monitor):
    mon, clock = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    assert mon.snapshot()["rows"][0]["candidate"]
    mon.backpack.stock_feed.latest["WEN"] = observed(clock, ask="7")
    row = mon.snapshot(include_hidden=True)["rows"][0]
    assert row["cost_raw"] == 116666620
    assert not row["candidate"]
    assert mon.jupiter.quote_pair.await_count == 1


@pytest.mark.parametrize("boundary", ["route", "provider", "metadata", "mint"])
async def test_stale_or_changed_inputs_never_remain_candidates(monitor, boundary):
    mon, clock = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    if boundary == "route":
        clock.now += 9
    elif boundary == "provider":
        mon.backpack.stock_feed.latest["WEN"].timestamp -= 100
    elif boundary == "metadata":
        mon.metadata_at -= 601
    else:
        mon.chain["WEN.US"] = "unsupported_mint_or_unit_mapping"
    assert not any(r["candidate"] or r["fresh"] for r in mon.snapshot()["rows"])


async def test_failed_refresh_drops_old_opportunity_and_hides_remote_payload(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    mon.jupiter.quote_pair.side_effect = ValueError("private-external-payload")
    await mon.quote(("WEN.US", FORWARD))
    document = mon.snapshot()
    assert not any(r["candidate"] for r in document["rows"])
    assert "private-external-payload" not in json.dumps(document)


async def test_high_impact_is_reported_but_cannot_be_candidate(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "5"}
    await mon.quote(("WEN.US", FORWARD))
    assert not mon.snapshot()["rows"][0]["candidate"]


async def test_mint_changed_during_quote_discards_result(monitor):
    mon, _ = monitor

    async def change_mint(*args, **kwargs):
        mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], mint=USDC_MINT)
        return {"outAmount": "104495958", "priceImpact": "0.1"}

    mon.jupiter.quote_pair.side_effect = change_mint
    await mon.quote(("WEN.US", FORWARD))
    assert mon.results == {}


def test_scheduler_reserves_every_fourth_slot_for_oldest_asset(monitor):
    mon, clock = monitor
    mon.assets["COLD.US"] = asset("COLD.US", USDC_MINT, deposit=False)
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], deposit_enabled=False)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["COLD"] = observed(clock)
    mon.attempted[("WEN.US", FORWARD)] = clock() - 10
    mon.attempted[("COLD.US", FORWARD)] = clock() - 200
    mon.estimate = lambda a, direction=None: Decimal("1000") if a.symbol == "WEN.US" else Decimal("-1000")
    assert [mon.choose()[0] for _ in range(4)] == ["WEN.US", "WEN.US", "WEN.US", "COLD.US"]


def test_missing_batch_price_still_gets_exact_quote_exploration(monitor):
    mon, _ = monitor
    assert mon.estimate(mon.assets["WEN.US"]) is None
    assert mon.choose() is not None


def test_provider_mid_and_rest_model_are_not_claimed_as_firm_rfq(monitor):
    mon, clock = monitor
    mon.backpack.stock_feed.latest.clear()
    mon.external = {"WEN.US": "6"}
    mon.external_at = clock()
    quote = mon.observation(mon.assets["WEN.US"])
    assert quote.session == "MODEL" and "no RFQ" in quote.source and "age unknown" in quote.source
    assert quote.ask == quote.bid == "6"


def test_discovery_filters_disabled_withdrawals_wrong_chains_and_ambiguous_mints():
    token = {
        "blockchain": "Solana",
        "withdrawEnabled": True,
        "depositEnabled": True,
        "contractAddress": STOCK_MINT,
        "nativeDecimals": 6,
        "withdrawalFee": "0.08",
        "minimumWithdrawal": "0.16",
        "minimumDeposit": "0.08",
    }
    assets = [
        {"symbol": "WEN.US", "tokens": [token]},
        {"symbol": "OFF.US", "tokens": [{**token, "withdrawEnabled": False}]},
        {"symbol": "ETH.US", "tokens": [{**token, "blockchain": "Ethereum"}]},
        {"symbol": "DUP.US", "tokens": [token, {**token, "contractAddress": USDC_MINT}]},
    ]
    securities = [{"asset": a["symbol"], "sessions": []} for a in assets]
    assert list(discover(assets, securities)) == ["WEN.US"]


async def test_metadata_batch_rejects_changed_scaled_unit_mapping(monitor):
    mon, _ = monitor
    mon.backpack.assets.return_value = [
        {
            "symbol": "WEN.US",
            "tokens": [
                {
                    "blockchain": "Solana",
                    "withdrawEnabled": True,
                    "depositEnabled": True,
                    "contractAddress": STOCK_MINT,
                    "nativeDecimals": 6,
                    "withdrawalFee": "0.08",
                    "minimumWithdrawal": "0.16",
                    "minimumDeposit": "0.08",
                }
            ],
        }
    ]
    mon.backpack.securities.return_value = [{"asset": "WEN.US", "sessions": []}]
    account = {
        "owner": TOKEN_2022,
        "data": {
            "parsed": {
                "info": {
                    "decimals": 6,
                    "isInitialized": True,
                    "extensions": [
                        {"extension": "scaledUiAmountConfig", "state": {"multiplier": 2, "newMultiplier": 2}},
                        {"extension": "defaultAccountState", "state": {"accountState": "initialized"}},
                    ],
                }
            }
        },
    }
    mon.rpc.accounts.return_value = {"value": [account]}
    await mon.refresh_metadata()
    mon.rpc.accounts.assert_awaited_once_with([STOCK_MINT], parsed=True, priority=10)
    assert mon.chain["WEN.US"] == "unsupported_mint_or_unit_mapping"
    assert mon.choose() is None


async def test_feed_is_latest_only_rejects_bad_ticks_and_preserves_class_share_symbol():
    feed = StockFeed()
    feed.tickers.update(["WEN", "BRK.B"])
    payload = {
        "e": "stockPrice",
        "symbol": "BRK.B",
        "ask": "10",
        "bid": "9.9",
        "timestamp": 10000,
        "session": "REGULAR",
    }
    await feed.ingest(payload)
    await feed.ingest({**payload, "timestamp": 9000})
    await feed.ingest({**payload, "ask": None})
    await feed.ingest({**payload, "symbol": ["BRK.B"]})
    await feed.ingest({**payload, "ask": "NaN"})
    assert len(feed.latest) == 1 and feed.latest["BRK.B"].timestamp == 10
    assert feed.invalid_messages == 1
    assert feed.version == 1


async def test_one_ws_task_is_shared_by_single_and_multi_asset_subscriptions():
    feed = StockFeed()

    async def blocked():
        await asyncio.Event().wait()

    feed.receive = AsyncMock(side_effect=blocked)
    try:
        await feed.subscribe(["WEN"])
        original = feed.task
        await feed.subscribe(["WEN", "AMD", "BRK.B"])
        assert feed.task is original
        assert feed.tickers == {"WEN", "AMD", "BRK.B"}
    finally:
        await feed.close()


async def test_execution_priority_and_request_pacing():
    limiter = PriorityLimiter(0.025)
    order = []
    times = []
    await limiter.acquire()

    async def request(name, priority):
        await limiter.acquire(priority)
        order.append(name)
        times.append(asyncio.get_running_loop().time())

    try:
        await asyncio.gather(request("monitor-1", 10), request("monitor-2", 10), request("execution", 0))
        assert order == ["execution", "monitor-1", "monitor-2"]
        assert all(b - a >= 0.020 for a, b in zip(times, times[1:], strict=False))
    finally:
        await limiter.close()


async def test_slow_monitor_response_does_not_hold_execution_request_lock():
    gate, started = asyncio.Event(), asyncio.Event()

    async def handler(request):
        amount = request.url.params["amount"]
        if amount == "1":
            started.set()
            await gate.wait()
        return httpx.Response(
            200,
            json={
                "inputMint": STOCK_MINT,
                "outputMint": USDC_MINT,
                "inAmount": amount,
                "outAmount": "100",
                "priceImpact": "0",
            },
        )

    jup = Jupiter(
        api_key="fixture-test-key",
        interval=0.02,
        transport=Transport(
            "https://example.com",
            client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler)),
        ),
    )
    background = asyncio.create_task(jup.quote_pair(STOCK_MINT, USDC_MINT, 1, priority=10))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        result = await asyncio.wait_for(jup.quote_pair(STOCK_MINT, USDC_MINT, 2), timeout=1)
        assert result["inAmount"] == "2" and not background.done()
    finally:
        gate.set()
        await background
        await jup.close()


async def test_every_jupiter_retry_is_rate_limited():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        return httpx.Response(429 if attempts == 1 else 200, json={})

    jup = Jupiter(
        interval=0.001,
        transport=Transport(
            "https://example.com",
            client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler)),
        ),
    )
    jup.limiter.acquire = AsyncMock()
    try:
        await jup.prices([STOCK_MINT])
        assert jup.limiter.acquire.await_count == 2
    finally:
        await jup.close()
