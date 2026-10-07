import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_indicative_coverage import published
from test_monitor import asset

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, raw
from backpack_stocks.monitor import FORWARD
from backpack_stocks.observability import QuoteUnavailable
from backpack_stocks.raydium import CLMM

indicative_monitor = indicative_monitor_fixture


async def unpriced(mon, clock, feed):
    await feed.ingest(published(clock, reason="stalePrice", ask=None))
    router = object.__new__(DirectRaydium)
    router.settings = mon.settings
    banks = {
        ("pool", STOCK_MINT, USDC_MINT, 10): {
            "observed": clock(),
            "response": {"context": {"slot": 1}},
        }
    }
    router.bank_feed = SimpleNamespace(
        peek=lambda key: banks.get(key),
        changed=asyncio.Event(),
        set_primary=Mock(),
        set_minimum_arrays=Mock(),
        promote=Mock(),
    )
    router.status = lambda: {}
    mon._quote_provider = router
    state = mon.coverage_state(mon.assets["WEN.US"])
    mon.sync_indicative_coverage(state)
    route = {
        "pool": "pool",
        "inputMint": STOCK_MINT,
        "outputMint": USDC_MINT,
        "inAmount": str(state.quantity_raw - raw(state.asset.fee, state.asset.decimals)),
        "outAmount": "480000",
        "otherAmountThreshold": "477600",
        "priceImpact": "0.1",
        "contextSlot": 1,
        "_observed_at": clock(),
        "_direct": {"program": CLMM, "arrays": ["used-array"], "accounts": {"ignored": "body"}},
    }
    router.quote_pair = AsyncMock(return_value=route)
    router.refreshed_plan_quote = AsyncMock(return_value=route)
    return router, banks, state, route


async def test_noquote_prewarm_saves_only_metadata_bound_topology_without_price_or_rfq(indicative_monitor):
    mon, clock, feed = indicative_monitor
    router, _, state, route = await unpriced(mon, clock, feed)
    check = state.check
    await mon.discover_coverage_route(state)
    assert state.plan is state.result is None and state.check is check
    assert state.prefetched_route == {
        "pool": "pool",
        "inputMint": STOCK_MINT,
        "outputMint": USDC_MINT,
        "inAmount": route["inAmount"],
        "contextSlot": 1,
        "_observed_at": clock(),
        "_direct": {"program": CLMM, "arrays": ["used-array"]},
    }
    router.quote_pair.assert_awaited_once_with(
        STOCK_MINT, USDC_MINT, raw("0.08"), priority=10, _rpc=router.bank_feed, minimum=True
    )
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with({})
    router.bank_feed.promote.assert_not_called()
    assert mon.prefetched_coverage_quote(state) is None
    assert mon.coverage_snapshot()["pricedfresh"] == 0
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


async def test_real_buy_activates_old_hint_dependencies_then_refreshes_and_recovers_actual_minimum(
    indicative_monitor,
):
    mon, clock, feed = indicative_monitor
    router, banks, state, route = await unpriced(mon, clock, feed)
    await mon.discover_coverage_route(state)
    original_stamp = state.prefetched_route["_observed_at"]
    clock.now += 40
    await feed.ingest(published(clock, ask="7", bid="6.9"))
    mon.sync_primary_banks()
    bank_key = "pool", STOCK_MINT, USDC_MINT, 10
    router.bank_feed.set_primary.assert_called_with({bank_key: 1})
    router.bank_feed.set_minimum_arrays.assert_called_with({bank_key: 2}, resolved=set())
    router.bank_feed.promote.assert_called_once()
    assert state.plan is state.result is None
    assert state.prefetched_route["_observed_at"] == original_stamp
    banks[bank_key] = {"observed": clock(), "response": {"context": {"slot": 2}}}
    router.refreshed_plan_quote.return_value = {**route, "_observed_at": clock(), "contextSlot": 2}
    callback = mon.prefetched_coverage_quote(state)
    assert callback is not None
    row = await mon.quote((state.asset.symbol, FORWARD), coverage=True, quote_pair=callback)
    assert row and row["backpack_price"] == "7" and row["backpack_source"] == "indicative"
    assert row["dex_observed_at"] == clock() and row["acquired_raw"] == raw("0.16")
    assert mon.coverage_snapshot()["pricedfresh"] == 1 and not row["backpack_quote_confirmed"]
    router.quote_pair.assert_awaited_once()  # Only original prewarm; no alternate-route lookup.
    assert router.refreshed_plan_quote.await_args.kwargs["plan"] is state.prefetched_route
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


@pytest.mark.parametrize("boundary", ["metadata", "mint", "mapping", "session", "fee", "semantics"])
async def test_prewarm_rechecks_identity_after_await_without_publishing_old_state(
    indicative_monitor, boundary
):
    mon, clock, feed = indicative_monitor
    router, _, state, route = await unpriced(mon, clock, feed)
    started, release = asyncio.Event(), asyncio.Event()

    async def wait(*args, **kwargs):
        started.set()
        await release.wait()
        return route

    router.quote_pair.side_effect = wait
    task = asyncio.create_task(mon.discover_coverage_route(state))
    await asyncio.wait_for(started.wait(), 1)
    if boundary == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif boundary == "mint":
        mon.assets[state.asset.symbol] = replace(state.asset, mint=USDC_MINT)
    elif boundary == "mapping":
        mon.chain[state.asset.symbol] = "unsupported_mint_or_unit_mapping"
    elif boundary == "session":
        mon.sessions[0]["name"] = "CLOSED"
    elif boundary == "fee":
        mon.assets[state.asset.symbol] = replace(state.asset, fee=state.asset.fee + 1)
    else:
        mon.settings.policy.withdrawal_semantics = "additional"
    release.set()
    await task
    assert state.prefetched_route is state.plan is state.result is None
    assert mon.prefetched_coverage_quote(state) is None
    mon.rfq.check.assert_not_awaited()


async def test_prewarm_uses_one_existing_background_job_and_does_not_repeat_good_unpriced_hint(
    indicative_monitor,
):
    mon, clock, feed = indicative_monitor
    router, _, _, route = await unpriced(mon, clock, feed)
    other = asset("OTHER.US", deposit=False)
    mon.assets[other.symbol] = other
    mon.chain[other.symbol] = "verified_unit_mapping"
    feed.tickers.add(other.ticker)
    await feed.ingest(published(clock, other.ticker, reason="stalePrice", ask=None))
    started, release = asyncio.Event(), asyncio.Event()
    active = peak = 0

    async def wait(*args, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        started.set()
        try:
            await release.wait()
            return route
        finally:
            active -= 1

    router.quote_pair.side_effect = wait
    mon.discovery_ready.set()
    loop = asyncio.create_task(mon.coverage_routes_loop())
    try:
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.sleep(0.03)
        assert len(mon.coverage_route_pending) == peak == router.quote_pair.await_count == 1
        release.set()
        async with asyncio.timeout(1):
            while not all(state.prefetched_route for state in mon.forward_coverage.values()):
                await asyncio.sleep(0)
        assert router.quote_pair.await_count == 2 and peak == 1
        clock.now += mon.settings.monitor.exploration_s + 1
        for ticker in ("WEN", "OTHER"):
            await feed.ingest(published(clock, ticker, reason="stalePrice", ask=None))
        await asyncio.sleep(0.3)
        assert router.quote_pair.await_count == 2
        assert all(state.plan is state.result is None for state in mon.forward_coverage.values())
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        release.set()
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)


async def test_expected_unusable_hint_falls_back_only_to_bounded_minimum_routing(indicative_monitor):
    mon, clock, feed = indicative_monitor
    router, _, state, route = await unpriced(mon, clock, feed)
    await mon.discover_coverage_route(state)
    await feed.ingest(published(clock))
    router.refreshed_plan_quote.side_effect = QuoteUnavailable(["insufficient_liquidity"])
    fallback = AsyncMock(return_value=route)
    callback = mon.prefetched_coverage_quote(state, fallback=fallback)
    assert callback is not None
    await callback(STOCK_MINT, USDC_MINT, raw("0.08"))
    fallback.assert_awaited_once_with(STOCK_MINT, USDC_MINT, raw("0.08"), priority=10, minimum=True)
    router.refreshed_plan_quote.side_effect = TimeoutError()
    fallback.reset_mock()
    with pytest.raises(TimeoutError):
        await callback(STOCK_MINT, USDC_MINT, raw("0.08"))
    fallback.assert_not_awaited()


async def test_route_adoption_waits_for_same_asset_minimum_collision_task(indicative_monitor):
    mon, clock, feed = indicative_monitor
    _, _, state, route = await unpriced(mon, clock, feed)
    state.plan = {"input_raw": raw("0.08"), "dex_plan": {"old": True}}
    state.route_candidate = route
    key = state.asset.symbol, FORWARD
    mon.minimum_pending[key] = asyncio.current_task()
    mon.adopt_coverage_routes()
    assert state.route_candidate is route and state.plan["dex_plan"] == {"old": True}
    mon.minimum_pending.pop(key)
    mon.adopt_coverage_routes()
    assert state.route_candidate is None and state.plan["dex_plan"] is route


async def test_returning_buy_reactivates_known_minimum_after_hot_retention_expired(indicative_monitor):
    mon, clock, feed = indicative_monitor
    router, banks, state, route = await unpriced(mon, clock, feed)
    await feed.ingest(published(clock))
    await mon.quote((state.asset.symbol, FORWARD), coverage=True, quote_pair=AsyncMock(return_value=route))
    router.bank_feed.hot = {}
    mon.sync_primary_banks()
    router.bank_feed.promote.assert_called_once()
    router.bank_feed.promote.reset_mock()
    router.bank_feed.hot = {("pool", STOCK_MINT, USDC_MINT, 10): clock() + 300}
    mon.sync_primary_banks()
    router.bank_feed.promote.assert_not_called()
    clock.now += 301
    await feed.ingest(published(clock, reason="stalePrice", ask=None))
    router.bank_feed.hot.clear()
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with({})
    router.bank_feed.promote.assert_not_called()
    old_stamp = banks[("pool", STOCK_MINT, USDC_MINT, 10)]["observed"]
    await feed.ingest(published(clock))
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with({("pool", STOCK_MINT, USDC_MINT, 10): 1})
    router.bank_feed.promote.assert_called_once()
    assert banks[("pool", STOCK_MINT, USDC_MINT, 10)]["observed"] == old_stamp
    assert not mon.coverage_result_fresh(state)
