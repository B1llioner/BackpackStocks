import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_indicative_coverage import published
from test_monitor import asset

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.monitor import FORWARD

indicative_monitor = indicative_monitor_fixture


async def prepared_minimums(mon, clock, feed, count):
    router = object.__new__(DirectRaydium)
    banks = {}
    router.bank_feed = SimpleNamespace(peek=lambda key: banks.get(key), changed=asyncio.Event())
    router.status = lambda: {}
    router.quote_pair = AsyncMock(side_effect=AssertionError("extra minimum cannot use route discovery"))
    mon._quote_provider = router
    symbols = ["WEN.US"]
    for index in range(count - 1):
        item = asset(f"H{index:02d}.US", deposit=False)
        mon.assets[item.symbol] = item
        mon.chain[item.symbol] = "verified_unit_mapping"
        symbols.append(item.symbol)
    for symbol in symbols:
        item = mon.assets[symbol]
        feed.tickers.add(item.ticker)
        await feed.ingest(published(clock, item.ticker))
        pool = "pool-" + symbol
        banks[(pool, STOCK_MINT, USDC_MINT, 10)] = {
            "observed": clock(),
            "response": {"context": {"slot": 1}},
        }
        route = {
            "outAmount": "480000",
            "priceImpact": "0.1",
            "pool": pool,
            "inputMint": STOCK_MINT,
            "outputMint": USDC_MINT,
            "contextSlot": 1,
            "_observed_at": clock(),
            "_direct": {},
        }
        assert await mon.quote((symbol, FORWARD), coverage=True, quote_pair=AsyncMock(return_value=route))
    clock.now += 0.1
    for bank in banks.values():
        bank["observed"] = clock()
        bank["response"]["context"]["slot"] = 2
    return router, banks, symbols


def blocked_network_jobs(mon, count):
    gate = asyncio.Event()
    for index in range(count):
        symbol = f"C{index:02d}.US"
        mon.assets[symbol] = asset(symbol, deposit=False)
        mon.chain[symbol] = "verified_unit_mapping"
        mon.pending[(symbol, FORWARD)] = asyncio.create_task(gate.wait())
    return gate


async def stop_scheduler(mon, scheduler):
    scheduler.cancel()
    await asyncio.gather(scheduler, return_exceptions=True)
    for task in mon.pending.values():
        task.cancel()
    await asyncio.gather(*mon.pending.values(), return_exceptions=True)


@pytest.mark.parametrize("concurrency", [1, 16])
async def test_all_network_slots_blocked_cannot_starve_ready_public_minimum(indicative_monitor, concurrency):
    mon, clock, feed = indicative_monitor
    mon.settings.monitor.concurrency = concurrency
    router, banks, symbols = await prepared_minimums(mon, clock, feed, 1)
    gate = blocked_network_jobs(mon, concurrency)
    started = asyncio.Event()

    async def cached(source, target, amount, *, plan, **kwargs):
        bank = banks[(plan["pool"], source, target, 10)]
        started.set()
        return {**plan, "contextSlot": 2, "_observed_at": bank["observed"]}

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    mon.choose = lambda: pytest.fail("full network lane cannot consume a regular/manual request")
    mon.choose_forward_coverage = lambda: pytest.fail(
        "full network lane cannot start another coverage request"
    )
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(started.wait(), 1)
        async with asyncio.timeout(1):
            while mon.forward_coverage[symbols[0]].result["dex_bank_versions"][0][3] != 2:
                await asyncio.sleep(0)
        assert not gate.is_set()
        assert sum(key not in mon.local_pending for key in mon.pending) == concurrency
        assert mon.coverage_snapshot()["pricedfresh"] == 1
        router.quote_pair.assert_not_awaited()
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        await stop_scheduler(mon, scheduler)


async def test_extra_lane_bounds_eight_local_and_sixteen_network_jobs_in_72_asset_universe(
    indicative_monitor,
):
    mon, clock, feed = indicative_monitor
    mon.settings.monitor.concurrency = 16
    router, banks, symbols = await prepared_minimums(mon, clock, feed, 10)
    network_gate = blocked_network_jobs(mon, 16)
    for index in range(46):
        item = asset(f"U{index:02d}.US", deposit=False)
        mon.assets[item.symbol] = item
        mon.chain[item.symbol] = "verified_unit_mapping"
    assert len(mon.assets) == 72
    local_gate, eight_started, all_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    current = maximum = 0
    seen = []

    async def cached(source, target, amount, *, plan, **kwargs):
        nonlocal current, maximum
        current += 1
        maximum = max(maximum, current)
        seen.append(plan["pool"])
        if len(seen) == 8:
            eight_started.set()
        if len(seen) == len(symbols):
            all_started.set()
        assert len(mon.pending) <= 24 and len(mon.local_pending) <= 8
        assert sum(key not in mon.local_pending for key in mon.pending) == 16
        try:
            await local_gate.wait()
            bank = banks[(plan["pool"], source, target, 10)]
            return {**plan, "contextSlot": 2, "_observed_at": bank["observed"]}
        finally:
            current -= 1

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    mon.choose = lambda: pytest.fail("ordinary requests cannot exceed the original network cap")
    mon.choose_forward_coverage = lambda: pytest.fail("cold requests cannot exceed the original network cap")
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(eight_started.wait(), 1)
        assert len(mon.pending) == 24 and len(mon.local_pending) == maximum == 8
        assert not network_gate.is_set()
        local_gate.set()
        await asyncio.wait_for(all_started.wait(), 1)
        async with asyncio.timeout(1):
            while mon.coverage_snapshot()["pricedfresh"] != 10:
                await asyncio.sleep(0)
        assert maximum == 8 and len(seen) == 10
        router.quote_pair.assert_not_awaited()
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        await stop_scheduler(mon, scheduler)


async def test_legacy_mode_cannot_bypass_full_original_scheduler_capacity(indicative_monitor):
    mon, clock, feed = indicative_monitor
    mon.settings.monitor.concurrency = 1
    router, _, _ = await prepared_minimums(mon, clock, feed, 1)
    mon.indicative_feed = None
    blocked_network_jobs(mon, 1)
    router.cached_plan_quote = AsyncMock(side_effect=AssertionError("legacy scheduling stays bounded"))
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.sleep(0.02)
        assert len(mon.pending) == 1 and not mon.local_pending
        router.cached_plan_quote.assert_not_awaited()
    finally:
        await stop_scheduler(mon, scheduler)
