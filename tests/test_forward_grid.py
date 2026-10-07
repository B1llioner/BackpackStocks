import asyncio
from functools import partial
from unittest.mock import AsyncMock

import pytest
from test_forward_coverage import direct_coverage_monitor
from test_indicative_coverage import published

from backpack_stocks.indicative_feed import IndicativeFeed
from backpack_stocks.models import STOCK_MINT, USDC_MINT


@pytest.fixture
async def public_direct_monitor():
    mon, clock, router, banks, key = await direct_coverage_monitor()
    feed = IndicativeFeed(clock=clock)
    feed.tickers.add("WEN")
    mon.indicative_feed = mon.backpack.indicative_feed = feed
    await feed.ingest(published(clock))
    state = mon.forward_coverage[key[0]]
    await mon.quote(
        key,
        coverage=True,
        quote_pair=partial(router.cached_plan_quote, plan=state.plan["dex_plan"]),
    )
    mon.rfq.inspect.reset_mock()
    mon.rfq.check = AsyncMock(wraps=mon.rfq.check)
    try:
        yield mon, clock, feed, router, banks, key
    finally:
        await feed.close()


async def reprice_owned_minimum(mon, router, state, key, slot):
    # Only the input-version scheduler can dispatch this refresh. Discovery and
    # ordinary selection cannot hide a scheduling marker overwritten by a grid.
    mon.choose = lambda: None
    mon.choose_forward_coverage = lambda: None
    mon.settings.monitor.concurrency = 1
    router.bank_feed.changed.set()
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        async with asyncio.timeout(1):
            while state.result["dex_bank_versions"][0][3] != slot:
                await asyncio.sleep(0)
        assert mon.local_inputs[key] == mon.local_input_version(key, state.plan)
        assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    finally:
        scheduler.cancel()
        await asyncio.gather(scheduler, return_exceptions=True)
        pending = list(mon.pending.values())
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("new_public_tick", [False, True])
async def test_larger_grid_cannot_mark_an_old_minimum_as_repriced_on_a_new_bank(
    public_direct_monitor, new_public_tick
):
    mon, clock, feed, router, banks, key = public_direct_monitor
    state = mon.forward_coverage[key[0]]
    owner_plan, owner_result = state.plan, state.result
    owner_marker = mon.local_inputs[key]
    minimum_input = owner_result["input_raw"]
    cached = router.cached_plan_quote.side_effect
    advanced = False

    async def advancing_bank(source, target, amount, *, plan, **kwargs):
        nonlocal advanced
        if amount > minimum_input and not advanced:
            advanced = True
            clock.now += 1
            banks[("old", STOCK_MINT, USDC_MINT, 10)] = {
                "observed": clock(),
                "response": {"context": {"slot": 2}},
            }
            if new_public_tick:
                await feed.ingest(published(clock))
        return await cached(source, target, amount, plan=plan, **kwargs)

    router.cached_plan_quote = AsyncMock(side_effect=advancing_bank)
    larger = await mon.screen_forward_sizes(state, owner_plan["dex_plan"])

    assert advanced and larger and larger["profit_raw"] < 0
    assert router.cached_plan_quote.await_count == 3
    assert all(call.args[2] > minimum_input for call in router.cached_plan_quote.await_args_list)
    assert larger["acquired_raw"] > owner_result["acquired_raw"]
    assert larger["dex_bank_versions"][0][3] == 2
    assert state.plan is owner_plan and state.result is owner_result
    assert state.result["dex_bank_versions"][0][3] == 1
    assert mon.local_inputs[key] == owner_marker
    assert mon.local_input_version(key, owner_plan) != owner_marker
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["pricedfresh"] == 0 and coverage["waitingbank"] == 1
    assert not coverage["rows"][0]["pair"]["fresh"]
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()

    await reprice_owned_minimum(mon, router, state, key, 2)
    assert router.cached_plan_quote.await_args.args[2] == minimum_input
    mon.rfq.check.assert_not_awaited()


async def test_minimum_reprices_to_the_latest_public_price_after_an_inflight_tick(
    public_direct_monitor,
):
    mon, clock, feed, router, _, key = public_direct_monitor
    state = mon.forward_coverage[key[0]]
    calculated_marker = mon.local_inputs[key]
    cached = router.cached_plan_quote.side_effect

    async def new_price_while_calculating(source, target, amount, *, plan, **kwargs):
        clock.now += 1
        await feed.ingest(published(clock, ask="6.3"))
        return await cached(source, target, amount, plan=plan, **kwargs)

    router.cached_plan_quote = AsyncMock(side_effect=new_price_while_calculating)
    row = await mon.quote(
        key,
        coverage=True,
        local_plan=state.plan,
        quote_pair=partial(router.cached_plan_quote, plan=state.plan["dex_plan"]),
    )

    assert row and row["backpack_price"] == "6.3"
    assert row["cost_raw"] == 1_008_000  # The same exact 0.16 shares at the newly observed limit.
    assert mon.local_inputs[key] != calculated_marker
    assert mon.local_input_version(key, state.plan) == mon.local_inputs[key]
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    mon.rfq.check.assert_not_awaited()
