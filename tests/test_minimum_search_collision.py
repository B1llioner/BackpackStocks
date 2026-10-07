import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from test_forward_grid import public_direct_monitor as public_direct_fixture
from test_indicative_coverage import indicative_monitor as public_monitor_fixture
from test_minimum_scheduler_lane import blocked_network_jobs, prepared_minimums

from backpack_stocks.direct import LocalQuoteNotReady
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.monitor import FORWARD

public_direct_monitor = public_direct_fixture
indicative_monitor = public_monitor_fixture


async def wait_until(predicate):
    async with asyncio.timeout(0.5):
        while not predicate():
            await asyncio.sleep(0.001)


async def stop(mon, scheduler):
    scheduler.cancel()
    await asyncio.gather(scheduler, return_exceptions=True)
    tasks = [*mon.pending.values(), *mon.minimum_pending.values()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("outcome", ["complete", "cancel", "unavailable", "replaced_owner"])
async def test_ready_minimum_shares_a_search_key_without_restoring_or_removing_its_larger_result(
    public_direct_monitor, outcome
):
    mon, clock, _, router, banks, key = public_direct_monitor
    mon.settings.monitor.concurrency = 1
    state = mon.forward_coverage[key[0]]
    owned = state.plan
    cached = router.cached_plan_quote.side_effect
    minimum_started, minimum_release = asyncio.Event(), asyncio.Event()
    larger_started, larger_release = asyncio.Event(), asyncio.Event()

    async def large(source, target, amount, **kwargs):
        assert amount > owned["input_raw"]
        larger_started.set()
        await larger_release.wait()
        return await cached(source, target, amount, plan=owned["dex_plan"], **kwargs)

    async def minimum(source, target, amount, **kwargs):
        assert amount == owned["input_raw"]
        minimum_started.set()
        await minimum_release.wait()
        if outcome == "unavailable":
            raise LocalQuoteNotReady()
        return await cached(source, target, amount, **kwargs)

    larger = asyncio.create_task(mon.quote(key, notional=100, quote_pair=large))
    mon.pending[key] = larger
    mon.search_pending.add(key)
    await asyncio.wait_for(larger_started.wait(), 0.5)
    clock.now += 0.1
    bank_key = "old", STOCK_MINT, USDC_MINT, 10
    banks[bank_key] = {"observed": clock(), "response": {"context": {"slot": 2}}}
    router.cached_plan_quote = AsyncMock(side_effect=minimum)
    router.quote_pair = AsyncMock(side_effect=AssertionError("minimum lane must not discover routes"))
    mon.choose = lambda: None
    mon.rfq.check.reset_mock()
    router.quote_pair.reset_mock()
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(minimum_started.wait(), 0.5)
        assert not larger.done() and key in mon.minimum_pending
        assert mon.coverage_snapshot()["pending"] == 1
        assert mon.snapshot()["pending"] == 2
        larger_release.set()
        larger_result = await asyncio.wait_for(larger, 0.5)
        assert larger_result and larger_result["acquired_raw"] > state.quantity_raw
        published = mon.results[key]
        history, large_plan = mon.price_history[key], mon.local_plans[key]
        mon.failures[key] = "larger_failure_owned"
        mon.failure_causes[key] = ["larger_failure_owned"]
        task = mon.minimum_pending[key]
        if outcome == "replaced_owner":
            mon.forward_coverage[key[0]] = replace(state, result=None, plan=None)
        if outcome == "cancel":
            task.cancel()
        else:
            minimum_release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert mon.results[key] is published
        assert mon.price_history[key] is history and mon.local_plans[key] is large_plan
        assert mon.failures[key] == "larger_failure_owned"
        assert mon.failure_causes[key] == ["larger_failure_owned"]
        if outcome == "complete":
            assert state.result["acquired_raw"] == state.quantity_raw
            assert state.result["dex_bank_versions"][0][3] == 2
            assert mon.local_inputs[key] == mon.local_input_version(key, state.plan)
            assert mon.coverage_result_fresh(state)
        elif outcome == "replaced_owner":
            assert mon.forward_coverage[key[0]].result is None
        else:
            assert state.result["dex_bank_versions"][0][3] == 1
        mon.rfq.check.assert_not_called()
        router.quote_pair.assert_not_called()
    finally:
        minimum_release.set()
        larger_release.set()
        await stop(mon, scheduler)


async def test_completed_large_search_cannot_dispatch_a_second_inflight_minimum(public_direct_monitor):
    mon, clock, _, router, banks, key = public_direct_monitor
    mon.settings.monitor.concurrency = 1
    state = mon.forward_coverage[key[0]]
    state.attempted_at = clock() - 100
    owned = state.plan
    cached = router.cached_plan_quote.side_effect
    larger_started, larger_release = asyncio.Event(), asyncio.Event()
    minimum_started, minimum_release = asyncio.Event(), asyncio.Event()

    async def large(source, target, amount, **kwargs):
        assert amount > owned["input_raw"]
        larger_started.set()
        await larger_release.wait()
        return await cached(source, target, amount, plan=owned["dex_plan"], **kwargs)

    async def minimum(source, target, amount, **kwargs):
        assert amount == owned["input_raw"]
        minimum_started.set()
        await minimum_release.wait()
        return await cached(source, target, amount, **kwargs)

    larger = asyncio.create_task(mon.quote(key, notional=100, quote_pair=large))
    mon.pending[key] = larger
    mon.search_pending.add(key)
    await asyncio.wait_for(larger_started.wait(), 0.5)
    clock.now += 0.1
    banks[("old", STOCK_MINT, USDC_MINT, 10)] = {
        "observed": clock(),
        "response": {"context": {"slot": 2}},
    }
    router.cached_plan_quote = AsyncMock(side_effect=minimum)
    router.quote_pair = AsyncMock(side_effect=AssertionError("minimum must not discover routes"))
    mon.choose = lambda: None
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(minimum_started.wait(), 0.5)
        task = mon.minimum_pending[key]
        # Ignoring the larger scheduler task must still retain the SDK task's
        # ownership, including callers outside the ordinary dispatch batch.
        assert mon.forward_coverage_candidates(ignore_pending=key) == []
        assert mon.choose_forward_coverage(ignore_pending=key) is None
        larger_release.set()
        result = await asyncio.wait_for(larger, 0.5)
        assert result and result["acquired_raw"] > state.quantity_raw
        await wait_until(lambda: key not in mon.pending)
        await asyncio.sleep(0.05)
        assert mon.minimum_pending[key] is task and not task.done()
        assert router.cached_plan_quote.await_count == 1
        assert mon.forward_coverage_candidates() == []
        assert mon.coverage_snapshot()["pending"] == mon.snapshot()["pending"] == 1
        minimum_release.set()
        await asyncio.wait_for(task, 0.5)
        assert mon.coverage_result_fresh(state)
        assert state.result["dex_bank_versions"][0][3] == 2
        mon.rfq.check.assert_not_called()
        router.quote_pair.assert_not_called()
    finally:
        minimum_release.set()
        larger_release.set()
        await stop(mon, scheduler)


async def test_four_collision_tasks_share_the_existing_eight_local_and_twentyfour_total_ceiling(
    indicative_monitor,
):
    mon, clock, public = indicative_monitor
    mon.settings.monitor.concurrency = 16
    router, banks, symbols = await prepared_minimums(mon, clock, public, 14)
    gate = asyncio.Event()
    for symbol in symbols[:10]:
        key = symbol, FORWARD
        mon.pending[key] = asyncio.create_task(gate.wait())
        mon.search_pending.add(key)
    blocked_network_jobs(mon, 6)
    local_gate = asyncio.Event()

    async def cached(source, target, amount, *, plan, **kwargs):
        await local_gate.wait()
        bank = banks[(plan["pool"], source, target, 10)]
        return {**plan, "contextSlot": 2, "_observed_at": bank["observed"]}

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await wait_until(lambda: router.cached_plan_quote.await_count == 8)
        assert len(mon.minimum_pending) == 4
        assert all(key in mon.search_pending and key in mon.pending for key in mon.minimum_pending)
        assert len(mon.local_pending) == 4
        assert len(mon.pending) + len(mon.minimum_pending) == 24
        assert mon.coverage_snapshot()["pending"] == 8
        router.quote_pair.assert_not_called()
        mon.rfq.check.assert_not_called()
        mon._owns_quote_provider = False
        await mon.close()
        assert not mon.pending and not mon.minimum_pending
    finally:
        gate.set()
        local_gate.set()
        await stop(mon, scheduler)


@pytest.mark.parametrize("boundary", ["ordinary_task", "live", "no_public", "no_coverage"])
async def test_collision_bypass_is_only_paper_owned_public_minimum_during_larger_search(
    indicative_monitor, boundary
):
    mon, clock, public = indicative_monitor
    mon.settings.monitor.concurrency = 1
    router, _, symbols = await prepared_minimums(mon, clock, public, 1)
    key = symbols[0], FORWARD
    gate = asyncio.Event()
    mon.pending[key] = asyncio.create_task(gate.wait())
    mon.search_pending.add(key)
    if boundary == "ordinary_task":
        mon.search_pending.clear()
    elif boundary == "live":
        mon.settings = mon.settings.model_copy(update={"mode": "live"})
    elif boundary == "no_public":
        mon.indicative_feed = None
    else:
        mon.settings.monitor.auto_size = False
    router.cached_plan_quote = AsyncMock()
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.sleep(0.05)
        assert mon.minimum_pending == {} and len(mon.pending) == 1
        router.cached_plan_quote.assert_not_called()
        router.quote_pair.assert_not_called()
    finally:
        gate.set()
        await stop(mon, scheduler)
