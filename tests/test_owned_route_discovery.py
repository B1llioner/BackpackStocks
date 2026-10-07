import asyncio
from unittest.mock import AsyncMock

import pytest
from test_direct_routes import quote
from test_forward_grid import public_direct_monitor as public_direct_fixture

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, Settings
from backpack_stocks.pool_bank_feed import PoolBankFeed

public_direct_monitor = public_direct_fixture


@pytest.mark.parametrize("prewarm", [False, True])
async def test_only_unpriced_topology_prewarm_uses_first_ready_minimum(public_direct_monitor, prewarm):
    mon, _, _, router, _, key = public_direct_monitor
    state = mon.forward_coverage[key[0]]
    route = {**state.plan["dex_plan"], "inAmount": str(state.plan["input_raw"])}
    if prewarm:
        state.plan = state.result = None
    router.quote_pair = AsyncMock(return_value=route)
    await mon.discover_coverage_route(state)
    kwargs = router.quote_pair.await_args.kwargs
    assert kwargs.get("minimum", False) is prewarm
    assert kwargs["_rpc"] is router.bank_feed and kwargs["priority"] == 10
    if prewarm:
        assert state.prefetched_route is not None and state.plan is state.result is None
    else:
        assert state.route_candidate is route


async def test_owned_background_keeps_its_slot_until_all_full_optimizer_candidates_finish(
    public_direct_monitor, monkeypatch
):
    mon, clock, _, router, _, key = public_direct_monitor
    monkeypatch.setattr("backpack_stocks.direct_routes.time.time", clock)
    owner = PoolBankFeed(None, mon.settings.policy.quote_max_age_s)
    owner.runner.cancel()
    await asyncio.gather(owner.runner, return_exceptions=True)
    previous_feed = router.bank_feed
    router.bank_feed = owner
    router.settings = Settings(direct={"via_sol": True, "split_pools": False})
    router.assets = {STOCK_MINT: {}}
    router.candidates = {STOCK_MINT: ["cached-minimum", "better-alternative"]}
    router.sol_candidates = {}
    router.ensure_candidates = AsyncMock()
    router.bridge_pools = AsyncMock(return_value=[])
    router.quote_count = router.failures = 0
    router.quote_pair = DirectRaydium.quote_pair.__get__(router)
    release, alternative_started = asyncio.Event(), asyncio.Event()

    async def calculate(pool, source, target, amount, **kwargs):
        if pool == "better-alternative":
            alternative_started.set()
            await release.wait()
        return quote(pool, source, target, amount, amount * (7 if pool == "better-alternative" else 6))

    router.pool_quote = AsyncMock(side_effect=calculate)
    mon.discovery_ready.set()
    discovery = asyncio.create_task(mon.coverage_routes_loop())
    try:
        await asyncio.wait_for(alternative_started.wait(), 0.5)
        await asyncio.sleep(0.03)
        state = mon.forward_coverage[key[0]]
        assert state.route_candidate is None
        assert key[0] in mon.coverage_route_pending
        assert not mon.coverage_route_pending[key[0]].done()
        assert router.pool_quote.await_count == 2
        release.set()
        async with asyncio.timeout(0.5):
            while state.route_candidate is None:
                await asyncio.sleep(0.001)
        assert state.route_candidate["pool"] == "better-alternative"
        assert not owner.tasks
        mon.rfq.check.assert_not_called()
    finally:
        release.set()
        discovery.cancel()
        await asyncio.gather(discovery, return_exceptions=True)
        await owner.close()
        router.bank_feed = previous_feed
