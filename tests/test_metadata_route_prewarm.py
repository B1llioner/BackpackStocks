import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from test_indicative_coverage import indicative_monitor as public_monitor_fixture
from test_indicative_coverage import published
from test_minimum_topology_prewarm import unpriced

from backpack_stocks.monitor import FORWARD

indicative_monitor = public_monitor_fixture


async def stop_loop(mon, task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    pending = [*mon.pending.values(), *mon.minimum_pending.values()]
    for job in pending:
        job.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("source", ["missing", "expired_price", "expired_noquote", "unavailable"])
async def test_current_metadata_warms_topology_without_a_usable_current_purchase_price(
    indicative_monitor, source
):
    mon, clock, public = indicative_monitor
    router, banks, state, route = await unpriced(mon, clock, public)
    if source == "missing":
        public.latest.clear()
    elif source in {"expired_price", "expired_noquote"}:
        if source == "expired_price":
            await public.ingest(published(clock))
        clock.now += mon.settings.monitor.max_quote_age_s + 1
        banks[("pool", route["inputMint"], route["outputMint"], 10)] = {
            "observed": clock(),
            "response": {"context": {"slot": 2}},
        }
        router.quote_pair.return_value = {**route, "_observed_at": clock(), "contextSlot": 2}
    check = state.check
    mon.discovery_ready.set()
    loop = asyncio.create_task(mon.coverage_routes_loop())
    try:
        async with asyncio.timeout(0.5):
            while state.prefetched_route is None:
                await asyncio.sleep(0.001)
        assert state.result is state.plan is None and state.check is check
        assert "outAmount" not in state.prefetched_route
        assert "backpack_price" not in state.prefetched_route
        assert state.prefetched_identity == mon.coverage_topology_identity(state)
        router.quote_pair.assert_awaited_once()
        assert router.quote_pair.await_args.kwargs["priority"] == 10
        assert router.quote_pair.await_args.kwargs["minimum"] is True
        mon.rfq.check.assert_not_called()
        mon.rfq.inspect.assert_not_called()
    finally:
        await stop_loop(mon, loop)


@pytest.mark.parametrize(
    "boundary",
    [
        "usable",
        "invalid_payload",
        "invalid_reason",
        "future",
        "future_receive",
        "zero_stamp",
        "metadata",
        "mapping",
    ],
)
async def test_current_price_or_invalid_inputs_do_not_start_metadata_prewarm(indicative_monitor, boundary):
    mon, clock, public = indicative_monitor
    router, _, state, _ = await unpriced(mon, clock, public)
    latest = public.latest[state.asset.ticker]
    if boundary == "usable":
        await public.ingest(published(clock))
    elif boundary in {"invalid_payload", "invalid_reason"}:
        public.latest[state.asset.ticker] = replace(latest, reason="indicative_" + boundary)
    elif boundary == "future":
        public.latest[state.asset.ticker] = replace(latest, published_at=clock() + 1)
    elif boundary == "future_receive":
        public.latest[state.asset.ticker] = replace(latest, received_at=clock() + 1)
    elif boundary == "zero_stamp":
        public.latest[state.asset.ticker] = replace(latest, published_at=0)
    elif boundary == "metadata":
        public.latest.clear()
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    else:
        public.latest.clear()
        mon.chain[state.asset.symbol] = "unsupported_mint_or_unit_mapping"
    mon.discovery_ready.set()
    loop = asyncio.create_task(mon.coverage_routes_loop())
    try:
        await asyncio.sleep(0.03)
        router.quote_pair.assert_not_called()
        assert state.prefetched_route is state.result is state.plan is None
    finally:
        await stop_loop(mon, loop)


async def test_price_rejection_after_selection_does_not_claim_a_dex_discovery_attempt(indicative_monitor):
    mon, clock, public = indicative_monitor
    router, _, state, _ = await unpriced(mon, clock, public)
    await public.ingest(published(clock))
    state.attempted_at = state.discovery_at = 0
    candidates = mon.forward_coverage_candidates
    selected = False

    def reject_after_selection(**kwargs):
        nonlocal selected
        if selected:
            return []
        rows = candidates(**kwargs)
        assert rows
        selected = True
        latest = public.latest[state.asset.ticker]
        public.latest[state.asset.ticker] = replace(latest, reason="NoQuote", ask=None, bid=None)
        return rows

    mon.forward_coverage_candidates = reject_after_selection
    mon.choose = lambda: None
    router.quote_pair = AsyncMock()
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        async with asyncio.timeout(0.5):
            while not state.attempted_at:
                await asyncio.sleep(0.001)
        assert state.discovery_at == 0
        assert state.result is state.plan is None
        router.quote_pair.assert_not_called()
        mon.rfq.check.assert_not_called()
    finally:
        await stop_loop(mon, scheduler)


async def test_discovery_deadline_starts_after_valid_purchase_before_real_cold_dex_call(indicative_monitor):
    mon, clock, public = indicative_monitor
    router, _, state, route = await unpriced(mon, clock, public)
    await public.ingest(published(clock))
    entered, release = asyncio.Event(), asyncio.Event()

    async def cold(source, target, amount, **kwargs):
        assert state.check.usable(clock())
        assert state.discovery_at == clock()
        entered.set()
        await release.wait()
        return route

    task = asyncio.create_task(mon.quote((state.asset.symbol, FORWARD), coverage=True, quote_pair=cold))
    try:
        await asyncio.wait_for(entered.wait(), 0.5)
        release.set()
        row = await asyncio.wait_for(task, 0.5)
        assert row and state.plan
        marked = state.discovery_at
        clock.now += 0.1
        await public.ingest(published(clock))
        router.cached_plan_quote = AsyncMock(return_value=route)
        await mon.quote(
            (state.asset.symbol, FORWARD),
            coverage=True,
            local_plan=state.plan,
            quote_pair=router.cached_plan_quote,
        )
        assert state.discovery_at == marked
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
