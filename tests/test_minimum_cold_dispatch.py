import asyncio
from unittest.mock import AsyncMock

from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_indicative_coverage import published
from test_minimum_scheduler_lane import prepared_minimums, stop_scheduler

from backpack_stocks.models import raw

indicative_monitor = indicative_monitor_fixture


async def test_cold_public_paper_minimum_uses_first_complete_route_dispatch_only(indicative_monitor):
    mon, clock, feed = indicative_monitor
    _, _, symbols = await prepared_minimums(mon, clock, feed, 1)
    state = mon.forward_coverage[symbols[0]]
    route = {**state.plan["dex_plan"], "contextSlot": 2, "_observed_at": clock()}
    state.plan = state.result = None
    mon.local_plans.clear()
    mon.local_inputs.clear()
    mon.choose = lambda: None
    mon.settings.monitor.concurrency = 1
    clock.now += mon.settings.monitor.quote_refresh_s
    await feed.ingest(published(clock))
    dispatch = AsyncMock(return_value=route)
    scheduler = asyncio.create_task(mon.schedule_quotes(dispatch))
    try:
        async with asyncio.timeout(1):
            while state.result is None:
                await asyncio.sleep(0)
        assert dispatch.await_args.args[2] == state.quantity_raw - raw(state.asset.fee, state.asset.decimals)
        assert dispatch.await_args.kwargs["minimum"] is True
        assert state.result["minimum_coverage"]
        assert mon.coverage_snapshot()["pricedfresh"] == 1
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        await stop_scheduler(mon, scheduler)
