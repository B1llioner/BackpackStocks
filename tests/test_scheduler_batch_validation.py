import asyncio
from unittest.mock import AsyncMock, Mock

from test_forward_coverage import coverage_monitor
from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_indicative_coverage import published
from test_minimum_scheduler_lane import prepared_minimums, stop_scheduler
from test_monitor import asset

from backpack_stocks.monitor import FORWARD

indicative_monitor = indicative_monitor_fixture


async def test_dispatch_batch_validates_each_ready_input_once_and_keeps_cold_work(indicative_monitor):
    mon, clock, feed = indicative_monitor
    mon.settings.monitor.concurrency = 8
    _, _, symbols = await prepared_minimums(mon, clock, feed, 8)
    cold = asset("COLD.US", deposit=False)
    mon.assets[cold.symbol] = cold
    mon.chain[cold.symbol] = "verified_unit_mapping"
    feed.tickers.add(cold.ticker)
    await feed.ingest(published(clock, cold.ticker))
    # Measure the scheduler's pass independently of primary-bank synchronization.
    mon.sync_primary_banks = lambda: None
    mon.choose = lambda: None
    candidates = mon.forward_coverage_candidates = Mock(wraps=mon.forward_coverage_candidates)
    versions = mon.local_input_version = Mock(wraps=mon.local_input_version)
    gate, started = asyncio.Event(), asyncio.Event()
    seen = []

    async def quote(key, **kwargs):
        seen.append((key, kwargs.get("local_plan") is not None))
        if len(seen) == 5:
            started.set()
        await gate.wait()

    mon.quote = AsyncMock(side_effect=quote)
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(started.wait(), 1)
        assert candidates.call_count == 1
        assert versions.call_count == len(symbols)
        assert len({key for key, _ in seen}) == len(seen)
        assert len(mon.local_pending) == 4 and len(mon.pending) == 5
        assert seen[-1] == ((cold.symbol, FORWARD), False)
        assert all(local for _, local in seen[:4])
        assert {call.args[0][0] for call in versions.call_args_list} == set(symbols)
    finally:
        await stop_scheduler(mon, scheduler)


async def test_next_outer_pass_reads_new_bank_inputs_and_never_repeats_same_minimum(indicative_monitor):
    mon, clock, feed = indicative_monitor
    mon.settings.monitor.concurrency = 4
    router, banks, symbols = await prepared_minimums(mon, clock, feed, 2)
    mon.choose = lambda: None
    called = []

    async def cached(source, target, amount, *, plan, **kwargs):
        current = banks[(plan["pool"], source, target, 10)]
        called.append((plan["pool"], current["response"]["context"]["slot"], amount))
        return {
            **plan,
            "contextSlot": current["response"]["context"]["slot"],
            "_observed_at": current["observed"],
        }

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    scheduler = asyncio.create_task(mon.schedule_quotes())
    try:
        async with asyncio.timeout(1):
            while not all(mon.forward_coverage[s].result["dex_bank_versions"][0][3] == 2 for s in symbols):
                await asyncio.sleep(0)
        await asyncio.sleep(0.03)
        assert len(called) == 2
        clock.now += 1.1
        for current in banks.values():
            current["observed"] = clock()
            current["response"]["context"]["slot"] = 3
        router.bank_feed.changed.set()
        async with asyncio.timeout(1):
            while not all(mon.forward_coverage[s].result["dex_bank_versions"][0][3] == 3 for s in symbols):
                await asyncio.sleep(0)
        await asyncio.sleep(0.03)
        assert len(called) == 4
        assert [(pool, slot) for pool, slot, _ in called] == [
            ("pool-" + symbols[0], 2),
            ("pool-" + symbols[1], 2),
            ("pool-" + symbols[0], 3),
            ("pool-" + symbols[1], 3),
        ]
        for pool, _, amount in called:
            state = mon.forward_coverage[pool.removeprefix("pool-")]
            assert amount == state.plan["input_raw"]
        assert mon.coverage_snapshot()["pricedfresh"] == 2
        router.quote_pair.assert_not_awaited()
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        await stop_scheduler(mon, scheduler)


async def test_extracted_candidates_preserve_default_and_ignore_pending_selection():
    mon, _ = coverage_monitor()
    key = "WEN.US", FORWARD
    assert mon.choose_forward_coverage() == key
    mon.pending[key] = asyncio.current_task()
    try:
        assert mon.forward_coverage_candidates() == []
        assert mon.choose_forward_coverage() is None
        assert mon.choose_forward_coverage(ignore_pending=key) == key
        assert mon.forward_coverage_candidates(ignore_pending=key) == [(False, 0, key[0])]
    finally:
        mon.pending.pop(key)
