import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_monitor import asset, observed
from test_monitor import monitor as monitor_fixture

from backpack_stocks.direct import DirectRaydium, LocalQuoteNotReady
from backpack_stocks.models import STOCK_MINT, USDC_MINT, ui
from backpack_stocks.monitor import FORWARD, REVERSE


async def owned_monitor(*, reverse=False):
    mon, clock = monitor_fixture.__wrapped__()
    mon.settings.monitor.rfq_validation = True
    mon.backpack.auth = object()

    async def inspect(symbol, side, quantity, session):
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", "6")

    mon.rfq.inspect = AsyncMock(side_effect=inspect)
    router = object.__new__(DirectRaydium)
    source, target = (USDC_MINT, STOCK_MINT) if reverse else (STOCK_MINT, USDC_MINT)
    banks = {("pool", source, target, 10): {"observed": clock(), "response": {"context": {"slot": 1}}}}

    def peek(key):
        bank = banks.get(key)
        return (
            bank if bank and 0 <= clock() - bank["observed"] < mon.settings.policy.quote_max_age_s else None
        )

    router.bank_feed = SimpleNamespace(peek=peek)
    quote = {
        "pool": "pool",
        "inputMint": source,
        "outputMint": target,
        "contextSlot": 1,
        "_observed_at": clock(),
        "_direct": {},
        "outAmount": "7000000" if reverse else "45000000",
        "otherAmountThreshold": "6965000" if reverse else "44775000",
        "priceImpact": "0.1",
    }
    router.cached_plan_quote = AsyncMock(return_value=quote)
    mon._quote_provider = router
    key = "WEN.US", REVERSE if reverse else FORWARD
    await mon.quote(key, notional=50, quote_pair=AsyncMock(return_value=quote))
    assert mon.results[key]["profit_raw"] < 0
    mon.rfq.inspect.reset_mock()
    return mon, clock, router, banks, key


async def test_negative_plan_renews_its_owned_exact_size_at_price_expiry_without_default_grid():
    mon, clock, router, _, key = await owned_monitor()
    plan = mon.local_plans[key]
    owned = str(ui(plan["acquired_raw"], mon.assets[key[0]].decimals))
    deadline = mon.rfq_deadline(mon.assets[key[0]], "Bid")
    clock.now = deadline
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    mon.validated_buy = AsyncMock(side_effect=AssertionError("do not rediscover a known size"))
    await mon.refresh_rfq_asset(mon.assets[key[0]], "Bid")
    mon.rfq.inspect.assert_awaited_once_with(key[0], "Bid", owned, plan["backpack_session"])
    router.cached_plan_quote.assert_not_awaited()
    mon.validated_buy.assert_not_awaited()
    check = mon.rfq.cached_exact(key[0], owned, plan["backpack_session"])
    assert check and check.retry_at == clock() + mon.settings.monitor.max_quote_age_s
    assert mon.rfq_deadline(mon.assets[key[0]], "Bid") == check.retry_at < check.eligibility_until


async def test_another_quantity_refusal_cannot_postpone_owned_price_renewal():
    mon, clock, _, _, key = await owned_monitor()
    deadline = mon.rfq_deadline(mon.assets[key[0]], "Bid")
    clock.now = deadline
    mon.rfq.latest[(key[0], "Bid")] = mon.rfq.result(
        key[0], "Bid", "1000", "US_EQUITIES_REGULAR", "unavailable", "rfq_insufficient_balance"
    )
    assert mon.rfq_deadline(mon.assets[key[0]], "Bid") == deadline
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    await mon.refresh_rfq_asset(mon.assets[key[0]], "Bid")
    mon.rfq.inspect.assert_awaited_once()


async def test_owned_size_refusal_keeps_the_existing_cooldown():
    mon, clock, _, _, key = await owned_monitor()
    plan = mon.local_plans[key]
    quantity = format(ui(plan["acquired_raw"]).normalize(), "f")
    check = mon.rfq.result(
        key[0], "Bid", quantity, plan["backpack_session"], "unavailable", "rfq_insufficient_balance"
    )
    mon.rfq.cache[(key[0], "Bid", quantity, plan["backpack_session"])] = check
    mon.rfq.latest[(key[0], "Bid")] = check
    assert mon.rfq_deadline(mon.assets[key[0]], "Bid") == check.eligibility_until
    assert check.eligibility_until == clock() + mon.settings.monitor.rfq_cache_s
    assert mon.local_input_version(key, plan) is None


async def test_reverse_renewal_requests_current_exact_sale_size_and_ask_side():
    mon, clock, router, _, key = await owned_monitor(reverse=True)
    plan = mon.local_plans[key]
    clock.now = mon.rfq_deadline(mon.assets[key[0]], "Ask")
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    router.cached_plan_quote.return_value = {"outAmount": "9000000", "otherAmountThreshold": "8955000"}
    output = 8955000
    quantity = mon.sale_quantity(mon.assets[key[0]], observed(clock), output)
    await mon.refresh_rfq_asset(mon.assets[key[0]], "Ask")
    router.cached_plan_quote.assert_awaited_once_with(
        USDC_MINT, STOCK_MINT, plan["input_raw"], plan=plan["dex_plan"], priority=10
    )
    mon.rfq.inspect.assert_awaited_once_with(
        key[0], "Ask", format(ui(quantity).normalize(), "f"), plan["backpack_session"]
    )


async def test_missing_reverse_bank_does_not_create_a_false_rfq_cooldown():
    mon, clock, router, _, key = await owned_monitor(reverse=True)
    previous = mon.rfq.latest[(key[0], "Ask")]
    clock.now = previous.eligibility_until + 1
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    router.cached_plan_quote.side_effect = LocalQuoteNotReady()
    await mon.refresh_rfq_asset(mon.assets[key[0]], "Ask")
    assert mon.rfq.latest[(key[0], "Ask")] is previous
    assert mon.rfq_deadline(mon.assets[key[0]], "Ask") == clock() + mon.settings.monitor.quote_refresh_s
    mon.rfq.inspect.assert_not_awaited()
    router.cached_plan_quote.side_effect = None
    await mon.refresh_rfq_asset(mon.assets[key[0]], "Ask")
    mon.rfq.inspect.assert_awaited_once()


@pytest.mark.parametrize("boundary", ["expired_rfq", "other_size", "other_side", "session", "stale_bank"])
async def test_local_scheduler_requires_usable_owned_inputs(boundary):
    mon, clock, _, banks, key = await owned_monitor()
    plan = mon.local_plans[key]
    assert mon.local_input_version(key, plan) == mon.local_inputs[key]
    if boundary == "expired_rfq":
        clock.now += mon.settings.monitor.max_quote_age_s
        banks[("pool", STOCK_MINT, USDC_MINT, 10)]["observed"] = clock()
    elif boundary in {"other_size", "other_side"}:
        mon.rfq.cache.clear()
        side = "Ask" if boundary == "other_side" else "Bid"
        await mon.rfq.check(key[0], "1", plan["backpack_session"], side=side)
    elif boundary == "session":
        mon.backpack.stock_feed.latest["WEN"] = observed(clock).model_copy(update={"session": "OVERNIGHT"})
    else:
        banks[("pool", STOCK_MINT, USDC_MINT, 10)]["observed"] -= mon.settings.policy.quote_max_age_s
    assert mon.local_input_version(key, plan) is None


async def test_same_bank_and_rfq_inputs_are_not_repeated_but_a_new_bank_runs_once():
    mon, clock, _, banks, key = await owned_monitor()
    mon.settings.monitor.concurrency = 1
    mon.choose = lambda: None
    called = asyncio.Event()

    async def local_quote(*args, **kwargs):
        called.set()
        return None

    mon.quote = AsyncMock(side_effect=local_quote)
    task = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.sleep(0.3)
        mon.quote.assert_not_awaited()
        banks[("pool", STOCK_MINT, USDC_MINT, 10)] = {
            "observed": clock(),
            "response": {"context": {"slot": 2}},
        }
        await asyncio.wait_for(called.wait(), 1)
        clock.now += 2
        mon.backpack.stock_feed.latest["WEN"] = observed(clock)
        await asyncio.sleep(0.55)
        mon.quote.assert_awaited_once()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for pending in mon.pending.values():
            pending.cancel()
        await asyncio.gather(*mon.pending.values(), return_exceptions=True)


async def test_local_work_leaves_scheduler_slots_for_unseen_network_coverage():
    mon, clock, _, banks, key = await owned_monitor()
    mon.settings.monitor.concurrency = 2
    mon.local_inputs.clear()
    mon.assets["COLD.US"] = asset("COLD.US", USDC_MINT, deposit=False)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["COLD"] = observed(clock)
    mon.rfq.latest[("COLD.US", "Bid")] = mon.rfq.result(
        "COLD.US", "Bid", "1", "US_EQUITIES_REGULAR", "available", "rfq_quote_observed", "6"
    )
    gate = asyncio.Event()

    async def wait_quote(*args, **kwargs):
        await gate.wait()

    mon.quote = AsyncMock(side_effect=wait_quote)
    task = asyncio.create_task(mon.schedule_quotes())
    try:
        async with asyncio.timeout(1):
            while mon.quote.await_count < 2:
                await asyncio.sleep(0.01)
        assert {call.args[0] for call in mon.quote.await_args_list} == {key, ("COLD.US", FORWARD)}
        assert len(mon.local_pending) == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        gate.set()
        await asyncio.gather(*mon.pending.values(), return_exceptions=True)


async def test_due_ask_is_not_postponed_by_a_recent_bid_and_rfq_jobs_stay_bounded():
    mon, clock = monitor_fixture.__wrapped__()
    mon.settings.monitor.rfq_validation = True
    mon.settings.monitor.rfq_concurrency = 1
    mon.assets["COLD.US"] = asset("COLD.US", USDC_MINT)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    for symbol in mon.assets:
        mon.rfq.latest[(symbol, "Bid")] = mon.rfq.result(
            symbol, "Bid", "1", "US_EQUITIES_REGULAR", "available", "rfq_quote_observed", "6"
        )
        check = mon.rfq.result(
            symbol, "Ask", "1", "US_EQUITIES_REGULAR", "unavailable", "rfq_insufficient_balance"
        )
        mon.rfq.latest[(symbol, "Ask")] = replace(
            check, checked_at=clock() - 120, retry_at=clock() - 60, eligibility_until=clock() - 60
        )
    gate = asyncio.Event()

    async def wait_rfq(*args):
        await gate.wait()

    mon.refresh_rfq_asset = AsyncMock(side_effect=wait_rfq)
    mon.refresh_external_prices = AsyncMock()
    mon.discovery_ready.set()
    task = asyncio.create_task(mon.rfq_loop())
    try:
        async with asyncio.timeout(1):
            while mon.refresh_rfq_asset.await_count == 0:
                await asyncio.sleep(0.01)
        assert mon.refresh_rfq_asset.await_args.args == (mon.assets["COLD.US"], "Ask")
        await asyncio.sleep(0.55)
        mon.refresh_rfq_asset.assert_awaited_once()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        gate.set()
