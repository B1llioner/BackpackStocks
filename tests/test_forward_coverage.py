import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_monitor import asset, observed
from test_monitor import monitor as monitor_fixture
from test_rfq_validation import setup as rfq_setup
from test_stock_books import setup_books

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, raw
from backpack_stocks.monitor import FORWARD
from backpack_stocks.observability import QuoteUnavailable
from backpack_stocks.transport import RemoteError


def coverage_monitor():
    mon, clock = monitor_fixture.__wrapped__()
    mon.settings.monitor.auto_size = True
    mon.settings.monitor.rfq_validation = True
    mon.backpack.auth = object()
    mon.sessions = [
        {
            "name": "US_EQUITIES_REGULAR",
            "timezone": "UTC",
            "startTime": "00:00:00",
            "endTime": "23:59:59",
            "startWeekday": 1,
            "endWeekday": 7,
        }
    ]

    async def inspect(symbol, side, quantity, session):
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", "6.2")

    async def dex(source, target, amount, **kwargs):
        assert target == USDC_MINT
        return {"outAmount": str(amount * 6), "priceImpact": "0.1", "_observed_at": clock()}

    mon.rfq.inspect = AsyncMock(side_effect=inspect)
    mon.jupiter.quote_pair.side_effect = dex
    return mon, clock


@pytest.mark.parametrize(
    "semantics,quantity,delivered", [("deduct", "0.16", "0.08"), ("additional", "0.24", "0.16")]
)
async def test_minimum_coverage_uses_real_exact_bid_and_only_cancels(semantics, quantity, delivered):
    mon, clock = coverage_monitor()
    mon.settings.policy.withdrawal_semantics = semantics
    bp, validator = rfq_setup(clock)
    validator.backpack = mon.backpack
    mon.rfq = validator
    for name in ("submit_rfq", "rfqs", "cancel_rfq", "accept_rfq"):
        setattr(mon.backpack, name, getattr(bp, name))
    mon.next_budget = lambda *_: pytest.fail("minimum coverage must not use the volume grid")
    key = "WEN.US", FORWARD
    row = await mon.quote(key, coverage=True)
    assert row and row["acquired_raw"] == raw(quantity) and row["delivered_raw"] == raw(delivered)
    assert row["backpack_price"] == "6.2" and row["profit_raw"] < 0
    assert row["cost_raw"] == raw(Decimal(quantity) * Decimal("6.2"))
    assert not mon.volume_search.samples[key]
    bp.submit_rfq.assert_awaited_once()
    assert bp.submit_rfq.await_args.args[0] == quantity
    assert bp.submit_rfq.await_args.kwargs["side"] == "Bid"
    bp.cancel_rfq.assert_awaited_once()
    bp.accept_rfq.assert_not_called()
    mon.jupiter.quote_pair.assert_awaited_once_with(STOCK_MINT, USDC_MINT, raw(delivered), priority=10)
    report = mon.snapshot()
    assert report["forward_coverage"]["pricedfresh"] == 1
    assert report["forward_coverage"]["done_universe"]
    assert report["prices"][0]["buy_quantity"] == quantity
    assert report["candidates"] == 0


async def test_every_verified_asset_reaches_numerical_dex_even_with_ask_refusals_and_hot_search():
    mon, clock = coverage_monitor()
    for index in range(72):
        item = asset(f"S{index:02d}.US", deposit=index % 2 == 0)
        mon.assets[item.symbol] = item
        mon.chain[item.symbol] = (
            "unsupported_mint_or_unit_mapping" if index == 71 else "verified_unit_mapping"
        )
        mon.backpack.stock_feed.latest[item.ticker] = observed(clock)
        mon.rfq.latest[(item.symbol, "Ask")] = mon.rfq.result(
            item.symbol, "Ask", "1", "US_EQUITIES_REGULAR", "unavailable", "rfq_insufficient_balance"
        )
    mon.optimization_requested.add(("WEN.US", FORWARD))
    mon.requested[("WEN.US", FORWARD)] = clock()
    mon.estimate = lambda item, direction=None: (
        Decimal("1000") if item.symbol == "WEN.US" else Decimal("-1000")
    )
    count = 0
    while (key := mon.choose_forward_coverage()) is not None:
        row = await mon.quote(key, coverage=True)
        assert row and row["profit_raw"] < 0
        count += 1
    assert count == 72
    assert mon.jupiter.quote_pair.await_count == 72
    assert {call.args[1] for call in mon.rfq.inspect.await_args_list} == {"Bid"}
    report = mon.snapshot()
    assert len(report["prices"]) == 72
    coverage = report["forward_coverage"]
    assert (coverage["universe"], coverage["eligible"], coverage["unverified"]) == (73, 72, 1)
    assert coverage["pricedfresh"] == coverage["first_pass_completed"] == 72
    assert coverage["done_universe"] and coverage["waiting"] == 0
    assert ("WEN.US", FORWARD) in mon.optimization_requested


async def test_owned_minimum_survives_grid_cache_eviction_other_size_refusal_and_price_change():
    mon, clock = coverage_monitor()
    key = "WEN.US", FORWARD
    first = await mon.quote(key, coverage=True)
    state = mon.forward_coverage[key[0]]
    owned = state.check
    deadline = mon.rfq_deadline(mon.assets[key[0]], "Bid")
    for quantity in range(1, 6):
        clock.now += 0.01
        await mon.rfq.check(key[0], str(quantity), state.session)
    assert mon.rfq.cached_exact(key[0], "0.16", state.session) is None
    mon.rfq.latest[(key[0], "Bid")] = mon.rfq.result(
        key[0], "Bid", "1000", state.session, "unavailable", "rfq_insufficient_balance"
    )
    assert mon.exact_rfq(mon.assets[key[0]], "0.16", state.session) is owned
    assert mon.exact_rfq(mon.assets[key[0]], "1000", state.session) is None
    assert mon.rfq_deadline(mon.assets[key[0]], "Bid") == deadline
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    clock.now = deadline
    mon.backpack.stock_feed.latest["WEN"] = observed(clock, ask="600", bid="599")

    async def repriced(symbol, side, quantity, session):
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", "12")

    mon.rfq.inspect.side_effect = repriced
    mon.rfq.inspect.reset_mock()
    await mon.refresh_rfq_side(mon.assets[key[0]], "Bid")
    second = await mon.quote(key, coverage=True)
    mon.rfq.inspect.assert_awaited_once_with(key[0], "Bid", "0.16", state.session)
    assert second["acquired_raw"] == first["acquired_raw"]
    assert second["cost_raw"] == raw("1.92")
    assert mon.forward_coverage[key[0]].plan["acquired_raw"] == raw("0.16")


async def test_larger_search_keeps_its_own_price_and_does_not_replace_minimum_owner():
    mon, _ = coverage_monitor()
    key = "WEN.US", FORWARD
    baseline = await mon.quote(key, coverage=True)
    baseline_plan = mon.forward_coverage[key[0]].plan

    async def priced_by_quantity(symbol, side, quantity, session):
        price = "9" if Decimal(quantity) > 1 else "6.2"
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", price)

    mon.rfq.inspect.side_effect = priced_by_quantity
    larger = await mon.quote(key, notional=Decimal("100"))
    assert larger and larger["acquired_raw"] > baseline["acquired_raw"]
    assert larger["backpack_price"] == "9"
    assert mon.forward_coverage[key[0]].result is not larger
    assert mon.forward_coverage[key[0]].plan is baseline_plan
    assert mon.forward_coverage[key[0]].result["backpack_price"] == "6.2"
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["jupiter_no_route"])
    assert await mon.quote(key, notional=Decimal("200")) is None
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    assert mon.snapshot()["prices"][0]["buy_quantity"] == "0.16"


@pytest.mark.parametrize("reason", ["insufficient_balance", "invalid_quantity", "market_closed"])
async def test_actual_api_refusal_at_minimum_prevents_dex_and_keeps_cooldown(reason):
    mon, clock = coverage_monitor()
    bp, validator = rfq_setup(clock)
    validator.backpack = mon.backpack
    mon.rfq = validator
    mon.backpack.submit_rfq = bp.submit_rfq
    bp.submit_rfq.side_effect = RemoteError("API rejected request: HTTP 400", status_code=400, reason=reason)
    key = "WEN.US", FORWARD
    assert await mon.quote(key, coverage=True) is None
    mon.jupiter.quote_pair.assert_not_awaited()
    assert mon.choose_forward_coverage() is None
    assert await mon.quote(key, coverage=True) is None
    bp.submit_rfq.assert_awaited_once()
    status = mon.snapshot()["forward_coverage"]
    assert status["checked"] == status["refused"] == status["first_pass_completed"] == 1
    assert status["pricedfresh"] == 0 and status["done_universe"]


@pytest.mark.parametrize("boundary", ["rfq", "metadata", "session", "mint"])
async def test_minimum_price_never_stays_fresh_after_ownership_boundary(boundary):
    mon, clock = coverage_monitor()
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True)
    if boundary == "rfq":
        clock.now = mon.forward_coverage[key[0]].check.retry_at
        mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    elif boundary == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif boundary == "session":
        mon.sessions[0]["name"] = "OVERNIGHT"
        mon.backpack.stock_feed.latest["WEN"] = observed(clock).model_copy(update={"session": "OVERNIGHT"})
    else:
        mon.assets[key[0]] = replace(mon.assets[key[0]], mint=USDC_MINT)
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 0
    assert mon.snapshot()["prices"] == []


async def test_current_bid_with_stale_bank_is_waiting_and_does_not_complete_the_pass():
    mon, _ = coverage_monitor()
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["stale_pool_quote"])
    assert await mon.quote(("WEN.US", FORWARD), coverage=True) is None
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["checked"] == coverage["waitingbank"] == coverage["waiting"] == 1
    assert (
        coverage["pricedfresh"] == coverage["first_pass_completed"] == coverage["currentroute_missing"] == 0
    )
    assert not coverage["done_universe"]
    assert coverage["rows"][0]["state"] == "waiting_bank"


async def test_scheduler_first_pass_precedes_manual_grid_and_search_still_runs_after_negative_pass():
    mon, clock = coverage_monitor()
    mon.settings.monitor.concurrency = 2
    for symbol in ("A.US", "B.US"):
        mon.assets[symbol] = asset(symbol, deposit=False)
        mon.chain[symbol] = "verified_unit_mapping"
        mon.backpack.stock_feed.latest[symbol[:-3]] = observed(clock)
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], deposit_enabled=False)
    mon.optimization_requested.add(("WEN.US", FORWARD))
    mon.requested[("WEN.US", FORWARD)] = clock()
    search_started = asyncio.Event()
    calls = []

    async def optimizer(key, **kwargs):
        assert all(state.result for state in mon.forward_coverage.values())
        calls.append((key, "search"))
        search_started.set()

    original_quote = mon.quote

    async def tracked_quote(key, **kwargs):
        calls.append((key, "minimum" if kwargs.get("coverage") else "search"))
        return await original_quote(key, **kwargs)

    mon.quote = tracked_quote
    mon.optimize = optimizer
    task = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(search_started.wait(), 2)
        assert [kind for _, kind in calls[:3]] == ["minimum"] * 3
        assert {key[0] for key, kind in calls[:3]} == set(mon.assets)
        assert len(mon.search_pending) <= 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for pending in mon.pending.values():
            pending.cancel()
        await asyncio.gather(*mon.pending.values(), return_exceptions=True)


async def test_reserved_optimizer_issues_one_larger_probe_before_yielding_to_due_coverage():
    mon, clock = coverage_monitor()
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True)
    clock.now += mon.settings.monitor.quote_refresh_s
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    mon.search_pending.add(key)
    mon.pending[key] = asyncio.current_task()
    mon.jupiter.quote_pair.reset_mock()
    await mon.optimize(key)
    assert mon.jupiter.quote_pair.await_count == 1
    assert mon.jupiter.quote_pair.await_args.args[2] > raw("0.08")
    assert key in mon.optimization_requested
    assert mon.forward_coverage[key[0]].quantity_raw == raw("0.16")


async def test_fresh_minimum_in_new_session_replaces_old_session_larger_pair():
    mon, clock = coverage_monitor()
    key = "WEN.US", FORWARD
    regular = mon.assets[key[0]].sessions[0]
    mon.assets[key[0]] = replace(mon.assets[key[0]], sessions=(regular, {**regular, "name": "POST"}))
    await mon.quote(key, coverage=True)
    larger = await mon.quote(key, notional=Decimal("100"))
    assert larger["acquired_raw"] > raw("0.16")
    clock.now += 1
    mon.sessions[0]["name"] = "POST"
    mon.backpack.stock_feed.latest["WEN"] = observed(clock).model_copy(update={"session": "POST"})
    await mon.quote(key, coverage=True)
    snapshot = mon.snapshot()
    assert snapshot["forward_coverage"]["pricedfresh"] == 1
    assert len(snapshot["prices"]) == 1
    assert snapshot["prices"][0]["buy_quantity"] == "0.16"


async def test_fresh_minimum_replaces_spot_pair_without_verified_current_capacity():
    mon, clock = coverage_monitor()
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True)
    await setup_books(mon, clock)
    larger = await mon.quote(key, notional=Decimal("100"))
    assert larger and larger["backpack_source"] == "spot"
    mon.books.capacities.clear()
    snapshot = mon.snapshot()
    assert snapshot["forward_coverage"]["pricedfresh"] == 1
    assert snapshot["prices"][0]["buy_quantity"] == "0.16"


@pytest.mark.parametrize("age", ["stale", "future"])
async def test_unusable_numeric_dex_response_does_not_earn_first_completion(age):
    mon, clock = coverage_monitor()
    started = clock()

    async def delayed(source, target, amount, **kwargs):
        if age == "stale":
            clock.now += mon.settings.monitor.max_quote_age_s + 1
            mon.backpack.stock_feed.latest["WEN"] = observed(clock)
        observed_at = started if age == "stale" else clock() + 1
        return {"outAmount": str(amount * 6), "priceImpact": "0.1", "_observed_at": observed_at}

    mon.jupiter.quote_pair.side_effect = delayed
    await mon.quote(("WEN.US", FORWARD), coverage=True)
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["pricedfresh"] == coverage["first_pass_completed"] == 0
    assert coverage["waitingbank"] == 1 and not coverage["done_universe"]


@pytest.mark.parametrize("failure", ["session", "quantity", "zero_step"])
def test_metadata_blocks_are_accounted_explicitly_without_claiming_an_api_refusal(failure):
    mon, clock = coverage_monitor()
    if failure == "session":
        mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], sessions=())
    elif failure in {"quantity", "zero_step"}:
        rules = (
            {**mon.assets["WEN.US"].sessions[0], "maxQuantity": "0.1"}
            if failure == "quantity"
            else {**mon.assets["WEN.US"].sessions[0], "stepSize": "0"}
        )
        mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], sessions=(rules,))
    assert mon.choose_forward_coverage() is None
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["unattempted"] == coverage["refused"] == coverage["checked"] == 0
    assert coverage["waiting"] == 1
    expected = {
        "session": "skipped_current_session",
        "quantity": "unsupported_quantity",
        "zero_step": "unsupported_quantity",
    }
    assert coverage["rows"][0]["state"] == expected[failure]
    assert coverage["rows"][0]["reason"]
    assert coverage["first_pass_completed"] == 1
    mon.rfq.inspect.assert_not_awaited()
    mon.jupiter.quote_pair.assert_not_awaited()


async def test_owned_bid_does_not_make_an_expired_direct_bank_a_fresh_pair():
    mon, clock = coverage_monitor()
    mon.settings.monitor.max_quote_age_s = 15
    mon.rfq.max_age = 15
    router = object.__new__(DirectRaydium)
    bank = {"observed": clock(), "response": {"context": {"slot": 1}}}
    router.bank_feed = SimpleNamespace(
        peek=lambda key: bank if clock() - bank["observed"] < mon.settings.policy.quote_max_age_s else None
    )
    router.status = lambda: {}

    async def dex(source, target, amount, **kwargs):
        return {
            "outAmount": str(amount * 6),
            "priceImpact": "0.1",
            "_observed_at": bank["observed"],
            "pool": "pool",
            "inputMint": source,
            "outputMint": target,
            "contextSlot": 1,
            "_direct": {},
        }

    router.quote_pair = AsyncMock(side_effect=dex)
    mon._quote_provider = router
    await mon.quote(("WEN.US", FORWARD), coverage=True)
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    clock.now += mon.settings.policy.quote_max_age_s
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["checked"] == coverage["waitingbank"] == 1
    assert coverage["pricedfresh"] == 0
    assert mon.snapshot()["prices"] == []


async def test_waiting_bank_retries_do_not_starve_renewal_of_completed_assets():
    mon, clock = coverage_monitor()
    mon.assets["A.US"] = asset("A.US")
    mon.chain["A.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["A"] = observed(clock)
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["stale_pool_quote"])
    await mon.quote(("WEN.US", FORWARD), coverage=True)
    clock.now += 1
    mon.backpack.stock_feed.latest["A"] = observed(clock)
    mon.jupiter.quote_pair.side_effect = None
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "480000",
        "priceImpact": "0.1",
        "_observed_at": clock(),
    }
    await mon.quote(("A.US", FORWARD), coverage=True)
    clock.now += mon.settings.monitor.quote_refresh_s
    for ticker in ("WEN", "A"):
        mon.backpack.stock_feed.latest[ticker] = observed(clock)
    assert mon.choose_forward_coverage() == ("WEN.US", FORWARD)
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["stale_pool_quote"])
    await mon.quote(("WEN.US", FORWARD), coverage=True)
    assert mon.choose_forward_coverage() == ("A.US", FORWARD)


async def test_no_public_model_still_gets_exact_minimum_bid_dex_and_displayed_prices():
    mon, clock = coverage_monitor()
    mon.backpack.stock_feed.latest.clear()
    mon.external.clear()
    mon.external_at = clock() - mon.settings.monitor.price_batch_s * 2 - 1
    mon.sessions[0]["name"] = "POST"
    rules = {**mon.assets["WEN.US"].sessions[0], "name": "POST"}
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], sessions=(rules,))
    key = "WEN.US", FORWARD
    assert mon.choose_forward_coverage() == key
    row = await mon.quote(key, coverage=True)
    assert row and row["acquired_raw"] == raw("0.16")
    mon.rfq.inspect.assert_awaited_once_with(key[0], "Bid", "0.16", "POST")
    assert mon.coverage_observation(mon.assets[key[0]]).bid == ""
    snapshot = mon.snapshot()
    assert snapshot["forward_coverage"]["pricedfresh"] == 1
    assert snapshot["forward_coverage"]["done_universe"]
    assert len(snapshot["prices"]) == 1 and snapshot["prices"][0]["backpack_price"] == "6.2"
    assert snapshot["prices"][0]["buy_quantity"] == "0.16"
    text = mon.text(snapshot=snapshot)
    assert "В 1 проверенных объёмах положительного net не найдено." in text
    assert "Минимальная законная партия" in text
    assert "Минимальная законная партия" not in mon.text("WEN.US", snapshot=snapshot)
    larger = await mon.quote(key, notional=Decimal("100"))
    assert larger and larger["acquired_raw"] > raw("0.16")
    assert mon.rfq.inspect.await_args.args[2] != "0.16"
    clock.now = mon.forward_coverage[key[0]].check.retry_at
    await mon.refresh_rfq_side(mon.assets[key[0]], "Bid")
    assert mon.forward_coverage[key[0]].check.usable(clock())
    assert mon.rfq.inspect.await_args.args[2] == "0.16"


@pytest.mark.parametrize("boundary", ["stale", "future", "closed", "holiday", "missing"])
async def test_no_public_model_requires_current_open_published_session(boundary):
    mon, clock = coverage_monitor()
    mon.backpack.stock_feed.latest.clear()
    mon.external.clear()
    if boundary == "stale":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif boundary == "future":
        mon.metadata_at = clock() + 1
    elif boundary == "closed":
        mon.sessions[0].update(startTime="12:00:00", endTime="13:00:00")
    elif boundary == "holiday":
        mon.holidays = [{"market": "US_EQUITIES", "timezone": "UTC", "date": "1970-01-01"}]
    else:
        mon.sessions = []
    assert mon.choose_forward_coverage() is None
    assert await mon.quote(("WEN.US", FORWARD), coverage=True) is None
    mon.rfq.inspect.assert_not_awaited()
    mon.jupiter.quote_pair.assert_not_awaited()
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 0


async def test_stale_negative_diagnostic_does_not_claim_current_nonpositive_net():
    mon, clock = coverage_monitor()
    await mon.quote(("WEN.US", FORWARD), coverage=True)
    clock.now += mon.settings.monitor.max_quote_age_s + 1
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    snapshot = mon.snapshot()
    forward = next(row for row in snapshot["diagnostics"] if row["direction"] == FORWARD)
    assert "nonpositive_net" not in forward["reasons"]


async def test_changed_minimum_banks_drain_without_stock_updates_while_cold_discovery_is_blocked():
    mon, clock = coverage_monitor()
    mon.settings.monitor.concurrency = 16
    mon.backpack.stock_feed.latest.clear()
    mon.external.clear()
    router = object.__new__(DirectRaydium)
    banks = {}
    changed = asyncio.Event()
    router.bank_feed = SimpleNamespace(peek=lambda key: banks.get(key), changed=changed)
    router.status = lambda: {}
    mon._quote_provider = router
    for index in range(71):
        symbol = f"S{index:02d}.US"
        mon.assets[symbol] = asset(symbol, deposit=False)
        mon.chain[symbol] = "verified_unit_mapping"
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], deposit_enabled=False)
    for symbol in mon.assets:
        pool = "pool-" + symbol
        banks[(pool, STOCK_MINT, USDC_MINT, 10)] = {
            "observed": clock(),
            "response": {"context": {"slot": 1}},
        }
        quote = {
            "outAmount": "480000",
            "priceImpact": "0.1",
            "pool": pool,
            "inputMint": STOCK_MINT,
            "outputMint": USDC_MINT,
            "contextSlot": 1,
            "_observed_at": clock(),
            "_direct": {},
        }
        await mon.quote((symbol, FORWARD), coverage=True, quote_pair=AsyncMock(return_value=quote))
        mon.forward_coverage[symbol].discovery_at = clock()
    repriced = asyncio.Event()
    calls = []

    async def cached(source, target, amount, *, plan, **kwargs):
        bank = banks[(plan["pool"], source, target, 10)]
        calls.append(plan["pool"])
        if len(calls) % 72 == 0:
            repriced.set()
        return {
            **plan,
            "outAmount": str(amount * 6),
            "contextSlot": bank["response"]["context"]["slot"],
            "_observed_at": bank["observed"],
        }

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    mon.assets["COLD.US"] = asset("COLD.US", deposit=False)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    gate = asyncio.Event()
    cold_started = asyncio.Event()

    async def cold_dex(*args, **kwargs):
        cold_started.set()
        await gate.wait()
        raise QuoteUnavailable(["stale_pool_quote"])

    router.quote_pair = cold_dex
    mon.choose = lambda: None
    clock.now += 0.1
    for bank in banks.values():
        bank["observed"] = clock()
        bank["response"]["context"]["slot"] = 2
    task = asyncio.create_task(mon.schedule_quotes())
    try:
        # Nine local batches need two seconds with the old 0.25-second polling
        # delay; leave room for CPU variation while still detecting that delay.
        await asyncio.wait_for(repriced.wait(), 1.5)
        await asyncio.wait_for(cold_started.wait(), 1.5)
        await asyncio.sleep(0)
        assert not gate.is_set() and ("COLD.US", FORWARD) in mon.pending
        assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 72
        repriced.clear()
        clock.now += 1.1
        for bank in banks.values():
            bank["observed"] = clock()
            bank["response"]["context"]["slot"] = 3
        changed.set()
        await asyncio.wait_for(repriced.wait(), 1.5)
        await asyncio.sleep(0)
        assert len(calls) == 144 and mon.backpack.stock_feed.version == 0
        assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 72
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for pending in mon.pending.values():
            pending.cancel()
        await asyncio.gather(*mon.pending.values(), return_exceptions=True)


async def test_continuous_local_repricing_cannot_postpone_periodic_route_discovery():
    mon, clock = coverage_monitor()
    mon.rfq.max_age = 15
    mon.settings.monitor.exploration_s = 10
    mon.backpack.stock_feed.latest.clear()
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True)
    state = mon.forward_coverage[key[0]]
    initial_attempt = state.attempted_at
    state.discovery_at = clock()
    for _ in range(6):
        clock.now += 2
        assert await mon.quote(key, coverage=True, local_plan=state.plan) is not None
    assert state.attempted_at == initial_attempt
    assert clock() - state.discovery_at >= mon.settings.monitor.exploration_s
    assert mon.choose_forward_coverage() == key


async def direct_coverage_monitor():
    mon, clock = coverage_monitor()
    mon.settings.monitor.max_quote_age_s = 15
    mon.rfq.max_age = 15
    mon.backpack.stock_feed.latest.clear()
    router = object.__new__(DirectRaydium)
    banks = {("old", STOCK_MINT, USDC_MINT, 10): {"observed": clock(), "response": {"context": {"slot": 1}}}}
    router.bank_feed = SimpleNamespace(peek=lambda key: banks.get(key), changed=asyncio.Event())
    router.status = lambda: {}
    mon._quote_provider = router
    route = {
        "outAmount": "480000",
        "priceImpact": "0.1",
        "pool": "old",
        "inputMint": STOCK_MINT,
        "outputMint": USDC_MINT,
        "contextSlot": 1,
        "_observed_at": clock(),
        "_direct": {},
    }
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True, quote_pair=AsyncMock(return_value=route))

    async def cached(source, target, amount, *, plan, **kwargs):
        bank = banks[(plan["pool"], source, target, 10)]
        return {
            **plan,
            "outAmount": str(amount * 6),
            "contextSlot": bank["response"]["context"]["slot"],
            "_observed_at": bank["observed"],
        }

    router.cached_plan_quote = AsyncMock(side_effect=cached)
    mon.forward_coverage[key[0]].discovery_at = clock() - mon.settings.monitor.exploration_s
    return mon, clock, router, banks, key


async def test_background_discovery_does_not_hold_baseline_and_adopts_only_route_topology():
    mon, clock, router, banks, key = await direct_coverage_monitor()
    mon.settings.monitor.concurrency = 1
    mon.choose = lambda: None
    state = mon.forward_coverage[key[0]]
    release = asyncio.Event()
    started = asyncio.Event()

    async def full(source, target, amount, **kwargs):
        assert amount == raw("0.08")
        assert kwargs["_rpc"] is router.bank_feed
        started.set()
        await release.wait()
        return {**state.plan["dex_plan"], "pool": "new", "_observed_at": clock()}

    router.quote_pair = AsyncMock(side_effect=full)
    mon.discovery_ready.set()
    discovery = asyncio.create_task(mon.coverage_routes_loop())
    pricing = asyncio.create_task(mon.schedule_quotes())
    try:
        await asyncio.wait_for(started.wait(), 1)
        assert key not in mon.pending and len(mon.coverage_route_pending) == 1
        for slot in (2, 3):
            clock.now += 1.1
            banks[("old", STOCK_MINT, USDC_MINT, 10)] = {
                "observed": clock(),
                "response": {"context": {"slot": slot}},
            }
            router.bank_feed.changed.set()
            async with asyncio.timeout(1):
                while state.result["dex_bank_versions"][0][3] != slot:
                    await asyncio.sleep(0)
            assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
        result, check = state.result, state.check
        prior_check_calls = mon.rfq.inspect.await_count
        pricing.cancel()
        await asyncio.gather(pricing, return_exceptions=True)
        for task in mon.pending.values():
            task.cancel()
        await asyncio.gather(*mon.pending.values(), return_exceptions=True)
        mon.pending.clear()
        banks[("new", STOCK_MINT, USDC_MINT, 10)] = {
            "observed": clock(),
            "response": {"context": {"slot": 1}},
        }
        release.set()
        async with asyncio.timeout(1):
            while state.route_candidate is None:
                await asyncio.sleep(0)
        mon.adopt_coverage_routes()
        assert state.result is result and state.check is check
        assert mon.rfq.inspect.await_count == prior_check_calls
        assert key not in mon.local_inputs

        async def reprice(source, target, amount, **kwargs):
            return await router.cached_plan_quote(
                source, target, amount, plan=state.plan["dex_plan"], **kwargs
            )

        await mon.quote(key, coverage=True, local_plan=state.plan, quote_pair=reprice)
        assert state.result["dex_bank_versions"][0][0] == "new"
    finally:
        pricing.cancel()
        discovery.cancel()
        await asyncio.gather(pricing, discovery, return_exceptions=True)


@pytest.mark.parametrize("boundary", ["mint", "session", "quantity"])
async def test_background_discovery_rechecks_current_owner_before_handoff(boundary):
    mon, clock, router, _, key = await direct_coverage_monitor()
    state = mon.forward_coverage[key[0]]
    original = state.plan["dex_plan"]
    gate = asyncio.Event()
    started = asyncio.Event()

    async def full(*args, **kwargs):
        started.set()
        await gate.wait()
        return {**original, "pool": "new", "_observed_at": clock()}

    router.quote_pair = AsyncMock(side_effect=full)
    task = asyncio.create_task(mon.discover_coverage_route(state))
    await started.wait()
    if boundary == "mint":
        mon.assets[key[0]] = replace(mon.assets[key[0]], mint=USDC_MINT)
    elif boundary == "session":
        mon.sessions[0]["name"] = "CLOSED"
    else:
        mon.settings.policy.withdrawal_semantics = "additional"
    gate.set()
    await task
    assert state.plan["dex_plan"] is original


async def test_unknown_early_bid_renewal_keeps_original_price_only_until_original_expiry():
    mon, clock = coverage_monitor()
    mon.settings.monitor.max_quote_age_s = 15
    mon.rfq.max_age = 15
    key = "WEN.US", FORWARD
    await mon.quote(key, coverage=True)
    state = mon.forward_coverage[key[0]]
    original = state.check
    clock.now = mon.rfq_deadline(mon.assets[key[0]], "Bid")
    mon.backpack.stock_feed.latest.clear()
    unknown = None

    async def fail(symbol, side, quantity, session):
        nonlocal unknown
        unknown = mon.rfq.result(symbol, side, quantity, session, "unknown", "rfq_check_timeout")
        return unknown

    mon.rfq.inspect.side_effect = fail
    await mon.refresh_rfq_side(mon.assets[key[0]], "Bid")
    assert state.check is original and original.retry_at == 10015
    assert state.renewal_reason == "rfq_check_timeout"
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    assert mon.rfq_deadline(mon.assets[key[0]], "Bid") == unknown.eligibility_until
    clock.now = original.retry_at
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 0


async def test_larger_probe_uses_its_own_bid_and_exact_cached_input_with_one_coverage_fallback():
    from backpack_stocks.clmm_worker import ClmmCalculationError

    mon, _, router, _, key = await direct_coverage_monitor()
    state = mon.forward_coverage[key[0]]

    async def larger(symbol, side, quantity, session):
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", "9")

    mon.rfq.inspect.side_effect = larger
    fallback = AsyncMock(
        return_value={"outAmount": "60000000", "priceImpact": "0.1", "_observed_at": mon.clock()}
    )

    async def probe(source, target, amount, **kwargs):
        return await mon.cached_forward_search_quote(
            source, target, amount, plan=state.plan["dex_plan"], fallback=fallback, **kwargs
        )

    row = await mon.quote(key, notional=Decimal("100"), quote_pair=probe)
    assert row and row["backpack_price"] == "9" and row["acquired_raw"] > raw("0.16")
    assert router.cached_plan_quote.await_args.args[2] == row["delivered_raw"]
    fallback.assert_not_awaited()
    router.cached_plan_quote.side_effect = ClmmCalculationError("insufficient_tick_coverage")
    row = await mon.quote(key, notional=Decimal("200"), quote_pair=probe)
    assert row and row["backpack_price"] == "9"
    fallback.assert_awaited_once()
    assert fallback.await_args.args[2] == row["delivered_raw"]
