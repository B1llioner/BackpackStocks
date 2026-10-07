import asyncio
import copy
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_indicative_coverage import published
from test_minimum_scheduler_lane import prepared_minimums

from backpack_stocks.models import USDC_MINT, raw, ui
from backpack_stocks.monitor import FORWARD

indicative_monitor = indicative_monitor_fixture


@pytest.fixture
async def proven_minimum(indicative_monitor):
    mon, clock, feed = indicative_monitor
    router, banks, symbols = await prepared_minimums(mon, clock, feed, 1)
    state = mon.forward_coverage[symbols[0]]
    for bank in banks.values():
        bank["observed"] = state.result["dex_observed_at"]
        bank["response"]["context"]["slot"] = 1
    router.cached_plan_quote = AsyncMock(side_effect=AssertionError("public cost revalue cannot call SDK"))
    yield mon, clock, feed, router, banks, state


async def test_changed_public_limit_recosts_exact_minimum_and_flips_profit_without_sdk_or_rfq(proven_minimum):
    mon, clock, feed, router, _, state = proven_minimum
    original = copy.deepcopy(state.result)
    route = state.plan["dex_plan"]
    assert original["profit_raw"] < 0
    clock.now += 1
    await feed.ingest(published(clock, ask="1", bid="0.9"))
    assert mon.sync_indicative_coverage(state)
    row = state.result
    assert row["cost_raw"] == raw("0.16")
    assert row["profit_raw"] == row["proceeds_raw"] - raw("0.16") - row["gas_raw"] > 0
    assert Decimal(row["edge_bps"]) == Decimal(row["profit_raw"]) * 10000 / row["cost_raw"]
    assert row["passes_model_limits"] and row["size_feasible"]
    assert row["search_budget_usdc"] == str(ui(row["cost_raw"]))
    assert state.plan["cost_raw"] == row["cost_raw"] and state.plan["profit_raw"] == row["profit_raw"]
    assert state.plan["dex_plan"] is route
    for key in (
        "dex_observed_at",
        "dex_bank_versions",
        "quote_started_at",
        "observed_at",
        "gas_raw",
        "quoted_output_raw",
        "minimum_output_raw",
        "proceeds_raw",
        "withdrawal_fee_raw",
        "dex_fee_bps",
    ):
        assert row[key] == original[key]
    assert row["backpack_timestamp"] == feed.latest["WEN"].published_at
    assert row["backpack_valid_until"] == state.check.retry_at
    assert not row["backpack_quote_confirmed"]
    key = state.asset.symbol, FORWARD
    assert mon.local_inputs[key] == mon.local_input_version(key, state.plan)
    assert mon.choose_forward_coverage() is None
    assert mon.coverage_snapshot()["pricedfresh"] == 1
    assert mon.snapshot()["signal_candidates"] == 0
    router.cached_plan_quote.assert_not_awaited()
    router.quote_pair.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


async def test_unchanged_public_limit_uses_actual_new_public_stamp_and_preserves_dex_acquisition(
    proven_minimum,
):
    mon, clock, feed, router, _, state = proven_minimum
    original = copy.deepcopy(state.result)
    clock.now += 1
    await feed.ingest(published(clock))
    assert mon.sync_indicative_coverage(state)
    assert (
        state.result["backpack_timestamp"] == feed.latest["WEN"].published_at > original["backpack_timestamp"]
    )
    for key in ("cost_raw", "profit_raw", "dex_observed_at", "observed_at", "dex_bank_versions"):
        assert state.result[key] == original[key]
    assert mon.choose_forward_coverage() is None
    router.cached_plan_quote.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()


@pytest.mark.parametrize(
    "invalid",
    [
        "expired_dex",
        "future_dex",
        "new_head",
        "mint",
        "session",
        "quantity",
        "row_quantity",
        "plan_quantity",
        "metadata",
        "private_source",
        "future_public",
    ],
)
async def test_invalid_or_different_owned_inputs_cannot_recost_an_old_minimum(proven_minimum, invalid):
    mon, clock, feed, router, banks, state = proven_minimum
    clock.now += 1
    changes = {"ask": "1", "bid": "0.9"}
    if invalid == "expired_dex":
        clock.now = state.result["dex_observed_at"] + mon.settings.policy.quote_max_age_s
    elif invalid == "future_dex":
        state.result["dex_observed_at"] = clock() + 1
    elif invalid == "new_head":
        for bank in banks.values():
            bank["observed"] = clock()
            bank["response"]["context"]["slot"] = 2
    elif invalid == "mint":
        mon.assets[state.asset.symbol] = replace(state.asset, mint=USDC_MINT)
    elif invalid == "session":
        changes["session"] = "US_EQUITIES_OVERNIGHT"
    elif invalid == "quantity":
        state.quantity_raw += 10
    elif invalid == "row_quantity":
        state.result["acquired_raw"] += 10
    elif invalid == "plan_quantity":
        state.plan["acquired_raw"] += 10
    elif invalid == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif invalid == "private_source":
        state.result["backpack_source"] = "rfq"
        state.result["backpack_quote_confirmed"] = True
    elif invalid == "future_public":
        changes["timestamp"] = int((clock() + 1) * 1000)
    original = copy.deepcopy(state.result)
    marker = mon.local_inputs.get((state.asset.symbol, FORWARD))
    await feed.ingest(published(clock, **changes))
    assert not mon.sync_indicative_coverage(state)
    assert state.result == original
    assert mon.local_inputs.get((state.asset.symbol, FORWARD)) == marker
    router.cached_plan_quote.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()


async def test_quote_uses_latest_real_public_price_that_arrived_during_dex_await(proven_minimum):
    mon, clock, feed, router, banks, state = proven_minimum
    key = state.asset.symbol, FORWARD
    started, release = asyncio.Event(), asyncio.Event()

    async def blocked_sdk(source, target, amount, *, plan, **kwargs):
        started.set()
        await release.wait()
        return {**plan, "_observed_at": state.result["dex_observed_at"]}

    router.cached_plan_quote = AsyncMock(side_effect=blocked_sdk)
    task = asyncio.create_task(
        mon.quote(
            key,
            coverage=True,
            local_plan=state.plan,
            quote_pair=lambda *args, **kwargs: router.cached_plan_quote(
                *args, plan=state.plan["dex_plan"], **kwargs
            ),
        )
    )
    try:
        await asyncio.wait_for(started.wait(), 1)
        clock.now += 1
        await feed.ingest(published(clock, ask="1", bid="0.9"))
        release.set()
        result = await task
        assert result and result["backpack_price"] == "1" and result["cost_raw"] == raw("0.16")
        assert result["profit_raw"] > 0 and state.result["backpack_price"] == "1"
        assert result["backpack_timestamp"] == feed.latest["WEN"].published_at
        assert mon.local_inputs[key][1] == (feed.latest["WEN"].published_at, "1")
        assert mon.coverage_result_fresh(state)
        mon.rfq.check.assert_not_awaited()
        mon.rfq.inspect.assert_not_awaited()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
