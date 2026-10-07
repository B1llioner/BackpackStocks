from dataclasses import replace
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest
from test_forward_grid import public_direct_monitor as direct_public_fixture
from test_indicative_coverage import published

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.pool_state import CPMM
from backpack_stocks.raydium import CLMM


@pytest.fixture
async def minimum_owner():
    async for mon, clock, public, router, banks, key in direct_public_fixture.__wrapped__():
        router.bank_feed.set_primary = Mock()
        router.bank_feed.set_minimum_arrays = Mock()
        router.quote_pair = AsyncMock(wraps=router.quote_pair)
        yield mon, clock, public, router, banks, key


def tick_leaf(leaf, used, **changes):
    return {**leaf, **changes, "_direct": {"program": CLMM, "arrays": [f"tick{i}" for i in range(used)]}}


async def test_minimum_array_owner_ignores_larger_grid_and_global_hint_pollution(minimum_owner):
    mon, _, _, router, _, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    leaf = state.plan["dex_plan"]
    state.plan["dex_plan"] = tick_leaf(leaf, 1)
    router.array_hints = {(leaf["pool"], STOCK_MINT): 8}
    mon.local_plans[key] = {
        **state.plan,
        "acquired_raw": state.quantity_raw * 10,
        "dex_plan": tick_leaf(leaf, 8),
    }
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_once_with(
        {("old", STOCK_MINT, USDC_MINT, 10): 2}, resolved={("old", STOCK_MINT, USDC_MINT, 10)}
    )
    assert router.array_hints[(leaf["pool"], STOCK_MINT)] == 8


async def test_nested_route_and_shared_owners_use_maximum_actual_consumed_array_count(minimum_owner):
    mon, clock, public, router, _, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    leaf = state.plan["dex_plan"]
    bridge = tick_leaf(leaf, 2, pool="bridge", inputMint="SOL")
    repeated = tick_leaf(leaf, 4, pool="bridge", inputMint="SOL")
    state.plan["dex_plan"] = {
        **leaf,
        "_direct_route": [tick_leaf(leaf, 1), {**leaf, "_direct_route": [bridge, repeated]}],
    }
    second = replace(state.asset, symbol="SECOND.US")
    mon.assets[second.symbol] = second
    mon.chain[second.symbol] = "verified_unit_mapping"
    public.tickers.add(second.ticker)
    await public.ingest(published(clock, second.ticker))
    second_state = mon.coverage_state(second)
    second_state.plan = {
        **state.plan,
        "asset": second.symbol,
        "dex_plan": tick_leaf(leaf, 5, pool="bridge", inputMint="SOL"),
    }
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_once_with(
        {("old", STOCK_MINT, USDC_MINT, 10): 2, ("bridge", "SOL", USDC_MINT, 10): 6},
        resolved={("old", STOCK_MINT, USDC_MINT, 10)},
    )
    assert router.bank_feed.set_primary.call_args.args[0][("bridge", "SOL", USDC_MINT, 10)] == 2


async def test_owned_array_requirement_is_clamped_to_the_configured_full_bound(minimum_owner):
    mon, _, _, router, _, key = minimum_owner
    mon.settings.direct.max_tick_arrays = 4
    state = mon.forward_coverage[key[0]]
    state.plan["dex_plan"] = tick_leaf(state.plan["dex_plan"], 8)
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_once_with(
        {("old", STOCK_MINT, USDC_MINT, 10): 4}, resolved={("old", STOCK_MINT, USDC_MINT, 10)}
    )


async def test_unavailable_owner_removes_array_hint_without_restamping_or_rpc(minimum_owner):
    mon, clock, public, router, _, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    state.plan["dex_plan"] = tick_leaf(state.plan["dex_plan"], 2)
    mon.sync_primary_banks()
    assert router.bank_feed.set_minimum_arrays.call_args.args[0]
    dex_observed = state.result["dex_observed_at"]
    clock.now += 1
    await public.ingest(published(clock, reason="stalePrice", ask=None))
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_with({}, resolved=set())
    assert state.result["dex_observed_at"] == dex_observed
    router.quote_pair.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()


async def test_cpmm_and_mock_routes_without_consumed_clmm_arrays_keep_legacy_primary_ownership(minimum_owner):
    mon, _, _, router, _, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    leaf = state.plan["dex_plan"]
    state.plan["dex_plan"] = {**leaf, "_direct": {"program": CPMM, "arrays": []}}
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with({("old", STOCK_MINT, USDC_MINT, 10): 1})
    router.bank_feed.set_minimum_arrays.assert_called_with({}, resolved={("old", STOCK_MINT, USDC_MINT, 10)})
    del router.bank_feed.set_minimum_arrays
    state.plan["dex_plan"] = leaf
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with({("old", STOCK_MINT, USDC_MINT, 10): 1})


async def test_shared_expansion_resolves_only_when_every_distinct_active_owner_matches_current_bank(
    minimum_owner,
):
    mon, clock, public, router, banks, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    leaf = state.plan["dex_plan"]
    state.plan["dex_plan"] = tick_leaf(leaf, 1)
    second = replace(state.asset, symbol="SECOND.US")
    mon.assets[second.symbol] = second
    mon.chain[second.symbol] = "verified_unit_mapping"
    public.tickers.add(second.ticker)
    await public.ingest(published(clock, second.ticker))
    second_state = mon.coverage_state(second)
    second_state.plan = {**state.plan, "asset": second.symbol, "dex_plan": tick_leaf(leaf, 5, contextSlot=0)}
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_with(
        {("old", STOCK_MINT, USDC_MINT, 10): 6}, resolved=set()
    )
    second_state.plan["dex_plan"]["contextSlot"] = 1
    mon.sync_primary_banks()
    router.bank_feed.set_minimum_arrays.assert_called_with(
        {("old", STOCK_MINT, USDC_MINT, 10): 6}, resolved={("old", STOCK_MINT, USDC_MINT, 10)}
    )
    # A matching slot alone cannot resolve an expired acquisition or old head.
    banks[("old", STOCK_MINT, USDC_MINT, 10)]["observed"] = clock() + 0.1
    mon.sync_primary_banks()
    assert router.bank_feed.set_minimum_arrays.call_args.kwargs["resolved"] == set()


@pytest.mark.parametrize("coverage", [True, False])
async def test_only_owned_minimum_tick_failure_enqueues_background_expansion_without_inline_rpc(
    minimum_owner, coverage
):
    mon, clock, _, router, banks, key = minimum_owner
    state = mon.forward_coverage[key[0]]
    bank_key = "old", STOCK_MINT, USDC_MINT, 10
    banks[bank_key] = {"observed": clock(), "response": {"context": {"slot": 2}}}
    error = ClmmCalculationError("insufficient_tick_coverage")
    error.bank_key = bank_key
    router.request_minimum_expansion = Mock()
    router.cached_plan_quote = AsyncMock(side_effect=error)
    assert (
        await mon.quote(
            key,
            coverage=coverage,
            local_plan=state.plan,
            quote_pair=partial(router.cached_plan_quote, plan=state.plan["dex_plan"]),
        )
        is None
    )
    if coverage:
        router.request_minimum_expansion.assert_called_once_with(bank_key)
    else:
        router.request_minimum_expansion.assert_not_called()
    router.quote_pair.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()
