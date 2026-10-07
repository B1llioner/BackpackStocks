import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_indicative_coverage import indicative_monitor as indicative_monitor_fixture
from test_minimum_scheduler_lane import prepared_minimums

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.direct import CPMM, DirectRaydium, LocalQuoteNotReady
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.raydium import CLMM

indicative_monitor = indicative_monitor_fixture


async def test_local_insufficient_ticks_identify_the_exact_bank_without_rpc():
    key = "pool", STOCK_MINT, USDC_MINT, 10
    bank = {"program": CLMM, "array_limit": 2}
    feed = SimpleNamespace(peek=Mock(return_value=bank))
    router = object.__new__(DirectRaydium)
    router.bank_feed = feed
    router.quote_bank = AsyncMock(side_effect=ClmmCalculationError("insufficient_tick_coverage"))
    router.quote_pair = AsyncMock(side_effect=AssertionError("local quote must not fall back to network"))
    plan = {"pool": key[0], "inputMint": key[1], "outputMint": key[2]}
    with pytest.raises(LocalQuoteNotReady) as error:
        await router.cached_plan_quote(STOCK_MINT, USDC_MINT, 123, plan=plan)
    assert error.value.code == "insufficient_tick_coverage"
    assert error.value.bank_key == key
    router.quote_bank.assert_awaited_once_with(*key[:3], 123, bank)
    router.quote_pair.assert_not_awaited()


@pytest.mark.parametrize("program,limit,expected", [(CLMM, 2, True), (CLMM, 8, False), (CPMM, 2, False)])
def test_expansion_request_is_only_synchronous_queueing_for_a_bounded_clmm_bank(program, limit, expected):
    router = object.__new__(DirectRaydium)
    router.settings = Settings()
    key = "pool", STOCK_MINT, USDC_MINT, 10
    request = Mock(return_value=True)
    router.bank_feed = SimpleNamespace(
        ready={key: {"program": program, "array_limit": limit}}, request_minimum_expansion=request
    )
    assert router.request_minimum_expansion(key) is expected
    if expected:
        request.assert_called_once_with(key, 8)
        assert not asyncio.iscoroutinefunction(request)
    else:
        request.assert_not_called()


async def test_owned_local_minimum_queues_bank_growth_without_using_a_network_lane(indicative_monitor):
    mon, clock, feed = indicative_monitor
    router, banks, symbols = await prepared_minimums(mon, clock, feed, 1)
    router.settings = mon.settings
    state = mon.forward_coverage[symbols[0]]
    key = state.plan["dex_plan"]["pool"], STOCK_MINT, USDC_MINT, 10
    banks[key].update(program=CLMM, array_limit=2)
    router.bank_feed.ready = banks
    router.bank_feed.request_minimum_expansion = Mock(return_value=True)
    router.cached_plan_quote = AsyncMock(
        side_effect=LocalQuoteNotReady("insufficient_tick_coverage", bank_key=key)
    )
    old_result = state.result
    result = await mon.quote(
        (symbols[0], "backpack_to_jupiter"),
        coverage=True,
        local_plan=state.plan,
        quote_pair=router.cached_plan_quote,
    )
    assert result is None and state.result is old_result
    router.bank_feed.request_minimum_expansion.assert_called_once_with(
        key, mon.settings.direct.max_tick_arrays
    )
    router.quote_pair.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()
