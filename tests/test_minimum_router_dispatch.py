from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.observability import QuoteUnavailable
from backpack_stocks.pool_bank_feed import PoolBankFeed
from backpack_stocks.quote_batch import QuoteAccountBatch


@pytest.mark.parametrize("minimum,continuous", [(True, True), (False, True), (True, False)])
async def test_first_route_dispatch_requires_explicit_minimum_and_continuous_feed(
    monkeypatch, minimum, continuous
):
    router = object.__new__(DirectRaydium)
    router.settings = SimpleNamespace(direct=SimpleNamespace(via_sol=True))
    router.assets = {STOCK_MINT: {}}
    router.candidates = {}
    router.sol_candidates = {}
    router.ensure_candidates = AsyncMock()
    quote = {"routePlan": []}
    first, full = AsyncMock(return_value=quote), AsyncMock(return_value=quote)
    monkeypatch.setattr("backpack_stocks.direct_routes.minimum_quote", first)
    monkeypatch.setattr("backpack_stocks.direct_routes.composed_quote", full)
    batch = PoolBankFeed(None, 8) if continuous else QuoteAccountBatch(None)
    try:
        assert (
            await router.quote_pair(STOCK_MINT, USDC_MINT, 123_456, priority=10, _rpc=batch, minimum=minimum)
            is quote
        )
        router.ensure_candidates.assert_awaited_once_with(STOCK_MINT, priority=10)
        if minimum and continuous:
            first.assert_awaited_once_with(router, STOCK_MINT, USDC_MINT, 123_456, 10, batch)
            full.assert_not_awaited()
        else:
            first.assert_not_awaited()
            full.assert_awaited_once()
            assert full.await_args.args[1:5] == (STOCK_MINT, USDC_MINT, 123_456, 10)
            assert full.await_args.args[5].rpc is batch
    finally:
        await batch.close()


async def test_known_minimum_pool_does_not_wait_for_discovery_of_missing_alternatives(monkeypatch):
    router = object.__new__(DirectRaydium)
    router.settings = SimpleNamespace(direct=SimpleNamespace(via_sol=True))
    router.assets = {STOCK_MINT: {}}
    router.candidates = {STOCK_MINT: ["known-pool"]}
    router.sol_candidates = {}
    router.ensure_candidates = AsyncMock(
        side_effect=AssertionError("known first route cannot wait for alternatives")
    )
    quote = {"routePlan": []}
    first = AsyncMock(return_value=quote)
    monkeypatch.setattr("backpack_stocks.direct_routes.minimum_quote", first)
    batch = PoolBankFeed(None, 8)
    try:
        assert (
            await router.quote_pair(STOCK_MINT, USDC_MINT, 123_456, priority=10, _rpc=batch, minimum=True)
            is quote
        )
        router.ensure_candidates.assert_not_awaited()
        first.assert_awaited_once_with(router, STOCK_MINT, USDC_MINT, 123_456, 10, batch)
    finally:
        await batch.close()


@pytest.mark.parametrize(
    "codes, changed, refreshed",
    [
        (["no_tick_liquidity", "insufficient_liquidity"], True, True),
        (["no_tick_liquidity"], False, True),
        (["stale_pool_quote"], True, False),
        (["insufficient_tick_coverage"], True, False),
        ([], True, False),
    ],
)
async def test_minimum_refreshes_dead_pool_selection_only_for_verified_liquidity_failure(
    monkeypatch, codes, changed, refreshed
):
    router = object.__new__(DirectRaydium)
    router.settings = SimpleNamespace(direct=SimpleNamespace(via_sol=True))
    router.assets = {STOCK_MINT: {}}
    router.candidates = {STOCK_MINT: ["old-empty"]}
    router.sol_candidates = {}

    async def refresh(*args, **kwargs):
        if changed:
            router.candidates[STOCK_MINT] = ["current-pool"]

    router.ensure_candidates = AsyncMock(side_effect=refresh)
    failure = QuoteUnavailable(codes)
    quote = {"routePlan": []}
    first = AsyncMock(side_effect=[failure, quote])
    monkeypatch.setattr("backpack_stocks.direct_routes.minimum_quote", first)
    batch = PoolBankFeed(None, 8)
    try:
        if refreshed and changed:
            assert (
                await router.quote_pair(STOCK_MINT, USDC_MINT, 123_456, priority=10, _rpc=batch, minimum=True)
                is quote
            )
            assert first.await_count == 2
        else:
            with pytest.raises(QuoteUnavailable) as error:
                await router.quote_pair(STOCK_MINT, USDC_MINT, 123_456, priority=10, _rpc=batch, minimum=True)
            assert error.value is failure
            assert first.await_count == 1
        if refreshed:
            router.ensure_candidates.assert_awaited_once_with(STOCK_MINT, priority=10, refresh=True)
        else:
            router.ensure_candidates.assert_not_awaited()
    finally:
        await batch.close()
