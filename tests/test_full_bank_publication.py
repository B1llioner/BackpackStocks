from unittest.mock import AsyncMock

import pytest

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.pool_bank_feed import PoolBankFeed


@pytest.mark.parametrize("full_fill", [True, False])
async def test_full_depth_fallback_publishes_the_new_head_before_amount_math(monkeypatch, full_fill):
    wall = [100.0]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8)
    router = object.__new__(DirectRaydium)
    router.settings = Settings()
    router.bank_feed = feed
    key = "pool", STOCK_MINT, USDC_MINT, 10
    fast = {"observed": 100.0, "array_limit": 2, "response": {"context": {"slot": 1}}}
    full = {"observed": 100.0, "array_limit": router.settings.direct.max_tick_arrays, "response": {"context": {"slot": 2}}}
    router._pool_bank = AsyncMock(side_effect=[fast, full])

    async def math(pool, source, target, amount, bank):
        if bank is fast:
            raise ClmmCalculationError("insufficient_tick_coverage")
        assert bank is full and feed.peek(key) is full
        if not full_fill:
            raise ClmmCalculationError("insufficient_liquidity")
        return {"pool": pool, "inputMint": source, "outputMint": target, "contextSlot": 2, "_observed_at": 100.0}

    router.quote_bank = AsyncMock(side_effect=math)
    try:
        if full_fill:
            quote = await router.pool_quote(*key[:3], 1_000_000, priority=10, _rpc=feed)
            assert router.bank_versions_are_current(router.bank_versions(quote))
        else:
            with pytest.raises(ClmmCalculationError) as error:
                await router.pool_quote(*key[:3], 1_000_000, priority=10, _rpc=feed)
            assert error.value.code == "insufficient_liquidity"
        assert feed.peek(key) is full  # The completed fast task cannot roll back this head.
        assert router._pool_bank.await_count == router.quote_bank.await_count == 2
        assert router._pool_bank.await_args.kwargs["_max_arrays"] == router.settings.direct.max_tick_arrays
        wall[0] = 108.0
        assert feed.peek(key) is None
    finally:
        await feed.close()
