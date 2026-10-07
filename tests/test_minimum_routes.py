import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_direct_routes import quote

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.direct_routes import WSOL, composed_quote, minimum_quote
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.observability import QuoteUnavailable
from backpack_stocks.quote_batch import QuoteAccountBatch


def router(direct=(), sol=(), bridges=()):
    return SimpleNamespace(
        settings=Settings(direct={"via_sol": True}),
        candidates={STOCK_MINT: list(direct)},
        sol_candidates={STOCK_MINT: list(sol)},
        bridge_pools=AsyncMock(return_value=list(bridges)),
        failures=0,
        quote_count=0,
    )


def tracker():
    reader = QuoteAccountBatch(None)
    reader.promote = Mock()
    return reader


async def test_first_current_direct_route_returns_before_slow_alternative_and_loser_drains():
    owner = tracker()
    provider = router(["fast", "slow"])
    slow_started, release = asyncio.Event(), asyncio.Event()

    async def calculate(pool, source, target, amount, **kwargs):
        if pool == "slow":
            slow_started.set()
            await release.wait()
            raise ClmmCalculationError("insufficient_liquidity")
        await asyncio.sleep(0)
        return quote(pool, source, target, amount, amount * 6)

    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        result = await asyncio.wait_for(minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner), 1)
        assert slow_started.is_set() and not release.is_set()
        assert result["pool"] == "fast" and result["inAmount"] == "1000"
        assert len(owner.tasks) == 1 and not next(iter(owner.tasks)).cancelled()
        owner.promote.assert_called_once_with(result, 10)
        release.set()
        await asyncio.gather(*owner.tasks, return_exceptions=True)
        await asyncio.sleep(0)
        assert not owner.tasks and provider.failures == 1
    finally:
        release.set()
        await owner.close()


async def test_first_complete_sol_route_uses_exact_credit_and_promotes_only_winner():
    owner = tracker()
    provider = router(["slow-direct"], ["stock-sol"], ["fast-bridge", "slow-bridge"])
    release = asyncio.Event()
    calls = []

    async def calculate(pool, source, target, amount, **kwargs):
        calls.append((pool, source, target, amount))
        if pool in {"slow-direct", "slow-bridge"}:
            await release.wait()
        output = amount * 1000 if target == WSOL else amount // 1000 * 6
        return quote(pool, source, target, amount, output)

    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        result = await asyncio.wait_for(minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner), 1)
        assert result["inAmount"] == "1000" and result["outAmount"] == "6000"
        assert result["_direct_route"][0]["outAmount"] == result["_direct_route"][1]["inAmount"] == "1000000"
        assert ("fast-bridge", WSOL, USDC_MINT, 1000000) in calls
        assert len(owner.tasks) == 2 and not release.is_set()
        owner.promote.assert_called_once_with(result, 10)
        assert {leg["swapInfo"]["ammKey"] for leg in result["routePlan"]} == {"stock-sol", "fast-bridge"}
    finally:
        release.set()
        await owner.close()


@pytest.mark.parametrize("age", [8, -1])
async def test_expired_or_future_result_cannot_win_over_a_fresh_alternative(monkeypatch, age):
    monkeypatch.setattr("backpack_stocks.direct_routes.time.time", lambda: 100.0)
    owner = tracker()
    provider = router(["unusable", "current"])

    async def calculate(pool, source, target, amount, **kwargs):
        return quote(
            pool, source, target, amount, amount * 6, observed=100 - age if pool == "unusable" else 100
        )

    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        result = await minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner)
        assert result["pool"] == "current"
        owner.promote.assert_called_once_with(result, 10)
    finally:
        await owner.close()


@pytest.mark.parametrize("mismatch", ["amount", "input", "output", "zero_minimum"])
async def test_mismatched_exact_inputs_never_publish_a_minimum(mismatch):
    owner = tracker()
    provider = router(["bad"])
    result = quote("bad", STOCK_MINT, USDC_MINT, 1000, 6000)
    result.update(
        {
            "amount": {"inAmount": "999"},
            "input": {"inputMint": USDC_MINT},
            "output": {"outputMint": STOCK_MINT},
            "zero_minimum": {"otherAmountThreshold": "0"},
        }[mismatch]
    )
    provider.pool_quote = AsyncMock(return_value=result)
    try:
        with pytest.raises(QuoteUnavailable) as error:
            await minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner)
        assert error.value.codes == ["clmm_input_mismatch"]
        owner.promote.assert_not_called()
    finally:
        await owner.close()


async def test_bounded_existing_candidate_fanout_and_root_close_owns_losers():
    owner = tracker()
    provider = router(
        [f"direct{i}" for i in range(6)], [f"sol{i}" for i in range(6)], [f"bridge{i}" for i in range(6)]
    )
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def calculate(pool, source, target, amount, **kwargs):
        calls.append(pool)
        if len(calls) == 6:
            entered.set()
        await release.wait()
        return quote(pool, source, target, amount, amount * 6)

    provider.pool_quote = AsyncMock(side_effect=calculate)
    task = asyncio.create_task(minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert len(owner.tasks) == 6
        assert set(calls) == {"direct0", "direct1", "sol0", "sol1"}
        await owner.close()
        with pytest.raises(QuoteUnavailable):
            await task
        assert not owner.tasks
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await owner.close()


async def test_every_failed_candidate_preserves_actual_market_reasons():
    owner = tracker()
    provider = router(["empty", "thin"])

    async def calculate(pool, *args, **kwargs):
        raise ClmmCalculationError(
            "insufficient_liquidity" if pool == "empty" else "insufficient_tick_coverage"
        )

    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        with pytest.raises(QuoteUnavailable) as error:
            await minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner)
        assert set(error.value.codes) == {"insufficient_liquidity", "insufficient_tick_coverage"}
        owner.promote.assert_not_called()
        assert provider.failures == 2
    finally:
        await owner.close()


async def test_cancelled_minimum_caller_leaves_running_sdk_jobs_owned_until_root_close():
    owner = tracker()
    provider = router(["first", "second"])
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def calculate(pool, source, target, amount, **kwargs):
        calls.append(pool)
        if len(calls) == 2:
            entered.set()
        await release.wait()
        return quote(pool, source, target, amount, amount * 6)

    provider.pool_quote = AsyncMock(side_effect=calculate)
    task = asyncio.create_task(minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        children = list(owner.tasks)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(owner.tasks) == 2 and all(not child.cancelled() and not child.done() for child in children)
        owner.promote.assert_not_called()
        await owner.close()
        assert all(child.cancelled() for child in children) and not owner.tasks
    finally:
        release.set()
        await owner.close()


async def test_minimum_preserves_disabled_sol_mode_and_configured_pool_limit():
    owner = tracker()
    provider = router(["direct0", "direct1"], ["sol0"], ["bridge0"])
    provider.settings.direct.via_sol = False
    provider.settings.direct.max_pools = 1

    async def calculate(pool, source, target, amount, **kwargs):
        return quote(pool, source, target, amount, amount * 6)

    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        result = await minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner)
        assert result["pool"] == "direct0"
        provider.pool_quote.assert_awaited_once()
        provider.bridge_pools.assert_not_awaited()
    finally:
        await owner.close()


async def test_ready_direct_result_never_waits_for_optional_bridge_discovery():
    owner = tracker()
    provider = router(["direct"], ["stock-sol"])
    discovery_started, release = asyncio.Event(), asyncio.Event()

    async def bridges():
        discovery_started.set()
        await release.wait()
        return ["bridge0", "bridge1"]

    async def calculate(pool, source, target, amount, **kwargs):
        await asyncio.sleep(0)
        return quote(pool, source, target, amount, amount * 6)

    provider.bridge_pools = AsyncMock(side_effect=bridges)
    provider.pool_quote = AsyncMock(side_effect=calculate)
    try:
        result = await asyncio.wait_for(minimum_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, owner), 1)
        assert result["pool"] == "direct" and not release.is_set()
        await asyncio.wait_for(discovery_started.wait(), 1)
        provider.bridge_pools.assert_awaited_once()
    finally:
        release.set()
        await owner.close()


async def test_a_supported_composed_route_aged_out_at_final_assembly_reports_staleness(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("backpack_stocks.direct_routes.time.time", lambda: now[0])
    provider = router([], ["stock-sol"], ["bridge"])

    async def family(router, pools, source, target, amount, priority, rpc, partials=None):
        if not pools:
            return [], []
        if source == WSOL:
            now[0] += 9
        return [quote(pools[0], source, target, amount, amount, observed=now[0])], []

    monkeypatch.setattr("backpack_stocks.direct_routes.family_quote", family)
    with pytest.raises(QuoteUnavailable) as error:
        await composed_quote(provider, STOCK_MINT, USDC_MINT, 1000, 10, None)
    assert error.value.codes == ["stale_pool_quote"] and error.value.code != "no_direct_pool"
