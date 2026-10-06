import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.direct import DirectRaydium, LocalQuoteNotReady
from backpack_stocks.models import Settings


@pytest.fixture
def exact_bank(monkeypatch):
    saved = json.loads((Path(__file__).parent / "fixtures/direct-raydium/wen-direct-quote.json").read_text())
    quote = saved["quote"]
    final = saved["rpc_records"][-1]
    bank = {
        "state": copy.deepcopy(quote["_direct"]["state"]),
        "program": quote["_direct"]["program"],
        "accounts": dict(zip(final["addresses"], final["result"]["value"], strict=True)),
        "arrays": list(quote["_direct"]["arrays"]),
        "bitmap": quote["_direct"]["bitmap"],
        "timestamp": int(quote["_observed_at"]),
        "observed": quote["_observed_at"],
        "response": final["result"],
        "array_limit": len(quote["_direct"]["arrays"]),
        "arrays_verified": True,
    }
    now = [bank["observed"]]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: now[0])
    router = object.__new__(DirectRaydium)
    router.settings = Settings()
    router.array_hints = {}
    router.cached_prices = {}

    def calculate(body):
        assert body["op"] == "quote"
        return {
            "output": str(int(body["amount"]) * 6),
            "sqrtPrice": str(1 << 64),
            "accounts": [bank["arrays"][0]],
        }

    router.worker = SimpleNamespace(call=AsyncMock(side_effect=calculate))
    return router, bank, quote, now


async def test_same_bank_exact_amount_computes_once_and_keeps_original_time_and_slot(exact_bank):
    router, bank, quote, now = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    first = await router.quote_bank(*args, bank)
    now[0] += 1
    second = await router.quote_bank(*args, bank)
    assert first == second and first is not second
    assert second["_observed_at"] == bank["observed"] < now[0]
    assert second["contextSlot"] == bank["response"]["context"]["slot"]
    router.worker.call.assert_awaited_once()


async def test_cache_keys_exact_quantity_direction_slippage_and_array_limit(exact_bank):
    router, bank, quote, _ = exact_bank
    pool, source, target, quantity = (
        quote["pool"],
        quote["inputMint"],
        quote["outputMint"],
        int(quote["inAmount"]),
    )
    await router.quote_bank(pool, source, target, quantity, bank)
    await router.quote_bank(pool, source, target, quantity + 1, bank)
    await router.quote_bank(pool, target, source, quantity, bank)
    router.settings.policy.slippage_bps = 100
    changed_slippage = await router.quote_bank(pool, source, target, quantity, bank)
    assert int(changed_slippage["otherAmountThreshold"]) == int(changed_slippage["outAmount"]) * 9900 // 10000
    bank["array_limit"] += 1
    await router.quote_bank(pool, source, target, quantity, bank)
    assert router.worker.call.await_count == 5


async def test_new_bank_even_with_identical_accounts_cannot_reuse_previous_math(exact_bank):
    router, bank, quote, now = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    first = await router.quote_bank(*args, bank)
    refreshed = dict(bank)  # An inherited memo must still belong to the original dict.
    assert await router.quote_bank(*args, refreshed) == first
    assert router.worker.call.await_count == 2
    refreshed = dict(bank)
    refreshed["response"] = copy.deepcopy(bank["response"])
    refreshed["response"]["context"]["slot"] += 1
    now[0] += 1
    refreshed["observed"] = now[0]
    second = await router.quote_bank(*args, refreshed)
    assert second["_observed_at"] > first["_observed_at"]
    assert second["contextSlot"] > first["contextSlot"]
    assert router.worker.call.await_count == 3


async def test_returned_mutations_cannot_poison_the_cache_or_bank_state(exact_bank):
    router, bank, quote, _ = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    first = await router.quote_bank(*args, bank)
    original = copy.deepcopy(first)
    first["outAmount"] = "1"
    first["routePlan"][0]["swapInfo"]["inputMint"] = "changed"
    first["_direct"]["state"]["fees"]["tradeFeeRate"] = 999999
    first["_direct"]["arrays"].clear()
    second = await router.quote_bank(*args, bank)
    assert second == original
    assert bank["state"]["fees"]["tradeFeeRate"] == original["_direct"]["state"]["fees"]["tradeFeeRate"]
    second["_direct"]["state"]["fees"]["tradeFeeRate"] = 999999
    assert await router.quote_bank(*args, bank) == original
    router.worker.call.assert_awaited_once()


async def test_failed_math_is_not_cached(exact_bank):
    router, bank, quote, _ = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    calculate = router.worker.call.side_effect
    router.worker.call.side_effect = [
        ClmmCalculationError("insufficient_liquidity"),
        calculate({"op": "quote", "amount": quote["inAmount"]}),
    ]
    with pytest.raises(ClmmCalculationError):
        await router.quote_bank(*args, bank)
    second = await router.quote_bank(*args, bank)
    assert await router.quote_bank(*args, bank) == second
    assert router.worker.call.await_count == 2


@pytest.mark.parametrize("age", [8, -1])
async def test_cached_math_rejects_expired_and_future_bank_before_reuse(exact_bank, age):
    router, bank, quote, now = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    await router.quote_bank(*args, bank)
    now[0] = bank["observed"] + age
    with pytest.raises(LocalQuoteNotReady):
        await router.quote_bank(*args, bank)
    router.worker.call.assert_awaited_once()


async def test_cache_is_bounded_per_bank_and_eviction_recomputes_exact_math(exact_bank):
    router, bank, quote, _ = exact_bank
    pool, source, target = quote["pool"], quote["inputMint"], quote["outputMint"]
    for quantity in range(1, 66):
        await router.quote_bank(pool, source, target, quantity, bank)
    assert len(bank["_exact_quote_math"]["quotes"]) == 64
    await router.quote_bank(pool, source, target, 65, bank)
    assert router.worker.call.await_count == 65
    await router.quote_bank(pool, source, target, 1, bank)
    assert router.worker.call.await_count == 66
    assert len(bank["_exact_quote_math"]["quotes"]) == 64


async def test_concurrent_identical_math_is_coalesced(exact_bank):
    router, bank, quote, _ = exact_bank
    args = quote["pool"], quote["inputMint"], quote["outputMint"], int(quote["inAmount"])
    started, release = asyncio.Event(), asyncio.Event()
    calculate = router.worker.call.side_effect

    async def blocked_math(body):
        started.set()
        await release.wait()
        return calculate(body)

    router.worker.call.side_effect = blocked_math
    first = asyncio.create_task(router.quote_bank(*args, bank))
    second = asyncio.create_task(router.quote_bank(*args, bank))
    await asyncio.wait_for(started.wait(), 1)
    release.set()
    results = await asyncio.gather(first, second)
    assert results[0] == results[1] and results[0] is not results[1]
    router.worker.call.assert_awaited_once()
