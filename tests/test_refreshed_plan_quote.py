from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backpack_stocks.direct import DirectRaydium, LocalQuoteNotReady
from backpack_stocks.models import Settings


def leaf(pool, source, target, amount="100"):
    return {
        "pool": pool,
        "inputMint": source,
        "outputMint": target,
        "inAmount": amount,
        "contextSlot": 1,
        "_observed_at": 10,
    }


@pytest.fixture
def router(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: clock[0])
    instance = object.__new__(DirectRaydium)
    instance.settings = Settings()
    banks = {}
    instance.bank_feed = SimpleNamespace(peek=lambda key: banks.get(key), promote=Mock())

    async def quote(pool, source, target, amount, *, priority, _rpc):
        assert _rpc is instance.bank_feed and priority == 10
        result = {
            **leaf(pool, source, target, str(amount)),
            "outAmount": str(amount * 2),
            "otherAmountThreshold": str(amount * 2 - 1),
            "priceImpact": "0.1",
            "contextSlot": 2,
            "_observed_at": 999.5,
            "direct_execution_supported": True,
            "routePlan": [{"swapInfo": {"ammKey": pool, "inputMint": source, "outputMint": target}}],
        }
        banks[(pool, source, target, priority)] = {
            "observed": 999.5,
            "response": {"context": {"slot": 2}},
        }
        return result

    instance.pool_quote = AsyncMock(side_effect=quote)
    instance.quote_pair = AsyncMock(
        side_effect=AssertionError("known topology must not discover alternatives")
    )
    return instance, clock, banks


async def test_known_leaf_refreshes_current_atomic_bank_and_ignores_old_quote_values(router):
    provider, _, _ = router
    plan = {**leaf("stock-pool", "stock", "USDC"), "outAmount": "999999", "cost_raw": 99}
    original = deepcopy(plan)
    result = await provider.refreshed_plan_quote("stock", "USDC", 123, plan=plan)
    assert result["inAmount"] == "123" and result["outAmount"] == "246"
    assert result["contextSlot"] == 2 and result["_observed_at"] == 999.5
    assert not result["direct_execution_supported"] and plan == original
    assert "cost_raw" not in result
    provider.bank_feed.promote.assert_called_once_with(result, 10)
    provider.quote_pair.assert_not_awaited()


async def test_sequential_refresh_uses_new_exact_intermediate_credit(router):
    provider, _, _ = router
    plan = {
        "inputMint": "stock",
        "outputMint": "USDC",
        "inAmount": "100",
        "_direct_route": [leaf("stock-sol", "stock", "SOL"), leaf("sol-usdc", "SOL", "USDC", "99999")],
    }
    result = await provider.refreshed_plan_quote("stock", "USDC", 123, plan=plan)
    assert [call.args[3] for call in provider.pool_quote.await_args_list] == [123, 246]
    assert result["outAmount"] == "492" and result["otherAmountThreshold"] == "491"
    assert result["_observed_at"] == 999.5


async def test_split_refresh_preserves_exact_input_conservation(router):
    provider, _, _ = router
    plan = {
        "inputMint": "stock",
        "outputMint": "USDC",
        "inAmount": "10",
        "_direct_route": [leaf("a", "stock", "USDC", "3"), leaf("b", "stock", "USDC", "7")],
    }
    result = await provider.refreshed_plan_quote("stock", "USDC", 123, plan=plan)
    assert [call.args[3] for call in provider.pool_quote.await_args_list] == [36, 87]
    assert result["inAmount"] == "123" and result["outAmount"] == "246"


@pytest.mark.parametrize("amount", [0, True, 1.5, 2**63])
async def test_invalid_exact_amount_never_starts_a_bank_read(router, amount):
    provider, _, _ = router
    with pytest.raises(ValueError):
        await provider.refreshed_plan_quote("stock", "USDC", amount, plan=leaf("a", "stock", "USDC"))
    provider.pool_quote.assert_not_awaited()


@pytest.mark.parametrize("kind", ["pair", "broken_hop", "bad_split", "repeated_pool", "cycle"])
async def test_invalid_topology_is_rejected_before_network_work(router, kind):
    provider, _, _ = router
    plan = leaf("a", "stock", "USDC")
    if kind == "pair":
        plan["outputMint"] = "SOL"
    elif kind == "cycle":
        plan["_direct_route"] = [plan]
    else:
        plan["_direct_route"] = [leaf("a", "stock", "USDC", "30"), leaf("b", "stock", "USDC", "70")]
        if kind == "broken_hop":
            plan["_direct_route"][1]["inputMint"] = "OTHER"
        elif kind == "bad_split":
            plan["_direct_route"][1]["inAmount"] = "69"
        else:
            plan["_direct_route"][1]["pool"] = "a"
    with pytest.raises(ValueError):
        await provider.refreshed_plan_quote("stock", "USDC", 123, plan=plan)
    provider.pool_quote.assert_not_awaited()


@pytest.mark.parametrize("kind", ["expiry", "new_head", "future"])
async def test_known_topology_cannot_publish_expired_future_or_superseded_prices(router, kind):
    provider, clock, banks = router
    original_quote = provider.pool_quote.side_effect

    async def changed(*args, **kwargs):
        result = await original_quote(*args, **kwargs)
        if kind == "expiry":
            clock[0] = 1008
        elif kind == "new_head":
            banks[("a", "stock", "USDC", 10)]["response"]["context"]["slot"] = 3
        else:
            result["_observed_at"] = 1001
        return result

    provider.pool_quote.side_effect = changed
    with pytest.raises(LocalQuoteNotReady):
        await provider.refreshed_plan_quote("stock", "USDC", 123, plan=leaf("a", "stock", "USDC"))
    provider.bank_feed.promote.assert_not_called()
