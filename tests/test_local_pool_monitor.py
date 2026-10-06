import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_direct import ROOT, fixture
from test_direct_routes import quote
from test_monitor import monitor as monitor_fixture

from backpack_stocks.direct import DirectRaydium, LocalQuoteNotReady
from backpack_stocks.direct_routes import WSOL, combine
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.monitor import FORWARD
from backpack_stocks.pool_bank_feed import PoolBankFeed


@pytest.mark.parametrize("age", [0, 8, -1])
async def test_real_cpmm_bank_local_reprice_has_no_rpc_and_never_restamps_old_data(monkeypatch, age):
    saved = json.loads((ROOT / "tests/fixtures/direct-raydium/cpmm-direct-quote.json").read_text())
    q = saved["quote"]
    now = [q["_observed_at"]]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: now[0])
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [rows.get(a) for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    router.pool_catalog[q["pool"]] = {
        "program": q["_direct"]["program"],
        "snapshot": {"state": q["_direct"]["state"]},
    }
    feed = PoolBankFeed(rpc, 8)
    router.bank_feed = feed
    try:
        bank = await router._pool_bank(
            q["pool"], q["inputMint"], q["outputMint"], int(q["inAmount"]), _rpc=feed
        )
        assert bank["observed"] == now[0]  # Acquisition time is distinct from the protocol Clock.
        feed.ready[(q["pool"], q["inputMint"], q["outputMint"], 10)] = bank
        rpc.accounts.reset_mock()
        now[0] = bank["observed"] + age
        if age:
            with pytest.raises(LocalQuoteNotReady):
                await router.cached_plan_quote(q["inputMint"], q["outputMint"], int(q["inAmount"]), plan=q)
        else:
            result = await router.cached_plan_quote(
                q["inputMint"], q["outputMint"], int(q["inAmount"]), plan=q
            )
            assert result["outAmount"] == q["outAmount"]
            assert result["_observed_at"] == bank["observed"]
            assert result["direct_execution_supported"] is False
        rpc.accounts.assert_not_awaited()
    finally:
        await feed.close()
        await router.close()


@pytest.mark.parametrize("tampered_vault", [False, True])
async def test_clmm_refresh_uses_one_complete_new_bank_and_still_rejects_bad_vault(
    monkeypatch, tampered_vault
):
    saved = fixture()
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [rows.get(a) for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    feed = PoolBankFeed(rpc, 8)
    try:
        key = q["pool"], q["inputMint"], q["outputMint"], 10
        bank = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
        feed.ready[key] = bank
        rpc.accounts.reset_mock()
        if tampered_vault:
            rows[bank["state"]["vaultA"]]["data"]["parsed"]["info"]["owner"] = (
                "11111111111111111111111111111111"
            )
            with pytest.raises(ValueError, match="vault ownership"):
                await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
        else:
            refreshed = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
            assert refreshed is not bank and refreshed["arrays"] == bank["arrays"]
            assert refreshed["arrays_verified"]
            router.worker.call = AsyncMock(wraps=router.worker.call)
            first = await router.quote_bank(*key[:3], int(q["inAmount"]), refreshed)
            second = await router.quote_bank(*key[:3], int(q["inAmount"]), refreshed)
            assert first["outAmount"] == second["outAmount"]
            assert [call.args[0]["op"] for call in router.worker.call.await_args_list] == ["quote", "quote"]
        rpc.accounts.assert_awaited_once()
        assert rpc.accounts.call_args.kwargs["parsed"] is True
        assert set(rpc.accounts.call_args.args[0]) == set(bank["accounts"])
    finally:
        await feed.close()
        await router.close()


async def test_local_route_reprices_split_remainder_and_exact_sol_intermediate():
    old = combine(
        [
            quote("direct", STOCK_MINT, USDC_MINT, 4, 9),
            combine(
                [quote("stock_sol", STOCK_MINT, WSOL, 7, 7000), quote("bridge", WSOL, USDC_MINT, 7000, 8)],
                sequential=True,
            ),
        ],
        sequential=False,
    )
    router = object.__new__(DirectRaydium)
    feed = SimpleNamespace(peek=lambda key: {"key": key}, promote=Mock(), local_quotes=0)
    router.bank_feed = feed
    jobs = []

    async def calculate(pool, source, target, amount, bank):
        jobs.append((pool, amount))
        return quote(pool, source, target, amount, amount * 1000 if target == WSOL else amount * 2)

    router.quote_bank = calculate
    result = await router.cached_plan_quote(STOCK_MINT, USDC_MINT, 13, plan=old)
    assert jobs == [("direct", 4), ("stock_sol", 9), ("bridge", 9000)]
    assert result["inAmount"] == "13" and result["outAmount"] == "18008"
    assert not result["direct_execution_supported"]


async def test_background_banks_refresh_without_a_quote_and_invalidate_failed_data(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: now[0])
    feed = PoolBankFeed(None, 8, timer=lambda: now[0])
    key = "pool", STOCK_MINT, USDC_MINT, 10
    producer = AsyncMock(side_effect=lambda: {"observed": now[0]})
    try:
        await feed.pool_snapshot(key, producer, 8)
        feed.promote(quote("pool", STOCK_MINT, USDC_MINT, 1, 2), 10)
        now[0] = 102.1
        await asyncio.wait_for(feed.changed.wait(), 1.5)
        assert feed.peek(key)["observed"] == 102.1
        assert feed.refreshed == 1
        feed.changed.clear()
        producer.side_effect = ValueError("invalid complete bank")
        now[0] = 104.2
        await asyncio.sleep(0.6)
        assert feed.peek(key) is None
    finally:
        await feed.close()


async def test_background_bank_refresh_precedes_discovery_without_raising_rpc_rate(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: now[0])
    rpc = SimpleNamespace(accounts=AsyncMock(return_value={"context": {"slot": 200}, "value": [None]}))
    feed = PoolBankFeed(rpc, 8, timer=lambda: now[0])
    key = "pool", STOCK_MINT, USDC_MINT, 10

    async def producer():
        await feed.accounts(["pool"], priority=10)
        return {"observed": now[0]}

    try:
        await feed.pool_snapshot(key, producer, 8)
        feed.promote(quote("pool", STOCK_MINT, USDC_MINT, 1, 2), 10)
        now[0] = 103.0
        await asyncio.wait_for(feed.changed.wait(), 1.5)
        assert [c.kwargs["priority"] for c in rpc.accounts.call_args_list] == [10, 5]
        assert all(c.kwargs["commitment"] == "confirmed" for c in rpc.accounts.call_args_list)
    finally:
        await feed.close()


async def prepared_monitor():
    mon, clock = monitor_fixture.__wrapped__()
    mon.settings.monitor.rfq_validation = True
    mon.backpack.auth = object()

    async def inspect(symbol, side, quantity, session):
        return mon.rfq.result(symbol, side, quantity, session, "available", "rfq_quote_observed", "6")

    mon.rfq.inspect = AsyncMock(side_effect=inspect)
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "priceImpact": "0.1",
        "_observed_at": clock(),
    }
    key = "WEN.US", FORWARD
    await mon.quote(key)
    return mon, clock, key, copy.deepcopy(mon.local_plans[key])


async def test_complete_local_monitor_calculation_uses_exact_live_rfq_without_any_network():
    mon, clock, key, plan = await prepared_monitor()
    local = AsyncMock(return_value={"outAmount": "104495958", "priceImpact": "0.1", "_observed_at": clock()})
    mon.rfq.inspect.reset_mock()
    mon.jupiter.quote_pair.reset_mock()
    old_attempt = mon.attempted[key]
    clock.now += 1
    result = await mon.quote(key, quote_pair=local, notional=100, local_plan=plan)
    assert result and result["profit_raw"] == 3943518
    assert result["acquired_raw"] == plan["acquired_raw"]
    assert result["dex_observed_at"] == plan["dex_observed_at"]
    assert mon.attempted[key] == old_attempt
    assert mon.local_calculations == 1
    mon.rfq.inspect.assert_not_awaited()
    mon.jupiter.quote_pair.assert_not_awaited()


async def test_new_pool_bank_invalidates_old_profitable_size_sample_even_in_same_second(monkeypatch):
    mon, clock, key, _ = await prepared_monitor()
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", clock)
    router = DirectRaydium(Settings(dex_backend="raydium"), mon.rpc)
    feed = PoolBankFeed(mon.rpc, 8)
    router.bank_feed = feed
    mon._quote_provider = router
    pool = "verified_pool"
    bank_key = pool, STOCK_MINT, USDC_MINT, 10

    def quoted(slot, output):
        return {
            "pool": pool,
            "inputMint": STOCK_MINT,
            "outputMint": USDC_MINT,
            "contextSlot": slot,
            "_observed_at": clock(),
            "outAmount": str(output),
            "priceImpact": "0.1",
        }

    try:
        feed.ready[bank_key] = {"observed": clock(), "response": {"context": {"slot": 1}}}
        old = await mon.quote(key, quote_pair=AsyncMock(return_value=quoted(1, 104495958)))
        assert old["profit_raw"] == 3943518
        assert next(r for r in mon.snapshot()["rows"] if r["direction"] == FORWARD)["fresh"]
        feed.ready[bank_key] = {"observed": clock(), "response": {"context": {"slot": 2}}}
        assert not any(
            r["fresh"] for r in mon.snapshot(include_hidden=True)["rows"] if r["direction"] == FORWARD
        )
        new = await mon.quote(key, quote_pair=AsyncMock(return_value=quoted(2, 103495958)))
        assert new["profit_raw"] == mon.results[key]["profit_raw"] == 2948518
    finally:
        await feed.close()
        await router.close()


@pytest.mark.parametrize("side", ["Bid", "Ask"])
async def test_local_rfq_uses_owned_quote_session_instead_of_indicative_model_session(side):
    mon, clock, key, plan = await prepared_monitor()
    obs = mon.observation(mon.assets[key[0]])
    mon.observation = Mock(return_value=obs.model_copy(update={"session": "MODEL"}))
    mon.quantity_session = Mock(return_value=obs.session)
    mon.rfq.inspect.reset_mock()
    if side == "Bid":
        local = AsyncMock(
            return_value={"outAmount": "104495958", "priceImpact": "0.1", "_observed_at": clock()}
        )
        result = await mon.quote(key, quote_pair=local, notional=100, local_plan=plan)
        assert result and result["backpack_session"] == obs.session
        assert result["backpack_model"] is False
        assert result["profit_raw"] == 3943518
    else:
        quantity = plan["acquired_raw"]
        await mon.rfq.check(key[0], str(quantity / 1_000_000), obs.session, side="Ask")
        mon.rfq.inspect.reset_mock()
        sellable, owned_obs, check, fill, _ = mon.local_sell(mon.assets[key[0]], quantity, plan)
        assert sellable == quantity and fill is None
        assert owned_obs.session == check.session == obs.session
        assert owned_obs.bid == check.price
    mon.rfq.inspect.assert_not_awaited()


@pytest.mark.parametrize("failure", ["expired", "other_quantity"])
async def test_local_calculator_cannot_substitute_stale_or_different_quantity_rfq(failure):
    mon, clock, key, plan = await prepared_monitor()
    local = AsyncMock(return_value={"outAmount": "104495958", "priceImpact": "0.1"})
    mon.rfq.inspect.reset_mock()
    if failure == "expired":
        clock.now += mon.settings.monitor.max_quote_age_s + 1
    else:
        mon.rfq.cache.clear()
        check = mon.rfq.result(
            "WEN.US", "Bid", "1", "US_EQUITIES_REGULAR", "available", "rfq_quote_observed", "1"
        )
        mon.rfq.cache[("WEN.US", "Bid", "1", check.session)] = check
    assert await mon.quote(key, quote_pair=local, notional=100, local_plan=plan) is None
    assert mon.local_calculations == 0
    local.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()
