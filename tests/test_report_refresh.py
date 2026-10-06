import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from solders.pubkey import Pubkey
from test_direct import ROOT, fixture
from test_monitor import asset, observed
from test_monitor import monitor as monitor_fixture

from backpack_stocks.direct import CLOCK, DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.monitor import FORWARD, REVERSE, SpreadMonitor, quote_details
from backpack_stocks.quote_batch import QuoteAccountBatch


@pytest.fixture
def monitor():
    mon, clock = monitor_fixture.__wrapped__()
    mon.refresh_metadata = AsyncMock()
    mon.refresh_external_prices = AsyncMock()

    async def prices():
        mon.prices_at = clock()
        mon.prices = {
            USDC_MINT: {"usdPrice": "1"},
            **{a.mint: {"usdPrice": "6.1", "_observed_at": clock()} for a in mon.assets.values()},
        }
        for a in mon.assets.values():
            mon.backpack.stock_feed.latest[a.ticker] = observed(clock)

    mon.refresh_prices = AsyncMock(side_effect=prices)
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    return mon, clock


async def test_every_report_requotes_displayed_pairs_even_inside_refresh_interval(monitor):
    mon, clock = monitor
    first = await mon.refresh_report()
    assert first["report_keys"] == [("WEN.US", FORWARD), ("WEN.US", REVERSE)]
    first_profit = next(r["profit_raw"] for r in first["rows"] if r["direction"] == FORWARD)
    mon.jupiter.quote_pair.return_value = {"outAmount": "105495958", "priceImpact": "0.1"}
    second = await mon.refresh_report()
    assert mon.jupiter.quote_pair.await_count == 4
    assert mon.refresh_metadata.await_count == mon.refresh_prices.await_count == 2
    assert next(r["profit_raw"] for r in second["rows"] if r["direction"] == FORWARD) != first_profit
    assert not mon.reporting and mon.attempted[("WEN.US", FORWARD)] == clock()


async def test_report_recalculates_largest_spot_spreads_despite_previous_depth_penalty(monitor):
    mon, clock = monitor
    mon.assets = {f"S{i}.US": asset(f"S{i}.US", str(Pubkey.new_unique())) for i in range(8)}
    mon.chain = {a.symbol: "verified_unit_mapping" for a in mon.assets.values()}
    mon.estimate = lambda a, direction=None: (
        Decimal(1000 - int(a.ticker[1:])) if direction == FORWARD else Decimal(-1)
    )
    for a in mon.assets.values():
        mon.backpack.stock_feed.latest[a.ticker] = observed(clock)
        mon.results[(a.symbol, FORWARD)] = {
            "asset": a.symbol,
            "direction": FORWARD,
            "mint": a.mint,
            "observed_at": clock() - 20,
            "quote_started_at": clock() - 20,
            "screen_spread_at_quote": "1000",
            "edge_bps": "-1000",
        }
    report = await mon.refresh_report()
    assert report["report_keys"] == [(f"S{i}.US", FORWARD) for i in range(5)]
    assert mon.jupiter.quote_pair.await_count == 5
    text = mon.text(snapshot=report)
    assert "полный расчёт устарел" not in text
    assert "S7.US" not in text and all(f"S{i}.US" in text for i in range(5))
    assert all(r["fresh"] for r in report["rows"] if (r["asset"], r["direction"]) in report["report_keys"])


async def test_report_has_exact_share_quantities_and_reconciled_forward_costs(monitor):
    mon, _ = monitor
    report = await mon.refresh_report("WEN.US")
    row = next(r for r in report["rows"] if r["direction"] == FORWARD)
    text = "\n".join(quote_details(row))
    assert "16.66666 акций за 99.99996 USDC" in text
    assert "комиссия 0.08 акций; в пул 16.58666 акций" in text
    assert "quote 104.495958; минимум 103.973478 USDC" in text
    assert "gas 0.03 USDC" in text and "сессия REGULAR" in text
    assert row["proceeds_raw"] - row["cost_raw"] - row["gas_raw"] == row["profit_raw"]
    assert row["settlement_required"] and not row["account_withdrawal_verified"]
    assert "16.66666 шт за 99.99996" in mon.text(snapshot=report)
    assert "rfqFilled" not in mon.text(snapshot=report)


async def test_reverse_report_does_not_credit_stock_dust(monitor):
    mon, _ = monitor
    mon.assets["WEN.US"] = replace(
        mon.assets["WEN.US"],
        sessions=(
            {"name": "US_EQUITIES_REGULAR", "stepSize": "1", "minQuantity": "1", "maxQuantity": "1000"},
        ),
    )
    mon.jupiter.quote_pair.return_value = {"outAmount": "16300001", "priceImpact": "0.1"}
    await mon.refresh_report("WEN.US")
    row = mon.results[("WEN.US", REVERSE)]
    text = "\n".join(quote_details(row))
    assert "quote 16.300001 акций; минимум 16.2185" in text
    assert "Продать в BP 16 акций" in text and "остаток 0.2185 акций" in text
    assert row["sellable_raw"] <= row["minimum_output_raw"]


@pytest.mark.parametrize(
    "hour,expected", [(12, "US_EQUITIES_PRE_MARKET"), (14, "US_EQUITIES_REGULAR"), (22, None)]
)
async def test_rest_model_sizing_uses_current_calendar_and_holidays(monitor, hour, expected):
    mon, clock = monitor
    clock.now = datetime(2026, 10, 2, hour, tzinfo=UTC).timestamp()
    mon.metadata_at = mon.external_at = clock()
    mon.backpack.stock_feed.latest.clear()
    mon.external = {"WEN.US": "6"}
    mon.sessions = [
        {
            "name": "US_EQUITIES_PRE_MARKET",
            "startTime": "04:00:00",
            "endTime": "09:30:00",
            "timezone": "America/New_York",
            "startWeekday": 1,
            "endWeekday": 5,
        },
        {
            "name": "US_EQUITIES_REGULAR",
            "startTime": "09:30:00",
            "endTime": "16:00:00",
            "timezone": "America/New_York",
            "startWeekday": 1,
            "endWeekday": 5,
        },
    ]
    mon.assets["WEN.US"] = replace(
        mon.assets["WEN.US"],
        sessions=(
            *mon.assets["WEN.US"].sessions,
            {"name": "US_EQUITIES_PRE_MARKET", "stepSize": "1", "minQuantity": "1", "maxQuantity": "1000"},
        ),
    )
    observation = mon.observation(mon.assets["WEN.US"])
    if expected is None:
        with pytest.raises(ValueError, match="session is open"):
            mon.sizing(mon.assets["WEN.US"], observation)
        return
    assert mon.quantity_session(observation) == expected
    acquired, _ = mon.sizing(mon.assets["WEN.US"], observation)
    if expected == "US_EQUITIES_PRE_MARKET":
        assert acquired == 16_000_000
        assert mon.sale_quantity(mon.assets["WEN.US"], observation, 16_218_500) == 16_000_000
    else:
        assert acquired % 1_000_000 != 0
    mon.holidays = [{"market": "US_EQUITIES", "date": "2026-10-02", "timezone": "America/New_York"}]
    with pytest.raises(ValueError, match="session is open"):
        mon.quantity_session(observation)


async def test_failed_report_refresh_never_displays_previous_profit_or_remote_payload(monitor):
    mon, _ = monitor
    await mon.refresh_report()
    mon.jupiter.quote_pair.side_effect = ValueError("private-remote-payload")
    report = await mon.refresh_report()
    assert not any(r["fresh"] for r in report["rows"])
    text = mon.text(snapshot=report)
    assert "WEN.US BP → Jup: net" not in text and "private-remote-payload" not in text
    assert not mon.reporting


async def test_jupiter_quote_overrides_direct_spot_and_single_pool_calculation(monitor):
    mon, clock = monitor
    mon.settings.dex_backend = "raydium"
    mon._quote_provider = SimpleNamespace(
        quote_pair=AsyncMock(
            return_value={
                "outAmount": "95000000",
                "priceImpact": "0.8",
                "feeBps": 10,
                "routePlan": [
                    {"bps": 6000, "swapInfo": {"ammKey": "pool-a", "label": "Raydium CLMM"}},
                    {"bps": 4000, "swapInfo": {"ammKey": "pool-b", "label": "Whirlpool"}},
                ],
            }
        ),
        status=lambda: {},
    )
    await mon.refresh_report("WEN.US")
    row = next(r for r in mon.snapshot(include_hidden=True)["rows"] if r["direction"] == FORWARD)
    assert row["screen_positive"] and row["profit_raw"] < 0
    assert mon.jupiter.quote_pair.await_count == 0
    mon._quote_provider.quote_pair.assert_any_await(STOCK_MINT, USDC_MINT, 16_586_660, priority=10)
    assert row["quoted_output_raw"] == 95_000_000 and row["minimum_output_raw"] == 94_525_000
    assert row["quote_backend"] == "jupiter" and not row["trading_supported"]
    text = mon.text("WEN.US")
    assert "BP → Jup: net -" not in text and "Pool spot" not in text
    assert not any(r.get("profit_raw", 1) <= 0 for r in mon.snapshot()["rows"])
    details = "\n".join(quote_details(row))
    assert "Raydium CLMM 60%" in details and "Whirlpool 40%" in details
    assert "уже включены в quote" in details
    assert row["fresh"] and row["quote_age_s"] == 0 and clock() == row["observed_at"]


async def test_multihop_route_percentages_show_each_leg_instead_of_one_total_split(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "priceImpact": "0.1",
        "routePlan": [
            {
                "bps": 10000,
                "swapInfo": {
                    "ammKey": "a",
                    "label": "Raydium",
                    "inputMint": STOCK_MINT,
                    "outputMint": "SOLMINT",
                },
            },
            {
                "bps": 10000,
                "swapInfo": {"ammKey": "b", "label": "Orca", "inputMint": "SOLMINT", "outputMint": USDC_MINT},
            },
        ],
    }
    await mon.refresh_report()
    text = "\n".join(quote_details(mon.results[("WEN.US", FORWARD)]))
    assert "WEN→SOLM Raydium 100%; SOLM→USDC Orca 100%" in text


async def test_provider_minimum_and_gasless_fees_are_not_overstated_or_counted_twice(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "otherAmountThreshold": "100000000",
        "priceImpact": "0.1",
        "feeBps": 81,
        "slippageBps": 431,
        "gasless": True,
    }
    await mon.refresh_report("WEN.US")
    row = next(r for r in mon.snapshot()["rows"] if r["direction"] == FORWARD)
    assert row["quoted_output_raw"] == 104_495_958 and row["proceeds_raw"] == 100_000_000
    assert row["gas_raw"] == 0 and row["profit_raw"] == row["proceeds_raw"] - row["cost_raw"]
    assert row["gas_source"] == "included_in_dex_quote" and row["slippage_bps"] == 431
    assert not row["candidate"] and "slippage_limit" in row["candidate_blocks"]
    assert "gasless уже включён" in "\n".join(quote_details(row))


@pytest.mark.parametrize("minimum", ["0", "104495959", "-1"])
async def test_invalid_provider_minimum_discards_the_quote(monitor, minimum):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {
        "outAmount": "104495958",
        "otherAmountThreshold": minimum,
        "priceImpact": "0.1",
    }
    await mon.refresh_report()
    assert not any(r["fresh"] for r in mon.snapshot()["rows"])


@pytest.mark.parametrize("quote_backend", ["jupiter", "execution"])
async def test_direct_monitor_selects_and_closes_its_owned_quote_client(monitor, monkeypatch, quote_backend):
    mon, clock = monitor
    settings = Settings(dex_backend="raydium")
    settings.monitor.jupiter_flow = False
    settings.monitor.quote_backend = quote_backend
    router = DirectRaydium(settings, mon.rpc)
    reader = SimpleNamespace(close=AsyncMock())
    factory = Mock(return_value=reader)
    monkeypatch.setattr("backpack_stocks.monitor.Jupiter", factory)
    try:
        created = SpreadMonitor(settings, mon.backpack, router, mon.rpc, clock=clock)
        assert created.quote_provider is (reader if quote_backend == "jupiter" else router)
        assert created.quote_backend == ("jupiter" if quote_backend == "jupiter" else "raydium")
        await created.close()
        assert reader.close.await_count == (1 if quote_backend == "jupiter" else 0)
    finally:
        await router.close()


async def test_cancelled_refresh_removes_cached_quote_and_resumes_scheduler(monitor):
    mon, _ = monitor
    await mon.refresh_report()
    entered = asyncio.Event()

    async def held(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    mon.jupiter.quote_pair.side_effect = held
    task = asyncio.create_task(mon.refresh_report())
    await asyncio.wait_for(entered.wait(), 0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not mon.reporting and not mon.results


async def test_fresh_direct_bank_age_does_not_include_pre_read_queue_time(monitor):
    mon, clock = monitor

    async def delayed(*args, **kwargs):
        started = clock()
        clock.now += 20
        mon.backpack.stock_feed.latest["WEN"] = observed(clock)
        return {
            "outAmount": "104495958",
            "priceImpact": "0.1",
            "_request_started_at": started,
            "_observed_at": clock(),
        }

    mon.jupiter.quote_pair.side_effect = delayed
    await mon.quote(("WEN.US", FORWARD))
    row = next(r for r in mon.snapshot()["rows"] if r["direction"] == FORWARD)
    assert row["fresh"] and row["quote_age_s"] == 0
    clock.now += mon.settings.monitor.max_quote_age_s + 1
    assert not any(r["fresh"] for r in mon.snapshot()["rows"])


async def test_account_batch_preserves_groups_minimum_slot_priority_and_no_cache():
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": 200},
                "value": [{"address": a} for a in refs],
            }
        )
    )
    batch = QuoteAccountBatch(rpc)
    try:
        one, two = await asyncio.gather(
            batch.accounts(["A", "B"], parsed=True, min_slot=100, priority=10),
            batch.accounts(["B", "C"], parsed=True, min_slot=120, priority=10),
        )
        rpc.accounts.assert_awaited_once_with(["A", "B", "C"], parsed=True, min_slot=120, priority=10)
        assert [a["address"] for a in one["value"]] == ["A", "B"]
        assert [a["address"] for a in two["value"]] == ["B", "C"]
        assert one["context"] == two["context"]
        await batch.accounts(["A", "B"], parsed=True, priority=10)
        assert rpc.accounts.await_count == 2
    finally:
        await batch.close()


async def test_shared_bank_keeps_its_time_refreshes_before_ttl_and_retries_failures(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("backpack_stocks.quote_batch.time.time", lambda: now[0])
    batch = QuoteAccountBatch(None)
    producer = AsyncMock(side_effect=lambda: {"observed": now[0]})
    key = ("bridge", "SOL", "USDC", 10)
    try:
        one, two = await asyncio.gather(
            batch.pool_snapshot(key, producer, 8), batch.pool_snapshot(key, producer, 8)
        )
        assert one == two == {"observed": 100}
        assert producer.await_count == 1
        now[0] = 103
        assert (await batch.pool_snapshot(key, producer, 8))["observed"] == 100
        now[0] = 104
        assert (await batch.pool_snapshot(key, producer, 8))["observed"] == 104
        assert producer.await_count == 2
        failing = AsyncMock(side_effect=[ValueError("bad bank"), {"observed": 104}])
        bad_key = ("other", "SOL", "USDC", 10)
        with pytest.raises(ValueError):
            await batch.pool_snapshot(bad_key, failing, 8)
        assert (await batch.pool_snapshot(bad_key, failing, 8))["observed"] == 104
        assert failing.await_count == 2
    finally:
        await batch.close()


async def test_batch_chunking_does_not_split_a_pool_between_banks():
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": 200},
                "value": refs,
            }
        )
    )
    batch = QuoteAccountBatch(rpc)
    try:
        requests = [[f"{pool}:{i}" for i in range(30)] for pool in range(5)]
        results = await asyncio.gather(*(batch.accounts(refs) for refs in requests))
        assert rpc.accounts.await_count == 2
        assert all(len(call.args[0]) <= 100 for call in rpc.accounts.call_args_list)
        assert [r["value"] for r in results] == requests
        for refs in requests:
            assert any(set(refs) <= set(call.args[0]) for call in rpc.accounts.call_args_list)
    finally:
        await batch.close()


@pytest.mark.parametrize("failure", ["short_response", "old_slot", "exception"])
async def test_account_batch_rejects_incomplete_old_or_failed_banks(failure):
    rpc = SimpleNamespace(accounts=AsyncMock(return_value={"context": {"slot": 1}, "value": [None]}))
    if failure == "exception":
        rpc.accounts.side_effect = RuntimeError("failed RPC")
    batch = QuoteAccountBatch(rpc)
    try:
        with pytest.raises((ValueError, RuntimeError)):
            await batch.accounts(["A", "B"] if failure == "short_response" else ["A"], min_slot=10)
    finally:
        await batch.close()


@pytest.mark.parametrize("via_sol", [False, True])
async def test_batch_direct_quotes_keep_full_live_account_validation(monkeypatch, via_sol):
    saved = fixture()
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    rows = dict(
        zip(saved["rpc_records"][-1]["addresses"], saved["rpc_records"][-1]["result"]["value"], strict=True)
    )
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [rows.get(a) for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium", direct={"via_sol": via_sol}), rpc)
    router.candidates = {STOCK_MINT: [q["pool"]]}
    router.sol_candidates = {}
    router.ensure_candidates = AsyncMock()
    try:
        async with router.batch_quotes() as quote_pair:
            result = await quote_pair(STOCK_MINT, USDC_MINT, 1_000_000, priority=10)
            assert result["outAmount"] == "6238893"
            assert rpc.accounts.await_count == 2  # One pool+bitmap read and one complete validated bank.
            rows[STOCK_MINT]["data"]["parsed"]["info"]["decimals"] = 5
            with pytest.raises(ValueError):
                await quote_pair(STOCK_MINT, USDC_MINT, 1_000_000, priority=10)
    finally:
        await router.close()


async def test_background_direct_quotes_share_rpc_batches_and_keep_concurrency_bound(monitor, monkeypatch):
    mon, _ = monitor
    rpc = SimpleNamespace(
        accounts=AsyncMock(side_effect=lambda refs, **kw: {"context": {"slot": 200}, "value": refs})
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    mon.jupiter = router
    mon.settings.monitor.concurrency = 8
    keys = [(f"S{i:02d}.US", FORWARD) for i in range(12)]
    remaining = iter(keys)
    started = []
    completed = asyncio.Event()

    def choose():
        return next(remaining, None)

    async def calculate(source, target, amount, *, priority=0, _rpc=None):
        result = await _rpc.accounts([source, "shared_bridge"], priority=priority)
        return {"outAmount": str(amount), "contextSlot": result["context"]["slot"]}

    async def quote(key, *, quote_pair, notional):
        started.append(key)
        result = await quote_pair(key[0], USDC_MINT, 100, priority=10)
        assert result["contextSlot"] == 200
        if len(started) == 12:
            completed.set()

    monkeypatch.setattr(mon, "choose", choose)
    monkeypatch.setattr(mon, "next_budget", lambda key: Decimal(100))
    monkeypatch.setattr(mon, "quote", quote)
    monkeypatch.setattr(router, "quote_pair", calculate)
    task = asyncio.create_task(mon.quotes_loop())
    try:
        await asyncio.wait_for(completed.wait(), 2)
        assert started == keys
        assert rpc.accounts.await_count == 2
        assert len(rpc.accounts.await_args_list[0].args[0]) == 9
        assert all(len(call.args[0]) <= 9 for call in rpc.accounts.await_args_list)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await mon.close()
        await router.close()


@pytest.mark.parametrize("broken", [None, "mint", "stale_clock"])
async def test_cpmm_batch_uses_catalogue_addresses_with_missing_oracle_and_validates_fresh_bank(
    monkeypatch, broken
):
    saved = json.loads((ROOT / "tests/fixtures/direct-raydium/cpmm-direct-quote.json").read_text())
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    if broken == "mint":
        rows[q["inputMint"]]["data"]["parsed"]["info"]["decimals"] += 1
    elif broken == "stale_clock":
        rows[CLOCK]["data"]["parsed"]["info"]["unixTimestamp"] -= 60
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [rows.get(a) for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    hint = dict(q["_direct"]["state"])
    hint.pop("observation", None)
    router.pool_catalog[q["pool"]] = {"program": q["_direct"]["program"], "snapshot": {"state": hint}}
    batch = QuoteAccountBatch(rpc)
    try:
        call = router.pool_quote(q["pool"], q["inputMint"], q["outputMint"], int(q["inAmount"]), _rpc=batch)
        if broken:
            with pytest.raises(ValueError):
                await call
        else:
            result = await call
            assert result["outAmount"] == q["outAmount"]
        assert rpc.accounts.await_count == 1
    finally:
        await batch.close()
        await router.close()
