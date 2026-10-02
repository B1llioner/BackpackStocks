import base64
import copy
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.pubkey import Pubkey
from test_direct import ROOT, fixture
from test_monitor import monitor as monitor_fixture

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.monitor import FORWARD, REVERSE
from backpack_stocks.pool_catalog import CLOCK


@pytest.fixture
def monitor():
    return monitor_fixture.__wrapped__()


def saved_accounts(saved):
    final = saved["rpc_records"][-1]
    return dict(zip(final["addresses"], final["result"]["value"], strict=True))


@pytest.mark.parametrize("kind", ["clmm", "cpmm"])
async def test_fresh_spot_screen_is_independent_of_exact_quote_cache(monkeypatch, kind):
    saved = (
        fixture()
        if kind == "clmm"
        else json.loads((ROOT / "tests/fixtures/direct-raydium/cpmm-direct-quote.json").read_text())
    )
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    accounts = saved_accounts(saved)
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [accounts[a] for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    stock = q["inputMint"]
    router.candidates = {stock: [q["pool"]]}
    try:
        assert not router.cached_prices
        result = await router.prices([USDC_MINT, stock])
        assert Decimal(result[stock]["usdPrice"]) > 0
        assert result[stock]["sellSpotPrice"] == result[stock]["buySpotPrice"]
        assert "outAmount" not in result[stock]
        assert router.worker.process is None and router.quote_count == 0
        assert router.screen_status["assets"] == 1
        assert all(len(call.args[0]) <= 100 for call in rpc.accounts.call_args_list)
    finally:
        await router.close()


async def test_spot_screen_chunks_100_accounts_and_clock_without_unbounded_fanout(monkeypatch):
    saved = fixture()
    q = saved["quote"]
    accounts = saved_accounts(saved)
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    addresses = [str(Pubkey.new_unique()) for _ in range(110)]
    pool = accounts[q["pool"]]
    for address in addresses:
        accounts[address] = pool
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [accounts[a] for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    row = router.pool_catalog[q["pool"]]
    router.pool_catalog = {a: row for a in addresses}
    router.candidates = {STOCK_MINT: addresses}
    try:
        prices = await router.prices([STOCK_MINT])
        assert STOCK_MINT in prices and rpc.accounts.await_count == 2
        assert all(CLOCK in call.args[0] and len(call.args[0]) <= 100 for call in rpc.accounts.call_args_list)
        assert router.screen_status["pools"] == 110
    finally:
        await router.close()


@pytest.mark.parametrize("change", ["owner", "pair", "precision", "disabled", "empty", "old_clock"])
async def test_unsafe_or_stale_pool_state_never_produces_a_screen_price(monkeypatch, change):
    saved = fixture()
    q = saved["quote"]
    accounts = copy.deepcopy(saved_accounts(saved))
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    account = accounts[q["pool"]]
    data = bytearray(base64.b64decode(account["data"][0]))
    if change == "owner":
        account["owner"] = USDC_MINT
    elif change == "pair":
        data[73:105] = bytes(Pubkey.from_string(USDC_MINT))
    elif change == "precision":
        data[233] = 9
    elif change == "disabled":
        data[389] = 16
    elif change == "empty":
        data[237:253] = bytes(16)
    else:
        accounts[CLOCK]["data"]["parsed"]["info"]["unixTimestamp"] -= 60
    account["data"][0] = base64.b64encode(data).decode()
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kw: {
                "context": {"slot": q["contextSlot"]},
                "value": [accounts[a] for a in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    router.candidates = {STOCK_MINT: [q["pool"]]}
    try:
        if change == "old_clock":
            with pytest.raises(ValueError, match="bank is too old"):
                await router.prices([STOCK_MINT])
        else:
            assert STOCK_MINT not in await router.prices([STOCK_MINT])
            assert sum(router.screen_status["rejected"].values()) == 1
    finally:
        await router.close()


def test_positive_price_spread_is_visible_without_claiming_a_profitable_exact_swap(monitor):
    mon, clock = monitor
    mon.prices_at = clock()
    mon.prices = {USDC_MINT: {"usdPrice": "1"}, STOCK_MINT: {"usdPrice": "6.1", "_observed_at": clock()}}
    doc = mon.snapshot()
    assert doc["screened_assets"] == 1 and doc["positive_price_spreads"] == 1
    assert doc["fresh_quotes"] == doc["positive_net_quotes"] == doc["candidates"] == 0
    assert doc["rows"][0]["candidate_blocks"] == ["no_exact_quote"]
    assert mon.estimate(mon.assets["WEN.US"], FORWARD) > 0
    assert mon.estimate(mon.assets["WEN.US"], REVERSE) < 0
    assert mon.choose() == ("WEN.US", FORWARD)
    clock.now += 21
    assert mon.snapshot()["positive_price_spreads"] == 0


async def test_positive_net_below_entry_threshold_is_reported_with_reasons(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "100683500", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    doc = mon.snapshot()
    row = next(r for r in doc["rows"] if r["fresh"])
    assert row["profit_raw"] > 0 and doc["positive_net_quotes"] == 1
    assert not row["candidate"]
    assert (
        "profit_below_minimum" in row["candidate_blocks"] or "edge_below_minimum" in row["candidate_blocks"]
    )


async def test_direct_screener_receives_whole_universe_in_one_refresh(monitor):
    from test_monitor import asset

    mon, clock = monitor
    mon.settings.dex_backend = "raydium"
    mon.assets = {f"S{i}.US": asset(f"S{i}.US", str(Pubkey.new_unique())) for i in range(68)}
    mon.jupiter.prices.return_value = {USDC_MINT: {"usdPrice": "1", "_observed_at": clock()}}
    await mon.refresh_jupiter_prices()
    mon.jupiter.prices.assert_awaited_once()
    assert len(mon.jupiter.prices.call_args.args[0]) == 69


def test_scheduler_uses_measured_depth_instead_of_repeating_the_largest_illiquid_spot(monitor):
    from test_monitor import asset, observed

    mon, clock = monitor
    mon.assets["DEEP.US"] = asset("DEEP.US", USDC_MINT, deposit=False)
    mon.chain["DEEP.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["DEEP"] = observed(clock)
    mon.estimate = lambda a, direction=None: Decimal("1000") if a.symbol == "WEN.US" else Decimal("500")
    mon.results[("WEN.US", FORWARD)] = {
        "observed_at": clock(),
        "edge_bps": "-1000",
        "screen_spread_at_quote": "1000",
    }
    mon.results[("WEN.US", REVERSE)] = {
        "observed_at": clock(),
        "edge_bps": "-1000",
        "screen_spread_at_quote": "1000",
    }
    assert mon.choose() == ("DEEP.US", FORWARD)


async def test_profitable_high_impact_quote_is_visible_but_cannot_enter(monitor):
    from test_monitor import observed

    mon, clock = monitor
    mon.jupiter.quote_pair.return_value = {"outAmount": "110000000", "priceImpact": "5"}
    await mon.quote(("WEN.US", FORWARD))
    row = next(r for r in mon.snapshot()["rows"] if r["fresh"])
    assert row["net_profitable"] and not row["candidate"]
    assert row["candidate_blocks"] == ["price_impact_limit"]
    mon.backpack.stock_feed.latest["WEN"] = observed(clock, ask="6.01")
    row = next(r for r in mon.snapshot()["rows"] if r["fresh"])
    assert not row["net_profitable"] and "cost_cap" in row["candidate_blocks"]
