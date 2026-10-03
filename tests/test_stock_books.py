from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_monitor import Clock, observed
from test_monitor import monitor as monitor_fixture

from backpack_stocks.models import raw, ui
from backpack_stocks.monitor import FORWARD, REVERSE, price_direction, quote_details
from backpack_stocks.observability import error_code
from backpack_stocks.stock_books import SPOT_SESSION, BookDepth, BookUnavailable, StockBookMarket, StockBooks

monitor = monitor_fixture


def market_row(asset="WEN.US"):
    return {
        "baseSymbol": asset,
        "symbol": asset + "_USDC",
        "quoteSymbol": "USDC",
        "marketType": "SPOT",
        "rwaMarketType": "STOCK",
        "orderBookState": "Open",
        "filters": {
            "quantity": {"stepSize": "0.01", "minQuantity": "0.01", "maxQuantity": None},
            "price": {
                "tickSize": "0.01",
                "minPrice": "0.01",
                "maxPrice": "10000",
                "maxImpactMultiplier": "1.05",
                "minImpactMultiplier": "0.95",
                "maxMultiplier": "2",
                "minMultiplier": "0.25",
            },
        },
    }


def depth_row(clock, asks=None, bids=None):
    return {
        "lastUpdateId": "1234",
        "timestamp": int(clock() * 1_000_000),
        "asks": asks if asks is not None else [["6.00", "10.00"], ["6.10", "20.00"]],
        "bids": bids if bids is not None else [["5.80", "20.00"], ["5.90", "10.00"]],
    }


def limit(symbol, side, price, **kwargs):
    return {
        "symbol": symbol,
        "side": side,
        "price": price,
        "maxOrderQuantity": "100",
        "autoBorrow": False,
        "autoBorrowRepay": False,
        "autoLendRedeem": False,
        "reduceOnly": False,
    }


async def setup_books(mon, clock):
    mon.backpack.auth = object()
    mon.backpack.account = AsyncMock(return_value={"spotTakerFee": "10", "liquidating": False})
    mon.backpack.depth = AsyncMock(return_value=depth_row(clock))
    mon.backpack.max_order_quantity = AsyncMock(side_effect=limit)
    mon.backpack.order = AsyncMock()
    mon.backpack.accept_rfq = AsyncMock()
    mon.books.discover([market_row()], set(mon.assets))
    await mon.books.refresh_fees()
    await mon.books.refresh("WEN.US")


def test_depth_walks_both_sides_with_quote_asset_fee_and_no_partial_fills():
    clock = Clock()
    market = StockBookMarket.parse(market_row())
    depth = BookDepth.parse(depth_row(clock), market, clock(), 15)
    buy = depth.fill(market, raw("15"), Decimal("10"), "Bid")
    sell = depth.fill(market, raw("15"), Decimal("10"), "Ask")
    assert buy.gross_raw == 90_500_000 and sell.gross_raw == 88_000_000
    assert buy.value_raw == 90_592_000 and sell.value_raw == 87_910_500
    assert buy.worst_price == "6.10" and sell.worst_price == "5.80"
    assert buy.levels == sell.levels == 2
    assert buy.quantity_raw == raw("15")  # Fee is USDC, no stock quantity deduction.
    with pytest.raises(BookUnavailable, match="route") as error:
        depth.fill(market, raw("30.01"), Decimal("10"), "Bid")
    assert error_code(error.value) == "spot_insufficient_depth"


@pytest.mark.parametrize("side", ["Bid", "Ask"])
@pytest.mark.parametrize("quantity", ["0", "0.001", "1.005"])
def test_fill_rejects_illegal_lots(side, quantity):
    clock = Clock()
    market = StockBookMarket.parse(market_row())
    depth = BookDepth.parse(depth_row(clock), market, clock(), 15)
    with pytest.raises(BookUnavailable):
        depth.fill(market, raw(quantity), Decimal("10"), side)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: r.update(timestamp=10_000),
        lambda r: r.update(timestamp=True),
        lambda r: r.update(lastUpdateId=123),
        lambda r: r.update(asks=[["NaN", "1"]]),
        lambda r: r.update(asks=[["6.001", "1"]]),
        lambda r: r.update(asks=[["6", "0.001"]]),
        lambda r: r.update(asks=[["6", "1"], ["6", "2"]]),
        lambda r: r.update(bids=[["6", "1"]]),
        lambda r: r.update(asks=[["6", "-1"]]),
        lambda r: r.update(asks=[["6", "1", "extra"]]),
    ],
)
def test_bad_depth_fails_closed(mutation):
    clock = Clock()
    data = depth_row(clock)
    mutation(data)
    with pytest.raises((ValueError, ArithmeticError)):
        BookDepth.parse(data, StockBookMarket.parse(market_row()), clock(), 15)


@pytest.mark.parametrize(
    "changes",
    [
        dict(marketType="PERP"),
        dict(quoteSymbol="USDT"),
        dict(rwaMarketType="CRYPTO"),
        dict(orderBookState="Closed"),
        dict(symbol="different"),
    ],
)
def test_discovery_does_not_treat_perps_or_other_pairs_as_deliverable_stock(changes):
    row = market_row()
    row.update(changes)
    books = StockBooks(SimpleNamespace(), ttl=15)
    books.discover([row], {"WEN.US"})
    assert not books.markets


async def test_budget_walk_obeys_integer_money_boundary_and_fee(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    quantity = mon.books.budget_quantity("WEN.US", 100_000_000, Decimal("0.16"))
    fill = mon.books.fill("WEN.US", quantity, "Bid")
    assert fill.value_raw <= 100_000_000
    assert mon.books.fill("WEN.US", quantity + 10_000, "Bid").value_raw > 100_000_000
    assert ui(quantity) < Decimal("16.66666")


async def test_book_and_fee_expiry_fail_closed_and_refresh_failure_discards_old_depth(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    assert mon.books.available("WEN.US")
    clock.now += 15
    assert not mon.books.available("WEN.US")
    mon.backpack.depth.return_value = depth_row(clock)
    await mon.books.refresh("WEN.US")
    assert mon.books.available("WEN.US")
    mon.books.fee_at = clock() - 300
    assert not mon.books.available("WEN.US")
    await mon.books.refresh_fees()
    mon.backpack.depth.side_effect = TimeoutError()
    await mon.books.refresh("WEN.US")
    assert "WEN.US" not in mon.books.depths


@pytest.mark.parametrize(
    "changes",
    [dict(spotTakerFee="NaN"), dict(spotTakerFee="10001"), dict(spotTakerFee=None), dict(liquidating=True)],
)
async def test_unknown_or_invalid_account_fee_never_defaults_to_zero(monitor, changes):
    mon, clock = monitor
    await setup_books(mon, clock)
    account = dict(mon.backpack.account.return_value)
    account.update(changes)
    mon.backpack.account.return_value = account
    with pytest.raises((ValueError, ArithmeticError)):
        await mon.books.refresh_fees()
    assert not mon.books.available("WEN.US")


@pytest.mark.parametrize(
    "changes",
    [
        dict(autoBorrow=True),
        dict(side="Ask"),
        dict(price="5"),
        dict(symbol="OTHER_USDC"),
        dict(maxOrderQuantity="0"),
    ],
)
async def test_limit_validation_binds_market_side_price_balance_and_no_borrow(monitor, changes):
    mon, clock = monitor
    await setup_books(mon, clock)
    fill = mon.books.fill("WEN.US", raw("1"), "Bid")
    answer = limit(fill.market, fill.side, fill.worst_price)
    answer.update(changes)
    mon.backpack.max_order_quantity.side_effect = None
    mon.backpack.max_order_quantity.return_value = answer
    with pytest.raises(ValueError):
        await mon.books.check_capacity("WEN.US", fill)
    mon.backpack.order.assert_not_awaited()
    mon.backpack.accept_rfq.assert_not_awaited()


def close_rfq(mon, clock):
    mon.settings.monitor.rfq_validation = True
    mon.quantity_session_original = mon.quantity_session

    def session(obs):
        if obs.session == SPOT_SESSION:
            return mon.quantity_session_original(obs)
        raise ValueError("No RFQ trading session is open")

    mon.quantity_session = session
    mon.rfq.check = AsyncMock()


async def test_closed_rfq_does_not_block_spot_prices_signals_or_scheduler(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    assert mon.choose() == ("WEN.US", FORWARD)
    mon.jupiter.quote_pair.return_value = {"outAmount": "110000000", "priceImpact": "0.1"}
    actual = await mon.quote(("WEN.US", FORWARD))
    assert actual and actual["backpack_source"] == "spot"
    assert actual["backpack_quote_confirmed"] and not actual["trading_supported"]
    snapshot = mon.snapshot()
    row = snapshot["rows"][0]
    assert row["fresh"] and row["signal_eligible"]
    assert snapshot["rfq_available_assets"] == 0 and snapshot["spot_available_assets"] == 1
    assert snapshot["fresh_quotes"] == 1 and snapshot["signal_candidates"] == 1
    assert "стакан" in price_direction(row) and "BP стакан" in "\n".join(quote_details(row))
    assert "стакан доступен" in mon.prices_text(["WEN.US"])
    assert row["withdrawal_fee_raw"] == 80_000 and row["delivered_raw"] == row["acquired_raw"] - 80_000
    mon.rfq.check.assert_not_awaited()
    mon.backpack.order.assert_not_awaited()
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_book_repriced_after_jupiter_and_worse_book_removes_signal(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.settings.monitor.auto_size = True

    async def dex(*args, **kwargs):
        mon.backpack.depth.return_value = depth_row(clock, asks=[["6.20", "30"]])
        await mon.books.refresh("WEN.US")
        return {"outAmount": "110000000", "priceImpact": "0.1"}

    mon.jupiter.quote_pair.side_effect = dex
    row = await mon.quote(("WEN.US", FORWARD))
    assert row and row["backpack_price"] == "6.20"
    assert mon.backpack.max_order_quantity.await_count == 2
    assert mon.snapshot()["signal_candidates"] == 1
    mon.backpack.depth.return_value = depth_row(clock, asks=[["7.00", "30"]])
    await mon.books.refresh("WEN.US")
    assert mon.snapshot()["signal_candidates"] == 0 and mon.snapshot()["fresh_quotes"] == 0


async def test_book_capacity_expires_even_with_fresh_depth_and_dex(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.jupiter.quote_pair.return_value = {"outAmount": "110000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    clock.now += 8
    mon.backpack.depth.return_value = depth_row(clock)
    await mon.books.refresh("WEN.US")
    mon.settings.monitor.max_quote_age_s = 15
    assert not mon.snapshot()["signal_candidates"]


async def test_balance_refusal_blocks_call_and_adaptive_search_shrinks(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.settings.monitor.auto_size = True

    async def capacity(symbol, side, price):
        result = limit(symbol, side, price)
        result["maxOrderQuantity"] = "0.1"
        return result

    mon.backpack.max_order_quantity.side_effect = capacity
    assert await mon.quote(("WEN.US", FORWARD), notional=Decimal("100")) is None
    assert mon.failures[("WEN.US", FORWARD)] == "spot_insufficient_balance"
    assert not mon.snapshot()["signal_candidates"]
    mon.jupiter.quote_pair.assert_not_awaited()
    assert ("WEN.US", FORWARD) in mon.volume_search.refusals
    assert not mon.errors["quote"]


async def test_reverse_uses_bid_depth_for_actual_minimum_dex_output(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.jupiter.quote_pair.return_value = {"outAmount": "20000000", "priceImpact": "0.1"}
    row = await mon.quote(("WEN.US", REVERSE))
    assert row and row["sellable_raw"] == 19_900_000
    fill = mon.books.fill("WEN.US", row["sellable_raw"], "Ask")
    assert row["proceeds_raw"] == fill.value_raw and row["backpack_source"] == "spot"
    assert mon.snapshot()["signal_candidates"] == 1
    assert mon.backpack.max_order_quantity.call_args.args[1] == "Ask"
    mon.backpack.order.assert_not_awaited()


async def test_best_entry_compares_depth_fee_and_withdrawn_tokens_and_fallback(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    mon.settings.monitor.rfq_validation = True

    async def inspect(symbol, quantity, session, **kwargs):
        return mon.rfq.result(symbol, "Bid", quantity, session, "available", "rfq_quote_observed", "6.04")

    mon.rfq.check = AsyncMock(side_effect=inspect)
    acquired, delivered, obs, check, fill = await mon.entry_buy(mon.assets["WEN.US"], Decimal("100"))
    # Best spot ask is 6.00 but the depth + taker fee makes RFQ cheaper at this size.
    assert check is not None and fill is None
    mon.rfq.check.side_effect = lambda symbol, quantity, session, **kw: mon.rfq.result(
        symbol, "Bid", quantity, session, "unavailable", "rfq_no_quote"
    )
    assert (await mon.entry_buy(mon.assets["WEN.US"], Decimal("100")))[4] is not None
    mon.books.depths.clear()
    mon.rfq.check.side_effect = inspect
    assert (await mon.entry_buy(mon.assets["WEN.US"], Decimal("100")))[3] is not None


async def test_market_filter_change_invalidates_depth_and_capacity(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    fill = mon.books.fill("WEN.US", raw("1"), "Bid")
    await mon.books.check_capacity("WEN.US", fill)
    row = deepcopy(market_row())
    row["filters"]["quantity"]["stepSize"] = "1"
    row["filters"]["quantity"]["minQuantity"] = "1"
    mon.books.discover([row], set(mon.assets))
    assert not mon.books.depths and not mon.books.capacities


async def test_fractional_micro_usdc_search_budget_is_rounded_down_before_pricing(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.settings.monitor.auto_size = True
    mon.jupiter.quote_pair.return_value = {"outAmount": "110000000", "priceImpact": "0.1"}
    row = await mon.quote(("WEN.US", FORWARD), notional=Decimal("99.12345678"))
    assert row is not None and row["search_budget_usdc"] == "99.123456"
    assert row["cost_raw"] <= 99_123_456
    reverse = await mon.quote(("WEN.US", REVERSE), notional=Decimal("99.12345678"))
    assert reverse is not None and reverse["cost_raw"] == 99_123_456


async def test_unverified_stock_mint_cannot_enter_spot_monitor(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.chain["WEN.US"] = "unsupported_mint_or_unit_mapping"
    assert mon.choose() is None
    assert await mon.quote(("WEN.US", FORWARD)) is None
    assert not mon.snapshot()["signal_candidates"]
    mon.jupiter.quote_pair.assert_not_awaited()


@pytest.mark.parametrize("side", ["Bid", "Ask"])
def test_depth_outside_exchange_taker_band_cannot_be_counted_as_filled(side):
    clock = Clock()
    row = market_row()
    row["filters"]["price"].update(maxImpactMultiplier="1.01", minImpactMultiplier="0.99")
    market = StockBookMarket.parse(row)
    depth = BookDepth.parse(depth_row(clock), market, clock(), 15)
    assert sum(q for p, q in depth.tradable(market, side)) == Decimal("10")
    with pytest.raises(BookUnavailable) as error:
        depth.fill(market, raw("15"), Decimal("10"), side)
    assert error_code(error.value) == "spot_price_band"


async def test_budget_ceiling_stops_at_taker_band_not_all_visible_depth(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    row = market_row()
    row["filters"]["price"].update(maxImpactMultiplier="1.01", minImpactMultiplier="0.99")
    mon.books.discover([row], set(mon.assets))
    await mon.books.refresh("WEN.US")
    assert mon.books.budget_quantity("WEN.US", raw("1000"), Decimal("0.16")) == raw("10")
    obs = mon.books.observation("WEN.US")
    assert mon.size_ceiling(mon.assets["WEN.US"], obs) == Decimal("60.061")


async def test_fee_change_requires_new_balance_check(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    fill = mon.books.fill("WEN.US", raw("1"), "Bid")
    await mon.books.check_capacity("WEN.US", fill)
    mon.backpack.account.return_value["spotTakerFee"] = "20"
    await mon.books.refresh_fees()
    assert not mon.books.capacity_valid("WEN.US", mon.books.fill("WEN.US", raw("1"), "Bid"), clock())


async def test_unconfirmed_fmv_does_not_override_verified_book_entry(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    assert not mon.settings.monitor.rfq_validation
    mon.backpack.stock_feed.latest["WEN"] = observed(clock, ask="1", bid="100")
    assert (await mon.entry_buy(mon.assets["WEN.US"], Decimal("100")))[4] is not None
    assert (await mon.entry_sell(mon.assets["WEN.US"], raw("15")))[3] is not None


async def test_auto_book_buy_resizes_to_actual_funds_and_caches_natural_ceiling(monitor):
    mon, clock = monitor
    await setup_books(mon, clock)
    close_rfq(mon, clock)
    mon.settings.monitor.auto_size = True

    async def capacity(symbol, side, price):
        answer = limit(symbol, side, price)
        answer["maxOrderQuantity"] = "1.005"
        return answer

    mon.backpack.max_order_quantity.side_effect = capacity

    async def dex(source, target, units, **kwargs):
        return {"outAmount": str(units * 7), "priceImpact": "0.1"}

    mon.jupiter.quote_pair.side_effect = dex
    row = await mon.quote(("WEN.US", FORWARD), notional=Decimal("100"))
    assert row and row["acquired_raw"] == 1_000_000 and row["cost_raw"] == 6_006_100
    assert mon.snapshot()["signal_candidates"] == 1
    assert mon.backpack.max_order_quantity.await_count == 2
    assert mon.size_ceiling(mon.assets["WEN.US"], mon.books.observation("WEN.US")) == Decimal("6.0061")
    assert mon.next_budget(("WEN.US", FORWARD)) <= Decimal("6.0061")
    mon.backpack.order.assert_not_awaited()
