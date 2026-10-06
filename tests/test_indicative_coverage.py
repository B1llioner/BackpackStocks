from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from test_forward_coverage import coverage_monitor
from test_monitor import asset

from backpack_stocks.indicative_feed import IndicativeFeed
from backpack_stocks.models import USDC_MINT, raw, ui
from backpack_stocks.monitor import FORWARD


@pytest.fixture
async def indicative_monitor():
    mon, clock = coverage_monitor()
    feed = IndicativeFeed(clock=clock)
    feed.tickers.add("WEN")
    mon.indicative_feed = mon.backpack.indicative_feed = feed
    mon.rfq.check = AsyncMock(wraps=mon.rfq.check)
    try:
        yield mon, clock, feed
    finally:
        await feed.close()


def published(clock, ticker="WEN", **changes):
    payload = {
        "e": "stockIndicativeQuote",
        "symbol": ticker,
        "bid": "6.1",
        "ask": "6.2",
        "estimatedBid": "2.9",
        "estimatedAsk": "3",
        "session": "US_EQUITIES_REGULAR",
        "minQuantity": "0.01",
        "maxQuantity": "50000",
        "stepSize": "0.00001",
        "reason": None,
        "timestamp": int(clock() * 1000),
    }
    payload.update(changes)
    return payload


async def test_fresh_negative_baseline_uses_public_limit_price_without_claiming_a_private_rfq(indicative_monitor):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))
    row = await mon.quote(("WEN.US", FORWARD), coverage=True)
    assert row and row["profit_raw"] < 0
    assert row["cost_raw"] == raw("0.992")  # 0.16 shares at the limit ask, not estimatedAsk.
    assert row["backpack_source"] == "indicative" and not row["backpack_quote_confirmed"]
    state = mon.forward_coverage["WEN.US"]
    assert state.check.source == "indicative" and state.check.quantity == "0.16"
    assert mon.exact_rfq(state.asset, "0.16", state.session) is None
    report = mon.snapshot(include_hidden=True)
    assert report["forward_coverage"]["pricedfresh"] == 1
    assert report["forward_coverage"]["rows"][0]["pair"]["backpack_source"] == "indicative"
    assert report["rfq_available_assets"] == report["signal_candidates"] == 0
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


async def test_public_negative_size_search_does_not_open_an_account_rfq(indicative_monitor):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))
    row = await mon.quote(("WEN.US", FORWARD), notional=Decimal("100"))
    assert row and row["profit_raw"] < 0 and row["backpack_source"] == "indicative"
    assert not row["backpack_quote_confirmed"]
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


async def test_all_72_negative_public_baselines_finish_without_private_quote_requests(indicative_monitor):
    mon, clock, feed = indicative_monitor
    for index in range(71):
        item = asset(f"STOCK{index:02d}.US", deposit=False)
        mon.assets[item.symbol] = item
        mon.chain[item.symbol] = "verified_unit_mapping"
        feed.tickers.add(item.ticker)
    for item in mon.assets.values():
        await feed.ingest(published(clock, item.ticker))
        row = await mon.quote((item.symbol, FORWARD), coverage=True)
        assert row and row["profit_raw"] < 0 and not row["backpack_quote_confirmed"]
    coverage = mon.snapshot()["forward_coverage"]
    assert coverage["universe"] == coverage["eligible"] == coverage["pricedfresh"] == 72
    assert coverage["done_universe"] and coverage["unattempted"] == coverage["waiting"] == 0
    assert mon.snapshot()["rfq_available_assets"] == 0
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


@pytest.mark.parametrize(
    "boundary", ["missing", "stale", "null_ask", "reason", "session", "filter", "step", "future"]
)
async def test_unusable_public_source_fails_closed_without_old_feed_or_private_fallback(
    indicative_monitor, boundary
):
    mon, clock, feed = indicative_monitor
    changes = {
        "null_ask": {"ask": None},
        "reason": {"reason": "quoterUnavailable"},
        "session": {"session": "US_EQUITIES_OVERNIGHT"},
        "filter": {"maxQuantity": "1"},
        "step": {"stepSize": "1"},
        "future": {"timestamp": int((clock() + 1) * 1000)},
    }
    if boundary != "missing":
        await feed.ingest(published(clock, **changes.get(boundary, {})))
    if boundary == "stale":
        clock.now += mon.settings.monitor.max_quote_age_s
    # The legacy stockPrice quote remains present; it cannot substitute for the new source.
    assert "WEN" in mon.backpack.stock_feed.latest
    assert await mon.quote(("WEN.US", FORWARD), coverage=True) is None
    report = mon.snapshot()
    assert report["forward_coverage"]["pricedfresh"] == report["signal_candidates"] == 0
    assert report["rfq_available_assets"] == 0
    mon.jupiter.quote_pair.assert_not_awaited()
    mon.rfq.check.assert_not_awaited()
    mon.rfq.inspect.assert_not_awaited()


@pytest.mark.parametrize("boundary", ["metadata", "mint", "mapping", "price_expiry"])
async def test_fresh_public_result_cannot_survive_metadata_mint_mapping_or_price_expiry(
    indicative_monitor, boundary
):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))
    assert await mon.quote(("WEN.US", FORWARD), coverage=True)
    if boundary == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif boundary == "mint":
        mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], mint=USDC_MINT)
    elif boundary == "mapping":
        mon.chain["WEN.US"] = "unsupported_mint_or_unit_mapping"
    else:
        clock.now += mon.settings.monitor.max_quote_age_s
    report = mon.snapshot()
    assert report["forward_coverage"]["pricedfresh"] == report["signal_candidates"] == 0
    assert not any(price["asset"] == "WEN.US" for price in report["prices"])
    mon.rfq.check.assert_not_awaited()


async def test_another_quantity_account_refusal_does_not_block_the_public_baseline(indicative_monitor):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))
    mon.rfq.latest[("WEN.US", "Bid")] = mon.rfq.result(
        "WEN.US", "Bid", "1000", "US_EQUITIES_REGULAR", "unavailable", "rfq_insufficient_balance"
    )
    row = await mon.quote(("WEN.US", FORWARD), coverage=True)
    assert row and row["backpack_source"] == "indicative" and row["profit_raw"] < 0
    assert mon.snapshot()["forward_coverage"]["pricedfresh"] == 1
    mon.rfq.check.assert_not_awaited()


async def test_positive_public_baseline_is_screening_until_a_sized_candidate_is_confirmed(indicative_monitor):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))

    async def profitable(source, target, amount, **kwargs):
        return {"outAmount": str(amount * 30), "priceImpact": "0.1", "_observed_at": clock()}

    mon.jupiter.quote_pair.side_effect = profitable
    baseline = await mon.quote(("WEN.US", FORWARD), coverage=True)
    assert baseline and baseline["profit_raw"] > 0 and not baseline["backpack_quote_confirmed"]
    assert mon.snapshot()["signal_candidates"] == 0
    mon.rfq.check.assert_not_awaited()
    candidate = await mon.quote(("WEN.US", FORWARD), notional=Decimal("100"))
    assert candidate and candidate["profit_raw"] > 0
    assert candidate["backpack_source"] == "rfq" and candidate["backpack_quote_confirmed"]
    expected = str(ui(candidate["acquired_raw"], candidate["stock_decimals"]))
    mon.rfq.check.assert_awaited_once_with("WEN.US", expected, "US_EQUITIES_REGULAR", side="Bid")
    assert mon.snapshot()["signal_candidates"] == 1


@pytest.mark.parametrize(
    "confirmation",
    [
        "refused", "unknown", "repriced_negative", "wrong_quantity", "wrong_side", "wrong_session",
        "wrong_source", "wrong_symbol", "expired",
    ],
)
async def test_private_confirmation_result_gates_a_positive_public_candidate(indicative_monitor, confirmation):
    mon, clock, feed = indicative_monitor
    await feed.ingest(published(clock))

    async def profitable(source, target, amount, **kwargs):
        return {"outAmount": str(amount * 30), "priceImpact": "0.1", "_observed_at": clock()}

    async def inspect(symbol, side, quantity, session):
        state, reason, price = {
            "refused": ("unavailable", "rfq_insufficient_balance", None),
            "unknown": ("unknown", "rfq_check_timeout", None),
            "repriced_negative": ("available", "rfq_quote_observed", "100"),
        }.get(confirmation, ("available", "rfq_quote_observed", "6.2"))
        check = mon.rfq.result(symbol, side, quantity, session, state, reason, price)
        changes = {
            "wrong_quantity": {"quantity": "1"},
            "wrong_side": {"side": "Ask"},
            "wrong_session": {"session": "US_EQUITIES_OVERNIGHT"},
            "wrong_source": {"source": "indicative"},
            "wrong_symbol": {"symbol": "OTHER.US"},
            "expired": {"retry_at": clock()},
        }
        return replace(
            check, **changes.get(confirmation, {})
        )

    mon.jupiter.quote_pair.side_effect = profitable
    mon.rfq.inspect.side_effect = inspect
    row = await mon.quote(("WEN.US", FORWARD), notional=Decimal("100"))
    mon.rfq.check.assert_awaited_once()
    report = mon.snapshot(include_hidden=True)
    assert report["signal_candidates"] == 0
    assert not any(result["signal_eligible"] for result in report["rows"])
    if confirmation == "repriced_negative":
        assert row and row["backpack_quote_confirmed"] and row["profit_raw"] < 0
        assert row["backpack_price"] == "100"
    elif row:
        assert not row["backpack_quote_confirmed"]
