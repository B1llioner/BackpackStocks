import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_monitor import Clock, observed
from test_monitor import monitor as monitor_fixture

from backpack_stocks.models import dec
from backpack_stocks.monitor import FORWARD, REVERSE
from backpack_stocks.rfq_validation import RfqValidator
from backpack_stocks.transport import RemoteError, UnknownOutcome


def setup(clock):
    request = None

    def submit(quantity, client_id, *, side, symbol):
        nonlocal request
        request = {
            "rfqId": "123",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "clientId": client_id,
            "executionMode": "AwaitAccept",
            "expiryTime": int((clock() + 2.5) * 1000),
            "status": "New",
        }
        return request

    async def records(*args, **kwargs):
        return [
            {
                "rfq": request,
                "quotes": [
                    {"rfqId": "123", "status": "New", "askPrice": "6.2", "bidPrice": "6.1"},
                    {"rfqId": "123", "status": "New", "askPrice": "6.3", "bidPrice": "6.0"},
                    {"rfqId": "foreign", "status": "New", "askPrice": "1", "bidPrice": "100"},
                    {"rfqId": "123", "status": "Expired", "askPrice": "1", "bidPrice": "100"},
                ],
            }
        ]

    bp = SimpleNamespace(
        auth=object(),
        submit_rfq=AsyncMock(side_effect=submit),
        rfqs=AsyncMock(side_effect=records),
        cancel_rfq=AsyncMock(),
        accept_rfq=AsyncMock(),
    )
    validator = RfqValidator(bp, max_age=8, retry_s=120, timeout=5, clock=clock)
    return bp, validator


async def test_exact_size_cache_best_prices_and_only_await_accept():
    clock = Clock()
    bp, validator = setup(clock)
    first, duplicate = await asyncio.gather(
        validator.check("WEN.US", "2", "REGULAR"), validator.check("WEN.US", "2.0", "REGULAR")
    )
    assert first == duplicate and first.price == "6.2" and first.usable(clock())
    bp.submit_rfq.assert_awaited_once()
    await validator.check("WEN.US", "3", "REGULAR")
    assert bp.submit_rfq.await_count == 2
    sell = await validator.check("WEN.US", "3", "REGULAR", side="Ask")
    assert sell.price == "6.1" and bp.submit_rfq.await_count == 3
    assert bp.cancel_rfq.await_count == 3
    bp.accept_rfq.assert_not_called()


async def test_early_exact_refresh_keeps_old_price_usable_and_coalesces_concurrent_requests():
    clock = Clock()
    bp, validator = setup(clock)
    old = await validator.check("WEN.US", "0.16", "REGULAR")
    clock.now += 1
    entered, release = asyncio.Event(), asyncio.Event()
    records = bp.rfqs.side_effect

    async def held_records(*args, **kwargs):
        entered.set()
        await release.wait()
        result = await records(*args, **kwargs)
        result[0]["quotes"][0]["askPrice"] = "12"
        result[0]["quotes"][1]["askPrice"] = "13"
        return result

    bp.rfqs.side_effect = held_records
    first = asyncio.create_task(validator.check("WEN.US", "0.16", "REGULAR", force_refresh=True))
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert validator.cached_exact("WEN.US", "0.16", "REGULAR") is old
        assert old.usable(clock()) and old.retry_at == old.checked_at + validator.max_age
        second = asyncio.create_task(validator.check("WEN.US", "0.160", "REGULAR", force_refresh=True))
        await asyncio.sleep(0)
        release.set()
        renewed, duplicate = await asyncio.gather(first, second)
        assert renewed is duplicate and renewed is not old
        assert renewed.price == "12" and renewed.quantity == "0.16" and renewed.side == "Bid"
        assert renewed.retry_at == renewed.checked_at + validator.max_age
        assert validator.cached_exact("WEN.US", "0.16", "REGULAR") is renewed
        assert await validator.check("WEN.US", "0.16", "REGULAR") is renewed
        assert bp.submit_rfq.await_count == bp.cancel_rfq.await_count == 2
        assert [call.args[0] for call in bp.submit_rfq.await_args_list] == ["0.16", "0.16"]
        assert all(call.kwargs["side"] == "Bid" for call in bp.submit_rfq.await_args_list)
        bp.accept_rfq.assert_not_called()
    finally:
        for task in (first, second):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)


@pytest.mark.parametrize("state,reason", [
    ("unavailable", "rfq_insufficient_balance"),
    ("unavailable", "rfq_no_quote"),
    ("unknown", "rfq_check_timeout"),
])
async def test_forced_refresh_does_not_bypass_same_quantity_cooldown(state, reason):
    clock = Clock()
    bp, validator = setup(clock)
    refused = validator.result("WEN.US", "Bid", "0.16", "REGULAR", state, reason)
    validator.cache[("WEN.US", "Bid", "0.16", "REGULAR")] = refused
    validator.latest[("WEN.US", "Bid")] = refused
    clock.now += 1
    assert await validator.check("WEN.US", "0.16", "REGULAR", force_refresh=True) is refused
    bp.submit_rfq.assert_not_called()
    bp.accept_rfq.assert_not_called()


@pytest.mark.parametrize("reason,allowed", [
    ("rfq_insufficient_balance", True),
    ("rfq_invalid_quantity", True),
    ("rfq_no_quote", True),
    ("rfq_session_unavailable", False),
])
async def test_forced_refresh_preserves_other_quantity_refusal_rules(reason, allowed):
    clock = Clock()
    bp, validator = setup(clock)
    refused = validator.result("WEN.US", "Bid", "1", "REGULAR", "unavailable", reason)
    validator.latest[("WEN.US", "Bid")] = refused
    result = await validator.check("WEN.US", "0.16", "REGULAR", force_refresh=True)
    if allowed:
        assert result.quantity == "0.16" and result.usable(clock())
        bp.submit_rfq.assert_awaited_once()
        bp.cancel_rfq.assert_awaited_once()
    else:
        assert result is refused
        bp.submit_rfq.assert_not_called()
    bp.accept_rfq.assert_not_called()


async def test_expired_query_is_unknown_and_expired_cancellation_is_expected(caplog):
    clock = Clock()
    bp, validator = setup(clock)
    bp.rfqs.side_effect = RemoteError("API rejected request: HTTP 400", status_code=400, reason="rfq_expired")
    bp.cancel_rfq.side_effect = RemoteError(
        "API rejected request: HTTP 400", status_code=400, reason="rfq_expired"
    )
    result = await validator.check("BLK.US", "1", "POST_MARKET")
    assert result.state == "unknown" and result.reason == "rfq_expired_before_check"
    assert not result.usable(clock())
    bp.accept_rfq.assert_not_called()
    assert "rfq_cancel_failed" not in caplog.text


async def test_inspection_covers_the_entire_short_acceptance_window(monkeypatch):
    clock = Clock()
    bp, validator = setup(clock)
    records = bp.rfqs.side_effect

    async def late(*args, **kwargs):
        result = await records(*args, **kwargs)
        if clock() < 10001.25:
            result[0]["quotes"] = []
        return result

    async def advance(seconds):
        clock.now += seconds

    bp.rfqs.side_effect = late
    monkeypatch.setattr("backpack_stocks.rfq_validation.asyncio.sleep", advance)
    result = await validator.check("WEN.US", "1", "REGULAR")
    assert result.usable(clock()) and bp.rfqs.await_count == 6
    bp.accept_rfq.assert_not_called()


async def test_no_quote_means_unavailable_until_cooldown_then_rechecked(monkeypatch):
    clock = Clock()
    bp, validator = setup(clock)
    bp.rfqs.return_value = []
    bp.rfqs.side_effect = None

    async def advance(seconds):
        clock.now += seconds

    monkeypatch.setattr("backpack_stocks.rfq_validation.asyncio.sleep", advance)
    result = await validator.check("PTN.US", "112", "PRE_MARKET")
    assert result.state == "unavailable" and result.reason == "rfq_no_quote"
    assert await validator.check("PTN.US", "112.0", "PRE_MARKET") == result
    bp.submit_rfq.assert_awaited_once()
    assert validator.blocked("PTN.US", "PRE_MARKET") and not validator.blocked("PTN.US", "REGULAR")
    smaller = await validator.check("PTN.US", "111", "PRE_MARKET")
    assert smaller.quantity == "111" and smaller.state == "unavailable"
    assert bp.submit_rfq.await_count == 2
    clock.now += 121
    assert not validator.blocked("PTN.US", "PRE_MARKET")
    await validator.check("PTN.US", "112", "PRE_MARKET")
    assert bp.submit_rfq.await_count == 3
    assert bp.cancel_rfq.await_count == 3
    bp.accept_rfq.assert_not_called()


@pytest.mark.parametrize("failure", ["auth", "network", "timeout", "mismatch"])
async def test_missing_or_failed_evidence_is_unknown_and_never_a_positive(failure):
    clock = Clock()
    bp, validator = setup(clock)
    if failure == "auth":
        bp.auth = None
    elif failure == "network":
        bp.submit_rfq.side_effect = ValueError("private-remote-body")
    elif failure == "timeout":
        validator.timeout = 0.001

        async def held(*args, **kwargs):
            await asyncio.sleep(10)

        bp.rfqs.side_effect = held
    else:
        records = bp.rfqs.side_effect

        async def mismatch(*args, **kwargs):
            result = await records(*args, **kwargs)
            result[0]["rfq"] = {**result[0]["rfq"], "quantity": "99"}
            return result

        bp.rfqs.side_effect = mismatch
    result = await validator.check("WEN.US", "1", "REGULAR")
    assert result.state == "unknown" and not result.usable(clock())
    if failure == "timeout":
        assert result.reason == "rfq_check_timeout"
    assert "private-remote-body" not in str(result.public(clock()))
    if failure in {"timeout", "mismatch"}:
        bp.cancel_rfq.assert_awaited_once_with("123")
    bp.accept_rfq.assert_not_called()


async def test_success_is_not_reused_after_age_session_or_metadata_change():
    clock = Clock()
    bp, validator = setup(clock)
    result = await validator.check("WEN.US", "1", "REGULAR")
    clock.now += 8
    assert not result.usable(clock()) and result.public(clock())["state"] == "available"
    assert result.eligible(clock())
    await validator.check("WEN.US", "1", "REGULAR")
    await validator.check("WEN.US", "1", "PRE_MARKET")
    validator.invalidate("WEN.US")
    await validator.check("WEN.US", "1", "PRE_MARKET")
    assert bp.submit_rfq.await_count == 4


@pytest.mark.parametrize("ambiguous", [False, True])
async def test_rejected_submit_has_cooldown_but_unknown_outcome_is_never_retried(ambiguous):
    clock = Clock()
    bp, validator = setup(clock)
    bp.submit_rfq.side_effect = (
        UnknownOutcome("lost response") if ambiguous else RemoteError("API rejected request: HTTP 400")
    )
    first = await validator.check("WEN.US", "1", "REGULAR")
    second = await validator.check("WEN.US", "1", "REGULAR")
    assert first == second and not first.usable(clock())
    assert first.state == ("unknown" if ambiguous else "unavailable")
    assert first.retry_at == clock() + 60
    bp.submit_rfq.assert_awaited_once()
    bp.accept_rfq.assert_not_called()


async def test_reverse_price_cannot_be_reused_for_a_changed_sell_quantity():
    from dataclasses import replace

    mon, _ = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    assert mon.snapshot()["rows"][0]["fresh"]
    mon.assets["WEN.US"] = replace(
        mon.assets["WEN.US"],
        sessions=(
            {"name": "US_EQUITIES_REGULAR", "stepSize": "1", "minQuantity": "1", "maxQuantity": "1000"},
        ),
    )
    assert not any(r["fresh"] for r in mon.snapshot()["rows"])


def monitored():
    mon, clock = monitor_fixture.__wrapped__()
    bp, validator = setup(clock)
    mon.backpack.auth = bp.auth
    for name in ("submit_rfq", "rfqs", "cancel_rfq", "accept_rfq"):
        setattr(mon.backpack, name, getattr(bp, name))
    mon.rfq = validator
    mon.settings.monitor.rfq_validation = True
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    return mon, clock


async def test_monitor_resizes_to_real_ask_and_never_replaces_it_with_fmv():
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    row = mon.snapshot()["rows"][0]
    assert row["backpack_quote_confirmed"] and row["backpack_price"] == "6.2"
    assert row["cost_raw"] <= 100_000_000 and row["acquired_raw"] == 16_129_030
    assert mon.backpack.submit_rfq.await_count == 2
    mon.backpack.stock_feed.latest["WEN"] = observed(clock, ask="1")
    assert mon.snapshot()["rows"][0]["cost_raw"] == row["cost_raw"]
    assert "BP 6.2 / Jup" in mon.prices_text(["WEN.US"])
    mon.backpack.accept_rfq.assert_not_called()
    clock.now += 8
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    assert mon.snapshot()["rows"] == []
    assert mon.snapshot()["positive_price_spreads"] == mon.snapshot()["candidates"] == 0


async def test_each_direction_requires_its_own_rfq_and_unavailable_sides_are_excluded():
    mon, clock = monitored()
    original = mon.backpack.rfqs.side_effect

    async def expired(*args, **kwargs):
        records = await original(*args, **kwargs)
        records[0]["rfq"]["status"] = "Expired"
        records[0]["quotes"] = []
        return records

    mon.backpack.rfqs.side_effect = expired
    mon.prices_at = clock()
    mon.prices = {
        mon.assets["WEN.US"].mint: {"usdPrice": "7"},
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {"usdPrice": "1"},
    }
    await mon.quote(("WEN.US", FORWARD))
    await mon.quote(("WEN.US", REVERSE))
    snapshot = mon.snapshot()
    assert snapshot["rows"] == [] and snapshot["rfq_unavailable_assets"] == 1
    assert snapshot["positive_price_spreads"] == snapshot["screened_assets"] == snapshot["candidates"] == 0
    assert mon.choose() is None
    assert "Backpack не дал RFQ на этот объём" in mon.text("WEN.US")
    mon.jupiter.quote_pair.assert_awaited_once()
    assert mon.backpack.submit_rfq.await_count == 2
    assert [call.kwargs["side"] for call in mon.backpack.submit_rfq.call_args_list] == ["Bid", "Ask"]
    mon.backpack.accept_rfq.assert_not_called()


async def test_reverse_requires_its_own_sell_quote_and_uses_best_bid():
    mon, _ = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    row = mon.snapshot()["rows"][0]
    assert row["direction"] == REVERSE and row["backpack_price"] == "6.1"
    assert row["proceeds_raw"] == int(dec("17.91") * dec("6.1") * 10**6)
    assert mon.backpack.submit_rfq.call_args.kwargs["side"] == "Ask"
    assert mon.backpack.submit_rfq.call_args.args[0] == "17.91"
    mon.backpack.accept_rfq.assert_not_called()


async def test_negative_net_is_hidden_even_with_positive_spot_and_valid_rfq():
    mon, _ = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "95000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    assert mon.results[("WEN.US", FORWARD)]["profit_raw"] < 0
    assert not mon.snapshot()["rows"]
    assert "net -" not in mon.text() and "Pool spot" not in mon.text()
    assert any(d["reasons"] == ["nonpositive_net"] for d in mon.snapshot()["diagnostics"])


async def test_background_eligibility_rechecks_after_60_seconds_and_outlives_price_cache():
    mon, clock = monitored()
    mon.refresh_external_prices = AsyncMock()
    mon.discovery_ready.set()
    task = asyncio.create_task(mon.rfq_loop())
    try:
        async with asyncio.timeout(2):
            while not mon.rfq_ready(mon.assets["WEN.US"], "US_EQUITIES_REGULAR"):
                await asyncio.sleep(0.01)
        calls = mon.backpack.submit_rfq.await_count
        clock.now += 15
        mon.backpack.stock_feed.latest["WEN"] = observed(clock)
        assert mon.rfq_ready(mon.assets["WEN.US"], "US_EQUITIES_REGULAR")
        assert mon.choose() is not None
        await asyncio.sleep(0.55)
        assert mon.backpack.submit_rfq.await_count == calls
        assert mon.snapshot()["rows"] == []
        clock.now += 45
        mon.backpack.stock_feed.latest["WEN"] = observed(clock)
        assert not mon.rfq_ready(mon.assets["WEN.US"], "US_EQUITIES_REGULAR")
        async with asyncio.timeout(2):
            while mon.backpack.submit_rfq.await_count == calls:
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    mon.backpack.accept_rfq.assert_not_called()


@pytest.mark.parametrize("boundary", ["dex", "rfq_price", "eligibility", "session", "negative"])
async def test_fast_report_revalidates_all_inputs_without_any_network(boundary):
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    assert mon.report()["fresh_calculations"] == 1
    assert mon.report()["rows"][0]["backpack_quote_confirmed"]
    mon.refresh_metadata = AsyncMock(side_effect=AssertionError("no metadata reads"))
    mon.refresh_prices = AsyncMock(side_effect=AssertionError("no price reads"))
    mon.jupiter.quote_pair.reset_mock()
    mon.backpack.submit_rfq.reset_mock()
    if boundary == "dex":
        mon.results[("WEN.US", FORWARD)]["dex_observed_at"] -= 9
    elif boundary == "rfq_price":
        clock.now += 8
    elif boundary == "eligibility":
        clock.now += 61
    elif boundary == "session":
        mon.backpack.stock_feed.latest["WEN"].session = "OVERNIGHT"
    else:
        mon.results[("WEN.US", FORWARD)]["proceeds_raw"] = 1
    report = mon.report("WEN.US")
    assert report["report_source"] == "background_cache"
    assert not report["rows"]
    assert not any("profit_raw" in row for row in report["diagnostics"])
    mon.refresh_metadata.assert_not_called()
    mon.refresh_prices.assert_not_called()
    mon.jupiter.quote_pair.assert_not_called()
    mon.backpack.submit_rfq.assert_not_called()


async def test_selected_refresh_is_prioritized_and_does_not_bypass_rfq_denial():
    from dataclasses import replace

    from test_monitor import asset

    mon, clock = monitored()
    mon.assets["COLD.US"] = asset("COLD.US", deposit=False)
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], deposit_enabled=False)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["COLD"] = observed(clock)
    await mon.refresh_rfq_asset(mon.assets["WEN.US"])
    await mon.refresh_rfq_asset(mon.assets["COLD.US"])
    mon.estimate = lambda a, direction=None: dec("1000") if a.symbol == "WEN.US" else dec("-1000")
    assert mon.choose() == ("WEN.US", FORWARD)
    assert mon.request_refresh("COLD.US")
    assert mon.choose() == ("COLD.US", FORWARD)
    mon.rfq.latest[("COLD.US", "Bid")] = mon.rfq.result(
        "COLD.US", "Bid", "1", "US_EQUITIES_REGULAR", "unavailable", "rfq_no_quote"
    )
    assert not mon.request_refresh("COLD.US")
    assert mon.choose() == ("WEN.US", FORWARD)


async def test_reverse_rechecks_refused_sell_before_spending_another_jupiter_request():
    mon, clock = monitored()
    old = mon.rfq.result("WEN.US", "Ask", "17.91", "US_EQUITIES_REGULAR", "unavailable", "rfq_no_quote")
    mon.rfq.latest[("WEN.US", "Ask")] = old
    clock.now += 61
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    original = mon.backpack.rfqs.side_effect

    async def reject_sell(*args, **kwargs):
        records = await original(*args, **kwargs)
        if records[0]["rfq"]["side"] == "Ask":
            records[0]["rfq"]["status"] = "Expired"
            records[0]["quotes"] = []
        return records

    mon.backpack.rfqs.side_effect = reject_sell
    await mon.quote(("WEN.US", REVERSE))
    mon.jupiter.quote_pair.assert_not_called()
    assert mon.rfq.latest[("WEN.US", "Ask")].state == "unavailable"
    mon.backpack.accept_rfq.assert_not_called()


async def test_reverse_requires_a_new_exact_sell_price_when_only_buy_eligibility_is_cached():
    mon, clock = monitored()
    await mon.refresh_rfq_asset(mon.assets["WEN.US"])
    clock.now += 9  # Buy price has expired, but the independent eligibility is valid.
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    mon.backpack.submit_rfq.reset_mock()
    mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    mon.backpack.submit_rfq.assert_awaited_once()
    assert mon.backpack.submit_rfq.call_args.kwargs["side"] == "Ask"
    assert mon.backpack.submit_rfq.call_args.args[0] == "17.91"
    assert mon.report()["rows"][0]["backpack_price"] == "6.1"
    mon.backpack.accept_rfq.assert_not_called()


async def test_rfq_buy_failure_does_not_hide_an_independent_profitable_sell_direction():
    mon, _ = monitored()
    original = mon.backpack.rfqs.side_effect

    async def reject_buy(*args, **kwargs):
        records = await original(*args, **kwargs)
        if records[0]["rfq"]["side"] == "Bid":
            records[0]["rfq"]["status"] = "Expired"
            records[0]["quotes"] = []
        return records

    mon.backpack.rfqs.side_effect = reject_buy
    assert await mon.quote(("WEN.US", FORWARD)) is None
    await mon.refresh_rfq_asset(mon.assets["WEN.US"])
    assert mon.rfq.latest[("WEN.US", "Ask")].state == "available"
    assert mon.choose() == ("WEN.US", REVERSE)
    assert not mon.rfq_cooldown(mon.assets["WEN.US"], REVERSE)
    mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    row = await mon.quote(("WEN.US", REVERSE))
    assert row and row["profit_raw"] == 9_191_000
    snapshot = mon.snapshot()
    assert snapshot["rfq_available_assets"] == 0 and snapshot["signal_candidates"] == 1
    assert [pair["direction"] for pair in snapshot["prices"]] == [REVERSE]
    assert "Jup → BP" in mon.text() and "после зачисления" in mon.text()
    # Ask availability probe and the later independent exact-size sale price.
    assert [call.kwargs["side"] for call in mon.backpack.submit_rfq.call_args_list] == ["Bid", "Ask", "Ask"]
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_sell_rfq_refusal_does_not_block_buy_rfq_direction():
    mon, _ = monitored()
    await mon.refresh_rfq_asset(mon.assets["WEN.US"])
    mon.rfq.latest[("WEN.US", "Ask")] = mon.rfq.result(
        "WEN.US", "Ask", "1", "US_EQUITIES_REGULAR", "unavailable", "rfq_insufficient_balance"
    )
    assert mon.rfq_cooldown(mon.assets["WEN.US"], REVERSE)
    assert not mon.rfq_cooldown(mon.assets["WEN.US"], FORWARD)
    assert mon.choose() == ("WEN.US", FORWARD)
    row = await mon.quote(("WEN.US", FORWARD))
    assert row and mon.snapshot()["signal_candidates"] == 1
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_expired_nonbinding_rfq_is_a_price_block_not_a_runtime_failure():
    mon, _ = monitored()
    mon.backpack.rfqs.side_effect = RemoteError(
        "API rejected request: HTTP 400", status_code=400, reason="rfq_expired"
    )
    assert await mon.quote(("WEN.US", FORWARD)) is None
    assert mon.blocked["rfq_expired_before_check"] == 1 and not mon.errors["quote"]
    assert mon.snapshot()["signal_candidates"] == 0 and not mon.snapshot()["prices"]
    mon.jupiter.quote_pair.assert_not_awaited()
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_a_failing_hot_route_backs_off_and_other_stocks_keep_running():
    from dataclasses import replace

    from test_monitor import asset

    from backpack_stocks.jupiter_replay import ReplayError

    mon, clock = monitored()
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], deposit_enabled=False)
    mon.assets["COLD.US"] = asset("COLD.US", deposit=False)
    mon.chain["COLD.US"] = "verified_unit_mapping"
    mon.backpack.stock_feed.latest["COLD"] = observed(clock)
    await mon.refresh_rfq_asset(mon.assets["COLD.US"])
    mon.jupiter.quote_pair.side_effect = ReplayError("fixture transport error")
    await mon.quote(("WEN.US", FORWARD))
    assert mon.retry_after[("WEN.US", FORWARD)] > clock()
    assert not mon.request_refresh("WEN.US")
    assert mon.choose() == ("COLD.US", FORWARD)
    assert "Повтор запроса WEN.US" in mon.text("WEN.US")
    assert not mon.report("WEN.US")["rows"]
    clock.now = mon.retry_after[("WEN.US", FORWARD)]
    for ticker in ("WEN", "COLD"):
        mon.backpack.stock_feed.latest[ticker] = observed(clock)
    assert mon.request_refresh("WEN.US")
    assert mon.choose() == ("WEN.US", FORWARD)
    mon.jupiter.quote_pair.side_effect = None
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    assert ("WEN.US", FORWARD) not in mon.retry_after
    assert mon.report("WEN.US")["rows"][0]["fresh"]
