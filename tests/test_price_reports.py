import httpx
import pytest
import test_telegram
from test_forward_coverage import coverage_monitor
from test_indicative_coverage import published
from test_monitor import asset
from test_rfq_validation import monitored, observed

from backpack_stocks.indicative_feed import IndicativeFeed
from backpack_stocks.jupiter_replay import JupiterReplay, ReplayError
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.monitor import FORWARD, REVERSE, compact_quote
from backpack_stocks.rejection import api_error_metadata
from backpack_stocks.telegram import TelegramAPI
from backpack_stocks.transport import RemoteError

bot = test_telegram.bot
engine = test_telegram.engine


async def test_negative_spread_keeps_fresh_price_count_and_shows_actual_percentages():
    mon, clock = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "90000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    clock.now += 3
    snapshot = mon.snapshot()
    assert snapshot["rows"] == []
    assert snapshot["fresh_quotes"] == snapshot["fresh_calculations"] == 1
    assert snapshot["prices"][0]["quote_age_s"] == 3
    assert snapshot["prices"][0]["backpack_price"] == "6.2"
    assert snapshot["prices"][0]["profit_raw"] < 0
    assert "Спред цены: -9.55%; net: -10.48% (-10.48 USDC)" in mon.text()
    assert "BP 6.2 / Jup" in mon.text()
    assert "УБЫТОК" in mon.prices_text(["WEN.US"])
    assert "Объём: 99.999986 USDC" in mon.text() and "Pool spot" not in mon.text()


async def test_queued_digest_rerender_removes_expired_percentages(bot):
    controller, _ = bot
    controller.queue_replies = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    controller.engine.monitor = mon
    await controller.reply(mon.text(), kind="digest", render=controller.cached_spread_text)
    _, _, original, kind, render = controller.outbox.get_nowait()
    assert kind == "digest" and "net: +" in original and "Спред цены:" in render()
    clock.now += mon.settings.policy.quote_max_age_s
    current = render()
    assert "Свежих полных котировок пока нет" in current
    assert "Спред цены:" not in current and "net:" not in current


async def test_positive_public_digest_requires_rfq_and_never_queues_a_confirmed_call(bot):
    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    mon, clock = coverage_monitor()
    feed = IndicativeFeed(clock=clock)
    feed.tickers.add("WEN")
    mon.indicative_feed = mon.backpack.indicative_feed = feed
    mon.jupiter.quote_pair.side_effect = lambda *args, **kwargs: {
        "outAmount": "2000000",
        "priceImpact": "0.1",
        "_observed_at": clock(),
    }
    try:
        await feed.ingest(published(clock))
        row = await mon.quote(("WEN.US", FORWARD), coverage=True)
        assert row["profit_raw"] > 0 and not row["backpack_quote_confirmed"]
        controller.engine.monitor = mon
        text = mon.text()
        assert "net: +" in text and "нужна своя RFQ на выбранный объём" in text
        assert "СПРЕД НАЙДЕН" not in text and "подтверждена" not in text
        await controller.announce_candidates(mon.snapshot())
        assert controller.outbox.empty() and mon.snapshot()["signal_candidates"] == 0
        mon.rfq.inspect.assert_not_awaited()
    finally:
        await feed.close()


async def test_expired_and_failed_prices_are_display_only_with_timestamp():
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    before = mon.price_history[("WEN.US", FORWARD)]
    clock.now += 9
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    snapshot = mon.snapshot()
    assert not snapshot["rows"] and not snapshot["prices"] and snapshot["fresh_quotes"] == 0
    assert "Спред цены:" not in mon.text() and "net:" not in mon.text()
    catalog = snapshot["price_catalog"][0]
    assert not catalog["fresh"] and catalog["quote_age_s"] == 9
    text = mon.prices_text()
    assert "устарела" in text and "Последний расчёт 05:46:40" in text
    assert "ПРИБЫЛЬ В РАСЧЁТЕ" not in text and "прибыль сейчас не оценена" in text
    assert "RFQ каждые 60с" in text and "до 8с" in text
    mon.jupiter.quote_pair.side_effect = ReplayError("fixture", code="jupiter_transport_error")
    await mon.quote(("WEN.US", FORWARD))
    assert mon.price_history[("WEN.US", FORWARD)] == before
    assert mon.snapshot()["rows"] == [] and mon.snapshot()["prices"] == []


async def test_all_stock_pages_preserve_every_ticker_and_rerender_expiry(bot):
    controller, server = bot
    controller.queue_replies = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    for index in range(67):
        item = asset(f"S{index:02d}.US", deposit=False)
        mon.assets[item.symbol] = item
        mon.chain[item.symbol] = "verified_unit_mapping"
    controller.engine.monitor = mon
    await controller.deliver_command("prices")
    items = [controller.outbox.get_nowait() for _ in range(controller.outbox.qsize())]
    assert 1 < len(items) < len(mon.assets)
    rendered = "\n".join(item[2] for item in items)
    assert all(symbol in rendered for symbol in mon.assets)
    assert all(len(item[2]) <= 3900 for item in items)
    clock.now += 9
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    rerendered = "\n".join(item[4]() for item in items)
    assert "устарела" in rerendered and "RFQ покупка доступна" in rerendered
    assert "S66.US" in rerendered and not server.replies()


async def test_price_catalog_marks_session_change_unconfirmed():
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    mon.backpack.stock_feed.latest["WEN"] = observed(clock).model_copy(update={"session": "PRE_MARKET"})
    catalog = mon.snapshot()["price_catalog"][0]
    assert catalog["rfq_state"] == "unknown" and not catalog["fresh"]


async def test_positive_quote_before_slippage_is_not_presented_as_a_profitable_call():
    mon, _ = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "100200000", "priceImpact": "0.1"}
    row = await mon.quote(("WEN.US", FORWARD))
    assert row and row["quoted_output_raw"] - row["cost_raw"] - row["gas_raw"] > 0
    assert row["profit_raw"] < 0 and mon.snapshot()["signal_candidates"] == 0
    text = mon.prices_text(["WEN.US"])
    assert "УБЫТОК -0.33 USDC" in text and "По quote: +0.17 USDC" in text
    assert "ПРИБЫЛЬ В РАСЧЁТЕ" not in text
    assert "Спред цены: +0.70%; net: -0.33% (-0.33 USDC)" in mon.text()
    assert "ПРИБЫЛЬ В РАСЧЁТЕ" not in mon.text()


async def test_prices_display_both_directions_without_treating_stock_output_as_usdc():
    mon, _ = monitored()
    await mon.quote(("WEN.US", FORWARD))
    mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", REVERSE))
    snapshot = mon.snapshot()
    assert {pair["direction"] for pair in snapshot["prices"]} == {FORWARD, REVERSE}
    reverse = snapshot["price_catalog"][0]["reverse"]
    assert reverse["fresh"] and reverse["quoted_profit_raw"] is None
    assert reverse["buy_quantity"] == reverse["sell_quantity"] == "17.91"
    text = mon.prices_text(["WEN.US"])
    assert "BP → Jup: ПРИБЫЛЬ В РАСЧЁТЕ" in text
    assert "Jup → BP: ПРИБЫЛЬ В РАСЧЁТЕ" in text
    assert "100 USDC → мин 17.91 шт; продать 17.91 шт за 109.251 USDC" in text
    assert "18000000 USDC" not in text
    assert "WEN.US · BP → Jup" in mon.text()
    assert "WEN.US · Jup → BP" in mon.text()
    assert "Продажа на BP после зачисления; цена за время перевода может измениться." in mon.text()
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_missing_reverse_quote_does_not_claim_a_loss_or_profit_in_that_direction():
    mon, _ = monitored()
    await mon.quote(("WEN.US", FORWARD))
    text = mon.prices_text(["WEN.US"])
    assert "Jup → BP: нет актуального расчёта" in text
    assert "Jup → BP: УБЫТОК" not in text and "Jup → BP: ПРИБЫЛЬ" not in text


@pytest.mark.parametrize("direction", [FORWARD, REVERSE])
async def test_spread_call_contains_actual_investment_and_directional_exchange_links(direction):
    from urllib.parse import parse_qs, urlparse

    mon, _ = monitored()
    if direction == REVERSE:
        mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", direction))
    row = mon.snapshot()["rows"][0]
    text = "\n".join(compact_quote(row))
    assert "WEN.US" in text and "Чистый спред: +" in text
    assert "Объём: 100 USDC" in text if direction == REVERSE else "Объём: 99.999986 USDC" in text
    assert "Backpack: https://backpack.exchange/stocks/WEN" in text
    jupiter = next(line.removeprefix("Jupiter: ") for line in text.splitlines() if line.startswith("Jupiter:"))
    query = parse_qs(urlparse(jupiter).query)
    assert query == {
        "sell": [STOCK_MINT if direction == FORWARD else USDC_MINT],
        "buy": [USDC_MINT if direction == FORWARD else STOCK_MINT],
    }
    assert jupiter in mon.prices_text(["WEN.US"])
    assert len(text) < 3900
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_rfq_rejection_has_safe_reason_without_remote_payload():
    mon, _ = monitored()
    mon.backpack.submit_rfq.side_effect = RemoteError(
        "API rejected request: HTTP 400",
        status_code=400,
        api_code="INSUFFICIENT_BALANCE",
        reason="insufficient_balance",
    )
    check = await mon.rfq.check("WEN.US", "16", "REGULAR")
    assert check.reason == "rfq_insufficient_balance" and check.state == "unavailable"
    assert api_error_metadata({"code": "private-secret", "message": "secret=value"}) == {}
    assert api_error_metadata({"code": "INSUFFICIENT_BALANCE", "message": "secret=value"}) == {
        "api_code": "INSUFFICIENT_BALANCE",
        "reason": "insufficient_balance",
    }


async def test_jupiter_failure_logs_http_status_and_fixed_error_code():
    response = httpx.Response(400, json={"error": "COULD_NOT_FIND_ANY_ROUTE"})
    client = JupiterReplay(client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: response)))
    try:
        with pytest.raises(ReplayError) as caught:
            await client.request("GET", "https://api.jup.ag/swap/v2a/order")
        assert caught.value.code == "jupiter_no_route" and caught.value.status_code == 400
    finally:
        await client.close()


@pytest.mark.parametrize("status", [200, 400])
@pytest.mark.parametrize(
    "payload",
    [
        {"error": "Insufficient liquidity", "token": "private-secret"},
        {"error": {"message": "Insufficient liquidity", "token": "private-secret"}},
        {"error": "INSUFFICIENT_LIQUIDITY"},
        {"code": "INSUFFICIENT_LIQUIDITY", "error": "private-secret"},
    ],
)
async def test_jupiter_liquidity_refusal_is_safe_and_never_retried(status, payload):
    requests = []

    def server(request):
        requests.append(request)
        return httpx.Response(status, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
        client = JupiterReplay(client=http, interval=0)
        with pytest.raises(ReplayError) as caught:
            await client.request("GET", client.base + "/order")
        assert caught.value.code == "jupiter_insufficient_liquidity"
        assert caught.value.status_code == status and len(requests) == 1
        assert "private-secret" not in str(caught.value)
        assert "private-secret" not in str(api_error_metadata(payload))


@pytest.mark.parametrize("direction", [FORWARD, REVERSE])
async def test_jupiter_liquidity_refusal_clears_old_call_without_runtime_error(direction, caplog):
    mon, _ = monitored()
    if direction == REVERSE:
        mon.jupiter.quote_pair.return_value = {"outAmount": "18000000", "priceImpact": "0.1"}
    key = ("WEN.US", direction)
    assert await mon.quote(key)
    assert mon.snapshot()["rows"]
    mon.jupiter.quote_pair.side_effect = ReplayError(
        "HTTP 400", code="jupiter_insufficient_liquidity", status_code=400
    )
    assert await mon.quote(key) is None
    assert key not in mon.results and not mon.snapshot()["rows"]
    assert mon.errors["quote"] == 0
    assert mon.blocked["jupiter_insufficient_liquidity"] == 1
    assert not mon.quote_failures[key] and key not in mon.retry_after
    assert "Jupiter: не хватает ликвидности на этот объём" in mon.prices_text(["WEN.US"])
    assert "quote_error" not in [record.message for record in caplog.records]
    mon.backpack.accept_rfq.assert_not_awaited()


async def test_telegram_long_price_text_never_silently_truncates():
    sent = []

    def server(request):
        import json

        sent.append(json.loads(request.content)["text"])
        return httpx.Response(200, json={"ok": True, "result": True})

    api = TelegramAPI("123456:unit-test-token-with-enough-length", transport=httpx.MockTransport(server))
    api.limiter.interval = 0
    api.limiter.categories["sendMessage"] = 0
    text = "\n".join(f"S{i:03d}.US " + "x" * 150 for i in range(68))
    try:
        await api.reply(123456, text)
        assert "\n".join(sent) == text and all(len(part) <= 3900 for part in sent)
    finally:
        await api.close()
