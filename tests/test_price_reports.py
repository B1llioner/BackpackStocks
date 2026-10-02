import httpx
import pytest
import test_telegram
from test_monitor import asset
from test_rfq_validation import monitored, observed

from backpack_stocks.jupiter_replay import JupiterReplay, ReplayError
from backpack_stocks.monitor import FORWARD
from backpack_stocks.rejection import api_error_metadata
from backpack_stocks.telegram import TelegramAPI
from backpack_stocks.transport import RemoteError

bot = test_telegram.bot
engine = test_telegram.engine


async def test_negative_spread_keeps_fresh_price_count_and_hides_negative_profit():
    mon, clock = monitored()
    mon.jupiter.quote_pair.return_value = {"outAmount": "90000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    clock.now += 3
    snapshot = mon.snapshot()
    assert snapshot["rows"] == []
    assert snapshot["fresh_quotes"] == snapshot["fresh_calculations"] == 1
    assert snapshot["prices"][0]["quote_age_s"] == 3
    assert snapshot["prices"][0]["backpack_price"] == "6.2"
    assert "profit_raw" not in snapshot["prices"][0] and "edge_bps" not in snapshot["prices"][0]
    assert "1 свежих" in mon.text() and "BP 6.2 / Jup" in mon.text()
    assert "net -" not in mon.text() and "Pool spot" not in mon.text()


async def test_expired_and_failed_prices_are_display_only_with_timestamp():
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    before = mon.price_history[("WEN.US", FORWARD)]
    clock.now += 9
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    snapshot = mon.snapshot()
    assert not snapshot["rows"] and not snapshot["prices"] and snapshot["fresh_quotes"] == 0
    catalog = snapshot["price_catalog"][0]
    assert not catalog["fresh"] and catalog["quote_age_s"] == 9
    text = mon.prices_text()
    assert "устарела" in text and "BP 05:46:40 / DEX 05:46:40" in text
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
    assert len(items) == 3
    rendered = "\n".join(item[2] for item in items)
    assert all(symbol in rendered for symbol in mon.assets)
    assert all(len(item[2]) <= 3900 for item in items)
    clock.now += 9
    mon.backpack.stock_feed.latest["WEN"] = observed(clock)
    rerendered = "\n".join(item[4]() for item in items)
    assert "устарела" in rerendered and "RFQ доступна" in rerendered
    assert "S66.US" in rerendered and not server.replies()


async def test_price_catalog_marks_session_change_unconfirmed():
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    mon.backpack.stock_feed.latest["WEN"] = observed(clock).model_copy(update={"session": "PRE_MARKET"})
    catalog = mon.snapshot()["price_catalog"][0]
    assert catalog["rfq_state"] == "unknown" and not catalog["fresh"]


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
