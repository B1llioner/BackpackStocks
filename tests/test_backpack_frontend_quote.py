from decimal import Decimal

from test_monitor import monitor as monitor_fixture

from backpack_stocks.monitor import FORWARD, quote_details


async def test_external_estimate_never_qualifies_as_a_confirmed_backpack_spread():
    mon, clock = monitor_fixture.__wrapped__()
    mon.backpack.stock_feed.latest.clear()
    mon.external_at = clock()
    mon.external = {"WEN.US": "6"}
    # A calendar is required to size a REST estimate; provide this test's active session.
    mon.quantity_session = lambda observation: "US_EQUITIES_REGULAR"
    mon.jupiter.quote_pair.return_value = {"outAmount": "120000000", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    row = next(r for r in mon.snapshot()["rows"] if r["direction"] == FORWARD)
    assert row["fresh"] and Decimal(row["edge_bps"]) > mon.settings.policy.min_edge_bps
    assert row["backpack_price"] == "6" and not row["backpack_quote_confirmed"]
    assert not row["candidate"] and not row["net_profitable"]
    assert row["candidate_blocks"] == ["backpack_rfq_unconfirmed"]
    assert "Оценка покупки:" in "\n".join(quote_details(row))
    assert "нет подтверждённой цены RFQ Backpack" in mon.text()
