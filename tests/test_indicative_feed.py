import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from test_monitor import Clock

from backpack_stocks.indicative_feed import IndicativeFeed, IndicativeUnavailable


def packet(clock, **updates):
    return {
        "e": "stockIndicativeQuote",
        "symbol": "BRK.B",
        "bid": "330.1",
        "ask": "332.22",
        "estimatedBid": "330.94",
        "estimatedAsk": "331.39",
        "session": "US_EQUITIES_OVERNIGHT",
        "minQuantity": "1",
        "maxQuantity": "1000",
        "stepSize": "1",
        "reason": None,
        "timestamp": int(clock() * 1000),
        **updates,
    }


def feed_fixture():
    clock = Clock()
    feed = IndicativeFeed(clock=clock)
    feed.tickers.add("BRK.B")
    return feed, clock


async def test_limit_sides_estimates_quantity_rules_and_original_publish_time():
    feed, clock = feed_fixture()
    await feed.ingest({"data": packet(clock, timestamp=int((clock() - 1) * 1000))})
    buy = feed.get("BRK.B.US", "Bid", "2", "US_EQUITIES_OVERNIGHT", 15, clock())
    sell = feed.get("BRK.B", "Ask", "1000", "US_EQUITIES_OVERNIGHT", 15, clock())
    assert buy.symbol == "BRK.B.US" and buy.price == "332.22" and buy.estimated_price == "331.39"
    assert sell.price == "330.1" and sell.estimated_price == "330.94"
    assert buy.published_at == clock() - 1 and buy.received_at == clock()
    assert buy.quantity_rules == {"minQuantity": "1", "maxQuantity": "1000", "stepSize": "1"}
    assert feed.latest["BRK.B"].min_quantity == Decimal(1)
    assert "repriced on RFQ" in buy.source
    await feed.close()


@pytest.mark.parametrize("quantity", ["0.5", "1.5", "1001"])
async def test_quantity_must_fit_public_session_rules(quantity):
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    with pytest.raises(IndicativeUnavailable, match="indicative_invalid_quantity"):
        feed.get("BRK.B", "Bid", quantity, "US_EQUITIES_OVERNIGHT", 15)
    await feed.close()


async def test_null_side_and_reason_invalidate_old_limits_and_notify():
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    version = feed.version
    waiter = asyncio.create_task(feed.wait(version))
    clock.now += 1
    await feed.ingest(packet(clock, ask=None, estimatedAsk=None))
    await asyncio.wait_for(waiter, 1)
    assert feed.changed.is_set()
    with pytest.raises(IndicativeUnavailable, match="indicative_no_quote"):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    assert feed.get("BRK.B", "Ask", "1", "US_EQUITIES_OVERNIGHT", 15).price == "330.1"
    clock.now += 1
    await feed.ingest(packet(clock, reason="noActiveSession", session=None, minQuantity=None))
    with pytest.raises(IndicativeUnavailable, match="noActiveSession"):
        feed.get("BRK.B", "Ask", "1", "US_EQUITIES_OVERNIGHT", 15)
    assert feed.latest["BRK.B"].bid is None and feed.latest["BRK.B"].ask is None
    await feed.close()


@pytest.mark.parametrize(
    "updates",
    [
        {"ask": "NaN"},
        {"ask": 332.22},
        {"bid": "333"},
        {"estimatedAsk": "-1"},
        {"stepSize": "0"},
        {"maxQuantity": "0"},
        {"minQuantity": "1001"},
        {"session": None},
        {"timestamp": True},
        {"timestamp": 10000001},
    ],
)
async def test_invalid_current_payload_cannot_keep_a_previous_valid_price(updates):
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    await feed.ingest(packet(clock, **updates))
    with pytest.raises(IndicativeUnavailable):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    assert feed.latest["BRK.B"].ask is None and feed.invalid_messages == 1
    await feed.close()


async def test_source_expiry_and_session_mismatch_fail_even_with_recent_receipt():
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    with pytest.raises(IndicativeUnavailable, match="indicative_session_mismatch"):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_REGULAR", 15)
    clock.now += 15
    await feed.ingest(packet(clock, timestamp=10000000))
    assert feed.latest["BRK.B"].received_at == clock()
    with pytest.raises(IndicativeUnavailable, match="indicative_quote_stale"):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    await feed.close()


async def test_older_update_cannot_replace_newer_unavailability_or_price():
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    clock.now += 1
    await feed.ingest(packet(clock, reason="stalePrice"))
    await feed.ingest(packet(clock, timestamp=10000000))
    with pytest.raises(IndicativeUnavailable, match="stalePrice"):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    await feed.close()


async def test_future_invalid_update_does_not_poison_subsequent_current_price():
    feed, clock = feed_fixture()
    await feed.ingest(packet(clock))
    await feed.ingest(packet(clock, timestamp=int((clock() + 60) * 1000)))
    with pytest.raises(IndicativeUnavailable):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    clock.now += 1
    await feed.ingest(packet(clock))
    assert feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15).price == "332.22"
    await feed.ingest(packet(clock, timestamp=10**400))
    with pytest.raises(IndicativeUnavailable):
        feed.get("BRK.B", "Bid", "1", "US_EQUITIES_OVERNIGHT", 15)
    await feed.close()


async def test_subscriptions_keep_class_share_dots_use_one_public_socket_and_close(monkeypatch):
    clock = Clock()
    feed = IndicativeFeed(clock=clock)
    running = asyncio.Event()

    async def receive():
        await running.wait()

    monkeypatch.setattr(feed._feed, "receive", receive)
    await feed.subscribe(["BRK.B.US", "AAPL.US"])
    task = feed._feed.task
    await feed.subscribe(["BRK.B", "AAPL"])
    assert feed._feed.task is task and feed.tickers == {"BRK.B", "AAPL"}
    socket = type("Socket", (), {"send": AsyncMock()})()
    await feed._feed.send_subscription(socket, ["AAPL", "BRK.B"])
    body = json.loads(socket.send.await_args.args[0])
    assert body == {
        "method": "SUBSCRIBE",
        "params": ["stockIndicativeQuote.AAPL", "stockIndicativeQuote.BRK.B"],
    }
    assert not set(body) & {"signature", "headers", "apiKey"}
    await feed.close()
    assert task.done() and feed._feed.task is None
