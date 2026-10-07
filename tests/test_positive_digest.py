import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from test_forward_coverage import coverage_monitor
from test_indicative_coverage import published
from test_report_worker_coalescing import fast_start as fast_start_fixture
from test_report_worker_coalescing import wait_until
from test_rfq_validation import monitored
from test_telegram import bot as bot_fixture
from test_telegram import engine as engine_fixture

from backpack_stocks.indicative_feed import IndicativeFeed
from backpack_stocks.monitor import FORWARD
from backpack_stocks.spread_reporting import dex_price
from backpack_stocks.telegram import report_worker

bot = bot_fixture
engine = engine_fixture
fast_start = fast_start_fixture


def exact_output(cost: int, gas: int, profit: int, slippage_bps: int) -> dict:
    minimum = cost + gas + profit
    multiplier = 10000 - slippage_bps
    return {
        "outAmount": str((minimum * 10000 + multiplier - 1) // multiplier),
        "otherAmountThreshold": str(minimum),
        "priceImpact": "0.1",
    }


async def priced_monitor(profit: int):
    monitor, clock = monitored()
    monitor.jupiter.quote_pair.return_value = exact_output(
        99_999_986, 30_000, profit, monitor.settings.policy.slippage_bps
    )
    result = await monitor.quote(("WEN.US", FORWARD))
    assert result and result["profit_raw"] == profit
    return monitor, clock


def prohibit_network(monitor):
    monitor.refresh_report = AsyncMock(side_effect=AssertionError("render cannot refresh the network"))
    monitor.jupiter.quote_pair.reset_mock()
    monitor.backpack.submit_rfq.reset_mock()


def assert_no_network(monitor):
    monitor.refresh_report.assert_not_called()
    monitor.jupiter.quote_pair.assert_not_called()
    monitor.backpack.submit_rfq.assert_not_called()


async def stop(worker):
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("profit", [-2_000_000, 0, -500_000])
async def test_automatic_digest_is_quiet_for_nonpositive_net_even_if_gross_is_positive(
    bot, fast_start, profit
):
    controller, _ = bot
    controller.queue_replies = True
    monitor, _ = await priced_monitor(profit)
    pair = monitor.snapshot()["prices"][0]
    if profit == -500_000:
        assert dex_price(pair) > Decimal(pair["backpack_price"])
    assert pair["profit_raw"] == profit
    prohibit_network(monitor)
    monitor.snapshot = Mock(wraps=monitor.snapshot)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count >= 1)
        assert controller.outbox.empty()
        assert controller.cached_positive_spread_text() is None
        manual = controller.cached_spread_text()
        assert "Положительных свежих спредов" in manual
        assert "Спред цены:" not in manual and "net:" not in manual
        assert_no_network(monitor)
    finally:
        await stop(worker)


async def test_positive_net_below_alert_threshold_still_queues_a_digest(bot, fast_start):
    controller, _ = bot
    controller.queue_replies = True
    monitor, _ = await priced_monitor(100_000)
    assert monitor.results[("WEN.US", FORWARD)]["passes_model_limits"] is False
    prohibit_network(monitor)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: not controller.outbox.empty())
        item = controller.outbox.get_nowait()
        controller.outbox.task_done()
        assert item[3] == "digest" and "net: +0.10%" in item[2]
        assert "СПРЕД НАЙДЕН" not in item[2] and not controller.pending_alerts
        assert item[4]() is not None
        assert_no_network(monitor)
    finally:
        await stop(worker)


async def test_public_positive_digest_requires_own_rfq_and_cannot_be_a_confirmed_alert(bot, fast_start):
    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    monitor, clock = coverage_monitor()
    public = IndicativeFeed(clock=clock)
    public.tickers.add("WEN")
    monitor.indicative_feed = monitor.backpack.indicative_feed = public
    monitor.jupiter.quote_pair.side_effect = None
    monitor.jupiter.quote_pair.return_value = exact_output(
        992_000, 30_000, 10_000, monitor.settings.policy.slippage_bps
    )
    worker = None
    try:
        await public.ingest(published(clock))
        row = await monitor.quote(("WEN.US", FORWARD), coverage=True)
        assert row and row["profit_raw"] == 10_000 and not row["backpack_quote_confirmed"]
        monitor.refresh_report = AsyncMock(side_effect=AssertionError("digest cannot refresh quotes"))
        monitor.jupiter.quote_pair.reset_mock()
        monitor.rfq.inspect.reset_mock()
        controller.engine.monitor = monitor
        worker = asyncio.create_task(report_worker(controller))
        await wait_until(lambda: not controller.outbox.empty())
        item = controller.outbox.get_nowait()
        controller.outbox.task_done()
        assert item[3] == "digest" and "net: +" in item[2]
        assert "нужна своя RFQ на выбранный объём" in item[2]
        assert "СПРЕД НАЙДЕН" not in item[2] and "подтверждена" not in item[2]
        assert not controller.pending_alerts and monitor.snapshot()["signal_candidates"] == 0
        assert item[4]() is not None
        monitor.rfq.inspect.assert_not_called()
        monitor.refresh_report.assert_not_called()
        monitor.jupiter.quote_pair.assert_not_called()
    finally:
        if worker is not None:
            await stop(worker)
        await public.close()


@pytest.mark.parametrize("change", ["negative", "zero", "expired"])
async def test_queued_positive_digest_is_dropped_when_current_net_or_prices_stop_qualifying(
    bot, fast_start, change
):
    controller, server = bot
    controller.queue_replies = True
    monitor, clock = await priced_monitor(100_000)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    sender = None
    try:
        await wait_until(lambda: not controller.outbox.empty())
        await stop(worker)
        if change == "expired":
            clock.now += monitor.settings.policy.quote_max_age_s
        else:
            profit = -100_000 if change == "negative" else 0
            monitor.volume_search.samples[("WEN.US", FORWARD)].clear()
            monitor.jupiter.quote_pair.return_value = exact_output(
                99_999_986, 30_000, profit, monitor.settings.policy.slippage_bps
            )
            result = await monitor.quote(("WEN.US", FORWARD))
            assert result["profit_raw"] == profit
        prohibit_network(monitor)
        assert controller.cached_positive_spread_text() is None
        sender = asyncio.create_task(controller.send_replies())
        await asyncio.wait_for(controller.outbox.join(), 0.5)
        assert not server.replies()
        assert_no_network(monitor)
    finally:
        await stop(worker)
        if sender is not None:
            await stop(sender)


@pytest.mark.parametrize("change", ["expired", "nonpositive"])
async def test_digest_rechecks_current_positive_price_after_telegram_throttle_before_http_submission(
    bot, change
):
    controller, server = bot
    controller.queue_replies = True
    monitor, clock = await priced_monitor(100_000)
    controller.engine.monitor = monitor
    text = controller.cached_positive_spread_text()
    assert text is not None
    await controller.reply(text, kind="digest", render=controller.cached_positive_spread_text)
    entered, release = asyncio.Event(), asyncio.Event()

    async def throttle(*args, **kwargs):
        entered.set()
        await release.wait()

    controller.api.limiter.acquire = AsyncMock(side_effect=throttle)
    sender = asyncio.create_task(controller.send_replies())
    try:
        await asyncio.wait_for(entered.wait(), 0.5)
        if change == "expired":
            clock.now += monitor.settings.policy.quote_max_age_s
        else:
            monitor.volume_search.samples[("WEN.US", FORWARD)].clear()
            monitor.jupiter.quote_pair.return_value = exact_output(
                99_999_986, 30_000, 0, monitor.settings.policy.slippage_bps
            )
            row = await monitor.quote(("WEN.US", FORWARD))
            assert row["profit_raw"] == 0
        prohibit_network(monitor)
        release.set()
        await asyncio.wait_for(controller.outbox.join(), 0.5)
        assert not server.replies()
        assert not any(event["kind"] == "telegram_reply_sent" for event in controller.engine.store.events())
        assert_no_network(monitor)
    finally:
        release.set()
        await stop(sender)
