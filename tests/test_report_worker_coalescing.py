import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_telegram import bot as telegram_bot_fixture
from test_telegram import engine as telegram_engine_fixture

from backpack_stocks.models import Settings
from backpack_stocks.monitor import FORWARD
from backpack_stocks.telegram import report_worker

bot = telegram_bot_fixture
engine = telegram_engine_fixture


@pytest.fixture
def fast_start(monkeypatch):
    original = asyncio.sleep
    started = False

    async def sleep(delay, *args, **kwargs):
        nonlocal started
        if delay == 1 and not started:
            started = True
            return await original(0)
        return await original(delay, *args, **kwargs)

    monkeypatch.setattr("backpack_stocks.telegram.asyncio.sleep", sleep)


async def wait_until(predicate, timeout=0.5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


def reports(tmp_path, *, alerts=False):
    settings = Settings(database=str(tmp_path / "state.sqlite3"), telegram_spread_alerts=alerts)
    settings.monitor.rfq_validation = True
    monitor = SimpleNamespace(updated=asyncio.Event(), results={}, forward_coverage={}, last_report={})
    monitor.snapshot = Mock(
        side_effect=lambda: {
            "observed_at": time.time(),
            "fresh_calculations": 0,
            "rows": list(monitor.results.values()),
            "completed_quotes": 0,
            "fresh_quotes": 0,
            "candidates": 0,
            "errors": {},
            "blocked": {},
            "ws_connected": True,
            "ws_reconnects": 0,
            "rate_limits": {"backpack": {}, "rpc": {}},
        }
    )
    monitor.text = Mock(return_value="current prices")
    monitor.report = Mock(side_effect=AssertionError("digest must reuse its fresh snapshot"))
    store = SimpleNamespace(get_meta=Mock(return_value=None))
    instance = SimpleNamespace(
        engine=SimpleNamespace(
            settings=settings,
            monitor=monitor,
            store=store,
            status=lambda: {"active": None, "cycles": []},
        ),
        pending_alerts=set(),
        clock=lambda: 10000,
        announce_candidates=AsyncMock(),
        reply=AsyncMock(),
        cached_spread_text=lambda: "current prices",
    )
    return instance, monitor


def positive(**changes):
    row = {
        "asset": "WEN.US",
        "direction": FORWARD,
        "acquired_raw": 160000,
        "input_raw": 159999,
        "backpack_source": "rfq",
        "backpack_quote_confirmed": True,
        "backpack_timestamp": 10000,
        "backpack_price": "6.2",
        "dex_observed_at": 10000,
        "dex_bank_versions": [["pool", "stock", "USDC", 1, 10000]],
        "profit_raw": 100,
        "cost_raw": 992000,
        "passes_model_limits": False,
        "trading_supported": True,
        "size_feasible": True,
    }
    row.update(changes)
    return row


async def test_dirty_bursts_coalesce_and_monotonic_deadline_rechecks_without_updates(
    tmp_path,
    fast_start,
    monkeypatch,
):
    controller, monitor = reports(tmp_path)
    wall = [10000.0]
    monkeypatch.setattr("backpack_stocks.telegram.time.time", lambda: wall[0])
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        for _ in range(30):
            monitor.updated.set()
            await asyncio.sleep(0.002)
        assert monitor.snapshot.call_count == 1
        monitor.report.assert_not_called()
        assert monitor.last_report["report_source"] == "background_cache"
        assert controller.reply.await_count == 1
        # A wall-clock rewind does not postpone or multiply the one-second
        # recalculation; no new event is needed to age out existing prices.
        wall[0] = 9990
        await wait_until(lambda: monitor.snapshot.call_count == 2, timeout=1.2)
        assert controller.announce_candidates.await_count == 2
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize(
    "changes",
    [
        {"acquired_raw": 320000},
        {"backpack_timestamp": 10001},
        {"backpack_source": "spot"},
        {"dex_bank_versions": [["pool", "stock", "USDC", 2, 10000]]},
        {"profit_raw": 101},
        {"passes_model_limits": True},
    ],
)
async def test_new_positive_exact_inputs_or_qualification_bypass_normal_deadline(
    tmp_path, fast_start, changes
):
    controller, monitor = reports(tmp_path, alerts=True)
    key = "WEN.US", FORWARD
    monitor.results[key] = positive()
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        monitor.results[key] = positive(**changes)
        monitor.updated.set()
        await wait_until(lambda: monitor.snapshot.call_count == 2, timeout=0.2)
        assert controller.announce_candidates.await_args.args[0]["rows"][0] == monitor.results[key]
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("kind", ["public", "cooldown", "unchanged"])
async def test_nonactionable_or_suppressed_positive_updates_do_not_force_heavy_reports(
    tmp_path, fast_start, kind
):
    controller, monitor = reports(tmp_path, alerts=True)
    key = "WEN.US", FORWARD
    monitor.results[key] = positive(
        **({"backpack_source": "indicative", "backpack_quote_confirmed": False} if kind == "public" else {})
    )
    if kind == "cooldown":
        controller.engine.store.get_meta.return_value = "10000"
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        for index in range(20):
            if kind != "unchanged":
                monitor.results[key] = {**monitor.results[key], "backpack_timestamp": 10001 + index}
            monitor.updated.set()
            await asyncio.sleep(0.002)
        assert monitor.snapshot.call_count == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_real_confirmed_positive_queues_immediately_and_delivery_still_rechecks_expiry(bot, fast_start):
    from test_rfq_validation import monitored

    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    controller.engine.settings.monitor.rfq_validation = True
    monitor, clock = monitored()
    monitor.snapshot = Mock(wraps=monitor.snapshot)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        await monitor.quote(("WEN.US", FORWARD))
        await wait_until(lambda: monitor.snapshot.call_count == 2, timeout=0.2)
        queued = []
        while not controller.outbox.empty():
            queued.append(controller.outbox.get_nowait())
            controller.outbox.task_done()
        alert = next(item for item in queued if item[3] == "alert")
        assert alert[0] == 5 and alert[4]() is not None
        clock.now += monitor.settings.monitor.max_quote_age_s + 1
        assert alert[4]() is None
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_monitor_and_prices_commands_recalculate_immediately_inside_background_interval(
    bot, fast_start
):
    from test_rfq_validation import monitored

    controller, _ = bot
    controller.queue_replies = True
    monitor, _ = monitored()
    await monitor.quote(("WEN.US", FORWARD))
    monitor.snapshot = Mock(wraps=monitor.snapshot)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        await controller.dispatch("monitor")
        assert monitor.snapshot.call_count == 2
        await controller.dispatch("prices")
        assert monitor.snapshot.call_count == 3
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_duplicate_owned_and_general_positive_versions_do_not_repeat_urgent_reports(
    tmp_path, fast_start
):
    controller, monitor = reports(tmp_path, alerts=True)
    key = "WEN.US", FORWARD
    monitor.results[key] = positive(dex_bank_versions=[["pool", "stock", "USDC", 2, 10000]])
    monitor.forward_coverage[key[0]] = SimpleNamespace(result=positive())
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        for _ in range(20):
            monitor.updated.set()
            await asyncio.sleep(0.002)
        assert monitor.snapshot.call_count == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_disk_deadline_forces_a_current_snapshot_inside_the_normal_interval(
    tmp_path, fast_start, monkeypatch
):
    import json

    controller, monitor = reports(tmp_path)
    wall = [10000.0]
    monkeypatch.setattr("backpack_stocks.telegram.time.time", lambda: wall[0])
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        wall[0] = 10010.1
        monitor.updated.set()
        await wait_until(lambda: monitor.snapshot.call_count == 2, timeout=0.2)
        recorded = json.loads((tmp_path / "direct-monitor.json").read_text())
        assert recorded["observed_at"] == wall[0]
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_negative_spot_row_turns_positive_on_new_book_and_queues_immediately(bot, fast_start):
    from test_monitor import monitor as monitor_fixture
    from test_stock_books import close_rfq, depth_row, setup_books

    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    controller.engine.settings.monitor.rfq_validation = True
    monitor, clock = monitor_fixture.__wrapped__()
    await setup_books(monitor, clock)
    close_rfq(monitor, clock)
    monitor.jupiter.quote_pair.return_value = {
        "outAmount": "99000000",
        "priceImpact": "0.1",
        "_observed_at": clock(),
    }
    await monitor.quote(("WEN.US", FORWARD))
    original = monitor.results[("WEN.US", FORWARD)]
    assert original["backpack_source"] == "spot" and original["profit_raw"] == -1_520_548
    monitor.snapshot = Mock(wraps=monitor.snapshot)
    monitor.revalue_book = Mock(wraps=monitor.revalue_book)
    controller.engine.monitor = monitor
    worker = asyncio.create_task(report_worker(controller))
    try:
        await wait_until(lambda: monitor.snapshot.call_count == 1)
        assert not controller.pending_alerts
        clock.now += 0.01
        monitor.backpack.depth.return_value = depth_row(clock, asks=[["5.92", "30"]])
        await monitor.books.refresh("WEN.US")
        monitor.jupiter.quote_pair.reset_mock()
        monitor.backpack.max_order_quantity.reset_mock()
        monitor.rfq.check.reset_mock()
        monitor.updated.set()
        await wait_until(lambda: monitor.snapshot.call_count == 2, timeout=0.2)
        assert controller.pending_alerts
        assert monitor.results[("WEN.US", FORWARD)] is original and original["profit_raw"] < 0
        revalues = monitor.revalue_book.call_count
        for _ in range(20):
            monitor.updated.set()
            await asyncio.sleep(0.002)
        assert monitor.revalue_book.call_count == revalues
        assert monitor.snapshot.call_count == 2
        queued = []
        while not controller.outbox.empty():
            queued.append(controller.outbox.get_nowait())
            controller.outbox.task_done()
        alert = next(item for item in queued if item[3] == "alert")
        assert alert[0] == 5 and alert[4]() is not None
        current = monitor.snapshot()["rows"][0]
        assert current["signal_eligible"] and current["profit_raw"] == 458_629
        assert current["dex_observed_at"] == original["dex_observed_at"]
        clock.now += monitor.settings.monitor.max_quote_age_s + 1
        assert alert[4]() is None
        monitor.jupiter.quote_pair.assert_not_called()
        monitor.backpack.max_order_quantity.assert_not_called()
        monitor.rfq.check.assert_not_called()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
