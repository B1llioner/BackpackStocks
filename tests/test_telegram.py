import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from backpack_stocks.engine import Engine
from backpack_stocks.environment import load_environment
from backpack_stocks.market import PaperBroker, ReplayMarket
from backpack_stocks.models import Policy, Settings
from backpack_stocks.store import Store
from backpack_stocks.telegram import COMMANDS, TelegramAPI, TelegramController, TelegramError, credentials

USER = 471057654
TOKEN = "123456:fake-token-used-only-in-unit-tests"


class Server:
    def __init__(self):
        self.calls = []
        self.backlog = []
        self.menu_fails = 0
        self.webhook = ""

    def __call__(self, request):
        method = request.url.path.rsplit("/", 1)[-1]
        payload = json.loads(request.content)
        self.calls.append((method, payload))
        if method == "getMe":
            result = {"id": 123456, "is_bot": True, "username": "test_stocks_bot"}
        elif method == "getWebhookInfo":
            result = {"url": self.webhook}
        elif method == "getUpdates":
            result = self.backlog
        elif method == "setMyCommands" and self.menu_fails:
            self.menu_fails -= 1
            return httpx.Response(400, json={"ok": False, "error_code": 400, "description": "chat not found"})
        else:
            result = True
        return httpx.Response(200, json={"ok": True, "result": result})

    def replies(self):
        return [p for method, p in self.calls if method == "sendMessage"]


def update(identity=1, text="/status", date=10000):
    return {
        "update_id": identity,
        "message": {
            "message_id": identity,
            "date": date,
            "from": {"id": USER, "is_bot": False},
            "chat": {"id": USER, "type": "private"},
            "text": text,
        },
    }


@pytest.fixture
def engine(tmp_path):
    replay = tmp_path / "replay.json"
    replay.write_text(json.dumps({"frames": [{"offset_s": 0, "ask": "6", "dex_bid": "6.3"}]}))
    settings = Settings(
        database=str(tmp_path / "state.sqlite3"),
        replay_file=str(replay),
        policy=Policy(withdrawal_semantics="deduct"),
    )
    store = Store(settings.database, settings)
    store.set_meta("stopped", "1")
    market = ReplayMarket(settings, store, clock=lambda: 10000)
    yield Engine(settings, store, market, PaperBroker(settings, market), clock=lambda: 10000)
    store.close()


@pytest.fixture
async def bot(engine):
    server = Server()
    api = TelegramAPI(TOKEN, transport=httpx.MockTransport(server))
    controller = TelegramController(engine, api, USER, clock=lambda: 10000)
    await controller.initialize()
    try:
        yield controller, server
    finally:
        await controller.close()


async def test_watch_command_preserves_public_address_and_owner_authorization(bot):
    controller, server = bot
    address = "EdsdsfTxpN64YcDBFf3HDwGwoVDfKyMZux38GhXnqTUA"
    journal = SimpleNamespace(add=Mock())
    controller.engine.wallet_watch = SimpleNamespace(journal=journal)
    await controller.handle(update(text="/watch " + address))
    journal.add.assert_called_once_with(address)
    assert address in server.replies()[-1]["text"]
    rejected = update(identity=2, text="/watch " + address)
    rejected["message"]["from"]["id"] = USER + 1
    await controller.handle(rejected)
    assert journal.add.call_count == 1


async def test_delivery_callback_waits_for_actual_send_and_reports_outbox_drop(bot):
    controller, _ = bot
    controller.queue_replies = True
    delivered = []
    await controller.reply("wallet transaction", kind="wallet", on_result=delivered.append)
    assert delivered == []
    worker = asyncio.create_task(controller.send_replies())
    try:
        await asyncio.wait_for(controller.outbox.join(), 1)
        assert delivered == [True]
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    dropped = []
    for _ in range(controller.outbox.maxsize):
        await controller.reply("wallet transaction", priority=5, kind="wallet", on_result=dropped.append)
    await controller.reply("read command", priority=0)
    assert dropped == [False]


@pytest.mark.parametrize(
    "bad",
    [
        "other_user",
        "spoofed_username",
        "group",
        "wrong_chat",
        "bot",
        "forward",
        "sender_chat",
        "string_user",
        "string_chat",
        "edited",
        "channel",
        "missing_author",
    ],
)
async def test_unauthorized_messages_are_silent_and_cannot_resume(bot, bad):
    controller, server = bot
    incoming = update(text="/resume")
    message = incoming["message"]
    if bad in {"other_user", "spoofed_username"}:
        message["from"] = {"id": USER + 1, "is_bot": False, "username": str(USER)}
    elif bad in {"group", "channel"}:
        message["chat"]["type"] = bad
    elif bad == "wrong_chat":
        message["chat"]["id"] += 1
    elif bad == "bot":
        message["from"]["is_bot"] = True
    elif bad == "forward":
        message["forward_origin"] = {"type": "user"}
    elif bad == "sender_chat":
        message["sender_chat"] = {"id": USER}
    elif bad == "string_user":
        message["from"]["id"] = str(USER)
    elif bad == "string_chat":
        message["chat"]["id"] = str(USER)
    elif bad == "edited":
        incoming["edited_message"] = incoming.pop("message")
    elif bad == "missing_author":
        del message["from"]
    await controller.handle(incoming)
    assert controller.engine.status()["stopped"]
    assert server.replies() == []


async def test_private_controls_and_start_help(bot):
    controller, server = bot
    await controller.handle(update(text="/start"))
    assert controller.engine.status()["stopped"]
    await controller.handle(update(2, "/resume@test_stocks_bot"))
    assert not controller.engine.status()["stopped"]
    await controller.handle(update(3, "/stop"))
    assert controller.engine.status()["stopped"]
    assert all(p["chat_id"] == USER and p["protect_content"] for p in server.replies())
    assert all("parse_mode" not in p for p in server.replies())


@pytest.mark.parametrize(
    "command,expected",
    [
        ("/help", "/resume"),
        ("/status", "paper"),
        ("/spread", "ориентировочный"),
        ("/balances", "ledger"),
        ("/cycles", "Циклов нет"),
        ("/events", "Событий нет"),
    ],
)
async def test_read_commands_do_not_start_trades(bot, command, expected):
    controller, server = bot
    await controller.handle(update(text=command))
    assert expected in server.replies()[-1]["text"]
    assert controller.engine.store.cycles() == []
    assert controller.engine.status()["stopped"]


async def test_cached_reports_do_not_request_network_or_delay_stop(bot):
    controller, _ = bot
    controller.queue_replies = True
    monitor = SimpleNamespace(
        assets={},
        requested={},
        pending={},
        report=Mock(return_value={}),
        refresh_report=AsyncMock(side_effect=AssertionError("network must stay in background")),
        text=lambda *a, **kw: "fresh cache",
    )
    controller.engine.monitor = monitor
    await controller.engine.resume()
    await asyncio.wait_for(controller.handle(update(1, "/spreads")), 0.5)
    await asyncio.wait_for(controller.handle(update(2, "/spreads")), 0.5)
    await asyncio.wait_for(controller.handle(update(3, "/stop")), 0.5)
    assert controller.engine.status()["stopped"]
    assert controller.report_task is None
    replies = [controller.outbox.get_nowait()[2] for _ in range(3)]
    assert replies[:2] == ["fresh cache", "fresh cache"]
    assert "остановлен" in replies[2]
    monitor.refresh_report.assert_not_called()
    assert controller.engine.store.cycles() == []


async def test_status_returns_while_trade_and_account_are_waiting(bot):
    controller, _ = bot
    controller.queue_replies = True
    entered, release = asyncio.Event(), asyncio.Event()

    async def balances():
        entered.set()
        await release.wait()
        return {}

    controller.engine.market.backpack = SimpleNamespace(
        auth=object(), balances=AsyncMock(side_effect=balances), account=AsyncMock(return_value={})
    )
    account = asyncio.create_task(controller.handle(update(1, "/account")))
    try:
        await asyncio.wait_for(entered.wait(), 0.5)
        async with controller.engine.lock:
            await asyncio.wait_for(controller.handle(update(2, "/status")), 0.5)
            assert "Режим: paper" in controller.outbox.get_nowait()[2]
            await asyncio.wait_for(controller.handle(update(3, "/account")), 0.5)
            assert "уже обновляются" in controller.outbox.get_nowait()[2]
        assert not account.done()
        release.set()
        await account
        controller.engine.market.backpack.balances.assert_awaited_once()
    finally:
        account.cancel()
        await asyncio.gather(account, return_exceptions=True)


async def test_control_order_and_idempotency_survive_concurrent_commands(bot):
    controller, _ = bot
    controller.queue_replies = True
    async with controller.engine.lock:
        resume = asyncio.create_task(controller.handle(update(1, "/resume")))
        await asyncio.sleep(0)
        stop = asyncio.create_task(controller.handle(update(2, "/stop")))
        await asyncio.sleep(0)
        await asyncio.wait_for(controller.handle(update(3, "/status")), 0.5)
        assert controller.engine.store.idempotent("telegram:123456:1") == {"consumed": True}
        assert controller.engine.store.idempotent("telegram:123456:2") == {"consumed": True}
    await asyncio.gather(resume, stop)
    assert controller.engine.status()["stopped"]
    await controller.handle(update(1, "/resume"))
    assert controller.engine.status()["stopped"]


async def test_outbox_prioritizes_commands_and_coalesces_digests(bot):
    controller, _ = bot
    controller.queue_replies = True
    await controller.reply("old digest", priority=10, kind="digest")
    await controller.reply("new digest", priority=10, kind="digest")
    assert controller.outbox.qsize() == 1
    for i in range(25):
        await controller.reply(f"alert {i}", priority=5, kind="alert")
    await controller.reply("status")
    await controller.reply("stop")
    assert controller.outbox.qsize() == 20
    assert [controller.outbox.get_nowait()[2] for _ in range(2)] == ["status", "stop"]


async def test_sender_refreshes_reports_and_discards_expired_alerts(bot):
    controller, _ = bot
    controller.queue_replies = True
    controller.api.reply = AsyncMock()
    await controller.reply("expired opportunity", priority=5, kind="alert", render=lambda: None)
    await controller.reply("old report", render=lambda: "current report")
    worker = asyncio.create_task(controller.send_replies())
    try:
        await asyncio.wait_for(controller.outbox.join(), 0.5)
        controller.api.reply.assert_awaited_once_with(USER, "current report")
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_selected_stock_followup_reuses_an_existing_pending_request(bot):
    controller, _ = bot
    controller.queue_replies = True
    key = ("FLWS.US", "backpack_to_jupiter")
    monitor = SimpleNamespace(
        assets={"FLWS.US": None},
        requested={},
        pending={key: object()},
        attempted={key: 9999},
        updated=asyncio.Event(),
        report=Mock(return_value={}),
        request_refresh=Mock(return_value=True),
        text=lambda *a, **kw: "fresh FLWS.US",
        refresh_report=AsyncMock(),
    )
    controller.engine.monitor = monitor
    await controller.handle(update(1, "/spread FLWS"))
    assert "приоритетной очереди" in controller.outbox.get_nowait()[2] or controller.report_task is not None
    await asyncio.sleep(0)
    monitor.pending.clear()
    monitor.updated.set()
    await asyncio.wait_for(controller.report_task, 0.5)
    assert controller.outbox.get_nowait()[2] == "fresh FLWS.US"
    monitor.refresh_report.assert_not_called()


async def test_digest_worker_never_blocks_on_full_network_refresh(bot):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD
    from backpack_stocks.telegram import report_worker

    controller, _ = bot
    controller.queue_replies = True
    mon, _ = monitored()
    await mon.quote(("WEN.US", FORWARD))
    mon.refresh_report = AsyncMock(side_effect=AssertionError("no network work in digests"))
    controller.engine.monitor = mon
    worker = asyncio.create_task(report_worker(controller))
    try:
        await asyncio.sleep(1.1)
        assert controller.outbox.qsize() == 1
        item = controller.outbox.get_nowait()
        assert item[3] == "digest" and "Котировки: 1 свежих" in item[2]
        assert "Режим:" not in item[2] and "rfqFilled" not in item[2]
        mon.refresh_report.assert_not_called()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_queued_alert_disappears_after_its_rfq_or_dex_price_expires(bot):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD

    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    controller.engine.monitor = mon
    await controller.announce_candidates(mon.snapshot())
    assert controller.outbox.qsize() == 1
    _, _, text, _, render = controller.outbox.get_nowait()
    assert "СПРЕД НАЙДЕН" in text and render() is not None
    clock.now += 8
    assert render() is None


async def test_alert_cooldown_starts_only_after_delivery_and_expired_alert_can_retry(bot):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD

    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    controller.engine.monitor = mon
    snapshot = mon.snapshot()
    await controller.announce_candidates(snapshot)
    await controller.announce_candidates(snapshot)
    assert controller.outbox.qsize() == 1
    key = "spread_alert:WEN.US:backpack_to_jupiter"
    assert controller.engine.store.get_meta(key) is None
    clock.now += 8
    worker = asyncio.create_task(controller.send_replies())
    try:
        await asyncio.wait_for(controller.outbox.join(), 1)
        assert not controller.pending_alerts and controller.engine.store.get_meta(key) is None
        assert any(e["kind"] == "spread_alert_dropped" for e in controller.engine.store.events())
        mon.backpack.stock_feed.latest["WEN"].timestamp = clock()
        await mon.quote(("WEN.US", FORWARD))
        await controller.announce_candidates(mon.snapshot())
        await asyncio.wait_for(controller.outbox.join(), 1)
        assert controller.engine.store.get_meta(key) is not None
        assert any(e["kind"] == "spread_alert_delivered" for e in controller.engine.store.events())
        sent = next(e for e in controller.engine.store.events() if e["kind"] == "telegram_reply_sent")
        assert sent["body"]["kind"] == "alert"
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_signal_expiring_inside_telegram_throttle_never_reaches_send_message(bot):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD

    controller, server = bot
    controller.engine.settings.telegram_spread_alerts = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    controller.engine.monitor = mon

    async def throttle(**kwargs):
        clock.now += 8

    controller.api.limiter.acquire = AsyncMock(side_effect=throttle)
    await controller.announce_candidates(mon.snapshot())
    assert not server.replies()
    assert controller.engine.store.get_meta("spread_alert:WEN.US:backpack_to_jupiter") is None
    assert any(e["kind"] == "spread_alert_dropped" for e in controller.engine.store.events())
    assert not any(e["kind"] == "spread_alert_delivered" for e in controller.engine.store.events())


@pytest.mark.parametrize("output, eligible", [("1008000000", True), ("1004000000", False)])
async def test_small_conservative_profit_alerts_without_changing_execution_thresholds(bot, output, eligible):
    from test_rfq_validation import monitored

    from backpack_stocks.models import dec
    from backpack_stocks.monitor import FORWARD

    controller, server = bot
    controller.engine.settings.telegram_spread_alerts = True
    mon, _ = monitored()
    mon.settings.monitor.notional_usdc = "1000"
    mon.settings.monitor.auto_size = True
    mon.settings.policy.min_profit_usdc = "5"
    mon.settings.policy.min_edge_bps = 50
    mon.jupiter.quote_pair.return_value = {"outAmount": output, "priceImpact": "0.9"}
    await mon.quote(("WEN.US", FORWARD))
    controller.engine.monitor = mon
    snapshot = mon.snapshot()
    row = mon.snapshot(include_hidden=True)["rows"][0]
    assert row["quoted_output_raw"] - row["cost_raw"] - row["gas_raw"] > 0
    assert not row["candidate"] and not row["net_profitable"]
    assert row["signal_eligible"] is eligible
    assert snapshot["signal_candidates"] == int(eligible) and snapshot["candidates"] == 0
    await controller.announce_candidates(snapshot)
    if eligible:
        assert 0 < row["profit_raw"] < 5_000_000 and dec(row["edge_bps"]) < 50
        assert {"profit_below_minimum", "edge_below_minimum"} <= set(row["candidate_blocks"])
        assert "СПРЕД НАЙДЕН" in server.replies()[-1]["text"]
        assert "Автоисполнение" in server.replies()[-1]["text"]
        assert controller.current_alert("WEN.US", FORWARD) is not None
    else:
        assert row["profit_raw"] <= 0
        assert not server.replies() and controller.current_alert("WEN.US", FORWARD) is None
    assert mon.settings.policy.min_profit_usdc == "5" and mon.settings.policy.min_edge_bps == 50
    mon.backpack.accept_rfq.assert_not_called()


@pytest.mark.parametrize("invalid", ["rfq", "dex", "unconfirmed", "session", "chain", "metadata", "slippage"])
async def test_new_informational_alert_gate_still_rejects_invalid_data(bot, invalid):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD

    controller, server = bot
    controller.engine.settings.telegram_spread_alerts = True
    mon, clock = monitored()
    await mon.quote(("WEN.US", FORWARD))
    row = mon.results[("WEN.US", FORWARD)]
    if invalid == "rfq":
        row["backpack_rfq_valid_until"] = clock()
    elif invalid == "dex":
        row["dex_observed_at"] = clock() - 9
    elif invalid == "unconfirmed":
        row["backpack_quote_confirmed"] = False
    elif invalid == "session":
        mon.backpack.stock_feed.latest["WEN"].session = "OVERNIGHT"
    elif invalid == "chain":
        mon.chain["WEN.US"] = "unverified"
    elif invalid == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    else:
        row["slippage_bps"] = mon.settings.policy.slippage_bps + 1
    controller.engine.monitor = mon
    assert mon.snapshot()["signal_candidates"] == 0
    assert not mon.snapshot(include_hidden=True)["rows"][0]["signal_eligible"]
    await controller.announce_candidates(mon.snapshot())
    assert not server.replies() and controller.current_alert("WEN.US", FORWARD) is None


async def test_explicit_signal_rejection_cannot_fall_back_to_candidate_flag(bot):
    controller, server = bot
    controller.engine.settings.telegram_spread_alerts = True
    await controller.announce_candidates(
        {
            "rows": [
                {
                    "fresh": True,
                    "candidate": True,
                    "net_profitable": True,
                    "profit_raw": 1,
                    "signal_eligible": False,
                }
            ]
        }
    )
    assert not server.replies()


@pytest.mark.parametrize("suppression", ["cooldown", "pending"])
async def test_top_five_suppressed_signals_do_not_hide_other_fresh_signals(bot, suppression):
    from test_rfq_validation import monitored

    from backpack_stocks.monitor import FORWARD

    controller, _ = bot
    controller.queue_replies = True
    controller.engine.settings.telegram_spread_alerts = True
    mon, _ = monitored()
    await mon.quote(("WEN.US", FORWARD))
    rows = [{**mon.snapshot()["rows"][0], "asset": f"STOCK{i}.US"} for i in range(6)]
    for row in rows[:5]:
        key = f"spread_alert:{row['asset']}:{FORWARD}"
        if suppression == "cooldown":
            controller.engine.store.set_meta(key, str(controller.clock()))
        else:
            controller.pending_alerts.add(key)
    await controller.announce_candidates({"rows": rows})
    assert controller.outbox.qsize() == 1
    assert "STOCK5.US" in controller.outbox.get_nowait()[2]


def test_monitor_status_distinguishes_signal_count_and_closed_market(bot):
    from backpack_stocks.telegram import status_text

    controller, _ = bot
    controller.engine.settings.monitor.auto_size = True
    controller.engine.monitor = SimpleNamespace(
        snapshot=lambda: {
            "universe": 70,
            "rfq_available_assets": 0,
            "fresh_quotes": 0,
            "signal_candidates": 0,
            "candidates": 0,
            "rfq_session_state": "closed",
            "rfq_next_open_at": 1791158400,
        }
    )
    text = status_text(controller.engine)
    assert "сигналов сейчас: 0" in text
    assert "RFQ-сессия закрыта" in text and "05.10 03:00 МСК" in text


@pytest.mark.parametrize(
    "text", ["/resume --live", "/resume FLWS", "/resume@another_bot", "/exec rm", "/live", "/withdraw"]
)
async def test_no_arbitrary_actions_or_wrong_bot_commands(bot, text):
    controller, _ = bot
    await controller.handle(update(text=text))
    assert controller.engine.status()["stopped"]
    assert controller.engine.store.cycles() == []


@pytest.mark.parametrize("command", ["/spread FLWS", "/spread flws.us", "/spread@test_stocks_bot FLWS"])
async def test_selected_stock_report_is_read_only_and_normalizes_the_symbol(bot, command):
    controller, server = bot
    monitor = SimpleNamespace(
        assets={"FLWS.US": None},
        report=Mock(return_value={}),
        request_refresh=Mock(return_value=False),
        requested={},
        pending={},
        text=lambda symbol, **kwargs: f"fresh {symbol}",
    )
    controller.engine.monitor = monitor
    await controller.handle(update(text=command))
    assert controller.report_task is None
    monitor.report.assert_called_with("FLWS.US")
    monitor.request_refresh.assert_called_once_with("FLWS.US")
    assert server.replies()[-1]["text"] == "fresh FLWS.US"
    assert controller.engine.status()["stopped"] and controller.engine.store.cycles() == []


@pytest.mark.parametrize("date", [9879, 9999, 10010, "10000", None])
async def test_stale_or_invalid_timestamp_cannot_resume(bot, date):
    controller, server = bot
    await controller.handle(update(text="/resume", date=date))
    assert controller.engine.status()["stopped"]
    assert "устарела" in server.replies()[-1]["text"]


async def test_resume_is_consumed_before_execution_and_not_replayed_after_restart(bot):
    controller, server = bot
    await controller.handle(update(text="/resume"))
    await controller.engine.stop()
    await controller.handle(update(text="/resume"))
    assert controller.engine.status()["stopped"]
    assert len(server.replies()) == 1
    await controller.close()
    restarted = TelegramController(
        controller.engine,
        TelegramAPI(TOKEN, transport=httpx.MockTransport(server)),
        USER,
        clock=lambda: 10000,
    )
    await restarted.initialize()
    try:
        await restarted.handle(update(text="/resume"))
        assert restarted.engine.status()["stopped"]
        assert len(server.replies()) == 1
    finally:
        await restarted.close()


async def test_failed_command_does_not_replay_or_expose_exception(bot):
    controller, server = bot
    controller.dispatch = AsyncMock(side_effect=RuntimeError(TOKEN))
    await controller.handle(update(text="/resume"))
    assert TOKEN not in server.replies()[-1]["text"]
    assert controller.engine.store.idempotent("telegram:123456:1") == {"consumed": True}
    await controller.handle(update(text="/resume"))
    assert controller.dispatch.await_count == 1
    assert TOKEN not in json.dumps(controller.engine.store.events())


async def test_menu_is_private_and_long_poll_only_receives_messages(bot):
    _, server = bot
    menu = next(p for method, p in server.calls if method == "setMyCommands")
    assert menu["scope"] == {"type": "chat", "chat_id": USER}
    assert menu["commands"] == COMMANDS
    poll = next(p for method, p in server.calls if method == "getUpdates")
    assert poll == {"offset": -1, "timeout": 0, "limit": 1, "allowed_updates": ["message"]}


async def test_pending_commands_are_discarded_at_startup(engine):
    server = Server()
    server.backlog = [update(80, "/resume")]
    controller = TelegramController(
        engine,
        TelegramAPI(TOKEN, transport=httpx.MockTransport(server)),
        USER,
        clock=lambda: 10000,
    )
    try:
        await controller.initialize()
        assert controller.offset == 81
        assert engine.store.get_meta("telegram:123456:offset") == "81"
        assert engine.status()["stopped"]
        assert server.replies() == []
    finally:
        await controller.close()


async def test_menu_retries_after_first_private_message(engine):
    server = Server()
    server.menu_fails = 1
    controller = TelegramController(
        engine,
        TelegramAPI(TOKEN, transport=httpx.MockTransport(server)),
        USER,
        clock=lambda: 10000,
    )
    try:
        await controller.initialize()
        assert not controller.menu_registered
        await controller.handle(update(text="/start"))
        assert controller.menu_registered
    finally:
        await controller.close()


async def test_configured_webhook_is_not_deleted(engine):
    server = Server()
    server.webhook = "https://example.com/existing"
    controller = TelegramController(engine, TelegramAPI(TOKEN, transport=httpx.MockTransport(server)), USER)
    try:
        with pytest.raises(ValueError, match="webhook"):
            await controller.initialize()
        assert not any(method == "deleteWebhook" for method, _ in server.calls)
    finally:
        await controller.close()


async def test_conflicting_poller_stops_engine(bot):
    controller, _ = bot
    await controller.engine.resume()
    controller.api.updates = AsyncMock(side_effect=TelegramError(409))
    with pytest.raises(TelegramError):
        await controller.poll()
    assert controller.engine.status()["stopped"]


async def test_reply_backlog_does_not_delay_stop(bot):
    controller, server = bot
    controller.queue_replies = True
    await controller.engine.resume()
    for index in range(1, 25):
        await controller.handle(update(index, "/status"))
    await controller.handle(update(25, "/stop"))
    assert controller.engine.status()["stopped"]
    assert controller.outbox.qsize() == 20
    assert server.replies() == []


async def test_limits_command_reports_only_local_budget_metadata(bot):
    controller, server = bot
    await controller.handle(update(text="/limits"))
    assert "telegram:" in server.replies()[-1]["text"]
    assert TOKEN not in server.replies()[-1]["text"]


async def test_negative_replay_spread_is_hidden_and_cannot_alert(bot):
    controller, server = bot
    controller.engine.settings.telegram_spread_alerts = True
    controller.engine.scan = AsyncMock(return_value=SimpleNamespace(profit_raw=-1))
    assert await controller.dispatch("spread") == "Положительного спреда после расходов сейчас нет."
    await controller.announce_candidates({"rows": [{"fresh": True, "candidate": True, "profit_raw": -1}]})
    assert server.replies() == []


async def test_http_errors_and_debug_logs_redact_token(caplog):
    caplog.set_level(logging.DEBUG)

    def fail(request):
        return httpx.Response(401, json={"ok": False, "error_code": 401, "description": TOKEN})

    api = TelegramAPI(TOKEN, transport=httpx.MockTransport(fail))
    try:
        with pytest.raises(TelegramError) as raised:
            await api.call("getMe")
        logging.getLogger("httpcore.http11").debug("request %s", TOKEN)
        assert raised.value.code == 401
        assert TOKEN not in str(raised.value)
        assert TOKEN not in caplog.text
        assert "<redacted>" in caplog.text
    finally:
        await api.close()


def test_environment_is_literal_allowlisted_and_exported_values_win(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("TG_TOKEN='${HOME}$(command)'\nTG_USER=123\nSIGNING_SECRET=do-not-load\n")
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.delenv("SIGNING_SECRET", raising=False)
    monkeypatch.setenv("TG_USER", "456")
    load_environment(path, ["TG_TOKEN", "TG_USER"])
    import os

    assert os.environ["TG_TOKEN"] == "${HOME}$(command)"
    assert os.environ["TG_USER"] == "456"
    assert "SIGNING_SECRET" not in os.environ
    monkeypatch.delenv("TG_TOKEN")


@pytest.mark.parametrize("identity", ["", "name", "-1", "123,456", "123.0", "0", str(2**52)])
def test_credentials_require_one_numeric_user_id(monkeypatch, identity):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_ID", identity)
    with pytest.raises(ValueError):
        credentials()
