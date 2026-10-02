import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest
from test_engine import Clock, finish
from websockets.exceptions import InvalidProxyMessage
from websockets.uri import parse_proxy, parse_uri

from backpack_stocks.engine import Engine
from backpack_stocks.market import PaperBroker, ReplayMarket
from backpack_stocks.models import Phase, Policy, Settings
from backpack_stocks.observability import LOGGER, configure, error_code, event
from backpack_stocks.store import Store
from backpack_stocks.telegram import TelegramController
from backpack_stocks.ws_proxy import CancellationSafeProxyConnection


def proxy_protocol():
    protocol = CancellationSafeProxyConnection(
        parse_uri("wss://ws.backpack.exchange"),
        parse_proxy("http://fixture:fake-password@proxy.invalid:8080"),
    )
    transport = Mock(spec=asyncio.Transport)
    protocol.connection_made(transport)
    return protocol, transport


async def test_cancelled_proxy_connect_aborts_and_late_callbacks_cannot_mutate_future():
    protocol, transport = proxy_protocol()
    protocol.response.cancel()
    await asyncio.sleep(0)
    transport.abort.assert_called_once()
    protocol.data_received(b"HTTP/1.1 200 OK\r\n\r\n")
    protocol.connection_lost(RuntimeError("fixture late callback"))
    protocol.eof_received()
    assert protocol.response.cancelled()


async def test_proxy_eof_rejects_pending_connect_and_completed_success_is_immutable():
    protocol, _ = proxy_protocol()
    protocol.connection_lost(None)
    with pytest.raises(InvalidProxyMessage):
        await protocol.response
    protocol, transport = proxy_protocol()
    protocol.data_received(b"HTTP/1.1 200 OK\r\n\r\n")
    assert (await protocol.response).status_code == 200
    protocol.connection_lost(None)
    await asyncio.sleep(0)
    transport.abort.assert_not_called()


async def test_json_logs_are_private_and_do_not_accept_secret_fields(tmp_path):
    previous = asyncio.get_running_loop().get_exception_handler()
    path = tmp_path / "runtime.jsonl"
    configure(str(path))
    try:
        event(
            "fixture_error",
            level="ERROR",
            code=error_code(ValueError("secret http://user:password@host")),
            password="do-not-log",
            transaction="do-not-log",
            component="fixture",
        )
        data = json.loads(path.read_text())
        assert data["code"] == "ValueError" and data["level"] == "ERROR"
        assert "password" not in path.read_text() and "do-not-log" not in path.read_text()
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        for handler in LOGGER.handlers[:]:
            LOGGER.removeHandler(handler)
            handler.close()
        asyncio.get_running_loop().set_exception_handler(previous)


async def test_paper_notional_freezes_quantity_for_entire_delivery_cycle(tmp_path):
    clock = Clock()
    replay = tmp_path / "replay.json"
    replay.write_text(json.dumps({"frames": [{"offset_s": 0, "ask": "6", "dex_bid": "6.3"}]}))
    settings = Settings(
        database=str(tmp_path / "paper.db"),
        replay_file=str(replay),
        paper_notional_usdc="1000",
        paper_initial_usdc="10000",
        paper_delivery_s=1,
        rebalance="each_cycle",
        policy=Policy(max_cost_usdc="1000", min_profit_usdc="5", withdrawal_semantics="deduct", cooldown_s=0),
    )
    store = Store(settings.database, settings)
    market = ReplayMarket(settings, store, clock)
    engine = Engine(settings, store, market, PaperBroker(settings, market, clock), clock)
    try:
        await finish(engine, clock)
        cycle = store.cycles()[0]
        assert cycle["phase"] == Phase.COMPLETE
        assert cycle["quantity_raw"] == 166_666_666
        assert 999_000_000 <= cycle["cost_raw"] <= 1_000_000_000
        assert cycle["delivered_raw"] == cycle["quantity_raw"] - 80_000
        assert store.balance("backpack", "USDC") > 10_000_000_000
    finally:
        store.close()


async def test_spread_alert_is_immediate_fresh_only_and_persistently_deduplicated(tmp_path):
    settings = Settings(database=str(tmp_path / "paper.db"), telegram_spread_alerts=True)
    store = Store(settings.database, settings)
    clock = Clock()
    engine = Mock(settings=settings, store=store, monitor=None)
    api = Mock(reply=AsyncMock())
    bot = TelegramController(engine, api, 471057654, clock=clock)
    row = {
        "asset": "BB.US",
        "direction": "backpack_to_jupiter",
        "fresh": True,
        "candidate": True,
        "cost_raw": 1_000_000_000,
        "profit_raw": 12_000_000,
        "edge_bps": "120",
        "price_impact_pct": "0.1",
        "quote_age_s": 2,
    }
    try:
        await bot.announce_candidates({"rows": [row]})
        await bot.announce_candidates({"rows": [row]})
        api.reply.assert_awaited_once()
        assert "СПРЕД НАЙДЕН" in api.reply.call_args.args[1] and "1000 USDC" in api.reply.call_args.args[1]
        restarted = TelegramController(engine, api, 471057654, clock=clock)
        await restarted.announce_candidates({"rows": [row]})
        api.reply.assert_awaited_once()
        clock.advance(301)
        await restarted.announce_candidates({"rows": [{**row, "fresh": False}]})
        api.reply.assert_awaited_once()
        await restarted.announce_candidates({"rows": [row]})
        assert api.reply.await_count == 2
    finally:
        store.close()


def test_paper_notional_cannot_change_live_admission():
    with pytest.raises(ValueError):
        Settings(mode="live", paper_notional_usdc="1000", policy=Policy(max_cost_usdc="1000"))
    with pytest.raises(ValueError):
        Settings(paper_notional_usdc="1001", policy=Policy(max_cost_usdc="1000"))


async def test_profitable_but_restricted_spread_alert_preserves_entry_restriction(tmp_path):
    settings = Settings(database=str(tmp_path / "paper.db"), telegram_spread_alerts=True)
    store = Store(settings.database, settings)
    api = Mock(reply=AsyncMock())
    bot = TelegramController(Mock(settings=settings, store=store, monitor=None), api, 471057654, clock=Clock())
    row = {
        "asset": "FLWS.US",
        "direction": "jupiter_to_backpack",
        "fresh": True,
        "candidate": False,
        "net_profitable": True,
        "candidate_blocks": ["price_impact_limit"],
        "cost_raw": 1_000_000_000,
        "profit_raw": 15_000_000,
        "edge_bps": "150",
        "price_impact_pct": "2",
        "quote_age_s": 1,
    }
    try:
        await bot.announce_candidates({"rows": [row]})
        api.reply.assert_awaited_once()
        text = api.reply.call_args.args[1]
        assert "price impact выше лимита" in text and "Автоисполнение не разрешено" in text
        assert store.events()[0]["body"]["qualified"] is False
        await bot.announce_candidates(
            {"rows": [{**row, "candidate": True, "candidate_blocks": [], "price_impact_pct": "0.5"}]}
        )
        assert api.reply.await_count == 2
        assert store.events()[0]["body"]["qualified"] is True
    finally:
        store.close()
