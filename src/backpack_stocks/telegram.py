from __future__ import annotations

import asyncio
import fcntl
import itertools
import json
import logging
import os
import re
import time
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TextIO

import httpx

from .coverage_reporting import format_forward_coverage
from .engine import Engine
from .limiter import PriorityLimiter
from .models import Phase, ui
from .observability import event
from .proxy import ProxyLogRedactor, checked_proxy, resolve_proxy
from .runtime import required_env
from .spread_reporting import best_spreads
from .transport import retry_after_seconds

COMMANDS = [
    {"command": name, "description": description}
    for name, description in [
        ("start", "Справка и доступные команды"),
        ("help", "Описание команд"),
        ("status", "Режим, активный цикл и лимиты"),
        ("spread", "Текущий расчёт спреда и расходов"),
        ("spreads", "Свежие плюсовые спреды доступных акций"),
        ("prices", "Цены Backpack/Jupiter на 1000 USDC и статус RFQ"),
        ("optimize", "Подбор объёма по реальным котировкам: /optimize PUSA"),
        ("wallet", "История отслеживаемого кошелька: /wallet PUSA"),
        ("watch", "Добавить публичный Solana-адрес: /watch <адрес>"),
        ("universe", "Акции, доступные для вывода в Solana"),
        ("monitor", "Покрытие, возраст котировок и задержки"),
        ("limits", "Лимиты провайдеров, 429 и cooldown"),
        ("balances", "Учётные балансы по площадкам"),
        ("account", "Реальные балансы и авторизация Backpack"),
        ("cycles", "Последние торговые циклы"),
        ("events", "Последние события движка"),
        ("stop", "Остановить следующие шаги движка"),
        ("resume", "Продолжить торговлю в настроенном режиме"),
    ]
]

HELP = (
    "/status — режим, состояние и лимиты\n"
    "/spread — текущий расчёт WEN; /spread FLWS — оба направления выбранной акции\n"
    "/spreads — свежие плюсовые спреды после расходов\n"
    "/prices — цены всех акций, время обновления и RFQ; /prices WEN — одна акция\n"
    "/optimize PUSA — проверить размеры и повторно запросить лучший\n"
    "/wallet PUSA — история сделок; /watch <адрес> — отслеживать кошелёк\n"
    "/universe — список акций с выводом в Solana\n"
    "/monitor — состояние монитора и задержки\n"
    "/limits — rate limits, 429 и cooldown\n"
    "/balances — учётные балансы\n"
    "/account — реальные балансы Backpack через API\n"
    "/cycles — последние 5 циклов\n"
    "/events — последние 8 событий\n"
    "/stop — остановить следующие шаги\n"
    "/resume — продолжить торговлю в режиме из TOML\n"
    "/start, /help — справка\n\n"
    "Команды без параметров. /start показывает справку. "
    "/resume разрешает торговые шаги, включая реальные операции в live. "
    "/stop ждёт завершения текущего шага и не закрывает позицию. "
    "Режим, адреса и лимиты меняются через локальную конфигурацию."
)


class TelegramError(RuntimeError):
    def __init__(self, code: int = 0, retry_after: int = 3):
        super().__init__(f"Telegram API request failed (code {code})")
        self.code = code
        self.retry_after = max(1, retry_after)

    @property
    def transient(self) -> bool:
        return self.code == 0 or self.code == 429 or self.code >= 500


class _TokenRedactor(logging.Filter):
    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for value in (self.token, self.token.replace(":", "%3A"), self.token.replace(":", "%3a")):
            message = message.replace(value, "<redacted>")
        record.msg, record.args = message, ()
        return True


class TelegramAPI:
    """Bot credentials stay inside the HTTP client; errors contain no URL or response body."""

    def __init__(
        self, token: str, *, transport: httpx.AsyncBaseTransport | None = None, proxy: str | None = None
    ):
        if not re.fullmatch(r"[1-9][0-9]*:[A-Za-z0-9_-]{20,}", token):
            raise ValueError("Invalid Telegram bot token format")
        proxy = checked_proxy(proxy)
        if proxy is not None and transport is not None:
            raise ValueError("A supplied Telegram transport must configure its own proxy")
        self.proxied = proxy is not None
        self._proxy_redactor = ProxyLogRedactor(proxy)
        self.client = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{token}/",
            timeout=httpx.Timeout(35, connect=10, write=10, pool=10),
            transport=transport,
            proxy=proxy,
            trust_env=False,
            follow_redirects=False,
        )
        self._redactor = _TokenRedactor(token)
        self.limiter = PriorityLimiter(0.10, categories={"sendMessage": 1.05, "setMyCommands": 1.05})
        names = {"httpx", "httpcore", "httpcore.connection", "httpcore.http11", "httpcore.http2"}
        names.update(name for name in logging.Logger.manager.loggerDict if name.startswith("httpcore."))
        self._loggers = [logging.getLogger(name) for name in names]
        for logger in self._loggers:
            logger.addFilter(self._redactor)

    async def close(self) -> None:
        await self.limiter.close()
        await self.client.aclose()
        for logger in self._loggers:
            logger.removeFilter(self._redactor)
        self._proxy_redactor.close()

    async def call(self, method: str, *, refresh_payload: Callable[[], dict | None] | None = None, **payload):
        if method not in {"getMe", "getWebhookInfo", "getUpdates", "setMyCommands", "sendMessage"}:
            raise ValueError("Unsupported Telegram method")
        try:
            await self.limiter.acquire(category=method)
            if refresh_payload is not None:
                current = refresh_payload()
                if current is None:
                    return None
                payload = current
            timeout = (
                httpx.Timeout(8, connect=5, write=5, pool=5)
                if method == "sendMessage"
                else self.client.timeout
            )
            response = await self.client.post(method, json=payload, timeout=timeout)
            body = response.json()
            if not isinstance(body, dict):
                raise TelegramError()
            if response.is_success and body.get("ok") is True and "result" in body:
                return body["result"]
            code = body.get("error_code", response.status_code)
            params = body.get("parameters")
            retry = params.get("retry_after", 3) if isinstance(params, dict) else 3
            if code == 429:
                self.limiter.responses_429 += 1
                header_delay = retry_after_seconds(response.headers.get("Retry-After"))
                self.limiter.defer(
                    max(retry if type(retry) is int else 3, header_delay or 0), category=method
                )
            raise TelegramError(
                code if type(code) is int else response.status_code,
                retry if type(retry) is int else 3,
            )
        except (httpx.HTTPError, ValueError):
            raise TelegramError() from None

    async def updates(self, offset: int, *, timeout: int = 25, limit: int = 100) -> list[dict]:
        result = await self.call(
            "getUpdates", offset=offset, timeout=timeout, limit=limit, allowed_updates=["message"]
        )
        if not isinstance(result, list) or not all(isinstance(item, dict) for item in result):
            raise TelegramError()
        return result

    async def reply(
        self, user_id: int, text: str, *, render: Callable[[], str | None] | None = None
    ) -> str | None:
        if render is not None:
            # A call fits in one message. Recheck it after Telegram's throttle,
            # immediately before HTTP submission, not before the rate-limit wait.
            sent = None

            def refresh() -> dict | None:
                nonlocal sent
                sent = render()
                if sent is None:
                    return None
                if len(sent) > 3900:
                    raise ValueError("Refreshed alert exceeds one Telegram message")
                return {
                    "chat_id": user_id,
                    "text": sent,
                    "protect_content": True,
                    "link_preview_options": {"is_disabled": True},
                }

            await self.call("sendMessage", refresh_payload=refresh)
            return sent
        original = text
        # Keep every ticker. Telegram's length limit must not silently truncate
        # the end of a price page or a diagnostic report.
        chunks = []
        while len(text) > 3900:
            end = text.rfind("\n", 0, 3901)
            if end <= 0:
                end = 3900
            chunks.append(text[:end])
            text = text[end:].lstrip("\n")
        chunks.append(text)
        for chunk in chunks:
            await self.call(
                "sendMessage",
                chat_id=user_id,
                text=chunk,
                protect_content=True,
                link_preview_options={"is_disabled": True},
            )
        return original


def credentials() -> tuple[str, int]:
    token = required_env("TELEGRAM_BOT_TOKEN")
    identity = required_env("TELEGRAM_ALLOWED_USER_ID")
    if not re.fullmatch(r"[1-9][0-9]{0,15}", identity) or int(identity) >= 2**52:
        raise ValueError("Telegram allowed user ID must be a positive numeric user ID")
    return token, int(identity)


def amount(value: int) -> str:
    return format(ui(value), ".6f").rstrip("0").rstrip(".") or "0"


def status_text(engine: Engine) -> str:
    if engine.monitor and engine.settings.monitor.auto_size:
        from .monitor import session_text

        snapshot = engine.monitor.snapshot()
        refresh = (
            "Публичные BP limit-котировки поступают раз в секунду; положительный кол требует своего RFQ. "
            if snapshot.get("forward_coverage", {}).get("quote_source") == "indicative"
            else f"Доступность RFQ проверяется каждые {engine.settings.monitor.rfq_cache_s:g}с; "
        )
        text = (
            f"Режим: {engine.settings.mode} · монитор {snapshot['universe']} акций\n"
            "Объём: автоподбор без потолка USDC\n"
            f"RFQ: {snapshot['rfq_available_assets']}/{snapshot['universe']}; "
            f"стаканы для расчёта: {snapshot.get('spot_supported_assets', 0)}/{snapshot.get('spot_markets', 0)}; "
            f"свежих котировок: {snapshot['fresh_quotes']}; "
            f"сигналов сейчас: {snapshot.get('signal_candidates', snapshot['candidates'])}\n"
            f"{refresh}"
            f"стаканы каждые {engine.settings.monitor.spot_refresh_s:g}с; "
            f"срок свежести {engine.settings.monitor.max_quote_age_s:g}с."
            "\nПроверяются BP → Jupiter и Jupiter → BP. Обратная продажа требует зачисления акции на BP."
        )
        closed = session_text(snapshot)
        return text + ("\n" + closed if closed else "")
    status = engine.status()
    active = status["active"]
    completed = [c for c in status["cycles"] if c["phase"] == Phase.COMPLETE]
    pnl = sum(c["realised_pnl_raw"] for c in completed)
    size = (
        f"до {engine.settings.paper_notional_usdc} USDC; количество по цене входа"
        if engine.settings.paper_notional_usdc is not None
        else f"{engine.settings.policy.quantity} WEN.US"
    )
    return (
        f"Режим: {engine.settings.mode}; источник: {engine.settings.source}; DEX: {engine.settings.dex_backend}\n"
        f"Движок: {'остановлен' if status['stopped'] else 'разрешён'}\n"
        f"Активный цикл: {active['phase'] + ' ' + active['id'][:8] if active else 'нет'}\n"
        f"Попытки: {len(status['cycles'])}/{engine.settings.policy.max_cycles}; "
        f"завершено: {len(completed)}\n"
        f"Объём: {size}; "
        f"лимит: {engine.settings.policy.max_cost_usdc} USDC\n"
        f"PnL завершённых циклов: {amount(pnl)} USDC (gas учитывается по модели)"
    )


class TelegramController:
    def __init__(self, engine: Engine, api: TelegramAPI, allowed_user_id: int, *, clock=time.time):
        if type(allowed_user_id) is not int or not 0 < allowed_user_id < 2**52:
            raise ValueError("Invalid Telegram allowed user ID")
        self.engine, self.api, self.allowed_user_id, self.clock = engine, api, allowed_user_id, clock
        self.username = ""
        self.bot_id = 0
        self.offset = 0
        self.started_at = int(clock())
        self.command_lock = asyncio.Lock()
        self._owner_lock: TextIO | None = None
        self.menu_registered = False
        self.outbox: asyncio.PriorityQueue[tuple[int, int, str, str, Callable[[], str | None] | None]] = (
            asyncio.PriorityQueue(maxsize=20)
        )
        self.reply_sequence = itertools.count()
        self.delivery_callbacks: dict[int, Callable[[bool], None]] = {}
        self.pending_alerts: set[str] = set()
        self.control_lock = asyncio.Lock()
        self.command_tasks: set[asyncio.Task] = set()
        self.account_task: asyncio.Task | None = None
        self.queue_replies = False
        self.report_task: asyncio.Task | None = None

    async def reply(
        self,
        text: str,
        *,
        priority: int = 0,
        kind: str = "command",
        render: Callable[[], str | None] | None = None,
        on_result: Callable[[bool], None] | None = None,
    ) -> None:
        if not self.queue_replies:
            current = render() if render else text
            if current is not None:
                sent = await self.send_reply(current, kind, render)
                if on_result:
                    on_result(sent)
            elif on_result:
                on_result(False)
            return
        item = (priority, next(self.reply_sequence), text, kind, render)
        if on_result:
            self.delivery_callbacks[item[1]] = on_result
        if kind == "digest" or self.outbox.full():
            queued = []
            while not self.outbox.empty():
                old = self.outbox.get_nowait()
                self.outbox.task_done()
                if kind != "digest" or old[3] != "digest":
                    queued.append(old)
                else:
                    callback = self.delivery_callbacks.pop(old[1], None)
                    if callback:
                        callback(False)
            queued.append(item)
            if len(queued) > self.outbox.maxsize:
                dropped = max(queued, key=lambda q: (q[0], -q[1]))
                queued.remove(dropped)
                callback = self.delivery_callbacks.pop(dropped[1], None)
                if callback:
                    callback(False)
                event(
                    "telegram_outbox_drop",
                    level="WARNING",
                    component="telegram",
                    state=dropped[3],
                    reason="queue_full",
                )
            for old in queued:
                self.outbox.put_nowait(old)
        else:
            self.outbox.put_nowait(item)

    def record_delivery(self, text: str, kind: str) -> None:
        self.engine.store.event(None, "telegram_reply_sent", {"characters": len(text), "kind": kind})
        event("telegram_delivered", component="telegram", state=kind)

    async def send_reply(self, text: str, kind: str, render: Callable[[], str | None] | None) -> bool:
        if kind in {"alert", "digest"} and render is not None:
            current = await self.api.reply(self.allowed_user_id, text, render=render)
            if current is None:
                event(
                    "telegram_outbox_drop",
                    component="telegram",
                    state=kind,
                    reason="expired_or_ineligible",
                )
                return False
            text = current if isinstance(current, str) else text
        else:
            await self.api.reply(self.allowed_user_id, text)
        self.record_delivery(text, kind)
        return True

    async def send_replies(self) -> None:
        while True:
            _, sequence, text, kind, render = await self.outbox.get()
            delivered = False
            for attempt in range(3):
                try:
                    current = render() if render else text
                    if current is None:
                        event(
                            "telegram_outbox_drop",
                            component="telegram",
                            state=kind,
                            reason="expired_or_ineligible",
                        )
                        break  # Expired opportunities must never leave the outbox.
                    delivered = await self.send_reply(current, kind, render)
                    break
                except TelegramError as error:
                    event(
                        "telegram_send_error",
                        level="WARNING" if error.transient else "ERROR",
                        component="telegram",
                        state=kind,
                        status_code=error.code,
                        attempt=attempt + 1,
                    )
                    if not error.transient:
                        await self.engine.stop()
                        raise
                    # Retrying a reply never re-executes a trading/control action.
                    if attempt == 2:
                        event("telegram_delivery_failed", level="ERROR", component="telegram", state=kind)
                    else:
                        await asyncio.sleep(error.retry_after)
            callback = self.delivery_callbacks.pop(sequence, None)
            if callback:
                callback(delivered)
            self.outbox.task_done()

    def alert_text(self, row: dict) -> str:
        from .monitor import compact_quote

        lines = ["СПРЕД НАЙДЕН · расчёт", *compact_quote(row)]
        if not row.get("candidate") and not row.get("candidate_blocks"):
            lines[-1] += ". Автоисполнение не разрешено."
        return "\n".join(lines)

    @staticmethod
    def eligible_alert(row: dict) -> bool:
        return bool(
            row.get("fresh")
            and row.get("profit_raw", 0) > 0
            and row.get("signal_eligible", row.get("candidate") or row.get("net_profitable"))
        )

    def current_alert(self, asset: str, direction: str) -> str | None:
        monitor = self.engine.monitor
        if monitor is None:
            return None
        row = next(
            (r for r in monitor.snapshot()["rows"] if r["asset"] == asset and r["direction"] == direction),
            None,
        )
        if row and self.eligible_alert(row):
            return self.alert_text(row)
        return None

    async def announce_candidates(self, snapshot: dict) -> None:
        if not self.engine.settings.telegram_spread_alerts:
            return
        queued = 0
        for row in (r for r in snapshot["rows"] if self.eligible_alert(r)):
            key = f"spread_alert:{row['asset']}:{row['direction']}"
            if not row.get("candidate"):
                # A restricted discovery must not suppress a newly eligible entry.
                key += ":restricted"
            previous = float(self.engine.store.get_meta(key) or "0")
            now = self.clock()
            if key in self.pending_alerts or now - previous < self.engine.settings.telegram_alert_cooldown_s:
                continue
            self.pending_alerts.add(key)

            def delivered(
                ok: bool,
                *,
                alert_key: str = key,
                asset: str = row["asset"],
                direction: str = row["direction"],
            ) -> None:
                self.pending_alerts.discard(alert_key)
                if ok:
                    self.engine.store.set_meta(alert_key, str(self.clock()))
                kind = "spread_alert_delivered" if ok else "spread_alert_dropped"
                self.engine.store.event(None, kind, {"asset": asset, "direction": direction})
                event(kind, component="telegram", asset=asset, direction=direction, delivered=ok)

            try:
                await self.reply(
                    self.alert_text(row),
                    priority=5,
                    kind="alert",
                    render=partial(self.current_alert, row["asset"], row["direction"])
                    if self.engine.monitor
                    else None,
                    on_result=delivered,
                )
            except BaseException:
                self.pending_alerts.discard(key)
                raise
            self.engine.store.event(
                None,
                "spread_alert",
                {
                    "asset": row["asset"],
                    "direction": row["direction"],
                    "cost_raw": row["cost_raw"],
                    "profit_raw": row["profit_raw"],
                    "qualified": bool(row.get("candidate")),
                },
            )
            event(
                "spread_found",
                asset=row["asset"],
                direction=row["direction"],
                cost_raw=row["cost_raw"],
                profit_raw=row["profit_raw"],
                edge_bps=row["edge_bps"],
                qualified=bool(row.get("candidate")),
            )
            queued += 1
            if queued >= 5:
                break

    async def initialize(self) -> None:
        me = await self.api.call("getMe")
        if (
            not isinstance(me, dict)
            or type(me.get("id")) is not int
            or me.get("is_bot") is not True
            or not isinstance(me.get("username"), str)
            or not re.fullmatch(r"[A-Za-z0-9_]{5,32}", me["username"])
        ):
            raise ValueError("Telegram bot identity is invalid")
        self.username, self.bot_id = me["username"], me["id"]
        path = Path(self.engine.settings.database).resolve().parent / f"telegram-{self.bot_id}.lock"
        self._owner_lock = path.open("a+")
        os.chmod(path, 0o600)
        try:
            fcntl.flock(self._owner_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Telegram bot already has a local polling owner") from None
        webhook = await self.api.call("getWebhookInfo")
        if not isinstance(webhook, dict) or webhook.get("url"):
            raise ValueError("Telegram webhook is configured; long polling cannot start")
        await self.register_menu()
        # Discard commands queued while offline. Resuming trades requires a fresh command.
        backlog = await self.api.updates(-1, timeout=0, limit=1)
        if backlog:
            identity = backlog[-1].get("update_id")
            if type(identity) is not int or identity < 0:
                raise TelegramError()
            self.offset = identity + 1
        self.engine.store.set_meta(f"telegram:{self.bot_id}:offset", str(self.offset))
        self.started_at = int(self.clock())

    async def register_menu(self) -> None:
        try:
            await self.api.call(
                "setMyCommands", commands=COMMANDS, scope={"type": "chat", "chat_id": self.allowed_user_id}
            )
        except TelegramError as error:
            # A private chat may not exist until the permitted user first presses Start.
            if error.code != 400:
                raise
        else:
            self.menu_registered = True

    def authorized_message(self, update: dict) -> dict | None:
        message = update.get("message")
        if not isinstance(message, dict):
            return None
        author, chat = message.get("from"), message.get("chat")
        if not isinstance(author, dict) or not isinstance(chat, dict):
            return None
        if (
            type(author.get("id")) is not int
            or type(chat.get("id")) is not int
            or author["id"] != self.allowed_user_id
            or chat["id"] != self.allowed_user_id
            or author.get("is_bot") is not False
            or chat.get("type") != "private"
            or any(key in message for key in ("forward_origin", "forward_from", "sender_chat"))
        ):
            return None
        return message

    async def handle(self, update: dict) -> None:
        async with self.command_lock:
            identity = update.get("update_id")
            if type(identity) is not int or identity < self.offset:
                return
            message = self.authorized_message(update)
            self.offset = identity + 1
            # Persist consumption before a control action. A lost reply never replays /resume.
            self.engine.store.set_meta(f"telegram:{self.bot_id}:offset", str(self.offset))
            if message is None:
                return
            key = f"telegram:{self.bot_id}:{identity}"
            if self.engine.store.idempotent(key) is not None:
                return
            self.engine.store.remember_request(key, {"consumed": True})
            date = message.get("date")
            if type(date) is not int or date < self.started_at or not -5 <= self.clock() - date <= 120:
                await self.reply("Команда устарела. Отправьте её ещё раз.")
                return
            text = message.get("text", "")
            parsed = (
                re.fullmatch(
                    r"/([a-z_]{1,32})(?:@([A-Za-z0-9_]{5,32}))?(?:[ \t]+([A-Za-z0-9.]{1,44}))?",
                    text.strip(),
                )
                if isinstance(text, str)
                else None
            )
            if parsed and parsed[2] and parsed[2].lower() != self.username.lower():
                return
            if parsed and parsed[3] and parsed[1] not in {"spread", "prices", "optimize", "wallet", "watch"}:
                parsed = None
            command = parsed[1] if parsed else ""
            symbol = parsed[3] if parsed and parsed[3] else None
            if symbol and command != "watch":
                symbol = symbol.upper()
            if symbol and command != "watch" and not symbol.endswith(".US"):
                symbol += ".US"
        # Consumption is serialized; network work is not. Keep control actions
        # ordered without blocking read commands on a trading step or account API.
        if command in {"stop", "resume"}:
            if self.engine.lock.locked() or self.control_lock.locked():
                await self.reply("Команда принята; будет выполнена после текущего торгового шага.")
            async with self.control_lock:
                await self.deliver_command(command, symbol)
        else:
            await self.deliver_command(command, symbol)

    async def deliver_command(self, command: str, symbol: str | None = None) -> None:
        if not self.menu_registered:
            try:
                await self.register_menu()
            except TelegramError:
                pass
        try:
            if command == "prices" and self.engine.monitor:
                monitor = self.engine.monitor
                if symbol and symbol not in monitor.assets:
                    await self.reply("Акция не найдена в мониторе. /universe — доступные тикеры.")
                    return
                symbols = [symbol] if symbol else sorted(monitor.assets)
                # Both directions add variable amounts of detail. Pack by actual
                # rendered size using one snapshot, then rerender on delivery.
                snapshot = monitor.snapshot()
                groups: list[list[str]] = []
                group: list[str] = []
                for item in symbols:
                    if (
                        group
                        and len(monitor.prices_text([*group, item], page=999, total=999, snapshot=snapshot))
                        > 3700
                    ):
                        groups.append(group)
                        group = []
                    group.append(item)
                groups.append(group)
                for index, group in enumerate(groups, 1):
                    render = partial(monitor.prices_text, group, page=index, total=len(groups))
                    await self.reply(render(), render=render)
                if symbol:
                    monitor.request_refresh(symbol)
                return
            text = await self.dispatch(command, symbol)
        except Exception:
            self.engine.store.event(None, "telegram_command_failed", {"command": command})
            text = "Не удалось выполнить команду. Проверьте состояние; повторите команду при необходимости."
            await self.reply(text)
            return
        if command in {"spreads", "spread"} and self.engine.monitor:
            selected = (symbol or "WEN.US") if command == "spread" else None
            await self.reply(text, render=lambda: self.cached_spread_text(selected))
        else:
            await self.reply(text)

    def cached_spread_text(self, symbol: str | None = None) -> str:
        monitor = self.engine.monitor
        if monitor is None:
            return "Монитор прогревается."
        if symbol and symbol not in monitor.assets:
            return "Акция не найдена в мониторе. /universe — доступные тикеры."
        text = monitor.text(symbol, snapshot=monitor.report(symbol))
        if symbol and (
            any(key[0] == symbol for key in monitor.requested)
            or any(key[0] == symbol for key in monitor.pending)
        ):
            text += f"\n{symbol}: обновление в приоритетной очереди."
        return text

    def cached_positive_spread_text(self) -> str | None:
        """Rerender only current positive numerical spreads, without network work."""
        monitor = self.engine.monitor
        if monitor is None:
            return None
        report = monitor.report()
        if not best_spreads(report.get("prices", [])):
            return None
        return monitor.text(snapshot=report)

    async def deliver_report(self, symbol: str, *, started: float, pending: set) -> None:
        """One optional follow-up for a selected stock, with a bounded wait."""
        monitor = self.engine.monitor
        if monitor is None:
            return
        try:
            async with asyncio.timeout(6):
                while True:
                    keys = [key for key in monitor.attempted if key[0] == symbol]
                    if any(
                        (monitor.attempted[key] >= started or key in pending) and key not in monitor.pending
                        for key in keys
                    ):
                        await self.reply(
                            self.cached_spread_text(symbol), render=lambda: self.cached_spread_text(symbol)
                        )
                        return
                    monitor.updated.clear()
                    await monitor.updated.wait()
        except TimeoutError:
            pass  # The first reply already states that background updating continues.
        except Exception as error:
            event("monitor_followup_error", level="ERROR", code=type(error).__name__)

    async def deliver_search(self, symbol: str) -> None:
        monitor = self.engine.monitor
        assert monitor is not None
        key = symbol, "backpack_to_jupiter"
        try:
            async with asyncio.timeout(45):
                while (
                    key in monitor.requested
                    or key in monitor.pending
                    or key in monitor.optimization_requested
                ):
                    monitor.updated.clear()
                    try:
                        await asyncio.wait_for(monitor.updated.wait(), 1)
                    except TimeoutError:
                        pass
            await self.reply(
                f"Подбор {symbol} завершён.\n"
                + monitor.text(symbol)
                + "\n\n"
                + monitor.prices_text([symbol]),
                render=lambda: (
                    f"Подбор {symbol} завершён.\n"
                    + monitor.text(symbol)
                    + "\n\n"
                    + monitor.prices_text([symbol])
                ),
            )
        except TimeoutError:
            await self.reply(
                f"{symbol}: подбор ещё в очереди или ограничен API. Результат: /spread {symbol}."
            )

    async def dispatch(self, command: str, symbol: str | None = None) -> str:
        if command in {"wallet", "watch"}:
            watcher = self.engine.wallet_watch
            if watcher is None:
                return "Отслеживание кошельков доступно с source=public."
            if command == "watch":
                if not symbol:
                    return "/watch <публичный адрес Solana>"
                try:
                    watcher.journal.add(symbol)
                except ValueError:
                    return "Некорректный адрес Solana или достигнут лимит 4 адресов."
                return f"Отслеживание включено. Проверка каждые 20с. /wallet\nhttps://solscan.io/account/{symbol}"
            return watcher.text(symbol)
        if command == "optimize":
            monitor = self.engine.monitor
            selected = symbol or "WEN.US"
            if monitor is None or selected not in monitor.assets:
                return "Акция не найдена в мониторе."
            if not monitor.request_optimization(selected):
                budget = monitor.budget_info(selected)
                if budget.get("within_budget") is False:
                    return f"{selected}: минимальный лот {budget['minimum_buy_quantity']} шт ≈ {float(budget['minimum_cost_usdc']):.2f} USDC выше лимита {budget['budget_usdc']}. /prices {selected}"
                return f"{selected}: расчёт уже идёт или RFQ сейчас недоступна. /spread {selected}"
            task = asyncio.create_task(self.deliver_search(selected))
            self.command_tasks.add(task)
            task.add_done_callback(self.command_tasks.discard)
            return (
                f"{selected}: подбор объёма {'без потолка USDC' if monitor.settings.monitor.auto_size else 'до ' + monitor.settings.monitor.notional_usdc + ' USDC'} запущен. "
                f"Проверю несколько размеров и повторно запрошу лучший. Результат: /spread {selected}."
            )
        if command == "account":
            bp = getattr(self.engine.market, "backpack", None)
            if bp is None or bp.auth is None:
                return "Приватный Backpack API не подключён. Нужен backpack_private_reads=true и ключ в .env."
            if self.account_task is not None and not self.account_task.done():
                return "Балансы Backpack уже обновляются. /account — проверить результат."
            self.account_task = asyncio.create_task(self.read_account(bp))
            balances, account = await self.account_task
            return (
                "Backpack API: авторизован (чтение)\n"
                + "\n".join(
                    f"{symbol}: available {balances.get(symbol, {}).get('available', '0')}"
                    for symbol in ("USDC", "USDT", "USD", "WEN.US")
                )
                + f"\nAuto lend: {account.get('autoLend')}; auto borrow: {account.get('autoBorrowSettlements')}"
            )
        if command == "limits":
            rates = self.engine.monitor.snapshot()["rate_limits"] if self.engine.monitor else {}
            rates["telegram"] = {**self.api.limiter.status(), "proxied": self.api.proxied}
            if "jupiter" in rates and "execute" in rates["jupiter"]:
                rates["jupiter_execute"] = rates["jupiter"]["execute"]
            return "Rate limits (общие для клиентов этого процесса):\n" + "\n".join(
                f"{name}: interval={value['interval_s']}s; "
                f"429={value.get('responses_429', value.get('cooldowns', 0))}; "
                f"cooldown={value['cooldown_s']:.1f}s; queued={value['queued']}; "
                f"proxy={value.get('proxied', 'shared')}; backoffs={value.get('adaptive_backoffs', 0)}; "
                f"last={(value.get('last_rejection') or {}).get('kind', 'ok')}"
                for name, value in rates.items()
            )
        if command in {"spreads", "prices", "universe", "monitor"} or (
            command == "spread" and self.engine.monitor
        ):
            monitor = self.engine.monitor
            if monitor is None:
                return "Монитор всех акций работает с source=public. Сейчас используется replay."
            if command == "universe":
                return f"Вывод в Solana: {len(monitor.assets)} акций\n" + ", ".join(sorted(monitor.assets))
            if command == "prices":
                return monitor.prices_text([symbol] if symbol else None)
            if command == "monitor":
                snapshot = monitor.snapshot()
                message = (
                    f"Акций: {snapshot['universe']}; unit mapping: {snapshot['unit_verified']}\n"
                    f"Свежих DEX quotes: {snapshot['fresh_quotes']}; кандидатов модели: {snapshot['candidates']}\n"
                    f"Оценка net > 0: {snapshot['positive_net_quotes']}; прибыль ≥ порога: {snapshot['profitable_after_costs']}\n"
                    f"Положительных ценовых спредов: {snapshot['positive_price_spreads']}\n"
                    f"Всего quotes: {snapshot['completed_quotes']}; pending: {snapshot['pending']}\n"
                    f"Полных расчётов в TTL: {snapshot.get('fresh_calculations', 0)}; "
                    f"арифметика: {snapshot.get('stock_value_backend', 'python')}\n"
                    f"DEX {snapshot['dex_backend']}: свежие цены пулов {snapshot['screened_assets']}/{snapshot['universe']}\n"
                    f"Расчёт на объём: {snapshot.get('quote_backend', snapshot['dex_backend'])}\n"
                    f"RPC interval: {snapshot['rpc_interval_s']}s; "
                    f"p95: {snapshot['latency_p95_ms'] or 0:.0f}ms\n"
                    f"Почему не проходят: {snapshot['candidate_block_counts']}\n"
                    f"RFQ доступны: {snapshot.get('rfq_available_assets', 0)}; без цены: {snapshot.get('rfq_unavailable_assets', 0)}; "
                    f"непроверены/недоступны: {snapshot.get('rfq_unknown_assets', 0)}\n"
                    f"Причины RFQ: {snapshot.get('rfq_reason_counts', {})}\n"
                    f"Ошибки: {snapshot['errors']}\nМонитор работает независимо от /stop."
                    f"\nОжидают повтора после ошибки DEX: {snapshot.get('quote_retry_waiting', 0)}"
                    f"\nНаправлений с отказом расчёта: {snapshot.get('failed_directions', 0)}; "
                    f"причины: {snapshot.get('route_failure_causes', {})}"
                    f"\nОтказы расчётов с запуска (сумма попыток): {snapshot.get('blocked', {})}"
                )
                coverage_lines = format_forward_coverage(snapshot)
                return message + ("\n\n" + "\n".join(coverage_lines) if coverage_lines else "")
            if symbol and symbol not in monitor.assets:
                return "Акция не найдена в мониторе. /universe — доступные тикеры."
            symbol = (symbol or "WEN.US") if command == "spread" else None
            snapshot = monitor.report(symbol)
            started = self.clock()
            if (
                symbol
                and monitor.request_refresh(symbol)
                and (self.report_task is None or self.report_task.done())
            ):
                self.report_task = asyncio.create_task(
                    self.deliver_report(symbol, started=started, pending=set(monitor.pending))
                )
            return monitor.text(symbol, snapshot=snapshot)
        if command in {"stop", "resume"}:
            await (self.engine.stop() if command == "stop" else self.engine.resume())
            return status_text(self.engine)
        # These reads execute synchronously on the same event loop as SQLite
        # commits. They need no trading lock and show the last committed state.
        if command in {"start", "help", "", "status", "balances", "balance", "cycles", "events"}:
            if command in {"start", "help", ""}:
                return status_text(self.engine) + "\n\n" + HELP
            if command == "status":
                return status_text(self.engine)
            if command in {"balances", "balance"}:
                rows = self.engine.store.ledger()
                return "Учётные балансы (ledger):\n" + (
                    "\n".join(f"{r['location']}/{r['bucket']}: {amount(r['raw'])} {r['asset']}" for r in rows)
                    or "Нет записей"
                )
            if command == "cycles":
                rows = self.engine.store.cycles()[:5]
                return "Последние циклы:\n" + (
                    "\n".join(
                        f"{c['id'][:8]}: {c['phase']}; PnL {amount(c['realised_pnl_raw'])} USDC" for c in rows
                    )
                    or "Циклов нет"
                )
            if command == "events":
                rows = self.engine.store.events(8)
                return "Последние события:\n" + (
                    "\n".join(f"#{e['seq']} {e['kind']} {str(e['cycle_id'] or '')[:8]}" for e in rows)
                    or "Событий нет"
                )
        if command == "spread":
            async with self.engine.lock:
                quote = await self.engine.scan()
            if quote.profit_raw <= 0:
                return "Положительного спреда после расходов сейчас нет."
            return (
                f"Спред: {self.engine.settings.mode}; источник: {quote.source}\n"
                f"Покупка: {amount(quote.cost_raw)} USDC\n"
                f"После вывода: {amount(quote.delivered_raw)} WEN.US\n"
                f"Выход с учётом slippage: {amount(quote.minimum_output_raw)} USDC\n"
                f"Gas budget: {self.engine.settings.policy.gas_budget_usdc} USDC\n"
                f"Net profit: {amount(quote.profit_raw)} USDC; edge: {float(quote.edge_bps):.2f} bps\n"
                f"Проходит лимиты: {'да' if quote.passes else 'нет'}\n"
                "Расчёт ориентировочный; реальная покупка требует исполнимой Backpack RFQ."
            )
        return "Неизвестная команда. /help — список команд."

    async def read_account(self, backpack):
        async with asyncio.timeout(6):
            return await asyncio.gather(backpack.balances(), backpack.account())

    async def poll(self) -> None:
        while True:
            try:
                updates = await self.api.updates(self.offset)
                for update in updates:
                    if len(self.command_tasks) >= 100:
                        done, _ = await asyncio.wait(self.command_tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            await task
                            self.command_tasks.discard(task)
                    self.command_tasks.add(asyncio.create_task(self.handle(update)))
                # Start consumers before fetching the next offset; retain errors
                # until observed, while slow commands continue independently.
                await asyncio.sleep(0)
                for task in list(self.command_tasks):
                    if task.done():
                        await task
                        self.command_tasks.discard(task)
            except TelegramError as error:
                event(
                    "telegram_poll_error",
                    level="WARNING" if error.transient else "ERROR",
                    component="telegram",
                    status_code=error.code,
                )
                if not error.transient:
                    await self.engine.stop()
                    raise
                await asyncio.sleep(error.retry_after)

    async def close(self) -> None:
        for task in self.command_tasks:
            task.cancel()
        await asyncio.gather(*self.command_tasks, return_exceptions=True)
        self.command_tasks.clear()
        if self.account_task is not None:
            self.account_task.cancel()
            await asyncio.gather(self.account_task, return_exceptions=True)
        if self.report_task is not None:
            self.report_task.cancel()
            await asyncio.gather(self.report_task, return_exceptions=True)
        await self.api.close()
        if self._owner_lock is not None:
            self._owner_lock.close()
            self._owner_lock = None


async def engine_worker(engine: Engine) -> None:
    while True:
        await engine.tick()
        await asyncio.sleep(engine.settings.poll_s)


async def report_worker(bot: TelegramController) -> None:
    engine = bot.engine
    previous = None
    last_digest = 0.0
    last_snapshot = 0.0
    last_rendered: float | None = None
    rendered_in = 0.0
    snapshot: dict | None = None
    previous_monitor = None
    positive_inputs: dict[tuple, set[tuple]] = {}
    spot_values: dict[int, tuple[dict, tuple, dict | None]] = {}

    def new_positive(monitor) -> bool:
        """Wake reports for new potential signals; the snapshot decides eligibility."""
        nonlocal positive_inputs, spot_values
        current: dict[tuple, set[tuple]] = {}
        current_spots: dict[int, tuple[dict, tuple, dict | None]] = {}
        urgent = False
        if engine.settings.telegram_spread_alerts:
            rows = list(getattr(monitor, "results", {}).values())
            rows.extend(
                state.result
                for state in getattr(monitor, "forward_coverage", {}).values()
                if state.result is not None
            )
            for row in rows:
                source = row.get("backpack_source")
                if source == "spot" and callable(revalue := getattr(monitor, "revalue_book", None)):
                    original = row
                    try:
                        symbol = row["asset"]
                        asset, books = monitor.assets[symbol], monitor.books
                        depth = books.depth(symbol)  # Check original book/fee TTL, never refresh them.
                        capacity = books.capacities.get(
                            (symbol, "Bid" if row["direction"] == "backpack_to_jupiter" else "Ask")
                        )
                        if row["direction"] == "backpack_to_jupiter" and (
                            capacity is None
                            or not 0 <= monitor.clock() - capacity.checked_at < books.ttl
                            or not 0 <= monitor.clock() - row["book_capacity_checked_at"] < books.ttl
                        ):
                            continue
                        p = monitor.settings.policy
                        version = (
                            id(asset),
                            books.markets[symbol],
                            id(depth),
                            depth.timestamp,
                            depth.update_id,
                            books.fee_at,
                            books.fee_bps,
                            capacity,
                            tuple(
                                row.get(field)
                                for field in (
                                    "asset",
                                    "direction",
                                    "mint",
                                    "acquired_raw",
                                    "sellable_raw",
                                    "input_raw",
                                    "minimum_output_raw",
                                    "cost_raw",
                                    "proceeds_raw",
                                    "gas_raw",
                                    "dex_observed_at",
                                    "backpack_market",
                                    "book_capacity_checked_at",
                                    "dex_price_impact_pct",
                                    "slippage_bps",
                                    "search_budget_usdc",
                                    "backpack_model",
                                )
                            ),
                            tuple(tuple(value) for value in row.get("dex_bank_versions", [])),
                            monitor.settings.monitor.auto_size,
                            p.max_cost_usdc,
                            p.min_profit_usdc,
                            p.min_edge_bps,
                            p.max_price_impact_pct,
                            p.slippage_bps,
                        )
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        continue
                    cached = spot_values.get(id(original))
                    if cached is not None and cached[0] is original and cached[1] == version:
                        valued = cached[2]
                    else:
                        try:
                            valued = revalue(asset, original)
                        except (ValueError, KeyError, TypeError, ArithmeticError):
                            valued = None
                    current_spots[id(original)] = original, version, valued
                    if valued is None:
                        continue
                    row = valued
                if (
                    row.get("profit_raw", 0) <= 0
                    or source == "indicative"
                    or not (
                        row.get("backpack_quote_confirmed")
                        or source == "spot"
                        or not engine.settings.monitor.rfq_validation
                    )
                ):
                    continue
                key = (
                    row["asset"],
                    row["direction"],
                    row.get("acquired_raw"),
                    row.get("sellable_raw"),
                    row.get("input_raw"),
                    source,
                )
                fingerprint = (
                    row.get("backpack_timestamp"),
                    row.get("backpack_price"),
                    row.get("dex_observed_at"),
                    tuple(tuple(version) for version in row.get("dex_bank_versions", [])),
                    row.get("profit_raw"),
                    row.get("cost_raw"),
                    row.get("passes_model_limits"),
                    row.get("trading_supported"),
                    row.get("size_feasible"),
                    row.get("account_withdrawal_verified"),
                    row.get("backpack_quote_confirmed"),
                )
                current.setdefault(key, set()).add(fingerprint)
            for key, fingerprints in current.items():
                if positive_inputs.get(key) == fingerprints:
                    continue
                # The raw row does not yet know candidate/restricted status.
                # Either unsuppressed alert key conservatively wakes a fresh
                # snapshot, including a newly qualified restricted opportunity.
                base = f"spread_alert:{key[0]}:{key[1]}"
                for alert_key in (base, base + ":restricted"):
                    delivered_at = float(engine.store.get_meta(alert_key) or "0")
                    if (
                        alert_key not in bot.pending_alerts
                        and bot.clock() - delivered_at >= engine.settings.telegram_alert_cooldown_s
                    ):
                        urgent = True
                        break
        positive_inputs = current
        spot_values = current_spots
        return urgent

    await asyncio.sleep(1)
    while True:
        status = engine.status()
        active = status["active"]
        marker = (active["id"], active["phase"]) if active else (None, len(status["cycles"]))
        now = time.time()
        monitor = engine.monitor
        digest_due = marker != previous or bool(
            engine.settings.telegram_digest_s and now - last_digest >= engine.settings.telegram_digest_s
        )
        disk_due = now - last_snapshot >= 10
        if monitor is not previous_monitor:
            previous_monitor = monitor
            last_rendered, snapshot, positive_inputs, spot_values = None, None, {}, {}
        if monitor:
            monitor.updated.clear()
            urgent = new_positive(monitor)
            if (
                last_rendered is None
                or time.monotonic() - last_rendered >= 1
                or digest_due
                or disk_due
                or urgent
            ):
                started = time.perf_counter()
                snapshot = monitor.snapshot()
                rendered_in = time.perf_counter() - started
                last_rendered = time.monotonic()
                coverage = snapshot.get("forward_coverage", {})
                if coverage.get("enabled"):
                    event(
                        "coverage_frame",
                        observed_at=snapshot["observed_at"],
                        eligible=coverage["eligible"],
                        fresh=coverage["pricedfresh"],
                        refused=coverage["refused"],
                        known_blocks=coverage["known_blocks"],
                        waiting_bank=coverage["waitingbank"],
                        waiting_assets=",".join(
                            row["asset"]
                            for row in coverage.get("rows", [])
                            if row["state"].startswith("waiting")
                        ),
                        first_pass_completed=coverage["first_pass_completed"],
                        snapshot_ms=rendered_in * 1000,
                    )
                await bot.announce_candidates(snapshot)
            assert snapshot is not None
            if disk_due:
                last_snapshot = now
                root = Path(engine.settings.database).resolve().parent
                target = root / "direct-monitor.json"
                temporary = target.with_suffix(".tmp")
                temporary.write_text(json.dumps(snapshot, indent=2, default=str))
                os.chmod(temporary, 0o600)
                temporary.replace(target)
                event(
                    "heartbeat",
                    mode=engine.settings.mode,
                    size_usdc=engine.settings.monitor.notional_usdc,
                    quotes=snapshot["completed_quotes"],
                    fresh_quotes=snapshot["fresh_quotes"],
                    rfq_available=snapshot.get("rfq_available_assets", 0),
                    candidates=snapshot["candidates"],
                    errors=sum(snapshot["errors"].values()),
                    blocked=sum(snapshot["blocked"].values()),
                    ws_connected=snapshot["ws_connected"],
                    ws_reconnects=snapshot["ws_reconnects"],
                    responses_429=sum(
                        snapshot["rate_limits"][name].get("responses_429_all_buckets", 0)
                        for name in ("backpack", "rpc")
                    ),
                    cycles=len(status["cycles"]),
                )
        if digest_due:
            previous, last_digest = marker, now
            text = None
            if monitor:
                try:
                    assert snapshot is not None
                    report = {
                        **snapshot,
                        "report_source": "background_cache",
                        "report_duration_s": rendered_in,
                    }
                    monitor.last_report = {
                        key: report[key]
                        for key in ("observed_at", "report_source", "report_duration_s", "fresh_calculations")
                    }
                    if best_spreads(report.get("prices", [])):
                        text = monitor.text(snapshot=report)
                except Exception as error:
                    event("monitor_report_error", level="ERROR", code=type(error).__name__)
            if text is not None:
                await bot.reply(
                    text,
                    priority=10,
                    kind="digest",
                    render=bot.cached_positive_spread_text,
                )
        if monitor:
            try:
                assert last_rendered is not None
                timeout = min(engine.settings.poll_s, 0.5, max(0.001, last_rendered + 1 - time.monotonic()))
                await asyncio.wait_for(monitor.updated.wait(), timeout=timeout)
            except TimeoutError:
                pass
        else:
            await asyncio.sleep(min(engine.settings.poll_s, 0.5))


async def run_telegram(engine: Engine) -> None:
    token, user_id = credentials()
    bot = TelegramController(engine, TelegramAPI(token, proxy=resolve_proxy("TELEGRAM")), user_id)
    tasks: list[asyncio.Task] = []
    # Live always requires /resume; explicitly configured paper can start automatically.
    try:
        await engine.stop()
        await bot.initialize()
        bot.queue_replies = True
        if engine.settings.paper_auto_start:
            await engine.resume()
        print(
            f"Telegram ready: @{bot.username}; mode={engine.settings.mode}; stopped={not engine.settings.paper_auto_start}",
            flush=True,
        )
        tasks = [
            asyncio.create_task(bot.poll()),
            asyncio.create_task(bot.send_replies()),
            asyncio.create_task(engine_worker(engine)),
        ]
        if engine.wallet_watch:
            tasks.append(asyncio.create_task(wallet_notification_worker(bot)))
        if (
            engine.settings.telegram_digest_s
            or engine.settings.telegram_spread_alerts
            or engine.settings.log_file
        ):
            tasks.append(asyncio.create_task(report_worker(bot)))
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.stop()
        await bot.close()


async def wallet_notification_worker(bot: TelegramController) -> None:
    from .wallet_watch import activity_text

    watcher = bot.engine.wallet_watch
    assert watcher is not None
    queued: set[tuple[str, str]] = set()
    while True:
        for record in watcher.journal.notifications():
            key = record["wallet"], record["signature"]
            if key in queued:
                continue
            queued.add(key)

            def result(delivered: bool, *, record=record, key=key):
                queued.discard(key)
                if delivered:
                    watcher.journal.ack(record)

            await bot.reply(
                "КОШЕЛЁК · новая транзакция\n" + activity_text(record),
                priority=5,
                kind="wallet",
                on_result=result,
            )
        await asyncio.sleep(5)
