from __future__ import annotations

import asyncio
import json
import re

import websockets
from websockets.asyncio.client import ClientConnection

from .limiter import PriorityLimiter
from .models import Observation, dec
from .observability import event
from .proxy import ProxyLogRedactor, checked_proxy
from .transport import retry_after_seconds
from .ws_proxy import install


class StockFeed:
    """One connection per Backpack client, latest-value storage and no tick backlog."""

    def __init__(self, *, proxy: str | None = None):
        install()
        self.proxy = checked_proxy(proxy)
        self._redactor = ProxyLogRedactor(self.proxy)
        self.tickers: set[str] = set()
        self.latest: dict[str, Observation] = {}
        self.version = 0
        self.condition = asyncio.Condition()
        self.task: asyncio.Task | None = None
        self.socket: ClientConnection | None = None
        self.reconnects = 0
        self.invalid_messages = 0
        self.connections = PriorityLimiter(2)
        self.frames = PriorityLimiter(1.05)

    async def send_subscription(self, socket, tickers: list[str]) -> None:
        await self.frames.acquire()
        await socket.send(json.dumps({"method": "SUBSCRIBE", "params": [f"stockPrice.{t}" for t in tickers]}))

    async def subscribe(self, tickers: list[str]) -> None:
        if any(not re.fullmatch(r"[A-Z0-9]+(?:\.[A-Z0-9]+)*", value) for value in tickers):
            raise ValueError("Stock feed requires bare stock tickers")
        added = set(tickers) - self.tickers
        self.tickers.update(added)
        if self.task is None:
            self.task = asyncio.create_task(self.receive())
        elif added and self.socket is not None:
            try:
                await self.send_subscription(self.socket, sorted(added))
            except (websockets.ConnectionClosed, OSError):
                pass  # Reconnection subscribes to the full current set.

    async def ingest(self, payload: dict) -> None:
        data = payload.get("data", payload)
        if not isinstance(data, dict) or data.get("e") != "stockPrice":
            return
        ticker = data.get("symbol")
        if (
            not isinstance(ticker, str)
            or ticker not in self.tickers
            or data.get("ask") is None
            or data.get("bid") is None
        ):
            return  # Provider mid alone is not an executable bid/ask.
        try:
            ask, bid = dec(str(data["ask"])), dec(str(data["bid"]))
            stamp = data["timestamp"]
            if type(stamp) is not int or stamp <= 0 or bid <= 0 or ask < bid:
                raise ValueError("Invalid stock feed quote")
            observation = Observation(
                ask=str(ask),
                bid=str(bid),
                timestamp=stamp / 1000,
                session=str(data.get("session") or "unknown"),
                source=f"Backpack indicative stockPrice.{ticker}",
            )
        except (ValueError, KeyError, TypeError, ArithmeticError):
            self.invalid_messages += 1
            return
        previous = self.latest.get(ticker)
        if previous and observation.timestamp < previous.timestamp:
            return
        self.latest[ticker] = observation
        async with self.condition:
            self.version += 1
            self.condition.notify_all()

    async def wait(self, version: int) -> None:
        async with self.condition:
            await self.condition.wait_for(lambda: self.version != version)

    async def receive(self) -> None:
        while True:
            try:
                await self.connections.acquire()
                async with websockets.connect(
                    "wss://ws.backpack.exchange", open_timeout=15, max_queue=16, proxy=self.proxy
                ) as socket:
                    self.socket = socket
                    event("ws_connected", component="backpack_ws", ws_reconnects=self.reconnects)
                    await self.send_subscription(socket, sorted(self.tickers))
                    async for message in socket:
                        try:
                            payload = json.loads(message)
                        except (ValueError, TypeError):
                            self.invalid_messages += 1
                            continue
                        if isinstance(payload, dict):
                            await self.ingest(payload)
            except (websockets.InvalidStatus, websockets.InvalidProxyStatus) as error:
                self.reconnects += 1
                event(
                    "ws_status_error",
                    level="WARNING",
                    component="backpack_ws",
                    status_code=error.response.status_code,
                )
                if error.response.status_code not in {429, 500, 502, 503, 504}:
                    event(
                        "ws_receiver_stopped",
                        level="ERROR",
                        component="backpack_ws",
                        status_code=error.response.status_code,
                    )
                    return
                delay = retry_after_seconds(error.response.headers.get("Retry-After"))
                self.connections.defer(delay if delay is not None else min(30, 2 ** min(5, self.reconnects)))
            except (websockets.ConnectionClosed, OSError, TimeoutError, websockets.InvalidHandshake) as error:
                self.reconnects += 1
                event(
                    "ws_reconnect",
                    level="WARNING",
                    component="backpack_ws",
                    error_type=type(error).__name__,
                    ws_reconnects=self.reconnects,
                )
                self.connections.defer(min(30, 2 ** min(5, self.reconnects)))
            finally:
                self.socket = None

    async def close(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        await self.connections.close()
        await self.frames.close()
        self._redactor.close()
