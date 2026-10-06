"""Public stock-quoter limits; never an account or accepted RFQ price.

Protocol: https://docs.backpack.exchange/#tag/Stocks/operation/stockIndicativeQuote
and the stockIndicativeQuote.<ticker> WebSocket stream.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal

from .feed import StockFeed
from .models import dec


class IndicativeUnavailable(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        self.code = "stock_indicative_unavailable"
        super().__init__(reason)


@dataclass(frozen=True)
class IndicativeQuote:
    symbol: str
    side: str
    quantity: str
    price: str
    estimated_price: str | None
    session: str
    published_at: float
    received_at: float
    quantity_rules: dict[str, str]
    source: str = "Backpack public stockIndicativeQuote limit; repriced on RFQ submission"


@dataclass(frozen=True)
class IndicativeSnapshot:
    symbol: str
    bid: str | None
    ask: str | None
    estimated_bid: str | None
    estimated_ask: str | None
    session: str | None
    min_quantity: Decimal | None
    max_quantity: Decimal | None
    step_size: Decimal | None
    published_at: float
    received_at: float
    reason: str | None


def bare_ticker(symbol: str) -> str:
    ticker = symbol.removesuffix(".US")
    if not re.fullmatch(r"[A-Z0-9]+(?:\.[A-Z0-9]+)*", ticker):
        raise ValueError("Indicative feed requires a bare stock ticker or .US symbol")
    return ticker


def price(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or dec(value) <= 0:
        raise ValueError("Invalid indicative price")
    return value


class _IndicativeSocket(StockFeed):
    def __init__(self, ingest: Callable[[dict], Awaitable[None]], *, proxy: str | None):
        super().__init__(proxy=proxy)
        self._ingest = ingest

    async def send_subscription(self, socket, tickers: list[str]) -> None:
        await self.frames.acquire()
        await socket.send(
            json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"stockIndicativeQuote.{ticker}" for ticker in tickers],
                }
            )
        )

    async def ingest(self, payload: dict) -> None:
        await self._ingest(payload)


class IndicativeFeed:
    """One public socket, latest values only, with strict side/lot/time checks."""

    def __init__(self, *, proxy: str | None = None, clock: Callable[[], float] = time.time):
        self.clock = clock
        self.latest: dict[str, IndicativeSnapshot] = {}
        self.tickers: set[str] = set()
        self.version = 0
        self.changed = asyncio.Event()
        self.condition = asyncio.Condition()
        self.invalid_messages = 0
        self._feed = _IndicativeSocket(self.ingest, proxy=proxy)

    @property
    def socket(self):
        return self._feed.socket

    @property
    def proxy(self) -> str | None:
        return self._feed.proxy

    async def subscribe(self, symbols: list[str]) -> None:
        tickers = [bare_ticker(symbol) for symbol in symbols]
        self.tickers.update(tickers)
        await self._feed.subscribe(tickers)

    async def wait(self, version: int) -> None:
        async with self.condition:
            await self.condition.wait_for(lambda: self.version != version)

    async def ingest(self, payload: dict) -> None:
        data = payload.get("data", payload)
        if not isinstance(data, dict) or data.get("e") != "stockIndicativeQuote":
            return
        ticker = data.get("symbol")
        if not isinstance(ticker, str) or ticker not in self.tickers:
            return
        received = self.clock()
        stamp = data.get("timestamp")
        published = stamp / 1000 if type(stamp) is int and 0 < stamp < 2**63 else 0
        previous = self.latest.get(ticker)
        if (
            previous
            and 0 < previous.published_at <= previous.received_at
            and published
            and published < previous.published_at
        ):
            return
        bid = ask = estimated_bid = estimated_ask = None
        lower = upper = step = None
        session = None
        reason: str | None = "indicative_invalid_payload"
        try:
            if not 0 < published <= received:
                raise ValueError("Invalid indicative publish time")
            supplied_reason = data.get("reason")
            if supplied_reason is not None:
                reason = (
                    supplied_reason
                    if isinstance(supplied_reason, str)
                    and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", supplied_reason)
                    else "indicative_invalid_reason"
                )
            else:
                session = data.get("session")
                if not isinstance(session, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", session):
                    raise ValueError("Invalid indicative session")
                lower, upper, step = (dec(data[name]) for name in ("minQuantity", "maxQuantity", "stepSize"))
                if lower < 0 or upper <= 0 or upper < lower or step <= 0:
                    raise ValueError("Invalid indicative quantity rules")
                bid, ask = price(data.get("bid")), price(data.get("ask"))
                estimated_bid, estimated_ask = (
                    price(data.get("estimatedBid")),
                    price(data.get("estimatedAsk")),
                )
                if bid is not None and ask is not None and dec(ask) < dec(bid):
                    raise ValueError("Crossed indicative quote")
                reason = None
        except (ValueError, KeyError, TypeError, ArithmeticError):
            self.invalid_messages += 1
            bid = ask = estimated_bid = estimated_ask = None
            lower = upper = step = None
            session = None
            reason = "indicative_invalid_payload"
        self.latest[ticker] = IndicativeSnapshot(
            ticker + ".US",
            bid,
            ask,
            estimated_bid,
            estimated_ask,
            session,
            lower,
            upper,
            step,
            published,
            received,
            reason,
        )
        self.changed.set()
        async with self.condition:
            self.version += 1
            self.condition.notify_all()

    def get(
        self,
        ticker: str,
        side: str,
        quantity: str,
        session: str,
        max_age: float,
        now: float | None = None,
    ) -> IndicativeQuote:
        if side not in {"Bid", "Ask"} or not math.isfinite(max_age) or max_age <= 0:
            raise ValueError("Invalid indicative quote request")
        ticker = bare_ticker(ticker)
        q = dec(quantity)
        if q <= 0:
            raise ValueError("Invalid indicative quantity")
        quote = self.latest.get(ticker)
        if quote is None:
            raise IndicativeUnavailable("indicative_quote_pending")
        now = self.clock() if now is None else now
        if not math.isfinite(now) or not 0 <= now - quote.published_at < max_age or now < quote.received_at:
            raise IndicativeUnavailable("indicative_quote_stale")
        if quote.reason:
            raise IndicativeUnavailable(quote.reason)
        if quote.session != session:
            raise IndicativeUnavailable("indicative_session_mismatch")
        if quote.min_quantity is None or quote.max_quantity is None or quote.step_size is None:
            raise IndicativeUnavailable("indicative_quantity_unknown")
        if not quote.min_quantity <= q <= quote.max_quantity or q % quote.step_size:
            raise IndicativeUnavailable("indicative_invalid_quantity")
        value = quote.ask if side == "Bid" else quote.bid
        if value is None:
            raise IndicativeUnavailable("indicative_no_quote")
        return IndicativeQuote(
            quote.symbol,
            side,
            format(q.normalize(), "f"),
            value,
            quote.estimated_ask if side == "Bid" else quote.estimated_bid,
            session,
            quote.published_at,
            quote.received_at,
            {
                "minQuantity": str(quote.min_quantity),
                "maxQuantity": str(quote.max_quantity),
                "stepSize": str(quote.step_size),
            },
        )

    def status(self) -> dict:
        return {
            "connected": self.socket is not None,
            "subscribed": len(self.tickers),
            "assets": len(self.latest),
            "updates": self.version,
            "invalid_messages": self.invalid_messages,
            "reconnects": self._feed.reconnects,
            "proxied": self.proxy is not None,
        }

    async def close(self) -> None:
        await self._feed.close()
