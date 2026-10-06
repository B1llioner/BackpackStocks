"""Bounded, non-binding RFQ inspection. An HTTP 200 alone never confirms availability."""

from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, replace
from typing import Literal

from .backpack import Backpack
from .models import dec
from .observability import error_code, event
from .rfq_query_batch import RfqQueryBatch
from .transport import RemoteError

SIZE_REFUSALS = {"rfq_insufficient_balance", "rfq_invalid_quantity", "rfq_no_quote"}


class RfqUnavailable(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class RfqCheck:
    symbol: str
    side: str
    quantity: str
    session: str
    state: Literal["available", "unavailable", "unknown"]
    reason: str
    checked_at: float
    retry_at: float
    eligibility_until: float
    price: str | None = None
    source: Literal["rfq", "indicative"] = "rfq"

    def usable(self, now: float) -> bool:
        return self.state == "available" and self.checked_at <= now < self.retry_at

    def eligible(self, now: float) -> bool:
        return self.state == "available" and self.checked_at <= now < self.eligibility_until

    def public(self, now: float) -> dict:
        return {
            "asset": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "session": self.session,
            "state": self.state if self.checked_at <= now < self.eligibility_until else "unknown",
            "reason": self.reason if self.checked_at <= now < self.eligibility_until else "rfq_check_stale",
            "checked_at": self.checked_at,
            "retry_at": self.eligibility_until,
            "price_expires_at": self.retry_at if self.price else None,
            "source": self.source,
        }


class RfqValidator:
    def __init__(
        self,
        backpack: Backpack,
        *,
        max_age: float,
        retry_s: float,
        timeout: float,
        cache_s: float = 60,
        concurrency: int = 4,
        clock=time.time,
    ):
        self.backpack = backpack
        self.max_age, self.retry_s, self.timeout, self.clock = max_age, retry_s, timeout, clock
        self.cache_s = cache_s
        self.cache: dict[tuple[str, str, str, str], RfqCheck] = {}
        self.latest: dict[tuple[str, str], RfqCheck] = {}
        # Avoid opening competing requests for the same stock; also bound account traffic.
        self.locks: dict[str, asyncio.Lock] = {}
        self.limit = asyncio.Semaphore(concurrency)
        self.query_batch = RfqQueryBatch(backpack, clock=clock) if type(backpack) is Backpack else None

    def invalidate(self, symbol: str) -> None:
        self.cache = {k: v for k, v in self.cache.items() if k[0] != symbol}
        self.latest = {k: v for k, v in self.latest.items() if k[0] != symbol}

    def blocked(self, symbol: str, session: str, side: str = "Bid") -> RfqCheck | None:
        check = self.latest.get((symbol, side))
        return (
            check
            if check
            and check.session == session
            and check.state != "available"
            and self.clock() < check.eligibility_until
            else None
        )

    def cached_exact(self, symbol: str, quantity: str, session: str, side: str = "Bid") -> RfqCheck | None:
        key = symbol, side, format(dec(quantity).normalize(), "f"), session
        cached = self.cache.get(key)
        return cached if cached and cached.usable(self.clock()) else None

    async def check(
        self,
        symbol: str,
        quantity: str,
        session: str,
        *,
        side: str = "Bid",
        force_refresh: bool = False,
    ) -> RfqCheck:
        if side not in {"Bid", "Ask"} or dec(quantity) <= 0:
            raise ValueError("Invalid RFQ validation request")
        quantity = format(dec(quantity).normalize(), "f")
        key = symbol, side, quantity, session
        previous = self.cache.get(key)
        async with self.locks.setdefault(symbol, asyncio.Lock()):
            cached = self.cache.get(key)
            # A concurrent renewal already replaced the entry while this call
            # waited for the stock lock. Reuse it rather than opening another RFQ.
            renew = (
                force_refresh and cached is previous and cached is not None and cached.usable(self.clock())
            )
            if cached and cached.checked_at <= self.clock() < cached.retry_at and not renew:
                return cached
            blocked = self.blocked(symbol, session, side)
            if blocked and not (blocked.reason in SIZE_REFUSALS and dec(blocked.quantity) != dec(quantity)):
                if blocked.state == "unknown" and dec(blocked.quantity) != dec(quantity):
                    # This shared transport pause has no price or size evidence.
                    # Preserve its original deadlines without returning another
                    # request's quantity as an exact RFQ response.
                    return replace(blocked, quantity=quantity)
                return blocked
            if self.backpack.auth is None:
                result = self.result(symbol, side, quantity, session, "unknown", "rfq_auth_required")
            else:
                async with self.limit:
                    result = await self.inspect(symbol, side, quantity, session)
            self.cache[key] = result
            # Keep a small exact-size window so a discovery probe does not
            # discard the still-valid price used by the local pool calculator.
            same_side = sorted(
                (k for k in self.cache if k[:2] == key[:2]),
                key=lambda k: self.cache[k].checked_at,
                reverse=True,
            )
            for old in same_side[4:]:
                self.cache.pop(old)
            self.latest[(symbol, side)] = result
            event(
                "rfq_check",
                level="WARNING" if result.state == "unknown" else "INFO",
                asset=symbol,
                side=side,
                quantity=quantity,
                state=result.state,
                reason=result.reason,
                backpack_price=result.price,
            )
            return result

    def result(
        self, symbol, side, quantity, session, state, reason, price=None, *, observed_at=None
    ) -> RfqCheck:
        now = self.clock() if observed_at is None else observed_at
        eligibility_ttl = min(self.retry_s, self.cache_s) if state == "unavailable" else self.cache_s
        ttl = min(self.max_age, eligibility_ttl) if state == "available" else eligibility_ttl
        return RfqCheck(
            symbol, side, quantity, session, state, reason, now, now + ttl, now + eligibility_ttl, price
        )

    @staticmethod
    def matches(rfq: dict, symbol: str, side: str, quantity: str, client_id: int) -> bool:
        return (
            isinstance(rfq.get("rfqId"), str)
            and rfq["rfqId"].isdigit()
            and rfq.get("symbol") == symbol
            and rfq.get("side") == side
            and rfq.get("clientId") == client_id
            and dec(rfq.get("quantity", "0")) == dec(quantity)
            and rfq.get("executionMode") == "AwaitAccept"
        )

    async def inspect(self, symbol: str, side: str, quantity: str, session: str) -> RfqCheck:
        market, client_id = symbol + "_USDC_RFQ", secrets.randbits(32)
        rfq_id = None
        submitted = False
        try:
            async with asyncio.timeout(self.timeout):
                rfq = await self.backpack.submit_rfq(quantity, client_id, side=side, symbol=market)
                submitted = True
                if not self.matches(rfq, market, side, quantity, client_id):
                    return self.result(symbol, side, quantity, session, "unknown", "rfq_response_mismatch")
                rfq_id = rfq["rfqId"]
                expiry = float(rfq["expiryTime"]) / 1000
                if not self.clock() < expiry <= self.clock() + 60:
                    return self.result(symbol, side, quantity, session, "unknown", "rfq_expired_before_check")
                registered = self.query_batch.register() if self.query_batch else None
                while self.clock() < expiry:
                    if self.query_batch and registered is not None:
                        records, observed_at = await self.query_batch.query(rfq_id, market, registered)
                    else:
                        records, observed_at = await self.backpack.rfqs(rfq_id, symbol=market), self.clock()
                    if self.clock() >= expiry:
                        break
                    for record in records:
                        current = record["rfq"]
                        if current.get("rfqId") != rfq_id:
                            continue
                        if not self.matches(current, market, side, quantity, client_id):
                            return self.result(
                                symbol, side, quantity, session, "unknown", "rfq_response_mismatch"
                            )
                        if current.get("status") in {"Expired", "Cancelled", "Canceled", "Rejected"}:
                            return self.result(symbol, side, quantity, session, "unavailable", "rfq_no_quote")
                        field = "askPrice" if side == "Bid" else "bidPrice"
                        prices = [
                            dec(q[field])
                            for q in record["quotes"]
                            if q.get("rfqId") == rfq_id
                            and q.get("status") == "New"
                            and dec(q.get(field, "0")) > 0
                        ]
                        if prices:
                            price = str(min(prices) if side == "Bid" else max(prices))
                            return self.result(
                                symbol,
                                side,
                                quantity,
                                session,
                                "available",
                                "rfq_quote_observed",
                                price,
                                observed_at=observed_at,
                            )
                    await asyncio.sleep(min(0.25, max(0, expiry - self.clock())))
                return self.result(symbol, side, quantity, session, "unavailable", "rfq_no_quote")
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return self.result(symbol, side, quantity, session, "unknown", "rfq_check_timeout")
        except RemoteError as error:
            # Transport emits this fixed label, never a remote payload. A refused
            # submission is unavailable for this size/side; an ambiguous mutation
            # or failed query remains unknown and is never repeated in-place.
            if submitted and error.reason == "rfq_expired":
                return self.result(symbol, side, quantity, session, "unknown", "rfq_expired_before_check")
            if not submitted and str(error) == "API rejected request: HTTP 400":
                event(
                    "rfq_rejected",
                    asset=symbol,
                    side=side,
                    quantity=quantity,
                    status_code=error.status_code,
                    api_code=error.api_code,
                    reason=error.reason,
                )
                reason = {
                    "insufficient_balance": "rfq_insufficient_balance",
                    "insufficient_collateral": "rfq_insufficient_balance",
                    "invalid_quantity": "rfq_invalid_quantity",
                    "market_closed": "rfq_session_unavailable",
                }.get(error.reason, "rfq_request_rejected")
                return self.result(symbol, side, quantity, session, "unavailable", reason)
            event(
                "rfq_error",
                level="ERROR",
                asset=symbol,
                side=side,
                code=error_code(error),
                status_code=error.status_code,
                api_code=error.api_code,
                reason=error.reason,
                phase="query" if submitted else "submit",
            )
            return self.result(symbol, side, quantity, session, "unknown", "rfq_check_failed")
        except Exception as error:
            # A transport/auth/schema error is not evidence that the stock cannot be bought.
            event("rfq_error", level="ERROR", asset=symbol, side=side, code=error_code(error))
            return self.result(symbol, side, quantity, session, "unknown", "rfq_check_failed")
        finally:
            if rfq_id is not None:
                try:
                    async with asyncio.timeout(2):
                        await self.backpack.cancel_rfq(rfq_id)
                except Exception as error:
                    event(
                        "rfq_cancel_expired"
                        if isinstance(error, RemoteError) and error.reason == "rfq_expired"
                        else "rfq_cancel_failed",
                        level="INFO"
                        if isinstance(error, RemoteError) and error.reason == "rfq_expired"
                        else "WARNING",
                        asset=symbol,
                        side=side,
                        code=error_code(error),
                    )
