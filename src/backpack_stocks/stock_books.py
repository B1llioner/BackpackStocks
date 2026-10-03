"""Read-only stock order books. Prices walk depth; no order is ever submitted."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext

from .backpack import Backpack
from .models import Observation, dec, raw, ui
from .observability import QuoteUnavailable, event

SPOT_SESSION = "BACKPACK_SPOT"


class BookUnavailable(QuoteUnavailable):
    def __init__(self, code: str):
        super().__init__([code])
        self.code, self.expected = code, True


def number(value: object, *, zero: bool = False) -> Decimal:
    if type(value) is not str or not 0 < len(value) <= 64:
        raise ValueError("Invalid book decimal")
    amount = dec(value)
    if amount < 0 or (not zero and amount == 0) or not -18 <= amount.adjusted() <= 18:
        raise ValueError("Invalid book amount")
    return amount


@dataclass(frozen=True)
class StockBookMarket:
    asset: str
    symbol: str
    step: Decimal
    minimum: Decimal
    maximum: Decimal | None
    price_step: Decimal
    price_min: Decimal
    price_max: Decimal
    max_impact: Decimal
    min_impact: Decimal
    max_multiplier: Decimal
    min_multiplier: Decimal

    @classmethod
    def parse(cls, row: dict) -> StockBookMarket:
        asset, symbol = row["baseSymbol"], row["symbol"]
        if (
            row.get("marketType") != "SPOT"
            or row.get("rwaMarketType") != "STOCK"
            or row.get("quoteSymbol") != "USDC"
            or row.get("orderBookState") != "Open"
            or type(asset) is not str
            or not asset.endswith(".US")
            or len(asset) > 32
            or symbol != asset + "_USDC"
        ):
            raise ValueError("Not an open USDC stock book")
        q, p = row["filters"]["quantity"], row["filters"]["price"]
        step, minimum = number(q["stepSize"]), number(q["minQuantity"])
        maximum = number(q["maxQuantity"]) if q.get("maxQuantity") is not None else None
        price_step, price_min, price_max = [number(p[k]) for k in ("tickSize", "minPrice", "maxPrice")]
        maximum_impact, minimum_impact, maximum_price, minimum_price = [
            number(p[k])
            for k in ("maxImpactMultiplier", "minImpactMultiplier", "maxMultiplier", "minMultiplier")
        ]
        raw(step)
        raw(minimum)
        if (
            minimum % step
            or (maximum is not None and maximum < minimum)
            or price_min > price_max
            or not (minimum_impact <= 1 <= maximum_impact and minimum_price <= 1 <= maximum_price)
        ):
            raise ValueError("Invalid stock book filters")
        return cls(
            asset,
            symbol,
            step,
            minimum,
            maximum,
            price_step,
            price_min,
            price_max,
            maximum_impact,
            minimum_impact,
            maximum_price,
            minimum_price,
        )


@dataclass(frozen=True)
class BookFill:
    market: str
    side: str
    quantity_raw: int
    gross_raw: int
    fee_raw: int
    value_raw: int
    price: str
    worst_price: str
    levels: int
    impact_pct: str
    timestamp: float
    update_id: str
    fee_bps: str


@dataclass(frozen=True)
class BookDepth:
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    timestamp: float
    received_at: float
    update_id: str

    @classmethod
    def parse(cls, row: dict, market: StockBookMarket, now: float, ttl: float) -> BookDepth:
        stamp, update = row.get("timestamp"), row.get("lastUpdateId")
        if type(stamp) is not int or type(update) is not str or not update.isdigit() or len(update) > 32:
            raise ValueError("Invalid book snapshot identity")
        timestamp = stamp / 1_000_000  # Backpack depth timestamps are Unix microseconds.
        if not -2 <= now - timestamp < ttl:
            raise BookUnavailable("spot_book_stale")

        def levels(side: str) -> tuple[tuple[Decimal, Decimal], ...]:
            data = row[side]
            if not isinstance(data, list) or len(data) > 1000:
                raise ValueError("Invalid book level count")
            parsed = []
            seen = set()
            for level in data:
                if not isinstance(level, list) or len(level) != 2:
                    raise ValueError("Invalid book level")
                price, quantity = number(level[0]), number(level[1])
                raw(quantity)
                if (
                    price in seen
                    or quantity % market.step
                    or price % market.price_step
                    or not market.price_min <= price <= market.price_max
                ):
                    raise ValueError("Invalid book price level")
                seen.add(price)
                parsed.append((price, quantity))
            return tuple(sorted(parsed, reverse=side == "bids"))

        bids, asks = levels("bids"), levels("asks")
        if bids and asks and bids[0][0] >= asks[0][0]:
            raise ValueError("Crossed stock book")
        return cls(bids, asks, timestamp, now, update)

    def fresh(self, now: float, ttl: float) -> bool:
        return -2 <= now - self.timestamp < ttl and 0 <= now - self.received_at < ttl

    def tradable(self, market: StockBookMarket, side: str) -> tuple[tuple[Decimal, Decimal], ...]:
        levels = self.asks if side == "Bid" else self.bids
        if not self.asks or not self.bids or side not in {"Bid", "Ask"}:
            raise BookUnavailable("spot_book_empty")
        # Active price is the median of best bid, ask and last trade: it always
        # lies between bid and ask. Use the strict endpoints without a stale last.
        low = max(self.bids[0][0] * market.min_impact, self.asks[0][0] * market.min_multiplier)
        high = min(self.asks[0][0] * market.max_impact, self.bids[0][0] * market.max_multiplier)
        return tuple((p, q) for p, q in levels if low <= p <= high)

    def fill(self, market: StockBookMarket, quantity_raw: int, fee_bps: Decimal, side: str) -> BookFill:
        quantity = ui(quantity_raw)
        if side not in {"Bid", "Ask"} or quantity < market.minimum or quantity % market.step:
            raise BookUnavailable("spot_invalid_quantity")
        if market.maximum is not None and quantity > market.maximum:
            raise BookUnavailable("spot_invalid_quantity")
        levels = self.tradable(market, side)
        if not levels:
            raise BookUnavailable("spot_book_empty")
        remaining, total, fee_total, used, worst = quantity, Decimal(0), Decimal(0), 0, levels[0][0]
        with localcontext() as ctx:
            ctx.prec = 80
            for price, available in levels:
                matched = min(remaining, available)
                # Depth aggregates resting orders. Reserve one micro-USDC per
                # quantity step for possible per-match fee rounding as well.
                value = matched * price * 10**6
                total += value
                fee_total += (value * fee_bps / 10000).to_integral_value(rounding=ROUND_CEILING)
                used += 1
                worst = price
                remaining -= matched
                if remaining == 0:
                    break
            if remaining > 0:
                all_levels = self.asks if side == "Bid" else self.bids
                raise BookUnavailable(
                    "spot_price_band" if len(levels) != len(all_levels) else "spot_insufficient_depth"
                )
            buy = side == "Bid"
            gross = int(total.to_integral_value(rounding=ROUND_CEILING if buy else ROUND_FLOOR))
            fee = int(fee_total) + (int(quantity / market.step) if fee_bps else 0)
            value_raw = gross + fee if buy else gross - fee
            price = total / 10**6 / quantity
            impact = abs(price / levels[0][0] - 1) * 100
        if not 0 < value_raw < 2**63:
            raise ValueError("Book valuation outside ledger range")
        return BookFill(
            market.symbol,
            side,
            quantity_raw,
            gross,
            fee,
            value_raw,
            str(price),
            str(worst),
            used,
            str(impact),
            self.timestamp,
            self.update_id,
            str(fee_bps),
        )


@dataclass(frozen=True)
class BookCapacity:
    quantity: Decimal
    price: Decimal
    fee_bps: Decimal
    checked_at: float


class StockBooks:
    def __init__(self, backpack: Backpack, *, ttl: float, fee_ttl: float = 300, clock=time.time):
        self.backpack, self.ttl, self.fee_ttl, self.clock = backpack, ttl, fee_ttl, clock
        self.markets: dict[str, StockBookMarket] = {}
        self.depths: dict[str, BookDepth] = {}
        self.failures: dict[str, str] = {}
        self.capacities: dict[tuple[str, str], BookCapacity] = {}
        self.fee_bps: Decimal | None = None
        self.fee_at = 0.0

    def discover(self, rows: list[dict], assets: set[str]) -> None:
        found: dict[str, StockBookMarket] = {}
        duplicates = set()
        for row in rows:
            try:
                market = StockBookMarket.parse(row)
            except (ValueError, KeyError, TypeError, ArithmeticError):
                continue
            if market.asset not in assets:
                continue
            if market.asset in found:
                duplicates.add(market.asset)
            found[market.asset] = market
        for asset in duplicates:
            found.pop(asset, None)
        for asset, market in self.markets.items():
            if found.get(asset) != market:
                self.depths.pop(asset, None)
                for side in ("Bid", "Ask"):
                    self.capacities.pop((asset, side), None)
        self.markets = found

    async def refresh_fees(self) -> None:
        self.fee_bps = None
        if not self.backpack.auth:
            return
        account = await self.backpack.account()
        fee = number(account.get("spotTakerFee"), zero=True)  # Account API expresses fees in basis points.
        if fee > 10000 or account.get("liquidating") is not False:
            raise ValueError("Account fee or trading state unavailable")
        self.fee_bps, self.fee_at = fee, self.clock()

    async def refresh(self, asset: str) -> None:
        market = self.markets[asset]
        try:
            async with asyncio.timeout(8):
                result = await self.backpack.depth(market.symbol, limit=1000)
            depth = BookDepth.parse(result, market, self.clock(), self.ttl)
            if self.markets.get(asset) == market:
                self.depths[asset] = depth
                self.failures.pop(asset, None)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.depths.pop(asset, None)
            code = error.codes[0] if isinstance(error, QuoteUnavailable) else "spot_book_unavailable"
            self.failures[asset] = code
            event("stock_book_blocked", component="backpack_spot", asset=asset, code=code)

    def depth(self, asset: str) -> BookDepth:
        depth = self.depths.get(asset)
        if not depth or asset not in self.markets or not depth.fresh(self.clock(), self.ttl):
            raise BookUnavailable(self.failures.get(asset, "spot_book_stale"))
        if self.fee_bps is None or not 0 <= self.clock() - self.fee_at < self.fee_ttl:
            raise BookUnavailable("spot_fee_unknown")
        return depth

    def observation(self, asset: str) -> Observation:
        depth = self.depth(asset)
        if not depth.asks or not depth.bids:
            raise BookUnavailable("spot_book_empty")
        return Observation(
            ask=str(depth.asks[0][0]),
            bid=str(depth.bids[0][0]),
            timestamp=depth.timestamp,
            session=SPOT_SESSION,
            source="Backpack spot depth " + self.markets[asset].symbol,
        )

    def available(self, asset: str) -> bool:
        try:
            self.observation(asset)
        except (ValueError, KeyError):
            return False
        return True

    def fill(self, asset: str, quantity_raw: int, side: str) -> BookFill:
        depth = self.depth(asset)
        assert self.fee_bps is not None  # depth() requires a current, validated fee.
        return depth.fill(self.markets[asset], quantity_raw, self.fee_bps, side)

    def budget_quantity(self, asset: str, budget_raw: int, minimum: Decimal) -> int:
        market = self.markets[asset]
        self.depth(asset)
        assert self.fee_bps is not None
        # Solve against real depth with integer money checks; avoid pricing the
        # whole purchase at the best ask or crediting a partially filled request.
        high_quantity = self.quantity_ceiling(asset, "Bid")
        if market.maximum is not None:
            high_quantity = min(high_quantity, market.maximum)
        low, high = 0, int(high_quantity // market.step)
        while low < high:
            middle = (low + high + 1) // 2
            quantity = market.step * middle
            try:
                value = self.fill(asset, raw(quantity), "Bid").value_raw if quantity >= market.minimum else 0
            except QuoteUnavailable:
                value = budget_raw + 1
            if value <= budget_raw:
                low = middle
            else:
                high = middle - 1
        quantity = market.step * low
        if quantity < minimum:
            raise BookUnavailable("budget_below_minimum_lot")
        return raw(quantity)

    def quantity_ceiling(self, asset: str, side: str) -> Decimal:
        market, depth = self.markets[asset], self.depth(asset)
        quantity = sum((q for _, q in depth.tradable(market, side)), Decimal(0))
        return min(quantity, market.maximum) if market.maximum is not None else quantity

    async def check_capacity(self, asset: str, fill: BookFill) -> None:
        market = self.markets[asset]
        if not self.backpack.auth:
            raise BookUnavailable("spot_account_unverified")
        started = self.clock()
        async with asyncio.timeout(5):
            result = await self.backpack.max_order_quantity(market.symbol, fill.side, fill.worst_price)
        if (
            result.get("symbol") != market.symbol
            or result.get("side") != fill.side
            or dec(result.get("price", "0")) != dec(fill.worst_price)
            or any(
                result.get(flag) is not False
                for flag in ("autoBorrow", "autoBorrowRepay", "autoLendRedeem", "reduceOnly")
            )
        ):
            raise ValueError("Spot order limit response mismatch")
        quantity = number(result.get("maxOrderQuantity"), zero=True)
        self.capacities[(asset, fill.side)] = BookCapacity(
            quantity, dec(fill.worst_price), dec(fill.fee_bps), started
        )
        if ui(fill.quantity_raw) > quantity:
            raise BookUnavailable("spot_insufficient_balance")

    def capacity_valid(self, asset: str, fill: BookFill, checked_at: float) -> bool:
        capacity = self.capacities.get((asset, fill.side))
        price_valid = bool(
            capacity
            and (
                dec(fill.worst_price) <= capacity.price
                if fill.side == "Bid"
                else dec(fill.worst_price) >= capacity.price
            )
        )
        return bool(
            capacity
            and price_valid
            and capacity.fee_bps == dec(fill.fee_bps)
            and 0 <= self.clock() - capacity.checked_at < self.ttl
            and 0 <= self.clock() - checked_at < self.ttl
            and ui(fill.quantity_raw) <= capacity.quantity
        )
