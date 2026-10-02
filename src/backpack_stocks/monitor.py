from __future__ import annotations

import asyncio
import os
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from solders.pubkey import Pubkey

from .backpack import Backpack
from .jupiter import Jupiter
from .jupiter_flow_quotes import JupiterFlowQuotes
from .models import STOCK_MINT, USDC_MINT, Observation, Settings, dec, floor_step, impact_percent, raw, ui
from .observability import MARKET_BLOCKS, QuoteUnavailable, error_code, event
from .proxy import resolve_proxy
from .rfq_validation import RfqCheck, RfqUnavailable, RfqValidator
from .sessions import active_session
from .solana import SolanaRPC, assert_stock_mint
from .stock_money import StockValueCalculator

if TYPE_CHECKING:
    from .direct import DirectRaydium

FORWARD = "backpack_to_jupiter"
REVERSE = "jupiter_to_backpack"
BLOCK_LABELS = {
    "cost_cap": "выше лимита размера",
    "profit_below_minimum": "прибыль ниже порога",
    "edge_below_minimum": "доходность ниже порога",
    "price_impact_limit": "price impact выше лимита",
    "slippage_limit": "slippage котировки выше лимита",
    "backpack_rfq_unconfirmed": "нет подтверждённой цены RFQ Backpack",
    "stale_exact_quote": "полный расчёт устарел",
    "no_exact_quote": "ожидает полного расчёта",
    "rfq_session_unavailable": "RFQ-сессия закрыта или недоступна",
    "rfq_no_quote": "Backpack не дал RFQ на этот объём",
    "rfq_request_rejected": "Backpack отклонил RFQ на этот объём/сторону",
    "rfq_insufficient_balance": "для RFQ не хватает доступного баланса",
    "rfq_invalid_quantity": "объём не соответствует ограничениям RFQ",
    "rfq_auth_required": "для проверки RFQ нужен Backpack API-ключ",
    "rfq_check_timeout": "проверка RFQ не завершилась вовремя",
    "rfq_check_failed": "проверка RFQ недоступна",
    "rfq_response_mismatch": "RFQ не соответствует запросу",
    "rfq_expired_before_check": "RFQ истекла до проверки",
    "rfq_check_stale": "проверка RFQ устарела",
    "rfq_check_pending": "доступность покупки RFQ ещё не проверена",
    "rfq_budget_changed": "RFQ не укладывается в бюджет",
    "nonpositive_net": "нет положительного net после расходов",
    "insufficient_liquidity": "не хватает ликвидности на заданный объём",
    "insufficient_tick_coverage": "достигнут лимит загруженных tick arrays",
    "no_tick_liquidity": "нет доступных directional tick arrays",
    "ReplayError": "Jupiter не дал корректную котировку",
    "jupiter_no_route": "Jupiter не нашёл маршрут на этот объём",
    "jupiter_http_error": "Jupiter вернул ошибку HTTP",
    "jupiter_transport_error": "нет ответа Jupiter через прокси",
    "jupiter_invalid_response": "Jupiter вернул некорректную котировку",
    "jupiter_order_pair_mismatch": "Jupiter вернул другую пару",
    "jupiter_order_amount_mismatch": "Jupiter вернул другой объём",
    "jupiter_order_missing_request_id": "Jupiter не сформировал заявку на обмен",
    "jupiter_order_wallet_mismatch": "Jupiter вернул другой адрес кошелька",
}


def block_text(codes: list[str]) -> str:
    return ", ".join(BLOCK_LABELS.get(code, code) for code in codes)


@dataclass(frozen=True)
class Asset:
    symbol: str
    mint: str
    decimals: int
    fee: Decimal
    withdrawal_min: Decimal
    deposit_min: Decimal
    deposit_enabled: bool
    sessions: tuple[dict, ...]

    @property
    def ticker(self) -> str:
        return self.symbol.removesuffix(".US")


def discover(assets: list[dict], securities: list[dict]) -> dict[str, Asset]:
    by_symbol = {a["symbol"]: a for a in assets}
    result = {}
    for security in securities:
        symbol = security["asset"]
        if not symbol.endswith(".US"):
            continue
        tokens = [
            t
            for t in by_symbol.get(symbol, {}).get("tokens", [])
            if t.get("blockchain") == "Solana"
            and t.get("withdrawEnabled") is True
            and t.get("contractAddress")
        ]
        if len(tokens) != 1:
            continue  # No arbitrary selection between different issuer mints.
        token = tokens[0]
        try:
            Pubkey.from_string(token["contractAddress"])
            decimals = token["nativeDecimals"]
            if type(decimals) is not int or not 0 <= decimals <= 18:
                continue
            fee, minimum, deposit = [
                dec(str(token[k])) for k in ("withdrawalFee", "minimumWithdrawal", "minimumDeposit")
            ]
            if min(fee, minimum, deposit) < 0:
                continue
        except (ValueError, TypeError, KeyError, ArithmeticError):
            continue
        result[symbol] = Asset(
            symbol,
            token["contractAddress"],
            decimals,
            fee,
            minimum,
            deposit,
            token.get("depositEnabled") is True,
            tuple(security["sessions"]),
        )
    return result


class SpreadMonitor:
    """Broad screener with non-binding RFQ validation; never accepts or executes a trade."""

    def __init__(
        self,
        settings: Settings,
        backpack: Backpack,
        jupiter: Jupiter | DirectRaydium,
        rpc: SolanaRPC,
        *,
        clock=time.time,
        quote_provider: Jupiter | JupiterFlowQuotes | None = None,
    ):
        from .direct import DirectRaydium

        self.settings, self.backpack, self.jupiter, self.rpc, self.clock = (
            settings,
            backpack,
            jupiter,
            rpc,
            clock,
        )
        self._quote_provider = quote_provider
        self._owns_quote_provider = False
        if (
            quote_provider is None
            and settings.monitor.quote_backend == "jupiter"
            and settings.monitor.jupiter_flow
        ):
            self._quote_provider = JupiterFlowQuotes(settings)
            self._owns_quote_provider = True
        elif (
            quote_provider is None
            and isinstance(jupiter, DirectRaydium)
            and settings.monitor.quote_backend == "jupiter"
        ):
            self._quote_provider = Jupiter(
                os.environ.get(settings.jupiter_key_env),
                settings.jupiter_interval_s,
                proxy=resolve_proxy("JUPITER"),
            )
            self._owns_quote_provider = True
        self.assets: dict[str, Asset] = {}
        self.chain: dict[str, str] = {}
        self.external: dict[str, str] = {}
        self.external_at = 0.0
        self.prices: dict = {}
        self.prices_at = 0.0
        self.metadata_at = 0.0
        self.sessions: list[dict] = []
        self.holidays: list[dict] = []
        self.results: dict[tuple[str, str], dict] = {}
        self.price_history: dict[tuple[str, str], dict] = {}
        self.price_repeat_intervals: deque[float] = deque(maxlen=136)
        self.attempted: dict[tuple[str, str], float] = {}
        self.failures: dict[tuple[str, str], str] = {}
        self.failure_causes: dict[tuple[str, str], list[str]] = {}
        self.errors: Counter = Counter()
        self.blocked: Counter = Counter()
        self.latencies: deque[float] = deque(maxlen=500)
        self.quote_starts: deque[float] = deque(maxlen=100)
        self.completed_quotes = 0
        self.tasks: list[asyncio.Task] = []
        self.pending: dict[tuple[str, str], asyncio.Task] = {}
        self._sequence = 0
        self.discovery_ready = asyncio.Event()
        self.report_lock = asyncio.Lock()
        self.reporting = False
        self.last_report: dict = {}
        self.requested: dict[tuple[str, str], float] = {}
        self.updated = asyncio.Event()
        self.stock_values = StockValueCalculator()
        self.retry_after: dict[tuple[str, str], float] = {}
        self.quote_failures: Counter[tuple[str, str]] = Counter()
        config = settings.monitor
        self.rfq = RfqValidator(
            backpack,
            max_age=config.max_quote_age_s,
            retry_s=config.rfq_retry_s,
            timeout=config.rfq_timeout_s,
            cache_s=config.rfq_cache_s,
            concurrency=config.rfq_concurrency,
            clock=clock,
        )

    @property
    def quote_provider(self) -> Jupiter | JupiterFlowQuotes | DirectRaydium:
        return self._quote_provider if self._quote_provider is not None else self.jupiter

    @property
    def quote_backend(self) -> str:
        return "jupiter" if self._quote_provider is not None else self.settings.dex_backend

    async def refresh_metadata(self) -> None:
        assets, securities, sessions, holidays = await asyncio.gather(
            self.backpack.assets(),
            self.backpack.securities(),
            self.backpack.sessions(),
            self.backpack.holidays(),
        )
        found = discover(assets, securities)
        for symbol, asset in self.assets.items():
            if found.get(symbol) != asset:
                self.rfq.invalidate(symbol)
                for direction in (FORWARD, REVERSE):
                    self.price_history.pop((symbol, direction), None)
        chain = {}
        entries = list(found.values())
        for start in range(0, len(entries), 100):
            group = entries[start : start + 100]
            try:
                accounts = await self.rpc.accounts([a.mint for a in group], parsed=True, priority=10)
                if len(accounts["value"]) != len(group):
                    raise ValueError("Mint batch length mismatch")
                for asset, account in zip(group, accounts["value"], strict=True):
                    try:
                        if asset.decimals != 6 or account is None:
                            raise ValueError("Unsupported mint")
                        assert_stock_mint(account)
                        chain[asset.symbol] = "verified_unit_mapping"
                    except (ValueError, KeyError, TypeError):
                        chain[asset.symbol] = "unsupported_mint_or_unit_mapping"
            except Exception:
                self.errors["mint_batch"] += 1
                chain.update({a.symbol: "mint_verification_unavailable" for a in group})
        for key in list(self.results):
            symbol = key[0]
            if symbol not in found or self.assets.get(symbol) != found[symbol]:
                self.results.pop(key, None)
                self.attempted.pop(key, None)
                self.retry_after.pop(key, None)
                self.quote_failures.pop(key, None)
        for records in (self.failures, self.failure_causes):
            for key in list(records):
                if key[0] not in found or self.assets.get(key[0]) != found[key[0]]:
                    records.pop(key, None)
        for key in list(self.retry_after):
            if key[0] not in found or self.assets.get(key[0]) != found[key[0]]:
                self.retry_after.pop(key, None)
                self.quote_failures.pop(key, None)
        self.assets, self.chain, self.metadata_at = found, chain, self.clock()
        self.sessions, self.holidays = sessions, holidays
        await self.backpack.stock_feed.subscribe([a.ticker for a in entries])
        self.discovery_ready.set()

    async def refresh_prices(self) -> None:
        await asyncio.gather(self.refresh_external_prices(), self.refresh_jupiter_prices())

    async def refresh_external_prices(self) -> None:
        tickers = await self.backpack.tickers()
        self.external = {t["symbol"].removesuffix("_USDC"): t["lastPrice"] for t in tickers}
        self.external_at = self.clock()

    def price_batch_period(self) -> float:
        chunks = (len(self.assets) + 50) // 50
        # Keep batch screening within a quarter of the main request budget.
        return max(self.settings.monitor.price_batch_s, chunks * self.jupiter.interval * 4)

    async def refresh_jupiter_prices(self) -> None:
        mints = [USDC_MINT, *(a.mint for a in self.assets.values())]
        prices = {}
        # Direct price screening batches RPC pool accounts itself. Never narrow it to
        # recently successful exact quotes; pass the whole universe in one refresh.
        batch_size = len(mints) if self.settings.dex_backend == "raydium" else 50
        for start in range(0, len(mints), batch_size):
            try:
                prices.update(await self.jupiter.prices(mints[start : start + batch_size]))
            except Exception as error:
                self.errors["price_batch"] += 1
                event(
                    "monitor_prices_error",
                    level="ERROR",
                    component="prices",
                    code=error_code(error),
                    error_type=type(error).__name__,
                )
        self.prices, self.prices_at = prices, self.clock()

    def observation(self, asset: Asset) -> Observation:
        latest = self.backpack.stock_feed.latest.get(asset.ticker)
        if latest and -2 <= self.clock() - latest.timestamp <= self.settings.policy.quote_max_age_s:
            return latest
        if self.clock() - self.external_at > self.settings.monitor.price_batch_s * 2:
            raise ValueError("No fresh provider quote or external-price model")
        price = dec(self.external[asset.symbol])
        if price <= 0:
            raise ValueError("Nonpositive external price")
        return Observation(
            ask=str(price),
            bid=str(price),
            timestamp=self.external_at,
            session="MODEL",
            source="Backpack frontend estimate: external FMV/last; no RFQ; source age unknown",
        )

    def quantity_session(self, observation: Observation) -> str:
        if self.sessions:
            if self.clock() - self.metadata_at > self.settings.monitor.metadata_s * 2:
                raise ValueError("No RFQ trading session is open")
            session = active_session(self.sessions, self.holidays, datetime.fromtimestamp(self.clock(), UTC))
            if session is None:
                raise ValueError("No RFQ trading session is open")
            return session
        if observation.session == "MODEL":
            raise ValueError("No RFQ trading session is open")
        return observation.session

    def sizing(
        self, asset: Asset, observation: Observation, *, notional: Decimal | None = None
    ) -> tuple[int, int]:
        if self.chain.get(asset.symbol) != "verified_unit_mapping":
            raise ValueError("Mint unit mapping is not verified")
        p = self.settings.policy
        session = self.quantity_session(observation)
        constraints = next((s for s in asset.sessions if s["name"] == session), None)
        if constraints is None:
            raise ValueError("No quantity constraints for observed session")
        budget = min(dec(self.settings.monitor.notional_usdc), dec(p.max_cost_usdc))
        if notional is not None:
            budget = min(budget, notional)
        ask_with_fee = dec(observation.ask) * (1 + Decimal(p.backpack_fee_bps) / 10000)
        quantity = floor_step(
            min(budget / ask_with_fee, dec(constraints["maxQuantity"])), dec(constraints["stepSize"])
        )
        if quantity < dec(constraints["minQuantity"]):
            raise ValueError("Notional below session minimum quantity")
        acquired, fee = raw(quantity, asset.decimals), raw(asset.fee, asset.decimals)
        if p.withdrawal_semantics == "unverified":
            raise ValueError("Withdrawal fee convention requires configuration")
        request = acquired if p.withdrawal_semantics == "deduct" else acquired - fee
        if acquired <= fee or ui(request, asset.decimals) < asset.withdrawal_min:
            raise ValueError("Below withdrawal minimum after fee")
        return acquired, acquired - fee

    def sale_quantity(self, asset: Asset, observation: Observation, output_raw: int) -> int:
        session = self.quantity_session(observation)
        constraints = next((s for s in asset.sessions if s["name"] == session), None)
        if constraints is None:
            raise ValueError("Missing RFQ sell quantity constraints")
        quantity = floor_step(
            min(ui(output_raw, asset.decimals), dec(constraints["maxQuantity"])), dec(constraints["stepSize"])
        )
        if quantity < dec(constraints["minQuantity"]):
            raise ValueError("Reverse sale below RFQ session minimum")
        return raw(quantity, asset.decimals)

    def estimate(self, asset: Asset, direction: str | None = None) -> Decimal | None:
        """Cheap shortlist only. Missing/stale batch prices never remove assets from exploration."""
        try:
            if self.clock() - self.prices_at > self.price_batch_period() * 2:
                return None
            price = self.prices[asset.mint]
            if self.clock() - price.get("_observed_at", self.prices_at) > self.price_batch_period() * 2:
                return None
            usd = dec(str(price["usdPrice"]))
            usdc = dec(str(self.prices[USDC_MINT]["usdPrice"]))
            if min(usd, usdc) <= 0:
                return None
            obs = self.observation(asset)
            sell = dec(str(price.get("sellSpotPrice", usd))) / usdc
            buy = dec(str(price.get("buySpotPrice", usd))) / usdc
            forward = (sell / dec(obs.ask) - 1) * 10000
            reverse = (dec(obs.bid) / buy - 1) * 10000
            return (
                forward
                if direction == FORWARD
                else reverse
                if direction == REVERSE
                else max(forward, reverse)
            )
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return None

    def choose(self) -> tuple[str, str] | None:
        available = []
        now, config = self.clock(), self.settings.monitor
        for asset in self.assets.values():
            if self.chain.get(asset.symbol) != "verified_unit_mapping":
                continue
            try:
                observation = self.observation(asset)
                session = self.quantity_session(observation)
            except (ValueError, KeyError, TypeError):
                continue
            if config.rfq_validation and self.rfq.blocked(asset.symbol, session):
                continue
            if config.rfq_validation and not self.rfq_ready(asset, session):
                continue
            for direction in (FORWARD, REVERSE):
                key = (asset.symbol, direction)
                if key in self.pending or (direction == REVERSE and not asset.deposit_enabled):
                    continue
                if (
                    config.rfq_validation
                    and direction == REVERSE
                    and self.rfq.blocked(asset.symbol, session, "Ask")
                ):
                    continue
                last = self.attempted.get(key, 0)
                if direction == FORWARD:
                    try:
                        self.sizing(asset, observation)
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        self.failures[key] = "notional_or_session_or_withdrawal_constraint"
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                else:
                    try:
                        session = self.quantity_session(observation)
                    except (ValueError, KeyError, TypeError):
                        self.failures[key] = "rfq_session_unavailable"
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                    if not any(s["name"] == session for s in asset.sessions):
                        self.failures[key] = "rfq_session_unavailable"
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                if now < self.retry_after.get(key, 0) or now - last < config.quote_refresh_s:
                    continue
                # Three hot slots followed by one oldest slot: no quote starvation.
                estimate = self.estimate(asset, direction)
                hot = estimate if estimate is not None else Decimal("-Infinity")
                previous = self.results.get(key)
                if (
                    estimate is not None
                    and previous
                    and now - previous["observed_at"] <= config.exploration_s
                    and previous.get("screen_spread_at_quote") is not None
                ):
                    # Ranking only: learn the measured 1k depth/fees from the last
                    # calculation. Cold slots still probe every direction; old math
                    # never becomes a fresh candidate or an execution price.
                    hot = dec(previous["edge_bps"]) + estimate - dec(previous["screen_spread_at_quote"])
                available.append((last, -hot, asset.symbol, direction))
        if not available:
            return None
        requested = [r for r in available if (r[2], r[3]) in self.requested]
        if requested:
            selected = min(requested, key=lambda r: (self.requested[(r[2], r[3])], r[3] != FORWARD))
            self.requested.pop((selected[2], selected[3]), None)
            return selected[2], selected[3]
        self._sequence += 1
        overdue = [row for row in available if now - row[0] >= config.exploration_s]
        if self._sequence % 4 == 0:
            selected = min(overdue or available)
        else:
            selected = min(available, key=lambda row: (row[1], row[0], row[2], row[3]))
        return selected[2], selected[3]

    async def validated_buy(self, asset: Asset, obs: Observation) -> tuple[int, int, Observation, RfqCheck]:
        acquired, delivered = self.sizing(asset, obs)
        session = self.quantity_session(obs)
        # FMV can be below the real ask. Resize down and request that exact size again.
        for _ in range(3):
            check = await self.rfq.check(asset.symbol, str(ui(acquired, asset.decimals)), session)
            if not check.usable(self.clock()):
                raise RfqUnavailable(check.reason)
            obs = obs.model_copy(
                update={
                    "ask": check.price,
                    "timestamp": check.checked_at,
                    "session": session,
                    "source": "Backpack AwaitAccept RFQ; cancelled after inspection",
                }
            )
            resized, delivered_resized = self.sizing(asset, obs)
            if resized >= acquired:
                return acquired, delivered, obs, check
            acquired, delivered = resized, delivered_resized
        raise RfqUnavailable("rfq_budget_changed")

    def rfq_cooldown(self, asset: Asset, direction: str) -> bool:
        if not self.settings.monitor.rfq_validation:
            return False
        try:
            session = self.quantity_session(self.observation(asset))
        except (ValueError, KeyError, TypeError):
            return True
        return bool(
            self.rfq.blocked(asset.symbol, session)
            or (direction == REVERSE and self.rfq.blocked(asset.symbol, session, "Ask"))
        )

    def rfq_ready(self, asset: Asset, session: str) -> bool:
        check = self.rfq.latest.get((asset.symbol, "Bid"))
        return bool(check and check.session == session and check.eligible(self.clock()))

    async def refresh_rfq_asset(self, asset: Asset) -> None:
        try:
            await self.validated_buy(asset, self.observation(asset))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            previous = self.rfq.latest.get((asset.symbol, "Bid"))
            if previous is None or self.clock() >= previous.eligibility_until:
                check = self.rfq.result(asset.symbol, "Bid", "0", "UNAVAILABLE", "unknown", error_code(error))
                self.rfq.latest[(asset.symbol, "Bid")] = check
        if self.assets.get(asset.symbol) != asset:
            self.rfq.invalidate(asset.symbol)
        self.updated.set()

    async def rfq_loop(self) -> None:
        """Refresh each stock at its eligibility deadline without overlapping probes."""
        await self.discovery_ready.wait()
        await self.refresh_external_prices()
        pending: dict[str, asyncio.Task] = {}
        try:
            while True:
                for symbol, task in list(pending.items()):
                    if task.done():
                        await task
                        pending.pop(symbol)
                available = sorted(
                    self.assets.values(),
                    key=lambda a: (
                        self.rfq.latest[(a.symbol, "Bid")].checked_at
                        if (a.symbol, "Bid") in self.rfq.latest
                        else 0,
                        a.symbol,
                    ),
                )
                for asset in available:
                    if len(pending) >= self.settings.monitor.rfq_concurrency:
                        break
                    previous = self.rfq.latest.get((asset.symbol, "Bid"))
                    if asset.symbol in pending or self.chain.get(asset.symbol) != "verified_unit_mapping":
                        continue
                    if previous and self.clock() < previous.eligibility_until:
                        continue
                    pending[asset.symbol] = asyncio.create_task(self.refresh_rfq_asset(asset))
                await asyncio.sleep(0.5)
        finally:
            for task in pending.values():
                task.cancel()
            await asyncio.gather(*pending.values(), return_exceptions=True)

    async def quote(
        self, key: tuple[str, str], *, quote_pair: Callable[..., Awaitable[dict]] | None = None
    ) -> None:
        symbol, direction = key
        started = self.clock()
        self.attempted[key] = started
        self.quote_starts.append(started)
        try:
            asset = self.assets[symbol]
            obs = self.observation(asset)
            p = self.settings.policy
            rfq_check = None
            if self.settings.monitor.rfq_validation and (
                direction == FORWARD or not self.rfq_ready(asset, self.quantity_session(obs))
            ):
                buy_acquired, buy_delivered, obs, rfq_check = await self.validated_buy(asset, obs)
            if direction == FORWARD:
                acquired, delivered = (
                    (buy_acquired, buy_delivered)
                    if self.settings.monitor.rfq_validation
                    else self.sizing(asset, obs)
                )
                source, target, input_raw = asset.mint, USDC_MINT, delivered
            else:
                if not asset.deposit_enabled or self.chain.get(symbol) != "verified_unit_mapping":
                    raise ValueError("Reverse deposit/mint mapping unavailable")
                source, target = USDC_MINT, asset.mint
                input_raw = raw(min(dec(self.settings.monitor.notional_usdc), dec(p.max_cost_usdc)))
                acquired = delivered = 0
                previous = self.rfq.latest.get((symbol, "Ask"))
                if (
                    self.settings.monitor.rfq_validation
                    and previous
                    and previous.session == self.quantity_session(obs)
                    and previous.state != "available"
                ):
                    # Recheck the last refused, exact sell size before spending a
                    # Jupiter slot again. The final successful quote still gets
                    # its own RFQ on the actual DEX minimum output below.
                    check = await self.rfq.check(symbol, previous.quantity, previous.session, side="Ask")
                    if not check.usable(self.clock()):
                        raise RfqUnavailable(check.reason)
            quote = await (quote_pair or self.quote_provider.quote_pair)(
                source, target, input_raw, priority=10
            )
            if self.assets.get(symbol) != asset:
                return  # Issuer/metadata changed during the request.
            if not self.settings.monitor.rfq_validation:
                obs = self.observation(asset)
            if self.quantity_session(obs) != obs.session and rfq_check:
                raise RfqUnavailable("rfq_session_unavailable")
            quoted_output = int(quote["outAmount"])
            reserved_output = quoted_output * (10000 - p.slippage_bps) // 10000
            provider_minimum = int(quote.get("otherAmountThreshold", quoted_output))
            if not 0 < provider_minimum <= quoted_output:
                raise ValueError("Invalid provider minimum output")
            output = min(reserved_output, provider_minimum)
            gasless = quote.get("gasless") is True
            gas = 0 if gasless else raw(p.gas_budget_usdc)
            if direction == FORWARD:
                cost = self.stock_values.value(
                    acquired, asset.decimals, obs.ask, p.backpack_fee_bps, buy=True
                )
                proceeds = output
                sellable = 0
            else:
                if ui(output, asset.decimals) < asset.deposit_min:
                    raise ValueError("Reverse amount below minimum deposit")
                cost = input_raw
                sellable = self.sale_quantity(asset, obs, output)
                if self.settings.monitor.rfq_validation:
                    rfq_check = await self.rfq.check(
                        symbol, str(ui(sellable, asset.decimals)), self.quantity_session(obs), side="Ask"
                    )
                    if not rfq_check.usable(self.clock()):
                        raise RfqUnavailable(rfq_check.reason)
                    obs = obs.model_copy(update={"bid": rfq_check.price, "timestamp": rfq_check.checked_at})
                proceeds = self.stock_values.value(
                    sellable, asset.decimals, obs.bid, p.backpack_fee_bps, buy=False
                )
            profit = proceeds - cost - gas
            if self.assets.get(symbol) != asset:
                return
            edge = Decimal(profit) * 10000 / cost
            impact = abs(impact_percent(quote))
            self.results[key] = {
                "asset": symbol,
                "direction": direction,
                "mint": asset.mint,
                "input_raw": input_raw,
                "acquired_raw": acquired,
                "delivered_raw": delivered,
                "minimum_output_raw": output,
                "quoted_output_raw": quoted_output,
                "sellable_raw": sellable,
                "stock_decimals": asset.decimals,
                "withdrawal_fee_raw": raw(asset.fee, asset.decimals) if direction == FORWARD else 0,
                "gas_raw": gas,
                "gas_source": "included_in_dex_quote" if gasless else "model",
                "slippage_bps": max(p.slippage_bps, int(quote.get("slippageBps") or 0)),
                "policy_slippage_bps": p.slippage_bps,
                "cost_raw": cost,
                "proceeds_raw": proceeds,
                "profit_raw": profit,
                "edge_bps": str(edge),
                "price_impact_pct": str(impact),
                "quote_started_at": quote.get("_request_started_at", started),
                "dex_observed_at": quote.get("_observed_at", quote.get("_request_started_at", started)),
                "observed_at": self.clock(),
                "backpack_timestamp": obs.timestamp,
                "backpack_session": self.quantity_session(obs),
                "backpack_model": obs.session == "MODEL",
                "backpack_quote_confirmed": rfq_check is not None,
                "backpack_rfq_checked_at": rfq_check.checked_at if rfq_check else None,
                "backpack_rfq_valid_until": rfq_check.retry_at if rfq_check else None,
                "backpack_price": obs.ask if direction == FORWARD else obs.bid,
                "backpack_fee_bps": p.backpack_fee_bps,
                "settlement_required": True,
                "withdrawal_enabled": True,
                "account_withdrawal_verified": False,
                "source": obs.source,
                "router": str(quote.get("router", "unknown")),
                "quote_backend": self.quote_backend,
                "quote_source": quote.get("quote_source", self.quote_backend),
                "quote_profile": quote.get("quote_profile"),
                "dex_fee_bps": int(quote.get("feeBps") or 0),
                "dex_fee_mint": quote.get("feeMint"),
                "dex_gasless": gasless,
                "dex_wallet_context": bool(quote.get("taker")),
                "route_markets": quote_markets(quote),
                "passes_model_limits": obs.session != "MODEL"
                and cost <= raw(p.max_cost_usdc)
                and profit >= raw(p.min_profit_usdc)
                and edge >= p.min_edge_bps
                and impact <= dec(p.max_price_impact_pct)
                and int(quote.get("slippageBps") or 0) <= p.slippage_bps,
                "trading_supported": asset.mint == STOCK_MINT
                and direction == FORWARD
                and quote.get("quote_source") != "jupiter_flow"
                and self.quote_backend == self.settings.dex_backend,
                "indicative": True,
                "screen_spread_at_quote": str(self.estimate(asset, direction))
                if self.estimate(asset, direction) is not None
                else None,
            }
            self.failures.pop(key, None)
            self.failure_causes.pop(key, None)
            pair = price_pair(self.results[key])
            previous_price = self.price_history.get(key)
            if previous_price and pair["updated_at"] > previous_price["updated_at"]:
                self.price_repeat_intervals.append(pair["updated_at"] - previous_price["updated_at"])
            self.price_history[key] = pair
            self.completed_quotes += 1
            self.retry_after.pop(key, None)
            self.quote_failures.pop(key, None)
            self.latencies.append((self.clock() - started) * 1000)
            event(
                "quote_complete",
                asset=symbol,
                direction=direction,
                size_usdc=self.settings.monitor.notional_usdc,
                cost_raw=cost,
                profit_raw=profit,
                edge_bps=str(edge),
                impact_pct=str(impact),
                backpack_price=pair["backpack_price"],
                dex_price=pair["dex_price"],
                quoted_output_raw=quoted_output,
                minimum_output_raw=output,
                elapsed_s=self.clock() - started,
            )
        except asyncio.CancelledError:
            self.results.pop(key, None)
            raise
        except Exception as error:
            code = error_code(error)
            expected = code in MARKET_BLOCKS or isinstance(error, QuoteUnavailable) and error.expected
            self.failures[key] = code
            self.failure_causes[key] = error.codes if isinstance(error, QuoteUnavailable) else [code]
            if expected:
                self.blocked[code] += 1
            else:
                self.errors["quote"] += 1
            event(
                "quote_blocked" if expected else "quote_error",
                level="INFO" if expected else "ERROR",
                asset=symbol,
                direction=direction,
                code=code,
                error_type=type(error).__name__,
                status_code=getattr(error, "status_code", None),
                api_code=getattr(error, "api_code", None),
            )
            self.results.pop(key, None)  # Failed refresh never retains an actionable old opportunity.
            if not isinstance(error, RfqUnavailable):
                self.quote_failures[key] += 1
                delay = min(60, self.settings.monitor.quote_refresh_s * 2 ** min(self.quote_failures[key], 3))
                self.retry_after[key] = self.clock() + delay
        finally:
            self.updated.set()

    async def metadata_loop(self) -> None:
        while True:
            try:
                await self.refresh_metadata()
            except Exception as error:
                self.errors["discovery"] += 1
                event(
                    "monitor_metadata_error",
                    level="ERROR",
                    component="discovery",
                    error_type=type(error).__name__,
                    code=error_code(error),
                )
            await asyncio.sleep(self.settings.monitor.metadata_s if self.assets else 5)

    async def prices_loop(self) -> None:
        await self.discovery_ready.wait()
        while True:
            if self.assets:
                try:
                    await self.refresh_jupiter_prices()
                except Exception as error:
                    self.errors["jupiter_prices"] += 1
                    event(
                        "monitor_prices_error",
                        level="ERROR",
                        component="prices",
                        error_type=type(error).__name__,
                        code=error_code(error),
                    )
            await asyncio.sleep(self.price_batch_period())

    async def external_prices_loop(self) -> None:
        await self.discovery_ready.wait()
        while True:
            if self.assets:
                try:
                    await self.refresh_external_prices()
                except Exception as error:
                    self.errors["external_prices"] += 1
                    event(
                        "monitor_external_error",
                        level="ERROR",
                        component="backpack_prices",
                        error_type=type(error).__name__,
                        code=error_code(error),
                    )
            await asyncio.sleep(self.settings.monitor.price_batch_s)

    async def quotes_loop(self) -> None:
        while True:
            for key, task in list(self.pending.items()):
                if task.done():
                    if not task.cancelled():
                        await task
                    self.pending.pop(key)
            while not self.reporting and len(self.pending) < self.settings.monitor.concurrency:
                selected = self.choose()
                if selected is None:
                    break
                self.pending[selected] = asyncio.create_task(self.quote(selected))
            version = self.backpack.stock_feed.version
            try:
                await asyncio.wait_for(self.backpack.stock_feed.wait(version), timeout=0.25)
            except TimeoutError:
                pass

    async def start(self) -> None:
        self.tasks = [
            asyncio.create_task(self.supervise(fn))
            for fn in (self.metadata_loop, self.external_prices_loop, self.prices_loop, self.quotes_loop)
        ]
        if self.settings.monitor.rfq_validation:
            self.tasks.append(asyncio.create_task(self.supervise(self.rfq_loop)))

    async def supervise(self, function) -> None:
        while True:
            try:
                await function()
                raise RuntimeError("Monitor loop exited unexpectedly")
            except Exception as error:
                self.errors["loop_crash"] += 1
                event(
                    "monitor_loop_crash",
                    level="ERROR",
                    component=function.__name__,
                    error_type=type(error).__name__,
                )
                await asyncio.sleep(5)

    async def close(self) -> None:
        tasks = [*self.tasks, *self.pending.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self.pending.clear()
        if self._owns_quote_provider and self._quote_provider is not None:
            await self._quote_provider.close()

    def model_blocks(self, row: dict) -> list[str]:
        p = self.settings.policy
        return [
            code
            for code, failed in (
                ("backpack_rfq_unconfirmed", row.get("backpack_model") is True),
                ("cost_cap", row["cost_raw"] > raw(p.max_cost_usdc)),
                ("profit_below_minimum", row["profit_raw"] < raw(p.min_profit_usdc)),
                ("edge_below_minimum", dec(row["edge_bps"]) < p.min_edge_bps),
                ("price_impact_limit", dec(row["price_impact_pct"]) > dec(p.max_price_impact_pct)),
                ("slippage_limit", row.get("slippage_bps", p.slippage_bps) > p.slippage_bps),
            )
            if failed
        ]

    def snapshot(self, *, include_hidden: bool = False) -> dict:
        now, config = self.clock(), self.settings.monitor
        rows = []
        prices = []
        screened_assets = set()
        diagnostics = []
        rfq_status = []
        fresh_calculations = 0
        # One FFI batch for confirmed RFQ prices; expiry, session and exact size
        # constraints are still checked independently below on every report.
        valued_keys = []
        value_jobs = []
        for key, cached_result in self.results.items():
            asset = self.assets.get(key[0])
            if asset and cached_result.get("backpack_quote_confirmed"):
                buy = key[1] == FORWARD
                valued_keys.append(key)
                value_jobs.append(
                    (
                        cached_result["acquired_raw"] if buy else cached_result["sellable_raw"],
                        asset.decimals,
                        cached_result["backpack_price"],
                        self.settings.policy.backpack_fee_bps,
                        buy,
                    )
                )
        stock_values = dict(zip(valued_keys, self.stock_values.batch(value_jobs), strict=True))
        for asset in self.assets.values():
            buy_check = self.rfq.latest.get((asset.symbol, "Bid"))
            rfq_reason = "rfq_check_pending"
            rfq_eligible = not config.rfq_validation
            if config.rfq_validation:
                rfq_status.append(
                    buy_check.public(now)
                    if buy_check
                    else {
                        "asset": asset.symbol,
                        "state": "unknown",
                        "reason": rfq_reason,
                    }
                )
                try:
                    session = self.quantity_session(self.observation(asset))
                    rfq_eligible = bool(
                        buy_check and buy_check.session == session and buy_check.eligible(now)
                    )
                    if buy_check:
                        rfq_reason = (
                            buy_check.reason if now < buy_check.eligibility_until else "rfq_check_stale"
                        )
                        if buy_check.session != session:
                            rfq_reason = "rfq_session_unavailable"
                except (ValueError, KeyError, TypeError):
                    rfq_reason = "rfq_session_unavailable"
                if not rfq_eligible and rfq_status[-1]["state"] == "available":
                    rfq_status[-1].update(state="unknown", reason=rfq_reason)
            for direction in (FORWARD, REVERSE):
                key = asset.symbol, direction
                result = self.results.get(key)
                row = dict(
                    result
                    or {"asset": asset.symbol, "direction": direction, "mint": asset.mint, "indicative": True}
                )
                age = now - result.get("dex_observed_at", result["quote_started_at"]) if result else None
                metadata_fresh = now - self.metadata_at <= config.metadata_s * 2
                fresh = (
                    age is not None
                    and 0 <= age <= config.max_quote_age_s
                    and metadata_fresh
                    and self.chain.get(asset.symbol) == "verified_unit_mapping"
                )
                if result and fresh:
                    try:
                        observation = self.observation(asset)
                        if row.get("backpack_quote_confirmed"):
                            if (
                                now >= row["backpack_rfq_valid_until"]
                                or self.quantity_session(observation) != row["backpack_session"]
                            ):
                                raise ValueError("RFQ check expired or session changed")
                            observation = observation.model_copy(
                                update={
                                    "ask" if direction == FORWARD else "bid": row["backpack_price"],
                                    "timestamp": row["backpack_timestamp"],
                                    "session": row["backpack_session"],
                                    "source": row["source"],
                                }
                            )
                        p = self.settings.policy
                        if direction == FORWARD:
                            if (
                                self.sale_quantity(asset, observation, row["acquired_raw"])
                                != row["acquired_raw"]
                            ):
                                raise ValueError(
                                    "Cached buy quantity violates current RFQ session constraints"
                                )
                            cost = (
                                stock_values[key]
                                if row.get("backpack_quote_confirmed")
                                else self.stock_values.value(
                                    row["acquired_raw"],
                                    asset.decimals,
                                    observation.ask,
                                    p.backpack_fee_bps,
                                    buy=True,
                                )
                            )
                            if cost is None:
                                raise ValueError("Invalid RFQ stock valuation")
                            row["cost_raw"] = cost
                        else:
                            sellable = self.sale_quantity(asset, observation, row["minimum_output_raw"])
                            if row.get("backpack_quote_confirmed") and sellable != row["sellable_raw"]:
                                raise ValueError(
                                    "Cached sell quantity violates current RFQ session constraints"
                                )
                            row["sellable_raw"] = sellable
                            proceeds = (
                                stock_values[key]
                                if row.get("backpack_quote_confirmed")
                                else self.stock_values.value(
                                    row["sellable_raw"],
                                    asset.decimals,
                                    observation.bid,
                                    p.backpack_fee_bps,
                                    buy=False,
                                )
                            )
                            if proceeds is None:
                                raise ValueError("Invalid RFQ stock valuation")
                            row["proceeds_raw"] = proceeds
                        row["profit_raw"] = row["proceeds_raw"] - row["cost_raw"] - row["gas_raw"]
                        row["edge_bps"] = str(Decimal(row["profit_raw"]) * 10000 / row["cost_raw"])
                        row["backpack_timestamp"] = observation.timestamp
                        row["backpack_session"] = self.quantity_session(observation)
                        row["backpack_model"] = observation.session == "MODEL"
                        row["backpack_quote_confirmed"] = result.get("backpack_quote_confirmed", False)
                        row["backpack_price"] = observation.ask if direction == FORWARD else observation.bid
                        row["source"] = observation.source
                        row["passes_model_limits"] = (
                            observation.session != "MODEL"
                            and row["cost_raw"] <= raw(p.max_cost_usdc)
                            and row["profit_raw"] >= raw(p.min_profit_usdc)
                            and dec(row["edge_bps"]) >= p.min_edge_bps
                            and dec(row["price_impact_pct"]) <= dec(p.max_price_impact_pct)
                            and row.get("slippage_bps", p.slippage_bps) <= p.slippage_bps
                        )
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        fresh = False
                row.update(
                    quote_age_s=age,
                    fresh=fresh,
                    candidate=fresh and bool(row.get("passes_model_limits")),
                    chain=self.chain.get(asset.symbol, "unverified"),
                    error=self.failures.get(key),
                    error_causes=self.failure_causes.get(
                        key, [self.failures[key]] if key in self.failures else []
                    ),
                    estimated_edge_bps=str(self.estimate(asset, direction)),
                )
                screen = (
                    self.estimate(asset, direction)
                    if metadata_fresh and self.chain.get(asset.symbol) == "verified_unit_mapping"
                    else None
                )
                row.update(
                    screen_fresh=screen is not None,
                    screen_spread_bps=str(screen) if screen is not None else None,
                    screen_positive=screen is not None and screen > 0,
                    candidate_blocks=(
                        self.model_blocks(row)
                        if fresh
                        else ["stale_exact_quote"]
                        if result
                        else ["no_exact_quote"]
                    ),
                )
                row["net_profitable"] = fresh and all(
                    code == "price_impact_limit" for code in row["candidate_blocks"]
                )
                positive = not result or row.get("profit_raw", 0) > 0
                fresh_calculations += bool(result and fresh and rfq_eligible)
                direction_eligible = rfq_eligible and (
                    not config.rfq_validation
                    or result is not None
                    and row.get("backpack_quote_confirmed")
                    and fresh
                )
                if rfq_eligible and screen is not None:
                    screened_assets.add(asset.symbol)
                if direction_eligible and result and fresh:
                    # Price availability is independent of profitability. Publish
                    # only validated prices here, without a hidden negative spread.
                    pair = price_pair(row)
                    pair["quote_age_s"] = max(0, now - pair["updated_at"])
                    prices.append(pair)
                if (direction_eligible and positive) or include_hidden:
                    rows.append(row)
                if not direction_eligible or not positive:
                    diagnostics.append(
                        {
                            "asset": asset.symbol,
                            "direction": direction,
                            "retry_in_s": max(0, self.retry_after.get(key, 0) - now),
                            "reasons": [rfq_reason]
                            if not rfq_eligible
                            else ["nonpositive_net"]
                            if not positive
                            else row["error_causes"] or row["candidate_blocks"],
                        }
                    )
        rows.sort(key=lambda r: (not r["fresh"], -Decimal(r.get("edge_bps", "-Infinity")), r["asset"]))
        prices.sort(key=lambda r: (r["quote_age_s"], r["asset"], r["direction"]))
        fresh_keys = {(r["asset"], r["direction"]) for r in prices}
        catalog = []
        rfq_by_asset = {r["asset"]: r for r in rfq_status}
        for symbol in sorted(self.assets):
            check = self.rfq.latest.get((symbol, "Bid"))
            status = rfq_by_asset.get(symbol, {"state": "unknown", "reason": "rfq_check_pending"})
            record = dict(self.price_history.get((symbol, FORWARD), {}))
            record.update(
                asset=symbol,
                rfq_state=status["state"],
                rfq_reason=status["reason"],
                fresh=(symbol, FORWARD) in fresh_keys,
            )
            if record.get("dex_price") is not None:
                record["quote_age_s"] = max(0, now - record["updated_at"])
            elif check and check.price:
                record.update(
                    backpack_price=check.price,
                    backpack_updated_at=check.checked_at,
                    backpack_fresh=check.usable(now) and status["state"] == "available",
                    buy_quantity=check.quantity,
                )
            catalog.append(record)
        repeat_intervals = sorted(self.price_repeat_intervals)
        latency = sorted(self.latencies)
        return {
            "observed_at": now,
            "dex_backend": self.settings.dex_backend,
            "quote_backend": self.quote_backend,
            "quote_source": "jupiter_flow"
            if isinstance(self.quote_provider, JupiterFlowQuotes)
            else self.quote_backend,
            "universe": len(self.assets),
            "notional_usdc": config.notional_usdc,
            "unit_verified": sum(value == "verified_unit_mapping" for value in self.chain.values()),
            "metadata_age_s": now - self.metadata_at if self.metadata_at else None,
            "batch_price_age_s": now - self.prices_at if self.prices_at else None,
            "batch_prices_available": len(self.prices),
            "screened_assets": len(screened_assets),
            "positive_price_spreads": sum(r["screen_positive"] for r in rows),
            "positive_net_quotes": sum(r["fresh"] and r.get("profit_raw", 0) > 0 for r in rows),
            "profitable_after_costs": sum(r["net_profitable"] for r in rows),
            "candidate_block_counts": dict(Counter(code for r in rows for code in r["candidate_blocks"])),
            "price_batch_period_s": self.price_batch_period(),
            "fresh_quotes": len(prices),
            "candidates": sum(r["candidate"] for r in rows),
            "completed_quotes": self.completed_quotes,
            "fresh_calculations": fresh_calculations,
            "stock_value_backend": self.stock_values.backend,
            "quote_retry_waiting": sum(now < deadline for deadline in self.retry_after.values()),
            "pending": len(self.pending),
            "reporting": self.reporting,
            "last_report": dict(self.last_report),
            "errors": dict(self.errors),
            "blocked": dict(self.blocked),
            "failed_directions": sum(bool(r["error"]) for r in rows),
            "route_failure_causes": dict(Counter(code for r in rows for code in r["error_causes"])),
            "latency_p50_ms": latency[len(latency) // 2] if latency else None,
            "latency_p95_ms": latency[min(len(latency) - 1, int(len(latency) * 0.95))] if latency else None,
            "quote_start_interval_s": (
                (self.quote_starts[-1] - self.quote_starts[0]) / (len(self.quote_starts) - 1)
                if len(self.quote_starts) > 1
                else None
            ),
            "rpc_interval_s": self.rpc.http.status().get("interval_s"),
            "ws_bid_ask_assets": len(self.backpack.stock_feed.latest),
            "ws_connected": self.backpack.stock_feed.socket is not None,
            "ws_proxied": self.backpack.stock_feed.proxy is not None,
            "ws_reconnects": self.backpack.stock_feed.reconnects,
            "rate_limits": {
                self.settings.dex_backend: self.jupiter.status(),
                "backpack": self.backpack.http.status(),
                "rpc": self.rpc.http.status(),
                **({"jupiter": self.quote_provider.status()} if self._quote_provider is not None else {}),
            },
            "rows": rows,
            "prices": prices,
            # History is display-only, explicitly timestamped and never used by
            # the spread engine. A failed refresh still removes the active row.
            "price_catalog": catalog,
            "last_quote_at": max((r["updated_at"] for r in self.price_history.values()), default=None),
            "price_repeat_median_s": repeat_intervals[len(repeat_intervals) // 2]
            if repeat_intervals
            else None,
            "diagnostics": diagnostics,
            "rfq_validation": config.rfq_validation,
            "rfq_cache_ttl_s": config.rfq_cache_s,
            "rfq_status": rfq_status,
            "rfq_sell_status": [
                check.public(now)
                for (symbol, side), check in self.rfq.latest.items()
                if side == "Ask" and symbol in self.assets
            ],
            "rfq_available_assets": sum(r["state"] == "available" for r in rfq_status),
            "rfq_unavailable_assets": sum(r["state"] == "unavailable" for r in rfq_status),
            "rfq_unknown_assets": sum(r["state"] == "unknown" for r in rfq_status),
            "rfq_reason_counts": dict(Counter(r["reason"] for r in rfq_status if r["state"] != "available")),
            "note": "Indicative: RFQ inspection is cancelled, fee/gas are modeled; a purchase still requires a new RFQ and fill. Reverse execution unavailable.",
        }

    def report(self, symbol: str | None = None) -> dict:
        """Current valid prices, synchronously. Never trigger network work while rendering."""
        started = time.perf_counter()
        snapshot = self.snapshot()
        if symbol:
            snapshot["rows"] = [row for row in snapshot["rows"] if row["asset"] == symbol]
            snapshot["prices"] = [row for row in snapshot["prices"] if row["asset"] == symbol]
        snapshot["report_source"] = "background_cache"
        snapshot["report_duration_s"] = time.perf_counter() - started
        self.last_report = {
            key: snapshot[key]
            for key in ("observed_at", "report_source", "report_duration_s", "fresh_calculations")
        }
        return snapshot

    def request_refresh(self, symbol: str) -> bool:
        asset = self.assets.get(symbol)
        if asset is None:
            return False
        now = self.clock()
        for direction in (FORWARD, REVERSE):
            if (direction == REVERSE and not asset.deposit_enabled) or self.rfq_cooldown(asset, direction):
                continue
            if now < self.retry_after.get((symbol, direction), 0):
                continue
            result = self.results.get((symbol, direction))
            if result and 0 <= now - result["observed_at"] < self.settings.monitor.quote_refresh_s:
                continue
            self.requested.setdefault((symbol, direction), now)
        return any(key[0] == symbol for key in self.requested) or any(
            key[0] == symbol for key in self.pending
        )

    async def refresh_report(self, symbol: str | None = None) -> dict:
        """Refresh public inputs and every displayed pair before producing a report."""
        from .direct import DirectRaydium

        async with self.report_lock:
            self.reporting = True
            started = self.clock()
            try:
                # Let the existing bounded work finish; no competing scheduler refreshes.
                async with asyncio.timeout(45):
                    await asyncio.shield(asyncio.gather(*list(self.pending.values())))
                    await self.refresh_metadata()
                    await self.refresh_prices()
                    snapshot = self.snapshot(include_hidden=True)
                    eligible = [
                        r
                        for r in snapshot["rows"]
                        if r["chain"] == "verified_unit_mapping"
                        and (r["direction"] != REVERSE or self.assets[r["asset"]].deposit_enabled)
                        and (symbol is None or r["asset"] == symbol)
                        and not self.rfq_cooldown(self.assets[r["asset"]], r["direction"])
                    ]
                    if symbol is None:
                        eligible.sort(
                            key=lambda r: (
                                not r["screen_positive"],
                                -dec(r["screen_spread_bps"]) if r["screen_fresh"] else Decimal("Infinity"),
                                r["asset"],
                                r["direction"],
                            )
                        )
                    keys = [(r["asset"], r["direction"]) for r in eligible[:5]]
                    if isinstance(self.quote_provider, DirectRaydium):
                        async with self.quote_provider.batch_quotes() as quote_pair:
                            await asyncio.gather(*(self.quote(key, quote_pair=quote_pair) for key in keys))
                    else:
                        limit = asyncio.Semaphore(self.settings.monitor.concurrency)

                        async def refresh(key):
                            async with limit:
                                await self.quote(key)

                        await asyncio.gather(*(refresh(key) for key in keys))
                    await self.refresh_external_prices()
                snapshot = self.snapshot()
                snapshot["report_keys"] = keys
                snapshot["report_duration_s"] = self.clock() - started
                self.last_report = {
                    "observed_at": snapshot["observed_at"],
                    "keys": keys,
                    "duration_s": snapshot["report_duration_s"],
                    "fresh_quotes": sum(
                        1 for r in snapshot["prices"] if (r["asset"], r["direction"]) in keys
                    ),
                }
                snapshot["last_report"] = dict(self.last_report)
                return snapshot
            finally:
                self.reporting = False

    def text(self, symbol: str | None = None, *, snapshot: dict | None = None) -> str:
        snapshot = snapshot if snapshot is not None else self.snapshot()
        keys = {tuple(key) for key in snapshot.get("report_keys", [])}

        def selected(r):
            return (symbol is None or r["asset"] == symbol) and (
                "report_keys" not in snapshot or (r["asset"], r["direction"]) in keys
            )

        rows = [r for r in snapshot["rows"] if selected(r) and r["fresh"] and r.get("profit_raw", 0) > 0][:5]
        prices = [r for r in snapshot.get("prices", []) if selected(r)]
        lines = [f"Котировки: {len(prices)} свежих · объём до {self.settings.monitor.notional_usdc} USDC"]
        if snapshot.get("rfq_validation"):
            lines[0] += f" · RFQ {snapshot['rfq_available_assets']}/{snapshot['universe']}"
        if snapshot.get("last_quote_at"):
            lines.append(f"Последнее обновление: {quote_time(snapshot['last_quote_at'])}")
        if rows:
            for row in rows:
                lines.extend(["", *compact_quote(row)])
        else:
            lines.append("Положительных спредов после расходов нет.")
            for pair in prices[:5]:
                lines.append(
                    f"{pair['asset']} {price_direction(pair)}: BP {pair['backpack_price']} / "
                    f"{dex_label(pair)} {pair['dex_price']} · {pair['quote_age_s']:.0f}с"
                )
            if symbol and not prices:
                reasons = sorted(
                    {
                        code
                        for d in snapshot.get("diagnostics", [])
                        if d["asset"] == symbol
                        for code in d["reasons"]
                        if code != "nonpositive_net"
                    }
                )
                lines.append(f"{symbol}: {block_text(reasons) or 'свежей котировки пока нет'}")
                retry = max(
                    (d.get("retry_in_s", 0) for d in snapshot.get("diagnostics", []) if d["asset"] == symbol),
                    default=0,
                )
                if retry > 0:
                    lines.append(f"Повтор запроса {symbol} через {retry:.0f}с.")
        return "\n".join(lines)

    def prices_text(self, symbols: list[str] | None = None, *, page: int = 1, total: int = 1) -> str:
        snapshot = self.snapshot()
        selected = sorted(self.assets) if symbols is None else symbols
        catalog = {r["asset"]: r for r in snapshot["price_catalog"]}
        interval = snapshot["price_repeat_median_s"]
        cadence = f"повтор цены ~{interval:.0f}с" if interval is not None else "цены по очереди"
        lines = [
            f"Цены · до {self.settings.monitor.notional_usdc} USDC · {page}/{total}",
            f"RFQ каждые {self.settings.monitor.rfq_cache_s:g}с; {cadence} "
            f"(запрос ≥{self.quote_provider.interval:g}с).",
            f"Свежими считаются цены до {self.settings.monitor.max_quote_age_s:g}с. Время МСК.",
        ]
        for symbol in selected:
            record = catalog.get(symbol)
            if record is None:
                continue
            rfq = {"available": "доступна", "unavailable": "нет", "unknown": "непроверена"}[
                record["rfq_state"]
            ]
            lines.append(f"\n{symbol} · RFQ {rfq}")
            if record.get("dex_price") is not None:
                flag = "" if record["fresh"] else " · устарела"
                lines.append(
                    f"BP {record['backpack_price']} / {dex_label(record)} {record['dex_price']} USDC/шт{flag}"
                )
                lines.append(
                    f"{record['buy_quantity']} шт за {amount(record['cost_raw'])} → "
                    f"{amount(record['quoted_output_raw'])} USDC (мин {amount(record['minimum_output_raw'])})"
                )
                lines.append(
                    f"BP {quote_time(record['backpack_updated_at'], zone=False)} / "
                    f"DEX {quote_time(record['dex_updated_at'], zone=False)} · {record['quote_age_s']:.0f}с"
                )
            elif record.get("backpack_price") is not None:
                flag = "" if record["backpack_fresh"] else " · устарела"
                lines.append(f"BP {record['backpack_price']} USDC/шт{flag}; DEX нет котировки")
                lines.append(
                    f"RFQ {record['buy_quantity']} шт · "
                    f"обновлена {quote_time(record['backpack_updated_at'], zone=False)}"
                )
            else:
                lines.append("BP / DEX: котировки нет")
            if record["rfq_state"] != "available":
                lines.append(block_text([record["rfq_reason"]]))
        return "\n".join(lines)


def amount(value: int, decimals: int = 6) -> str:
    return format(ui(value, decimals), f".{decimals}f").rstrip("0").rstrip(".") or "0"


def quote_time(timestamp: float, *, zone: bool = True) -> str:
    local = datetime.fromtimestamp(timestamp, ZoneInfo("Europe/Moscow"))
    return local.strftime("%H:%M:%S") + (" МСК" if zone else "")


def dex_label(row: dict) -> str:
    return "Raydium" if row.get("quote_backend") == "raydium" else "Jup"


def price_direction(row: dict) -> str:
    label = dex_label(row)
    return f"BP → {label}" if row["direction"] == FORWARD else f"{label} → BP"


def price_pair(row: dict) -> dict:
    decimals = row["stock_decimals"]
    if row["direction"] == FORWARD:
        dex_price = ui(row["quoted_output_raw"]) / ui(row["delivered_raw"], decimals)
    else:
        dex_price = ui(row["cost_raw"]) / ui(row["quoted_output_raw"], decimals)
    bp_at = row["backpack_timestamp"]
    dex_at = row.get("dex_observed_at", row["quote_started_at"])
    return {
        "asset": row["asset"],
        "direction": row["direction"],
        "quote_backend": row["quote_backend"],
        "backpack_price": str(row["backpack_price"]),
        "dex_price": format(dex_price, ".6f"),
        "backpack_updated_at": bp_at,
        "dex_updated_at": dex_at,
        "updated_at": min(bp_at, dex_at),
        "quote_age_s": max(0, row.get("quote_age_s", 0), row.get("observed_at", dex_at) - min(bp_at, dex_at)),
        "buy_quantity": amount(row["acquired_raw"], decimals),
        "cost_raw": row["cost_raw"],
        "quoted_output_raw": row["quoted_output_raw"],
        "minimum_output_raw": row["minimum_output_raw"],
    }


def compact_quote(row: dict) -> list[str]:
    lines = [
        f"{row['asset']} {price_direction(row)}: +{amount(row['profit_raw'])} USDC "
        f"({dec(row['edge_bps']) / 100:.2f}%) · {row['quote_age_s']:.0f}с",
    ]
    if "quoted_output_raw" in row:
        pair = price_pair(row)
        lines.append(f"BP {pair['backpack_price']} / {dex_label(pair)} {pair['dex_price']} USDC/шт")
        if row["direction"] == FORWARD:
            lines.append(
                f"{pair['buy_quantity']} шт за {amount(row['cost_raw'])} → "
                f"мин {amount(row['proceeds_raw'])} USDC"
            )
        else:
            lines.append(
                f"{amount(row['cost_raw'])} USDC → {amount(row['sellable_raw'], row['stock_decimals'])} шт "
                f"→ {amount(row['proceeds_raw'])} USDC"
            )
        lines.append(f"Обновлено {quote_time(pair['updated_at'])}")
    else:
        lines.append(f"Размер: {amount(row['cost_raw'])} USDC")
    if not row.get("candidate") and row.get("candidate_blocks"):
        lines.append("Вход ограничен: " + block_text(row["candidate_blocks"]))
    return lines


def quote_markets(quote: dict) -> list[dict]:
    """Preserve the venues and splits of the actual size quote, not the spot shortlist."""
    markets = []
    for route in quote.get("routePlan", [])[:20]:
        info = route["swapInfo"]
        markets.append(
            {
                "pool": info["ammKey"],
                "label": str(info["label"])[:60],
                "bps": route.get("bps", int(dec(str(route.get("percent", 100))) * 100)),
                "input_mint": info.get("inputMint"),
                "output_mint": info.get("outputMint"),
            }
        )
    return markets


def quote_details(row: dict) -> list[str]:
    """Decimal quantities describe the exact-in calculation, not a guaranteed future fill."""
    if "quoted_output_raw" not in row:
        return []
    decimals = row["stock_decimals"]

    def shares(value: int) -> str:
        return amount(value, decimals)

    if row["direction"] == FORWARD:
        lines = [
            f"{'Оценка покупки:' if row.get('backpack_model') else 'Купить'} "
            f"{shares(row['acquired_raw'])} акций за {amount(row['cost_raw'])} USDC",
            f"Вывод: комиссия {shares(row['withdrawal_fee_raw'])} акций; "
            f"в пул {shares(row['delivered_raw'])} акций",
            f"Продажа: quote {amount(row['quoted_output_raw'])}; минимум {amount(row['proceeds_raw'])} USDC",
        ]
        dex_price = ui(row["quoted_output_raw"]) / ui(row["delivered_raw"], decimals)
        price_side = "продажи"
    else:
        dust = row["minimum_output_raw"] - row["sellable_raw"]
        lines = [
            f"Купить за {amount(row['cost_raw'])} USDC: quote {shares(row['quoted_output_raw'])} акций; "
            f"минимум {shares(row['minimum_output_raw'])}",
            f"{'Оценка продажи в BP:' if row.get('backpack_model') else 'Продать в BP'} "
            f"{shares(row['sellable_raw'])} акций за {amount(row['proceeds_raw'])} USDC; "
            f"остаток {shares(dust)} акций",
        ]
        dex_price = ui(row["cost_raw"]) / ui(row["quoted_output_raw"], decimals)
        price_side = "покупки"
    label = "Raydium" if row.get("quote_backend") == "raydium" else "Jup"
    lines.append(f"{label} цена {price_side}: {dex_price:.6f} USDC/акция")
    if row.get("quote_source") == "jupiter_flow":
        lines.append(f"Jupiter flow: {row.get('quote_profile', 'manual')}; котировка без исполнения")
    markets = row.get("route_markets", [])
    if markets:
        names = {USDC_MINT: "USDC", row.get("mint"): row.get("asset", "акции").removesuffix(".US")}

        def leg(market: dict) -> str:
            source, target = market.get("input_mint"), market.get("output_mint")
            pair = (
                f"{names.get(source, source[:4])}→{names.get(target, target[:4])} "
                if source and target
                else ""
            )
            return f"{pair}{market['label']} {Decimal(market['bps']) / 100:g}%"

        lines.append("Маршрут: " + "; ".join(leg(market) for market in markets))
    if row.get("dex_fee_bps"):
        gasless_label = " (включая gasless)" if row.get("dex_gasless") else ""
        lines.append(f"Комиссии Jupiter{gasless_label} {row['dex_fee_bps'] / 100:g}% уже включены в quote")
    bp_label = (
        "BP RFQ (проверена, отменена)"
        if row.get("backpack_quote_confirmed")
        else "BP оценка (FMV/last, без RFQ)"
        if row.get("backpack_model")
        else "BP"
    )
    gas_text = (
        "gasless уже включён" if row.get("dex_gasless") else f"gas {amount(row['gas_raw'])} USDC (модель)"
    )
    lines.append(
        f"{bp_label} {row['backpack_price']} USDC/акция; сессия {row['backpack_session'].removeprefix('US_EQUITIES_')}; "
        f"fee {row['backpack_fee_bps'] / 100:g}%; slippage {row['slippage_bps'] / 100:g}%; "
        f"{gas_text}"
    )
    return lines
