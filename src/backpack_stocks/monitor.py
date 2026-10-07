from __future__ import annotations

import asyncio
import os
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from functools import partial
from typing import TYPE_CHECKING
from urllib.parse import quote as url_quote
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from solders.pubkey import Pubkey

from .backpack import Backpack
from .indicative_feed import IndicativeFeed, IndicativeUnavailable
from .jupiter import Jupiter
from .jupiter_flow_quotes import JupiterFlowQuotes
from .models import STOCK_MINT, USDC_MINT, Observation, Settings, dec, floor_step, impact_percent, raw, ui
from .observability import MARKET_BLOCKS, QuoteUnavailable, error_code, event
from .proxy import resolve_proxy
from .rfq_validation import SIZE_REFUSALS, RfqCheck, RfqUnavailable, RfqValidator
from .sessions import ActiveSessionCache, next_session_open
from .solana import SolanaRPC, assert_stock_mint
from .spread_reporting import compact_coverage, format_spreads
from .stock_books import SPOT_SESSION, BookFill, BookUnavailable, StockBooks
from .stock_money import StockValueCalculator
from .volume_search import VolumeSearch

if TYPE_CHECKING:
    from .direct import DirectRaydium

FORWARD = "backpack_to_jupiter"
REVERSE = "jupiter_to_backpack"
BLOCK_LABELS = {
    "unsupported_mint_or_unit_mapping": "не подтверждено соответствие единиц акции и токена",
    "mint_verification_unavailable": "проверка токена Solana недоступна",
    "unverified": "токен Solana ещё не проверен",
    "spot_book_stale": "стакан устарел",
    "spot_book_unavailable": "стакан недоступен",
    "spot_book_empty": "стакан пуст",
    "spot_fee_unknown": "не подтверждена taker-комиссия Backpack",
    "spot_account_unverified": "нет проверки доступного spot-баланса",
    "spot_capacity_stale": "проверка spot-баланса устарела",
    "spot_insufficient_balance": "для стакана не хватает доступного баланса",
    "spot_insufficient_depth": "в стакане не хватает глубины на весь объём",
    "spot_invalid_quantity": "объём не соответствует лоту стакана",
    "spot_price_band": "объём выходит за ценовой диапазон taker-исполнения",
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
    "rfq_sell_check_pending": "доступность продажи RFQ ещё не проверена",
    "rfq_budget_changed": "RFQ не укладывается в бюджет",
    "indicative_quote_pending": "ожидает публичной котировки Backpack",
    "indicative_requires_rfq": "публичная цена: требуется RFQ на точный объём",
    "indicative_quantity_mismatch": "ограничения публичной котировки отличаются от метаданных",
    "stock_indicative_unavailable": "нет текущей публичной котировки Backpack",
    "indicative_stalePrice": "источник цены Backpack устарел",
    "indicative_noActiveSession": "нет открытой сессии Backpack",
    "indicative_quoterUnavailable": "котировщик Backpack недоступен",
    "indicative_quote_stale": "публичная котировка Backpack устарела",
    "budget_below_minimum_lot": "минимальный лот дороже лимита",
    "below_withdrawal_minimum": "объём ниже минимума вывода",
    "nonpositive_net": "нет положительного net после расходов",
    "insufficient_liquidity": "не хватает ликвидности на заданный объём",
    "insufficient_tick_coverage": "достигнут лимит загруженных tick arrays",
    "no_tick_liquidity": "нет доступных directional tick arrays",
    "ReplayError": "Jupiter не дал корректную котировку",
    "jupiter_no_route": "Jupiter не нашёл маршрут на этот объём",
    "jupiter_insufficient_liquidity": "Jupiter: не хватает ликвидности на этот объём",
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


@dataclass
class ForwardCoverage:
    asset: Asset
    session: str
    quantity_raw: int
    check: RfqCheck | None = None
    result: dict | None = None
    plan: dict | None = None
    attempted_at: float = 0
    discovery_at: float = 0
    retry_at: float = 0
    failures: int = 0
    renewal_retry_at: float = 0
    renewal_reason: str | None = None
    route_candidate: dict | None = None
    prefetched_route: dict | None = None
    prefetched_identity: tuple | None = None
    discovery_retry_at: float = 0
    discovery_failures: int = 0
    reason: str | None = None
    completed: bool = False


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
        self.focus_until: dict[str, float] = {}
        self.local_plans: dict[tuple[str, str], dict] = {}
        self.local_after: dict[tuple[str, str], float] = {}
        self.local_inputs: dict[tuple[str, str], tuple] = {}
        self.local_calculations = 0
        self.local_pending: set[tuple[str, str]] = set()
        self.local_turn = True
        self.rfq_after: dict[tuple[str, str], float] = {}
        self.updated = asyncio.Event()
        self.stock_values = StockValueCalculator()
        self.retry_after: dict[tuple[str, str], float] = {}
        self.quote_failures: Counter[tuple[str, str]] = Counter()
        self.volume_search = VolumeSearch()
        self.optimization_requested: set[tuple[str, str]] = set()
        self.forward_coverage: dict[str, ForwardCoverage] = {}
        self.forward_coverage_blocks: dict[str, dict] = {}
        self.coverage_route_pending: dict[str, asyncio.Task] = {}
        self.coverage_pending: set[tuple[str, str]] = set()
        self.search_pending: set[tuple[str, str]] = set()
        self.minimum_pending: dict[tuple[str, str], asyncio.Task] = {}
        self.coverage_turn = True
        self.quote_observer: Callable[[dict], None] | None = None
        config = settings.monitor
        self.books = StockBooks(backpack, ttl=config.max_quote_age_s, clock=clock)
        self.rfq = RfqValidator(
            backpack,
            max_age=config.max_quote_age_s,
            retry_s=config.rfq_retry_s,
            timeout=config.rfq_timeout_s,
            cache_s=config.rfq_cache_s,
            concurrency=config.rfq_concurrency,
            clock=clock,
        )
        # The public quoter stream covers the paper universe without opening
        # hundreds of account RFQs. Positive calls still need an exact RFQ.
        self.indicative_feed: IndicativeFeed | None = (
            backpack.indicative_feed
            if type(backpack) is Backpack and settings.mode == "paper" and self.forward_coverage_enabled
            else None
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
                self.forward_coverage.pop(symbol, None)
                self.forward_coverage_blocks.pop(symbol, None)
                for direction in (FORWARD, REVERSE):
                    self.local_plans.pop((symbol, direction), None)
                    self.local_after.pop((symbol, direction), None)
                    self.local_inputs.pop((symbol, direction), None)
                    self.price_history.pop((symbol, direction), None)
                    self.volume_search.samples.pop((symbol, direction), None)
                    self.volume_search.turns.pop((symbol, direction), None)
                for side in ("Bid", "Ask"):
                    self.rfq_after.pop((symbol, side), None)
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
        from .direct import DirectRaydium

        if isinstance(self.jupiter, DirectRaydium):
            self.jupiter.update_assets(
                [
                    {"symbol": a.symbol, "mint": a.mint, "decimals": a.decimals}
                    for a in entries
                    if chain.get(a.symbol) == "verified_unit_mapping"
                ]
            )
        self.sessions, self.holidays = sessions, holidays
        if self.settings.monitor.spot_books:
            try:
                self.books.discover(await self.backpack.markets(), set(found))
            except Exception as error:
                self.books.discover([], set())
                event("stock_markets_error", component="backpack_spot", code=error_code(error))
        await self.backpack.stock_feed.subscribe([a.ticker for a in entries])
        if self.indicative_feed is not None:
            await self.indicative_feed.subscribe([a.ticker for a in entries])
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
        if self.indicative_feed is not None:
            try:
                state = self.coverage_state(asset)
                check = self.indicative_check(asset, state.quantity_raw, state.session)
                observation = self.check_observation(check)
                try:
                    sale = self.indicative_check(asset, state.quantity_raw, state.session, side="Ask")
                    observation = observation.model_copy(update={"bid": sale.price})
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    pass
                return observation
            except (ValueError, KeyError, TypeError, ArithmeticError):
                pass
        latest = self.backpack.stock_feed.latest.get(asset.ticker)
        if latest and -2 <= self.clock() - latest.timestamp <= self.settings.policy.quote_max_age_s:
            return latest
        if (
            self.clock() - self.external_at > self.settings.monitor.price_batch_s * 2
            or asset.symbol not in self.external
        ):
            if self.forward_coverage_enabled:
                # An exact, current owned Bid can guide later sizing. Every
                # larger purchase still obtains its own exact RFQ before DEX.
                try:
                    return self.coverage_observation(asset)
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    pass
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

    def calendar_session(self, now: float) -> str | None:
        """Share one content-checked calendar decision across all monitor callers."""
        calendar = getattr(self, "_coverage_calendar", None)
        if calendar is None:
            calendar = self._coverage_calendar = ActiveSessionCache()
        return calendar.get(self.sessions, self.holidays, datetime.fromtimestamp(now, UTC))

    def quantity_session(self, observation: Observation) -> str:
        now = self.clock()
        if observation.session == SPOT_SESSION:
            if not 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2:
                raise BookUnavailable("spot_book_stale")
            return SPOT_SESSION
        if now < self.metadata_at:
            raise ValueError("No RFQ trading session is open")
        if self.sessions:
            if not 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2:
                raise ValueError("No RFQ trading session is open")
            session = self.calendar_session(now)
            if session is None:
                raise ValueError("No RFQ trading session is open")
            return session
        if observation.session == "MODEL":
            raise ValueError("No RFQ trading session is open")
        return observation.session

    def quantity_rules(self, asset: Asset, observation: Observation) -> dict:
        session = self.quantity_session(observation)
        if session == SPOT_SESSION:
            market = self.books.markets[asset.symbol]
            ceiling = max(
                self.books.quantity_ceiling(asset.symbol, "Bid"),
                self.books.quantity_ceiling(asset.symbol, "Ask"),
            )
            return {
                "stepSize": str(market.step),
                "minQuantity": str(market.minimum),
                "maxQuantity": str(min(ceiling, market.maximum) if market.maximum else ceiling),
            }
        constraints = next((s for s in asset.sessions if s["name"] == session), None)
        if constraints is None:
            raise ValueError("No quantity constraints for observed session")
        return constraints

    def sizing(
        self, asset: Asset, observation: Observation, *, notional: Decimal | None = None
    ) -> tuple[int, int]:
        if self.chain.get(asset.symbol) != "verified_unit_mapping":
            raise ValueError("Mint unit mapping is not verified")
        p = self.settings.policy
        constraints = self.quantity_rules(asset, observation)
        budget = self.budget(asset, observation, notional)
        ask_with_fee = dec(observation.ask) * (1 + Decimal(p.backpack_fee_bps) / 10000)
        quantity = (
            ui(self.books.budget_quantity(asset.symbol, raw(budget), self.minimum_buy(asset, observation)))
            if observation.session == SPOT_SESSION
            else floor_step(
                min(budget / ask_with_fee, dec(constraints["maxQuantity"])), dec(constraints["stepSize"])
            )
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

    def rfq_observation(self, asset: Asset, direction: str = FORWARD) -> Observation:
        obs = self.observation(asset)
        check = self.rfq.latest.get((asset.symbol, "Bid" if direction == FORWARD else "Ask"))
        if (
            check
            and check.price
            and check.session == self.quantity_session(obs)
            and check.eligible(self.clock())
        ):
            obs = obs.model_copy(update={"ask" if direction == FORWARD else "bid": check.price})
        return obs

    def planning_observation(self, asset: Asset, direction: str = FORWARD) -> Observation:
        spot = self.books.observation(asset.symbol) if self.books.available(asset.symbol) else None
        try:
            obs = self.rfq_observation(asset, direction)
            session = self.quantity_session(obs)
            if (
                spot
                and self.settings.monitor.rfq_validation
                and not self.rfq_ready(asset, session, direction)
            ):
                return spot
        except (ValueError, KeyError, TypeError):
            if spot:
                return spot
            raise
        if spot:
            fee = self.books.fee_bps
            assert fee is not None
            if direction == FORWARD and dec(spot.ask) * (1 + fee / 10000) < dec(obs.ask) * (
                1 + Decimal(self.settings.policy.backpack_fee_bps) / 10000
            ):
                return spot
            if direction == REVERSE and dec(spot.bid) * (1 - fee / 10000) > dec(obs.bid) * (
                1 - Decimal(self.settings.policy.backpack_fee_bps) / 10000
            ):
                return spot
        return obs

    def minimum_buy(self, asset: Asset, obs: Observation) -> Decimal:
        constraints = self.quantity_rules(asset, obs)
        return self.minimum_quantity(asset, constraints)

    def minimum_quantity(self, asset: Asset, constraints: dict) -> Decimal:
        step = dec(constraints["stepSize"])
        withdrawal = asset.withdrawal_min
        if self.settings.policy.withdrawal_semantics == "additional":
            withdrawal += asset.fee
        quantity = max(
            dec(constraints["minQuantity"]), withdrawal, asset.fee + Decimal(1).scaleb(-asset.decimals)
        )
        quantity = (quantity / step).to_integral_value(rounding=ROUND_CEILING) * step
        if quantity > dec(constraints["maxQuantity"]):
            raise RfqUnavailable("rfq_invalid_quantity")
        return quantity

    @property
    def forward_coverage_enabled(self) -> bool:
        return self.settings.monitor.auto_size and self.settings.monitor.rfq_validation

    def coverage_rules(self, asset: Asset) -> tuple[str, dict]:
        """Published active session and lots do not require an indicative price."""
        now = self.clock()
        if not 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2:
            raise ValueError("No RFQ trading session is open")
        session = self.calendar_session(now)
        if session is None:
            raise ValueError("No RFQ trading session is open")
        rules = next((row for row in asset.sessions if row["name"] == session), None)
        if rules is None:
            raise ValueError("No quantity constraints for observed session")
        return session, rules

    def current_rfq_session(self, asset: Asset) -> str:
        if self.forward_coverage_enabled and self.sessions:
            return self.coverage_rules(asset)[0]
        return self.quantity_session(self.observation(asset))

    def coverage_observation(self, asset: Asset, check: RfqCheck | None = None) -> Observation:
        session, _ = self.coverage_rules(asset)
        state = self.forward_coverage.get(asset.symbol)
        check = check or (state.check if state else None)
        if (
            state is None
            or state.asset != asset
            or self.assets.get(asset.symbol) != asset
            or self.chain.get(asset.symbol) != "verified_unit_mapping"
            or check is None
            or not check.usable(self.clock())
            or check.symbol != asset.symbol
            or check.side != "Bid"
            or check.session != session
            or dec(check.quantity) != ui(state.quantity_raw, asset.decimals)
            or check.price is None
            or dec(check.price or "0") <= 0
        ):
            raise RfqUnavailable("rfq_check_stale")
        return self.check_observation(check)

    @staticmethod
    def check_observation(check: RfqCheck) -> Observation:
        if not check.price:
            raise RfqUnavailable(check.reason)
        return Observation(
            ask=check.price,
            # Only the purchase ask was observed. Empty bid is unavailable,
            # rather than an invented sale price; reverse still needs its Ask.
            bid="",
            timestamp=check.checked_at,
            session=check.session,
            source="Backpack stockIndicativeQuote limit; requires own RFQ"
            if check.source == "indicative"
            else "Backpack AwaitAccept RFQ; cancelled after inspection",
        )

    def indicative_check(
        self, asset: Asset, quantity_raw: int, session: str, *, side: str = "Bid"
    ) -> RfqCheck:
        assert self.indicative_feed is not None
        quantity = str(ui(quantity_raw, asset.decimals))
        quote = self.indicative_feed.get(
            asset.ticker,
            side,
            quantity,
            session,
            max_age=self.settings.monitor.max_quote_age_s,
            now=self.clock(),
        )
        rules = self.coverage_rules(asset)[1]
        if any(
            dec(quote.quantity_rules[k]) != dec(rules[k]) for k in ("minQuantity", "maxQuantity", "stepSize")
        ):
            raise RfqUnavailable("indicative_quantity_mismatch")
        stamp = quote.published_at
        expiry = stamp + self.settings.monitor.max_quote_age_s
        return RfqCheck(
            asset.symbol,
            side,
            quantity,
            session,
            "available",
            "indicative_quote_observed",
            stamp,
            expiry,
            expiry,
            quote.price,
            source="indicative",
        )

    def revalue_indicative_minimum(self, state: ForwardCoverage) -> bool:
        """Recost an owned exact minimum against a real update on unchanged DEX banks."""
        from .direct import DirectRaydium

        provider, asset, row, plan, check = (
            self.quote_provider,
            state.asset,
            state.result,
            state.plan,
            state.check,
        )
        if (
            not isinstance(provider, DirectRaydium)
            or provider.bank_feed is None
            or not row
            or not plan
            or not check
            or check.source != "indicative"
            or self.indicative_feed is None
            or not self.forward_coverage_enabled
        ):
            return False
        try:
            now = self.clock()
            session, rules = self.coverage_rules(asset)
            if (
                self.assets.get(asset.symbol) != asset
                or self.chain.get(asset.symbol) != "verified_unit_mapping"
                or session != state.session
                or raw(self.minimum_quantity(asset, rules), asset.decimals) != state.quantity_raw
                or not check.usable(now)
                or check.symbol != asset.symbol
                or check.side != "Bid"
                or check.session != session
                or dec(check.quantity) != ui(state.quantity_raw, asset.decimals)
                or not row.get("minimum_coverage")
                or row.get("backpack_source") != "indicative"
                or row.get("backpack_quote_confirmed")
                or row["asset"] != asset.symbol
                or row["direction"] != FORWARD
                or row["mint"] != asset.mint
                or row["acquired_raw"] != state.quantity_raw
                or row["backpack_session"] != session
                or row["input_raw"] != state.quantity_raw - raw(asset.fee, asset.decimals)
                or plan["mint"] != asset.mint
                or plan["asset"] != asset.symbol
                or plan["direction"] != FORWARD
                or plan["acquired_raw"] != state.quantity_raw
                or not plan.get("minimum_coverage")
                or plan.get("backpack_source") != "indicative"
                or plan["backpack_session"] != session
                or plan["input_raw"] != row["input_raw"]
                or not 0 <= now - row["dex_observed_at"] < self.settings.policy.quote_max_age_s
                or not provider.bank_versions_are_current(row.get("dex_bank_versions", []))
            ):
                return False
            observation = self.check_observation(check)
            changed = row["backpack_timestamp"] != check.checked_at or dec(row["backpack_price"]) != dec(
                check.price or "0"
            )
            if changed:
                p = self.settings.policy
                cost = self.stock_values.value(
                    state.quantity_raw, asset.decimals, observation.ask, p.backpack_fee_bps, buy=True
                )
                profit = row["proceeds_raw"] - cost - row["gas_raw"]
                edge = Decimal(profit) * 10000 / cost
                row = {
                    **row,
                    "cost_raw": cost,
                    "profit_raw": profit,
                    "edge_bps": str(edge),
                    "backpack_price": observation.ask,
                    "backpack_fee_bps": p.backpack_fee_bps,
                    "backpack_timestamp": check.checked_at,
                    "backpack_valid_until": check.retry_at,
                    "source": observation.source,
                    "search_budget_usdc": str(ui(cost)),
                    "size_feasible": dec(row["price_impact_pct"]) <= dec(p.max_price_impact_pct)
                    and row["slippage_bps"] <= p.slippage_bps,
                    "passes_model_limits": (self.settings.monitor.auto_size or cost <= raw(p.max_cost_usdc))
                    and profit >= raw(p.min_profit_usdc)
                    and edge >= p.min_edge_bps
                    and dec(row["price_impact_pct"]) <= dec(p.max_price_impact_pct)
                    and row["slippage_bps"] <= p.slippage_bps,
                }
                state.result = row
                plan = {**plan, **row, "dex_plan": plan["dex_plan"]}
                state.plan = plan
                self.updated.set()
            # The marker owns exactly the public observation and unchanged
            # validated heads just costed; it cannot skip a newer DEX input.
            key = asset.symbol, FORWARD
            version = self.local_input_version(key, plan)
            if version is not None and version[0] == tuple(tuple(v) for v in row["dex_bank_versions"]):
                self.local_inputs[key] = (version[0], (check.checked_at, check.price))
            state.completed = True
            state.reason = None
            return True
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return False

    def sync_indicative_coverage(self, state: ForwardCoverage) -> bool:
        if self.indicative_feed is None:
            return False
        try:
            state.check = self.indicative_check(state.asset, state.quantity_raw, state.session)
            if state.reason and state.reason.startswith(("indicative_", "stock_indicative_")):
                state.reason = None
            return self.revalue_indicative_minimum(state)
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            code = error_code(error)
            latest = self.indicative_feed.latest.get(state.asset.ticker)
            stamp = latest.published_at if latest else 0
            expiry = stamp + self.settings.monitor.max_quote_age_s
            provider_unavailable = bool(
                latest
                and 0 <= self.clock() - stamp < self.settings.monitor.max_quote_age_s
                and stamp <= latest.received_at <= self.clock()
                and latest.reason
                and latest.reason not in {"indicative_invalid_payload", "indicative_invalid_reason"}
            )
            state.check = RfqCheck(
                state.asset.symbol,
                "Bid",
                str(ui(state.quantity_raw, state.asset.decimals)),
                state.session,
                "unavailable" if provider_unavailable else "unknown",
                code,
                stamp,
                expiry,
                expiry,
                source="indicative",
            )
            if state.check.state == "unavailable" and not state.attempted_at:
                state.attempted_at = self.clock()
            state.reason = (
                (error.reason if error.reason.startswith("indicative_") else "indicative_" + error.reason)
                if isinstance(error, IndicativeUnavailable)
                else code
            )
            state.completed = state.completed or state.check.state == "unavailable"
            return False

    def coverage_state(self, asset: Asset) -> ForwardCoverage:
        """Own one legal minimum Bid, independently of the size search cache."""
        if self.chain.get(asset.symbol) != "verified_unit_mapping":
            raise ValueError("Mint unit mapping is not verified")
        session, rules = self.coverage_rules(asset)
        if self.settings.policy.withdrawal_semantics == "unverified":
            raise ValueError("Withdrawal fee convention requires configuration")
        try:
            step, lower, upper = (dec(rules[key]) for key in ("stepSize", "minQuantity", "maxQuantity"))
            if step <= 0 or lower < 0 or upper <= 0 or upper < lower:
                raise ValueError("Invalid quantity constraints")
            quantity = raw(self.minimum_quantity(asset, rules), asset.decimals)
        except (ValueError, KeyError, TypeError, ArithmeticError) as error:
            raise RfqUnavailable("rfq_invalid_quantity") from error
        state = self.forward_coverage.get(asset.symbol)
        if (
            state is None
            or state.asset != asset
            or state.session != session
            or state.quantity_raw != quantity
        ):
            state = ForwardCoverage(asset, session, quantity)
            self.forward_coverage[asset.symbol] = state
        self.forward_coverage_blocks.pop(asset.symbol, None)
        return state

    def exact_rfq(self, asset: Asset, quantity: str, session: str, side: str = "Bid") -> RfqCheck | None:
        state = self.forward_coverage.get(asset.symbol) if self.forward_coverage_enabled else None
        check = state.check if state and state.asset == asset and side == "Bid" else None
        if (
            check
            and check.source == "rfq"
            and check.symbol == asset.symbol
            and check.side == side
            and check.session == session
            and dec(check.quantity) == dec(quantity)
            and check.usable(self.clock())
        ):
            return check
        return self.rfq.cached_exact(asset.symbol, quantity, session, side)

    async def coverage_buy(
        self, asset: Asset, *, refresh: bool = False
    ) -> tuple[int, int, Observation, RfqCheck, None]:
        state = self.coverage_state(asset)
        if self.indicative_feed is not None:
            self.sync_indicative_coverage(state)
            check = state.check
            if check is None or not check.usable(self.clock()):
                raise RfqUnavailable(check.reason if check else "indicative_quote_pending")
            return (
                state.quantity_raw,
                state.quantity_raw - raw(asset.fee, asset.decimals),
                self.coverage_observation(asset, check),
                check,
                None,
            )
        previous = state.check
        quantity = str(ui(state.quantity_raw, asset.decimals))
        check = None if refresh else self.exact_rfq(asset, quantity, state.session)
        if check is None:
            check = await self.rfq.check(asset.symbol, quantity, state.session, force_refresh=refresh)
        if (
            check.symbol != asset.symbol
            or check.side != "Bid"
            or check.session != state.session
            or dec(check.quantity) != dec(quantity)
        ):
            raise RfqUnavailable("rfq_response_mismatch")
        if check.state == "unknown" and previous and previous.usable(self.clock()):
            state.renewal_retry_at = check.eligibility_until
            state.renewal_reason = check.reason
            state.reason = check.reason
            event("coverage_bid_renewal_unknown", level="WARNING", asset=asset.symbol, code=check.reason)
            check = previous
        else:
            state.check = check
            state.renewal_retry_at = 0
            state.renewal_reason = None
        if check.state == "unavailable":
            state.completed = True
        if not check.usable(self.clock()):
            raise RfqUnavailable(check.reason)
        obs = self.coverage_observation(asset, check)
        return state.quantity_raw, state.quantity_raw - raw(asset.fee, asset.decimals), obs, check, None

    def coverage_result_fresh(self, state: ForwardCoverage) -> bool:
        from .direct import DirectRaydium

        row, check, now = state.result, state.check, self.clock()
        try:
            return bool(
                row
                and check
                and state.asset == self.assets.get(state.asset.symbol)
                and self.chain.get(state.asset.symbol) == "verified_unit_mapping"
                and self.coverage_rules(state.asset)[0] == state.session
                and 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2
                and 0 <= now - row["dex_observed_at"] <= self.settings.monitor.max_quote_age_s
                and check.usable(now)
                and check.symbol == state.asset.symbol
                and check.side == "Bid"
                and row["mint"] == state.asset.mint
                and row["acquired_raw"] == state.quantity_raw
                and check.session == row["backpack_session"]
                and dec(check.quantity) == ui(row["acquired_raw"], state.asset.decimals)
                and (
                    check.checked_at == row["backpack_timestamp"]
                    or check.source == "indicative"
                    and 0 <= now - row["backpack_timestamp"] <= self.settings.monitor.max_quote_age_s
                )
                and row.get("backpack_source", "rfq") == check.source
                and dec(check.price or "0") == dec(row["backpack_price"])
                and (
                    not isinstance(self.quote_provider, DirectRaydium)
                    or self.quote_provider.bank_versions_are_current(row.get("dex_bank_versions", []))
                )
            )
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return False

    def choose_forward_coverage(
        self, *, ignore_pending: tuple[str, str] | None = None
    ) -> tuple[str, str] | None:
        available = self.forward_coverage_candidates(ignore_pending=ignore_pending)
        return (min(available)[2], FORWARD) if available else None

    def forward_coverage_candidates(
        self, *, ignore_pending: tuple[str, str] | None = None
    ) -> list[tuple[bool, float, str]]:
        """Validate current minimum owners before ranking a dispatch batch."""
        if not self.forward_coverage_enabled:
            return []
        available = []
        now = self.clock()
        for asset in self.assets.values():
            key = asset.symbol, FORWARD
            if (
                self.chain.get(asset.symbol) != "verified_unit_mapping"
                or key in self.minimum_pending
                or key in self.pending
                and key != ignore_pending
            ):
                continue
            try:
                state = self.coverage_state(asset)
            except (ValueError, KeyError, TypeError, ArithmeticError) as error:
                reason = error_code(error)
                metadata_fresh = 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2
                status = "waiting_bid"
                if str(error) in {
                    "No RFQ trading session is open",
                    "No quantity constraints for observed session",
                }:
                    status = "skipped_current_session" if metadata_fresh else "waiting_metadata"
                    reason = "rfq_session_unavailable" if metadata_fresh else "metadata_stale"
                elif (
                    reason == "rfq_invalid_quantity"
                    or str(error) == "Withdrawal fee convention requires configuration"
                ):
                    status = "unsupported_quantity"
                    reason = (
                        "rfq_invalid_quantity"
                        if reason == "rfq_invalid_quantity"
                        else "withdrawal_semantics_unverified"
                    )
                elif str(error) == "No fresh provider quote or external-price model":
                    reason = "backpack_price_unavailable"
                self.forward_coverage_blocks[asset.symbol] = {
                    "asset": asset,
                    "state": status,
                    "reason": reason,
                    "terminal": metadata_fresh
                    and status in {"skipped_current_session", "unsupported_quantity"},
                }
                continue
            check = state.check
            if self.indicative_feed is not None:
                if self.sync_indicative_coverage(state):
                    continue
                check = state.check
            if check and check.state != "available" and now < check.eligibility_until:
                continue
            if now < state.retry_at or (
                state.attempted_at and now - state.attempted_at < self.settings.monitor.quote_refresh_s
            ):
                continue
            # After every first attempt, incomplete routes join the same age
            # queue; repeatedly missing banks cannot starve already priced assets.
            available.append((bool(state.attempted_at), state.attempted_at, asset.symbol))
        return available

    def coverage_topology_identity(self, state: ForwardCoverage) -> tuple:
        """Bind a topology hint to the current legal minimum without a purchase price."""
        asset = state.asset
        session, rules = self.coverage_rules(asset)
        if (
            self.assets.get(asset.symbol) != asset
            or self.chain.get(asset.symbol) != "verified_unit_mapping"
            or self.forward_coverage.get(asset.symbol) is not state
            or session != state.session
            or raw(self.minimum_quantity(asset, rules), asset.decimals) != state.quantity_raw
            or self.settings.policy.withdrawal_semantics == "unverified"
            or state.quantity_raw <= raw(asset.fee, asset.decimals)
        ):
            raise ValueError("Minimum topology identity changed")
        return (
            asset.symbol,
            asset.mint,
            asset.decimals,
            asset.fee,
            asset.withdrawal_min,
            session,
            state.quantity_raw,
            self.settings.policy.withdrawal_semantics,
            *(dec(rules[key]) for key in ("minQuantity", "maxQuantity", "stepSize")),
        )

    @staticmethod
    def minimum_route_topology(route: dict) -> dict:
        """Retain route addresses, input weights and original heads, never prices or proceeds."""
        result = {
            key: route[key] for key in ("inputMint", "outputMint", "inAmount", "contextSlot", "_observed_at")
        }
        legs = route.get("_direct_route")
        if legs:
            result["_direct_route"] = [SpreadMonitor.minimum_route_topology(leg) for leg in legs]
        else:
            result["pool"] = route["pool"]
            direct = route.get("_direct", {})
            result["_direct"] = {
                key: list(direct[key]) if key == "arrays" else direct[key]
                for key in ("program", "arrays")
                if key in direct
            }
        return result

    def valid_prefetched_route(self, state: ForwardCoverage) -> dict | None:
        try:
            if state.prefetched_route and state.prefetched_identity == self.coverage_topology_identity(state):
                return state.prefetched_route
        except (ValueError, KeyError, TypeError, ArithmeticError):
            pass
        return None

    def prefetched_coverage_quote(
        self, state: ForwardCoverage, *, fallback: Callable[..., Awaitable[dict]] | None = None
    ) -> Callable[..., Awaitable[dict]] | None:
        """Activate only a legal hint and a real current purchase quote in the cold lane."""
        from .direct import DirectRaydium

        provider = self.quote_provider
        if (
            not isinstance(provider, DirectRaydium)
            or provider.bank_feed is None
            or self.indicative_feed is None
        ):
            return None
        template = self.valid_prefetched_route(state)
        if template is None:
            return None
        try:
            self.sync_indicative_coverage(state)
            self.coverage_observation(state.asset)
            identity = self.coverage_topology_identity(state)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return None

        async def refresh(source, target, amount, *, priority=10):
            if (
                self.coverage_topology_identity(state) != identity
                or source != state.asset.mint
                or target != USDC_MINT
                or amount != state.quantity_raw - raw(state.asset.fee, state.asset.decimals)
            ):
                raise ValueError("Minimum topology identity changed")
            try:
                route = await provider.refreshed_plan_quote(
                    source, target, amount, plan=template, priority=priority
                )
            except (QuoteUnavailable, ValueError) as error:
                codes = error.codes if isinstance(error, QuoteUnavailable) else [error_code(error)]
                if not codes or not set(codes) <= {
                    "pool_bank_not_ready",
                    "stale_pool_quote",
                    "pool_changed_during_refresh",
                    "insufficient_tick_coverage",
                    "insufficient_liquidity",
                    "no_tick_liquidity",
                    "no_direct_pool",
                    "ValueError",
                }:
                    raise
                if self.coverage_topology_identity(state) != identity:
                    raise ValueError("Minimum topology identity changed") from error
                route = await (fallback or partial(provider.quote_pair, _rpc=provider.bank_feed))(
                    source, target, amount, priority=priority, minimum=True
                )
            if self.coverage_topology_identity(state) != identity:
                raise ValueError("Minimum topology identity changed")
            return route

        return refresh

    async def discover_coverage_route(self, state: ForwardCoverage) -> None:
        """Explore one fixed minimum in the background; never publish its price."""
        from .direct import DirectRaydium

        provider, asset = self.quote_provider, state.asset
        if not isinstance(provider, DirectRaydium) or provider.bank_feed is None:
            return
        try:
            prewarm = (
                state.plan is None and self.indicative_feed is not None and self.settings.mode == "paper"
            )
            if state.plan is None and not prewarm:
                return
            identity = self.coverage_topology_identity(state)
            if not prewarm:
                self.coverage_observation(asset)
            quantity = state.quantity_raw
            delivered = quantity - raw(asset.fee, asset.decimals)
            route = await provider.quote_pair(
                asset.mint,
                USDC_MINT,
                delivered,
                priority=10,
                _rpc=provider.bank_feed,
                **({"minimum": True} if self.indicative_feed is not None else {}),
            )
            session, rules = self.coverage_rules(asset)
            if (
                self.forward_coverage.get(asset.symbol) is not state
                or self.assets.get(asset.symbol) != asset
                or self.chain.get(asset.symbol) != "verified_unit_mapping"
                or state.session != session
                or raw(self.minimum_quantity(asset, rules), asset.decimals) != quantity
                or self.settings.policy.withdrawal_semantics == "unverified"
                or self.coverage_topology_identity(state) != identity
                or not prewarm
                and (state.plan is None or state.plan["input_raw"] != delivered)
                or not 0 <= self.clock() - route["_observed_at"] < self.settings.policy.quote_max_age_s
            ):
                return
            state.discovery_failures = 0
            state.discovery_retry_at = 0
            if prewarm:
                if (
                    route["inputMint"] != asset.mint
                    or route["outputMint"] != USDC_MINT
                    or int(route["inAmount"]) != delivered
                ):
                    raise ValueError("Minimum topology pair/quantity mismatch")
                state.prefetched_route = self.minimum_route_topology(route)
                state.prefetched_identity = identity
                provider.bank_feed.changed.set()
                event("coverage_route_prefetched", asset=asset.symbol, quantity_raw=quantity)
                return
            # A local reprice may have replaced plan while discovery waited.
            # Adopt only the topology into the current owner, then recompute
            # locally with its current exact Bid and current bank inputs.
            state.route_candidate = route
            provider.bank_feed.changed.set()
            self.updated.set()
            event("coverage_route_discovered", asset=asset.symbol, quantity_raw=quantity)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            state.discovery_failures += 1
            state.discovery_retry_at = self.clock() + min(60, 5 * 2 ** min(state.discovery_failures, 4))
            event("coverage_route_discovery_unavailable", asset=asset.symbol, code=error_code(error))

    def adopt_coverage_routes(self) -> None:
        """Apply topology only between baseline tasks, so old math cannot overwrite it."""
        for symbol, state in self.forward_coverage.items():
            route = state.route_candidate
            if (
                route is None
                or (symbol, FORWARD) in self.pending
                or (symbol, FORWARD) in getattr(self, "minimum_pending", {})
            ):
                continue
            state.route_candidate = None
            try:
                session, rules = self.coverage_rules(state.asset)
                if (
                    self.assets.get(symbol) != state.asset
                    or self.chain.get(symbol) != "verified_unit_mapping"
                    or state.session != session
                    or raw(self.minimum_quantity(state.asset, rules), state.asset.decimals)
                    != state.quantity_raw
                    or state.plan is None
                ):
                    continue
                state.plan["dex_plan"] = route
                self.local_inputs.pop((symbol, FORWARD), None)
                self.local_after.pop((symbol, FORWARD), None)
            except (ValueError, KeyError, TypeError, ArithmeticError):
                continue

    async def coverage_routes_loop(self) -> None:
        """One slow route search cannot hold a healthy baseline's scheduler key."""
        from .direct import DirectRaydium

        await self.discovery_ready.wait()
        try:
            while True:
                for symbol, task in list(self.coverage_route_pending.items()):
                    if task.done():
                        await task
                        self.coverage_route_pending.pop(symbol)
                if (
                    not self.reporting
                    and not self.coverage_route_pending
                    and isinstance(self.quote_provider, DirectRaydium)
                ):
                    now = self.clock()
                    due = []
                    if self.indicative_feed is not None and self.settings.mode == "paper":
                        for asset in self.assets.values():
                            if self.chain.get(asset.symbol) != "verified_unit_mapping":
                                continue
                            try:
                                self.coverage_state(asset)
                            except (ValueError, KeyError, TypeError, ArithmeticError):
                                continue
                    for state in self.forward_coverage.values():
                        if (
                            now - state.discovery_at < self.settings.monitor.exploration_s
                            or now < state.discovery_retry_at
                            or now < getattr(self.quote_provider, "discovery_provider_retry_at", 0)
                        ):
                            continue
                        try:
                            self.coverage_topology_identity(state)
                            if state.plan is None:
                                latest = (
                                    self.indicative_feed.latest.get(state.asset.ticker)
                                    if self.indicative_feed
                                    else None
                                )
                                if (
                                    self.settings.mode != "paper"
                                    or latest is None
                                    or self.valid_prefetched_route(state) is not None
                                    or not 0
                                    <= now - latest.published_at
                                    < self.settings.monitor.max_quote_age_s
                                    or not latest.published_at <= latest.received_at <= now
                                    or latest.reason
                                    in {"indicative_invalid_payload", "indicative_invalid_reason"}
                                    or latest.reason is None
                                    and latest.ask is not None
                                    or (state.asset.symbol, FORWARD) in self.pending
                                    or (state.asset.symbol, FORWARD) in getattr(self, "minimum_pending", {})
                                ):
                                    continue
                            else:
                                self.coverage_observation(state.asset)
                        except (ValueError, KeyError, TypeError, ArithmeticError):
                            continue
                        due.append(state)
                    if due:
                        state = min(due, key=lambda item: (item.discovery_at, item.asset.symbol))
                        state.discovery_at = now
                        self.coverage_route_pending[state.asset.symbol] = asyncio.create_task(
                            self.discover_coverage_route(state)
                        )
                if self.coverage_route_pending:
                    await asyncio.wait(list(self.coverage_route_pending.values()), timeout=0.25)
                else:
                    await asyncio.sleep(0.25)
        finally:
            for task in self.coverage_route_pending.values():
                task.cancel()
            await asyncio.gather(*self.coverage_route_pending.values(), return_exceptions=True)
            self.coverage_route_pending.clear()

    def size_floor(self, asset: Asset, obs: Observation) -> Decimal:
        if obs.session == SPOT_SESSION:
            return ui(self.books.fill(asset.symbol, raw(self.minimum_buy(asset, obs)), "Bid").value_raw)
        return ui(
            self.stock_values.value(
                raw(self.minimum_buy(asset, obs), asset.decimals),
                asset.decimals,
                obs.ask,
                self.settings.policy.backpack_fee_bps,
                buy=True,
            )
        )

    def size_ceiling(self, asset: Asset, obs: Observation) -> Decimal:
        if obs.session == SPOT_SESSION:
            market = self.books.markets[asset.symbol]
            total = self.books.affordable_quantity_ceiling(asset.symbol)
            quantity = floor_step(min(total, market.maximum) if market.maximum else total, market.step)
            return ui(self.books.fill(asset.symbol, raw(quantity), "Bid").value_raw)
        constraints = self.quantity_rules(asset, obs)
        quantity = floor_step(dec(constraints["maxQuantity"]), dec(constraints["stepSize"]))
        return ui(
            self.stock_values.value(
                raw(quantity, asset.decimals),
                asset.decimals,
                obs.ask,
                self.settings.policy.backpack_fee_bps,
                buy=True,
            )
        )

    def size_bounds(self, asset: Asset, obs: Observation, direction: str) -> tuple[Decimal, Decimal]:
        if direction == FORWARD:
            return self.size_floor(asset, obs), self.size_ceiling(asset, obs)
        rules = self.quantity_rules(asset, obs)
        step = dec(rules["stepSize"])
        minimum = max(dec(rules["minQuantity"]), asset.deposit_min)
        minimum = (minimum / step).to_integral_value(rounding=ROUND_CEILING) * step
        maximum = dec(rules["maxQuantity"])
        if obs.session == SPOT_SESSION:
            maximum = min(maximum, self.books.quantity_ceiling(asset.symbol, "Ask"))
        maximum = floor_step(maximum, step)
        if maximum < minimum:
            raise ValueError("Reverse sale below minimum deposit or lot")
        # Planning bounds only. The subsequent DEX quote and exact sale validate
        # the acquired quantity; the BP USDC buy balance does not bound this side.
        return minimum * dec(obs.bid), maximum * dec(obs.bid)

    def budget(
        self, asset: Asset, obs: Observation, notional: Decimal | None = None, *, direction: str = FORWARD
    ) -> Decimal:
        initial = dec(self.settings.monitor.notional_usdc)
        if self.settings.monitor.auto_size:
            minimum, maximum = self.size_bounds(asset, obs, direction)
            chosen = min(
                maximum,
                max(minimum, initial if notional is None else notional),
            )
        else:
            cap = min(initial, dec(self.settings.policy.max_cost_usdc))
            chosen = cap if notional is None else min(cap, notional)
        return floor_step(chosen, Decimal("0.000001"))

    def next_budget(self, key: tuple[str, str]) -> Decimal:
        asset = self.assets[key[0]]
        obs = self.planning_observation(asset, key[1])
        minimum, maximum = self.size_bounds(asset, obs, key[1])
        return self.volume_search.next(
            key,
            self.budget(asset, obs, direction=key[1]),
            self.clock(),
            auto_size=self.settings.monitor.auto_size,
            minimum=minimum,
            maximum=maximum,
        )

    def budget_info(self, symbol: str) -> dict:
        asset = self.assets.get(symbol)
        if asset is None:
            return {}
        try:
            obs = self.planning_observation(asset)
            minimum = self.size_floor(asset, obs)
            cap = min(dec(self.settings.monitor.notional_usdc), dec(self.settings.policy.max_cost_usdc))
            return {
                "minimum_buy_quantity": str(self.minimum_buy(asset, obs)),
                "minimum_cost_usdc": str(minimum),
                "budget_usdc": str(cap),
                "within_budget": self.settings.monitor.auto_size or minimum <= cap,
                "auto_size": self.settings.monitor.auto_size,
            }
        except (ValueError, KeyError, TypeError, ArithmeticError, StopIteration):
            return {}

    def sale_quantity(self, asset: Asset, observation: Observation, output_raw: int) -> int:
        constraints = self.quantity_rules(asset, observation)
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
            obs = self.planning_observation(asset, direction or FORWARD)
            sell = dec(str(price.get("sellSpotPrice", usd))) / usdc
            buy = dec(str(price.get("buySpotPrice", usd))) / usdc
            forward = (sell / dec(obs.ask) - 1) * 10000
            if direction == FORWARD or not obs.bid and direction is None:
                return forward
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
            size_probe = (
                self.settings.monitor.auto_size or (asset.symbol, FORWARD) in self.optimization_requested
            ) and self.size_refusal(asset)
            spot = self.books.available(asset.symbol)
            for direction in (FORWARD, REVERSE):
                key = (asset.symbol, direction)
                if key in self.pending or (direction == REVERSE and not asset.deposit_enabled):
                    continue
                if self.forward_coverage_enabled and direction == FORWARD and self.search_pending:
                    continue
                try:
                    observation = self.planning_observation(asset, direction)
                    session = self.quantity_session(observation)
                except (ValueError, KeyError, TypeError):
                    continue
                if (
                    config.rfq_validation
                    and not (self.indicative_feed is not None and direction == FORWARD)
                    and not spot
                    and self.rfq.blocked(asset.symbol, session, "Bid" if direction == FORWARD else "Ask")
                    and not (direction == FORWARD and size_probe)
                ):
                    continue
                if (
                    config.rfq_validation
                    and not (self.indicative_feed is not None and direction == FORWARD)
                    and not spot
                    and not self.rfq_ready(asset, session, direction)
                    and not (direction == FORWARD and size_probe)
                ):
                    continue
                last = self.attempted.get(key, 0)
                if direction == FORWARD:
                    try:
                        self.sizing(asset, observation)
                    except (ValueError, KeyError, TypeError, ArithmeticError) as error:
                        self.failures[key] = error_code(error)
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                else:
                    try:
                        session = self.quantity_session(observation)
                    except (ValueError, KeyError, TypeError):
                        self.failures[key] = "rfq_session_unavailable"
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                    if session != SPOT_SESSION and not any(s["name"] == session for s in asset.sessions):
                        self.failures[key] = "rfq_session_unavailable"
                        self.failure_causes[key] = [self.failures[key]]
                        continue
                if now < self.retry_after.get(key, 0) or (
                    key not in self.requested and now - last < config.quote_refresh_s
                ):
                    continue
                # Point prices rank work only before its refresh deadline.
                estimate = self.estimate(asset, direction)
                hot = estimate if estimate is not None else Decimal("-Infinity")
                previous = self.results.get(key)
                if (
                    estimate is not None
                    and previous
                    and now - previous["observed_at"] <= config.exploration_s
                    and previous.get("screen_spread_at_quote") is not None
                ):
                    # Ranking only: learn depth/fees at the last checked size.
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
        self.focus_until = {s: end for s, end in self.focus_until.items() if end > now and s in self.assets}

        def deadline(row):
            period = config.quote_refresh_s if row[2] in self.focus_until else config.exploration_s
            return row[0] + period

        overdue = [row for row in available if deadline(row) <= now]
        focused = [row for row in available if row[2] in self.focus_until]
        unseen = [row for row in available if row[0] == 0]
        fresh_positive = any(
            row.get("profit_raw", 0) > 0 and 0 <= now - row["dex_observed_at"] <= config.max_quote_age_s
            for row in self.results.values()
        )
        hot_available = [row for row in available if row[1] < 0]
        if focused and self._sequence % 2:
            # Wallet focus must also run during the initial universe sweep and
            # while cold keys are overdue. Half the slots remain available to
            # the ordinary deadline queue, so wallet activity cannot monopolize it.
            selected = min(focused)
        elif unseen and not fresh_positive:
            # Alternate sides during discovery; neither direction waits for an
            # entire universe pass, and stale point-price leaders cannot starve it.
            side = FORWARD if self._sequence % 2 else REVERSE
            selected = min(
                [row for row in unseen if row[3] == side] or unseen, key=lambda row: (row[1], row[2])
            )
        elif overdue:
            # A missed refresh deadline takes priority on every slot. Reserving
            # only every fourth slot multiplied a universe pass by four.
            selected = min(overdue, key=lambda row: (deadline(row), row))
        elif self._sequence % 4 == 0 or not hot_available:
            selected = min(available)
        else:
            selected = min(hot_available, key=lambda row: (row[1], row[0], row[2], row[3]))
        return selected[2], selected[3]

    async def validated_buy(
        self, asset: Asset, obs: Observation, *, notional: Decimal | None = None
    ) -> tuple[int, int, Observation, RfqCheck]:
        session = self.quantity_session(obs)
        try:
            acquired, delivered = self.sizing(asset, obs, notional=notional)
        except ValueError as error:
            if error_code(error) not in {"budget_below_minimum_lot", "below_withdrawal_minimum"}:
                raise
            # A model price cannot rule out a legal minimum lot. Probe it without
            # acceptance, then enforce the original budget against the real ask.
            acquired = raw(self.minimum_buy(asset, obs), asset.decimals)
            delivered = acquired - raw(asset.fee, asset.decimals)
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
            resized, delivered_resized = self.sizing(asset, obs, notional=notional)
            if resized >= acquired:
                return acquired, delivered, obs, check
            acquired, delivered = resized, delivered_resized
        raise RfqUnavailable("rfq_budget_changed")

    def rfq_cooldown(self, asset: Asset, direction: str) -> bool:
        if self.indicative_feed is not None and direction == FORWARD:
            # Account inspection failures gate confirmation, not public screening.
            return False
        if self.books.available(asset.symbol):
            return False
        if not self.settings.monitor.rfq_validation:
            return False
        try:
            session = self.quantity_session(self.observation(asset))
        except (ValueError, KeyError, TypeError):
            return True
        block = self.rfq.blocked(asset.symbol, session, "Bid" if direction == FORWARD else "Ask")
        if (
            direction == FORWARD
            and self.settings.monitor.auto_size
            and block
            and block.reason in SIZE_REFUSALS
        ):
            block = None
        return bool(block)

    def rfq_ready(self, asset: Asset, session: str, direction: str = FORWARD) -> bool:
        check = self.rfq.latest.get((asset.symbol, "Bid" if direction == FORWARD else "Ask"))
        return bool(check and check.session == session and check.eligible(self.clock()))

    def size_refusal(self, asset: Asset) -> bool:
        try:
            check = self.rfq.blocked(asset.symbol, self.quantity_session(self.observation(asset)))
        except (ValueError, KeyError, TypeError):
            return False
        return bool(check and check.reason in SIZE_REFUSALS)

    def owned_rfq_plan(self, asset: Asset, side: str) -> dict | None:
        from .direct import DirectRaydium

        direction = FORWARD if side == "Bid" else REVERSE
        coverage = self.forward_coverage.get(asset.symbol) if self.forward_coverage_enabled else None
        plan = (
            coverage.plan if coverage and side == "Bid" else self.local_plans.get((asset.symbol, direction))
        )
        if (
            isinstance(self.quote_provider, DirectRaydium)
            and plan
            and plan["mint"] == asset.mint
            and plan["backpack_source"] == "rfq"
            and self.chain.get(asset.symbol) == "verified_unit_mapping"
            and (side == "Bid" or asset.deposit_enabled)
        ):
            try:
                session = (
                    self.coverage_rules(asset)[0]
                    if plan.get("minimum_coverage")
                    else self.quantity_session(self.observation(asset))
                )
                if plan["backpack_session"] == session:
                    return plan
            except (ValueError, KeyError, TypeError):
                pass
        return None

    def rfq_deadline(self, asset: Asset, side: str) -> float:
        """Price renewals share the bounded queue with both availability sides."""
        coverage = self.forward_coverage.get(asset.symbol) if self.forward_coverage_enabled else None
        if coverage and coverage.asset == asset and side == "Bid":
            check = coverage.check
            try:
                if coverage.session != self.coverage_rules(asset)[0]:
                    return 0
            except (ValueError, KeyError, TypeError, ArithmeticError):
                pass
            deadline = self.coverage_bid_deadline(coverage)
            return max(deadline, self.rfq_after.get((asset.symbol, side), 0))
        plan = self.owned_rfq_plan(asset, side)
        check = self.rfq.latest.get((asset.symbol, side))
        if plan:
            quantity = plan["acquired_raw"] if side == "Bid" else plan["sellable_raw"]
            normalized = format(ui(quantity, asset.decimals).normalize(), "f")
            exact = self.rfq.cache.get(
                (
                    asset.symbol,
                    side,
                    normalized,
                    plan["backpack_session"],
                )
            )
            # A refusal keeps its existing eligibility cooldown. A fresh probe
            # for another quantity cannot postpone this plan's price renewal.
            if (
                check is None
                or check.state == "available"
                or check.reason in SIZE_REFUSALS
                and dec(check.quantity) != dec(normalized)
            ):
                check = exact
        deadline = (
            (check.retry_at if plan and check.state == "available" else check.eligibility_until)
            if check
            else 0
        )
        return max(deadline, self.rfq_after.get((asset.symbol, side), 0))

    @staticmethod
    def coverage_bid_deadline(state: ForwardCoverage) -> float:
        check = state.check
        if check is None:
            return 0
        if check.state != "available":
            return check.eligibility_until
        if state.plan is not None:
            # Start inspection while the old exact price is still usable;
            # its original expiry is unchanged during the network wait.
            return max(check.checked_at + (check.retry_at - check.checked_at) * 2 / 3, state.renewal_retry_at)
        return check.retry_at

    async def refresh_rfq_asset(self, asset: Asset, side: str | None = None) -> None:
        sides = [side] if side else ["Bid", "Ask"] if asset.deposit_enabled else ["Bid"]
        for current in sides:
            if current == "Ask" and (not asset.deposit_enabled or self.books.available(asset.symbol)):
                continue
            await self.refresh_rfq_side(asset, current)
        self.updated.set()

    async def refresh_rfq_side(self, asset: Asset, side: str) -> None:
        session = "UNAVAILABLE"
        owned = False
        check = None
        try:
            if side == "Bid" and self.forward_coverage_enabled and self.sessions:
                session = self.coverage_rules(asset)[0]
                owned = True
                coverage = self.forward_coverage.get(asset.symbol)
                refresh = bool(coverage and coverage.plan is not None)
                _, _, _, check, _ = await self.coverage_buy(asset, refresh=refresh)
            else:
                obs = self.observation(asset)
                session = self.quantity_session(obs)
                plan = self.owned_rfq_plan(asset, side)
                if plan and plan["backpack_session"] == session:
                    owned = True
                    check = await self.confirm_local_price(
                        (asset.symbol, FORWARD if side == "Bid" else REVERSE), plan
                    )
                elif side == "Bid":
                    _, _, _, check = await self.validated_buy(asset, obs)
                else:
                    rules = self.quantity_rules(asset, obs)
                    step = dec(rules["stepSize"])
                    quantity = max(dec(rules["minQuantity"]), asset.deposit_min)
                    quantity = (quantity / step).to_integral_value(rounding=ROUND_CEILING) * step
                    if quantity > dec(rules["maxQuantity"]):
                        raise RfqUnavailable("rfq_invalid_quantity")
                    check = await self.rfq.check(asset.symbol, str(quantity), session, side="Ask")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self.books.available(asset.symbol):
                key = asset.symbol, FORWARD if side == "Bid" else REVERSE
                self.failures[key] = error_code(error)
                self.failure_causes[key] = [error_code(error)]
            previous = self.rfq.latest.get((asset.symbol, side))
            if not owned and (previous is None or self.clock() >= previous.eligibility_until):
                check = self.rfq.result(asset.symbol, side, "0", session, "unknown", error_code(error))
                self.rfq.latest[(asset.symbol, side)] = check
        self.rfq_after[(asset.symbol, side)] = (
            (check.retry_at if owned and check and check.state == "available" else check.eligibility_until)
            if check
            else self.clock() + self.settings.monitor.quote_refresh_s
        )
        if side == "Bid" and self.forward_coverage_enabled:
            coverage = self.forward_coverage.get(asset.symbol)
            if check and coverage and check is coverage.check and check.state == "available":
                self.rfq_after[(asset.symbol, side)] = self.coverage_bid_deadline(coverage)
        if self.assets.get(asset.symbol) != asset:
            self.rfq.invalidate(asset.symbol)

    async def rfq_loop(self) -> None:
        """Refresh due sides fairly; only owned available plans use price deadlines."""
        await self.discovery_ready.wait()
        if self.indicative_feed is not None:
            await self.confirmed_rfq_loop()
            return
        if not self.forward_coverage_enabled:
            await self.refresh_external_prices()
        pending: dict[tuple[str, str], asyncio.Task] = {}
        try:
            while True:
                for key, task in list(pending.items()):
                    if task.done():
                        await task
                        pending.pop(key)
                available = sorted(
                    (self.rfq_deadline(asset, side), asset.symbol, side)
                    for asset in self.assets.values()
                    if self.chain.get(asset.symbol) == "verified_unit_mapping"
                    for side in ("Bid", "Ask")
                    if side == "Bid" or asset.deposit_enabled and not self.books.available(asset.symbol)
                )
                first_bid_pass = self.forward_coverage_enabled and any(
                    self.chain.get(asset.symbol) == "verified_unit_mapping"
                    and not (
                        self.forward_coverage.get(asset.symbol)
                        and self.forward_coverage[asset.symbol].check
                        or asset.symbol in self.forward_coverage_blocks
                    )
                    for asset in self.assets.values()
                )
                if first_bid_pass:
                    available.sort(key=lambda row: (row[2] != "Bid", row[0], row[1]))
                elif self.forward_coverage_enabled:
                    available.sort(
                        key=lambda row: (not self.owned_bid_renewal(row[1], row[2]), row[0], row[1], row[2])
                    )
                background_due = any(
                    self.clock() >= deadline
                    and not self.owned_bid_renewal(symbol, side)
                    and not any(key[0] == symbol for key in pending)
                    for deadline, symbol, side in available
                )
                for deadline, symbol, side in available:
                    if len(pending) >= self.settings.monitor.rfq_concurrency:
                        break
                    if self.clock() < deadline or any(key[0] == symbol for key in pending):
                        continue
                    if (
                        self.forward_coverage_enabled
                        and not first_bid_pass
                        and background_due
                        and self.owned_bid_renewal(symbol, side)
                        and sum(self.owned_bid_renewal(*key) for key in pending)
                        >= max(1, self.settings.monitor.rfq_concurrency - 1)
                    ):
                        continue
                    pending[(symbol, side)] = asyncio.create_task(
                        self.refresh_rfq_asset(self.assets[symbol], side)
                    )
                await asyncio.sleep(0.5)
        finally:
            for task in pending.values():
                task.cancel()
            await asyncio.gather(*pending.values(), return_exceptions=True)

    async def confirmed_rfq_loop(self) -> None:
        """Renew only confirmed positive plans; baseline prices arrive over WS."""
        pending: dict[tuple[str, str], asyncio.Task] = {}

        async def renew(key: tuple[str, str], plan: dict) -> None:
            check = await self.confirm_local_price(key, plan, force_refresh=True)
            if check:
                side = "Bid" if key[1] == FORWARD else "Ask"
                self.rfq_after[(key[0], side)] = (
                    check.checked_at + (check.retry_at - check.checked_at) * 2 / 3
                    if check.state == "available"
                    else check.eligibility_until
                )
            self.updated.set()

        try:
            while True:
                for key, task in list(pending.items()):
                    if task.done():
                        try:
                            await task
                        except (ValueError, KeyError, TypeError):
                            self.rfq_after[(key[0], "Bid" if key[1] == FORWARD else "Ask")] = (
                                self.clock() + self.settings.monitor.rfq_retry_s
                            )
                        pending.pop(key)
                for key, plan in sorted(self.local_plans.items()):
                    if len(pending) >= min(2, self.settings.monitor.rfq_concurrency):
                        break
                    if (
                        key in pending
                        or plan.get("backpack_source") != "rfq"
                        or plan.get("profit_raw", 0) <= 0
                    ):
                        continue
                    side = "Bid" if key[1] == FORWARD else "Ask"
                    deadline = (
                        plan["backpack_timestamp"]
                        + (plan["backpack_rfq_valid_until"] - plan["backpack_timestamp"]) * 2 / 3
                    )
                    if self.clock() >= max(deadline, self.rfq_after.get((key[0], side), 0)):
                        pending[key] = asyncio.create_task(renew(key, dict(plan)))
                if len(pending) < min(2, self.settings.monitor.rfq_concurrency):
                    for asset in sorted(
                        self.assets.values(), key=lambda row: self.rfq_after.get((row.symbol, "Ask"), 0)
                    ):
                        key = asset.symbol, REVERSE
                        if key in pending or not asset.deposit_enabled or self.books.available(asset.symbol):
                            continue
                        if self.chain.get(asset.symbol) != "verified_unit_mapping":
                            continue
                        previous = self.rfq.latest.get((asset.symbol, "Ask"))
                        deadline = max(
                            self.rfq_after.get((asset.symbol, "Ask"), 0),
                            previous.eligibility_until if previous else 0,
                        )
                        if self.clock() >= deadline:
                            pending[key] = asyncio.create_task(self.refresh_rfq_asset(asset, "Ask"))
                            break
                await asyncio.sleep(0.25)
        finally:
            for task in pending.values():
                task.cancel()
            await asyncio.gather(*pending.values(), return_exceptions=True)

    def owned_bid_renewal(self, symbol: str, side: str) -> bool:
        state = self.forward_coverage.get(symbol)
        return bool(
            side == "Bid"
            and state
            and state.plan is not None
            and state.check
            and state.check.state == "available"
        )

    async def entry_buy(
        self, asset: Asset, budget: Decimal
    ) -> tuple[int, int, Observation, RfqCheck | None, BookFill | None]:
        """Compare independently validated entry sources at this budget."""
        candidates: list[tuple[int, int, Observation, RfqCheck | None, BookFill | None, int]] = []
        failures: list[Exception] = []
        if self.books.available(asset.symbol):
            try:
                obs = self.books.observation(asset.symbol)
                acquired, delivered = self.sizing(asset, obs, notional=budget)
                for attempt in range(3):
                    fill = self.books.fill(asset.symbol, acquired, "Bid")
                    try:
                        await self.books.check_capacity(asset.symbol, fill)
                        break
                    except BookUnavailable as error:
                        if (
                            error.code != "spot_insufficient_balance"
                            or attempt == 2
                            or not self.settings.monitor.auto_size
                        ):
                            raise
                        maximum = self.books.capacities[(asset.symbol, "Bid")].quantity
                        quantity = floor_step(maximum, self.books.markets[asset.symbol].step)
                        if quantity < self.minimum_buy(asset, obs) or raw(quantity) >= acquired:
                            raise
                        acquired = raw(quantity)
                        delivered = acquired - raw(asset.fee)
                candidates.append((acquired, delivered, obs, None, fill, fill.value_raw))
            except Exception as error:
                failures.append(error)
        try:
            obs = self.observation(asset) if self.indicative_feed is not None else self.rfq_observation(asset)
            session = self.quantity_session(obs)
            blocked = self.rfq.blocked(asset.symbol, session)
            if blocked and candidates:
                raise RfqUnavailable(blocked.reason)
            if self.indicative_feed is not None:
                acquired, delivered = self.sizing(asset, obs, notional=budget)
                check = self.indicative_check(asset, acquired, session)
                obs = self.check_observation(check)
            elif self.settings.monitor.rfq_validation:
                acquired, delivered, obs, check = await self.validated_buy(asset, obs, notional=budget)
            else:
                acquired, delivered = self.sizing(asset, obs, notional=budget)
                check = None
            cost = self.stock_values.value(
                acquired, asset.decimals, obs.ask, self.settings.policy.backpack_fee_bps, buy=True
            )
            if check is not None or not candidates:
                candidates.append((acquired, delivered, obs, check, None, cost))
        except Exception as error:
            failures.append(error)
        if not candidates:
            raise failures[0]
        best = min(candidates, key=lambda c: Decimal(c[5]) / c[1])
        return best[:5]

    async def entry_sell(
        self, asset: Asset, output: int
    ) -> tuple[int, Observation, RfqCheck | None, BookFill | None, int]:
        candidates: list[tuple[int, Observation, RfqCheck | None, BookFill | None, int]] = []
        failures: list[Exception] = []
        if self.books.available(asset.symbol):
            try:
                obs = self.books.observation(asset.symbol)
                sellable = self.sale_quantity(asset, obs, output)
                fill = self.books.fill(asset.symbol, sellable, "Ask")
                # This route buys stock on-chain before depositing it. Current
                # BP inventory cannot validate (or disqualify) that future sale.
                candidates.append((sellable, obs, None, fill, fill.value_raw))
            except Exception as error:
                failures.append(error)
        try:
            obs = self.rfq_observation(asset, REVERSE)
            sellable = self.sale_quantity(asset, obs, output)
            check = None
            if self.settings.monitor.rfq_validation:
                check = await self.rfq.check(
                    asset.symbol, str(ui(sellable)), self.quantity_session(obs), side="Ask"
                )
                if not check.usable(self.clock()):
                    raise RfqUnavailable(check.reason)
                obs = obs.model_copy(update={"bid": check.price, "timestamp": check.checked_at})
            proceeds = self.stock_values.value(
                sellable, asset.decimals, obs.bid, self.settings.policy.backpack_fee_bps, buy=False
            )
            if check is not None or not candidates:
                candidates.append((sellable, obs, check, None, proceeds))
        except Exception as error:
            failures.append(error)
        if not candidates:
            raise failures[0]
        return max(candidates, key=lambda c: c[4])

    def local_buy(self, asset: Asset, plan: dict):
        from .direct import LocalQuoteNotReady

        acquired = plan["acquired_raw"]
        if self.chain.get(asset.symbol) != "verified_unit_mapping" or plan["mint"] != asset.mint:
            raise LocalQuoteNotReady()
        if plan["backpack_source"] == "spot":
            obs = self.books.observation(asset.symbol)
            fill = self.books.fill(asset.symbol, acquired, "Bid")
            capacity = self.books.capacities.get((asset.symbol, "Bid"))
            if not capacity or not self.books.capacity_valid(asset.symbol, fill, capacity.checked_at):
                raise LocalQuoteNotReady()
            check = None
        elif plan["backpack_source"] == "indicative":
            check = self.indicative_check(asset, acquired, self.coverage_rules(asset)[0])
            if plan.get("minimum_coverage"):
                self.forward_coverage[asset.symbol].check = check
            obs = self.check_observation(check)
            fill = None
        else:
            obs = (
                self.coverage_observation(asset)
                if plan.get("minimum_coverage")
                else self.rfq_observation(asset)
            )
            check = self.exact_rfq(asset, str(ui(acquired, asset.decimals)), self.quantity_session(obs))
            if check is None:
                raise RfqUnavailable("rfq_check_stale")
            obs = obs.model_copy(
                update={
                    "ask": check.price,
                    "timestamp": check.checked_at,
                    "session": check.session,
                    "source": "Backpack AwaitAccept RFQ; cancelled after inspection",
                }
            )
            fill = None
        rules = self.quantity_rules(asset, obs)
        quantity = ui(acquired, asset.decimals)
        if not dec(rules["minQuantity"]) <= quantity <= dec(rules["maxQuantity"]) or quantity % dec(
            rules["stepSize"]
        ):
            raise LocalQuoteNotReady()
        fee = raw(asset.fee, asset.decimals)
        request = acquired if self.settings.policy.withdrawal_semantics == "deduct" else acquired - fee
        if (
            self.settings.policy.withdrawal_semantics == "unverified"
            or acquired <= fee
            or ui(request, asset.decimals) < asset.withdrawal_min
        ):
            raise LocalQuoteNotReady()
        return acquired, acquired - fee, obs, check, fill

    def local_sell(self, asset: Asset, output: int, plan: dict):

        if plan["backpack_source"] == "spot":
            obs = self.books.observation(asset.symbol)
            sellable = self.sale_quantity(asset, obs, output)
            fill = self.books.fill(asset.symbol, sellable, "Ask")
            return sellable, obs, None, fill, fill.value_raw
        obs = self.rfq_observation(asset, REVERSE)
        sellable = self.sale_quantity(asset, obs, output)
        check = self.rfq.cached_exact(
            asset.symbol, str(ui(sellable, asset.decimals)), self.quantity_session(obs), "Ask"
        )
        if check is None:
            raise RfqUnavailable("rfq_check_stale")
        obs = obs.model_copy(
            update={
                "bid": check.price,
                "timestamp": check.checked_at,
                "session": check.session,
                "source": "Backpack AwaitAccept RFQ; cancelled after inspection",
            }
        )
        proceeds = self.stock_values.value(
            sellable, asset.decimals, obs.bid, self.settings.policy.backpack_fee_bps, buy=False
        )
        return sellable, obs, check, None, proceeds

    async def confirm_local_price(
        self, key: tuple[str, str], plan: dict, *, force_refresh: bool = False
    ) -> RfqCheck | None:
        """Renew a plan's exact side and size through the fair RFQ queue."""
        from .direct import DirectRaydium

        provider = self.quote_provider
        if not isinstance(provider, DirectRaydium) or not self.settings.monitor.rfq_validation:
            return None
        asset = self.assets[key[0]]
        if self.chain.get(asset.symbol) != "verified_unit_mapping" or asset.mint != plan["mint"]:
            return None
        forward = key[1] == FORWARD
        minimum = forward and plan.get("minimum_coverage")
        obs = None if minimum else self.rfq_observation(asset, key[1])
        if minimum:
            session = self.coverage_rules(asset)[0]
        else:
            assert obs is not None
            session = self.quantity_session(obs)
        if session != plan["backpack_session"]:
            raise RfqUnavailable("rfq_session_unavailable")
        if forward:
            quantity = plan["acquired_raw"]
            if minimum:
                legal = (
                    raw(self.minimum_quantity(asset, self.coverage_rules(asset)[1]), asset.decimals)
                    == quantity
                )
            else:
                assert obs is not None
                legal = self.sale_quantity(asset, obs, quantity) == quantity
            if not legal:
                raise RfqUnavailable("rfq_invalid_quantity")
        else:
            assert obs is not None
            # A new DEX bank can change the actual sale size. Derive it locally
            # before spending this side's one bounded RFQ renewal slot.
            quote = await provider.cached_plan_quote(
                USDC_MINT, asset.mint, plan["input_raw"], plan=plan["dex_plan"], priority=10
            )
            output = min(
                int(quote["otherAmountThreshold"]),
                int(quote["outAmount"]) * (10000 - self.settings.policy.slippage_bps) // 10000,
            )
            if ui(output, asset.decimals) < asset.deposit_min:
                raise RfqUnavailable("rfq_invalid_quantity")
            quantity = self.sale_quantity(asset, obs, output)
        event(
            "local_price_check_requested",
            asset=asset.symbol,
            direction=key[1],
            side="Bid" if forward else "Ask",
        )
        check = await self.rfq.check(
            asset.symbol,
            str(ui(quantity, asset.decimals)),
            session,
            side="Bid" if forward else "Ask",
            force_refresh=force_refresh,
        )
        coverage = self.forward_coverage.get(asset.symbol)
        if (
            self.indicative_feed is None
            and forward
            and coverage
            and coverage.asset == asset
            and coverage.quantity_raw == quantity
        ):
            coverage.check = check
        return check

    def local_input_version(self, key: tuple[str, str], plan: dict) -> tuple | None:
        """Schedule a ready input combination once, including failed local probes."""
        from .direct import DirectRaydium

        provider = self.quote_provider
        asset = self.assets.get(key[0])
        if (
            not isinstance(provider, DirectRaydium)
            or provider.bank_feed is None
            or asset is None
            or plan["mint"] != asset.mint
            or self.chain.get(asset.symbol) != "verified_unit_mapping"
            or (key[1] == REVERSE and not asset.deposit_enabled)
        ):
            return None
        try:
            versions = provider.bank_versions(plan["dex_plan"])
            if not versions:
                return None
            current = []
            for pool, source, target, _, _ in versions:
                bank = provider.bank_feed.peek((pool, source, target, 10))
                if bank is None:
                    return None
                current.append((pool, source, target, bank["response"]["context"]["slot"], bank["observed"]))
            source_version: tuple
            if plan["backpack_source"] == "spot":
                if not self.books.available(asset.symbol):
                    return None
                depth = self.books.depths[asset.symbol]
                capacity = self.books.capacities.get((asset.symbol, "Bid"))
                source_version = (depth.timestamp, depth.update_id, self.books.fee_at, capacity)
            elif plan["backpack_source"] == "rfq":
                session = (
                    self.coverage_rules(asset)[0]
                    if plan.get("minimum_coverage")
                    else self.quantity_session(self.observation(asset))
                )
                if session != plan["backpack_session"]:
                    return None
                side = "Bid" if key[1] == FORWARD else "Ask"
                quantity = format(ui(plan["acquired_raw"], asset.decimals).normalize(), "f")
                exact = [
                    (cache_key, check.checked_at, check.price)
                    for cache_key, check in self.rfq.cache.items()
                    if cache_key[0] == asset.symbol
                    and cache_key[1] == side
                    and cache_key[3] == session
                    and (side == "Ask" or cache_key[2] == quantity)
                    and check.usable(self.clock())
                ]
                owned = self.exact_rfq(asset, quantity, session, side) if side == "Bid" else None
                if owned:
                    exact.append(((asset.symbol, side, quantity, session), owned.checked_at, owned.price))
                    exact = list(set(exact))
                if not exact:
                    return None
                # Ask quantity is determined by the newly repriced DEX output;
                # local_sell still requires that exact quantity after repricing.
                source_version = tuple(sorted(exact))
            elif plan["backpack_source"] == "indicative" and self.indicative_feed is not None:
                check = self.indicative_check(asset, plan["acquired_raw"], self.coverage_rules(asset)[0])
                source_version = (check.checked_at, check.price)
            else:
                return None
            return tuple(current), source_version
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return None

    def book_fields(self, asset: Asset, fill: BookFill) -> dict:
        capacity = self.books.capacities[(asset.symbol, "Bid")] if fill.side == "Bid" else None
        return {
            "backpack_source": "spot",
            "backpack_market": fill.market,
            "backpack_fee_bps": fill.fee_bps,
            "backpack_timestamp": fill.timestamp,
            "backpack_price": fill.price,
            "backpack_session": SPOT_SESSION,
            "source": "Backpack spot depth " + fill.market,
            "backpack_quote_confirmed": True,
            "backpack_model": False,
            "backpack_valid_until": min(
                fill.timestamp + self.books.ttl,
                capacity.checked_at + self.books.ttl if capacity else float("inf"),
                self.books.fee_at + self.books.fee_ttl,
            ),
            "book_capacity_checked_at": capacity.checked_at if capacity else None,
            "backpack_deposit_required": fill.side == "Ask",
            "book_update_id": fill.update_id,
            "book_levels": fill.levels,
            "book_gross_raw": fill.gross_raw,
            "book_fee_raw": fill.fee_raw,
            "book_worst_price": fill.worst_price,
            "book_impact_pct": fill.impact_pct,
            "settlement_required": False,
            "trading_supported": False,
        }

    def current_book_fill(self, asset: Asset, row: dict) -> BookFill:
        buy = row["direction"] == FORWARD
        fill = self.books.fill(
            asset.symbol, row["acquired_raw"] if buy else row["sellable_raw"], "Bid" if buy else "Ask"
        )
        if not buy and (
            not asset.deposit_enabled or ui(row["minimum_output_raw"], asset.decimals) < asset.deposit_min
        ):
            raise ValueError("Reverse deposit unavailable")
        if self.books.markets[asset.symbol].symbol != row["backpack_market"] or (
            buy and not self.books.capacity_valid(asset.symbol, fill, row["book_capacity_checked_at"])
        ):
            raise BookUnavailable("spot_capacity_stale")
        return fill

    def revalue_book(self, asset: Asset, original: dict) -> dict:
        row = dict(original)
        fill = self.current_book_fill(asset, row)
        row.update(self.book_fields(asset, fill))
        row["cost_raw" if row["direction"] == FORWARD else "proceeds_raw"] = fill.value_raw
        row["profit_raw"] = row["proceeds_raw"] - row["cost_raw"] - row["gas_raw"]
        row["edge_bps"] = str(Decimal(row["profit_raw"]) * 10000 / row["cost_raw"])
        row["price_impact_pct"] = str(max(dec(row["dex_price_impact_pct"]), dec(fill.impact_pct)))
        row["passes_model_limits"] = not self.model_blocks(row)
        row["size_feasible"] = (
            row["cost_raw"] <= raw(row["search_budget_usdc"])
            and dec(row["price_impact_pct"]) <= dec(self.settings.policy.max_price_impact_pct)
            and row["slippage_bps"] <= self.settings.policy.slippage_bps
        )
        return row

    async def books_loop(self) -> None:
        await self.discovery_ready.wait()
        semaphore = asyncio.Semaphore(4)

        async def refresh(symbol: str) -> None:
            async with semaphore:
                if symbol in self.books.markets:
                    await self.books.refresh(symbol)

        while True:
            started = self.clock()
            if self.books.markets:
                if self.books.fee_bps is None or self.clock() - self.books.fee_at >= self.books.fee_ttl / 2:
                    try:
                        async with asyncio.timeout(8):
                            await self.books.refresh_fees()
                    except Exception as error:
                        event("stock_fee_unavailable", component="backpack_spot", code=error_code(error))
                await asyncio.gather(*(refresh(symbol) for symbol in list(self.books.markets)))
                self.updated.set()
            await asyncio.sleep(max(0.1, self.settings.monitor.spot_refresh_s - (self.clock() - started)))

    async def quote(
        self,
        key: tuple[str, str],
        *,
        quote_pair: Callable[..., Awaitable[dict]] | None = None,
        notional: Decimal | None = None,
        local_plan: dict | None = None,
        coverage: bool = False,
    ) -> dict | None:
        symbol, direction = key
        started = self.clock()
        coverage_state = None
        if local_plan is None:
            if not coverage:
                self.attempted[key] = started
            self.quote_starts.append(started)
        try:
            asset = self.assets[symbol]
            if coverage:
                if direction != FORWARD or not self.forward_coverage_enabled:
                    raise ValueError("Forward coverage requires validated automatic sizing")
                coverage_state = self.coverage_state(asset)
                if local_plan is None:
                    coverage_state.attempted_at = started
            obs = None if coverage else self.planning_observation(asset, direction)
            p = self.settings.policy
            if coverage:
                budget = Decimal(0)
            else:
                assert obs is not None
                budget = self.budget(asset, obs, notional, direction=direction)
            if not coverage and budget <= 0:
                raise ValueError("Invalid size budget")
            if direction == FORWARD and not coverage:
                assert obs is not None
                # Fractional budget probes must respect the active session's lot
                # and withdrawal floor, while never increasing the hard cap.
                floor = self.size_floor(asset, self.planning_observation(asset))
                cap = (
                    self.size_ceiling(asset, obs)
                    if self.settings.monitor.auto_size
                    else min(dec(self.settings.monitor.notional_usdc), dec(p.max_cost_usdc))
                )
                budget = min(cap, max(budget, floor))
            rfq_check = None
            book_fill = None
            if direction == FORWARD:
                acquired, delivered, obs, rfq_check, book_fill = (
                    self.local_buy(asset, local_plan)
                    if local_plan is not None
                    else await self.coverage_buy(asset)
                    if coverage
                    else await self.entry_buy(asset, budget)
                )
                if coverage:
                    budget = ui(
                        self.stock_values.value(
                            acquired, asset.decimals, obs.ask, p.backpack_fee_bps, buy=True
                        )
                    )
                source, target, input_raw = asset.mint, USDC_MINT, delivered
            else:
                assert obs is not None
                if not asset.deposit_enabled or self.chain.get(symbol) != "verified_unit_mapping":
                    raise ValueError("Reverse deposit/mint mapping unavailable")
                source, target, input_raw = USDC_MINT, asset.mint, raw(budget)
                if local_plan is not None:
                    input_raw = local_plan["input_raw"]
                acquired = delivered = 0
                if local_plan is None and not self.books.available(symbol):
                    previous = self.rfq.latest.get((symbol, "Ask"))
                    if (
                        self.settings.monitor.rfq_validation
                        and previous
                        and previous.session == self.quantity_session(obs)
                        and previous.state != "available"
                    ):
                        check = await self.rfq.check(symbol, previous.quantity, previous.session, side="Ask")
                        if not check.usable(self.clock()):
                            raise RfqUnavailable(check.reason)
            quote = await (quote_pair or self.quote_provider.quote_pair)(
                source, target, input_raw, priority=10
            )
            if self.assets.get(symbol) != asset:
                return None  # Issuer/metadata changed during the request.
            if not self.settings.monitor.rfq_validation and book_fill is None and obs.session != SPOT_SESSION:
                obs = self.observation(asset)
            if self.quantity_session(obs) != obs.session and rfq_check:
                raise RfqUnavailable("rfq_session_unavailable")
            if direction == FORWARD and rfq_check and rfq_check.source == "indicative":
                # A usable prior public observation can already have been
                # superseded during the SDK await. Cost the actual latest price.
                session = self.coverage_rules(asset)[0]
                if session != obs.session:
                    raise RfqUnavailable("rfq_session_unavailable")
                rfq_check = self.indicative_check(asset, acquired, session)
                obs = self.check_observation(rfq_check)
                if coverage_state:
                    coverage_state.check = rfq_check
            elif direction == FORWARD and rfq_check and not rfq_check.usable(self.clock()):
                if rfq_check.source == "rfq":
                    if local_plan is not None:
                        from .direct import LocalQuoteNotReady

                        raise LocalQuoteNotReady()
                    rfq_check = await self.rfq.check(
                        symbol, str(ui(acquired, asset.decimals)), self.quantity_session(obs)
                    )
                    if coverage_state:
                        if (
                            rfq_check.session != coverage_state.session
                            or rfq_check.side != "Bid"
                            or dec(rfq_check.quantity) != ui(acquired, asset.decimals)
                        ):
                            raise RfqUnavailable("rfq_response_mismatch")
                        coverage_state.check = rfq_check
                        if rfq_check.state == "unavailable":
                            coverage_state.completed = True
                    if not rfq_check.usable(self.clock()):
                        raise RfqUnavailable(rfq_check.reason)
                    obs = obs.model_copy(update={"ask": rfq_check.price, "timestamp": rfq_check.checked_at})
            quoted_output = int(quote["outAmount"])
            reserved_output = quoted_output * (10000 - p.slippage_bps) // 10000
            provider_minimum = int(quote.get("otherAmountThreshold", quoted_output))
            if not 0 < provider_minimum <= quoted_output:
                raise ValueError("Invalid provider minimum output")
            output = min(reserved_output, provider_minimum)
            gasless = quote.get("gasless") is True
            gas = 0 if gasless else raw(p.gas_budget_usdc)
            deposit_gas = raw(p.gas_budget_usdc) if direction == REVERSE else 0
            gas += deposit_gas
            if direction == FORWARD:
                if book_fill:
                    # Revalue the same stock quantity after the DEX network wait.
                    current = self.books.fill(symbol, acquired, "Bid")
                    checked = self.books.capacities[(symbol, "Bid")].checked_at
                    if not self.books.capacity_valid(symbol, current, checked):
                        if local_plan is not None:
                            from .direct import LocalQuoteNotReady

                            raise LocalQuoteNotReady()
                        await self.books.check_capacity(symbol, current)
                    book_fill = current
                    cost = book_fill.value_raw
                else:
                    cost = self.stock_values.value(
                        acquired, asset.decimals, obs.ask, p.backpack_fee_bps, buy=True
                    )
                if coverage:
                    budget = ui(cost)
                proceeds, sellable = output, 0
            else:
                if ui(output, asset.decimals) < asset.deposit_min:
                    raise ValueError("Reverse amount below minimum deposit")
                cost = input_raw
                sellable, obs, rfq_check, book_fill, proceeds = (
                    self.local_sell(asset, output, local_plan)
                    if local_plan is not None
                    else await self.entry_sell(asset, output)
                )
            profit = proceeds - cost - gas
            confirmation_reason = None
            if (
                not coverage
                and direction == FORWARD
                and rfq_check
                and rfq_check.source == "indicative"
                and profit > 0
            ):
                confirmed = await self.rfq.check(
                    symbol, str(ui(acquired, asset.decimals)), self.quantity_session(obs), side="Bid"
                )
                if (
                    confirmed.usable(self.clock())
                    and confirmed.source == "rfq"
                    and confirmed.symbol == symbol
                    and confirmed.side == "Bid"
                    and confirmed.session == self.quantity_session(obs)
                    and dec(confirmed.quantity) == ui(acquired, asset.decimals)
                    and dec(confirmed.price or "0") > 0
                ):
                    rfq_check = confirmed
                    obs = self.check_observation(confirmed)
                    cost = self.stock_values.value(
                        acquired, asset.decimals, obs.ask, p.backpack_fee_bps, buy=True
                    )
                    profit = proceeds - cost - gas
                else:
                    confirmation_reason = (
                        "rfq_response_mismatch" if confirmed.state == "available" else confirmed.reason
                    )
            if self.assets.get(symbol) != asset:
                return None
            if coverage_state is not None:
                session, rules = self.coverage_rules(asset)
                if (
                    self.forward_coverage.get(symbol) is not coverage_state
                    or coverage_state.asset != asset
                    or coverage_state.session != session
                    or raw(self.minimum_quantity(asset, rules), asset.decimals) != coverage_state.quantity_raw
                    or acquired != coverage_state.quantity_raw
                ):
                    return None
            edge = Decimal(profit) * 10000 / cost
            dex_impact = abs(impact_percent(quote))
            impact = max(dex_impact, dec(book_fill.impact_pct)) if book_fill else dex_impact
            result = {
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
                "deposit_gas_raw": deposit_gas,
                "backpack_deposit_required": direction == REVERSE,
                "gas_source": "included_in_dex_quote" if gasless else "model",
                "slippage_bps": max(p.slippage_bps, int(quote.get("slippageBps") or 0)),
                "policy_slippage_bps": p.slippage_bps,
                "cost_raw": cost,
                "proceeds_raw": proceeds,
                "profit_raw": profit,
                "edge_bps": str(edge),
                "price_impact_pct": str(impact),
                "dex_price_impact_pct": str(dex_impact),
                "quote_started_at": quote.get("_request_started_at", started),
                "dex_observed_at": quote.get("_observed_at", quote.get("_request_started_at", started)),
                "observed_at": self.clock(),
                "backpack_timestamp": obs.timestamp,
                "backpack_session": self.quantity_session(obs),
                "backpack_model": obs.session == "MODEL",
                "backpack_source": rfq_check.source if rfq_check else "estimate",
                "backpack_valid_until": rfq_check.retry_at if rfq_check else None,
                "backpack_quote_confirmed": rfq_check is not None and rfq_check.source == "rfq",
                "backpack_rfq_checked_at": rfq_check.checked_at
                if rfq_check and rfq_check.source == "rfq"
                else None,
                "backpack_rfq_valid_until": rfq_check.retry_at
                if rfq_check and rfq_check.source == "rfq"
                else None,
                "backpack_confirmation_reason": confirmation_reason,
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
                and (self.settings.monitor.auto_size or cost <= raw(p.max_cost_usdc))
                and profit >= raw(p.min_profit_usdc)
                and edge >= p.min_edge_bps
                and impact <= dec(p.max_price_impact_pct)
                and int(quote.get("slippageBps") or 0) <= p.slippage_bps,
                "trading_supported": asset.mint == STOCK_MINT
                and quote.get("direct_execution_supported", True)
                and direction == FORWARD
                and quote.get("quote_source") != "jupiter_flow"
                and self.quote_backend == self.settings.dex_backend,
                "indicative": True,
                "minimum_coverage": coverage,
                "search_budget_usdc": str(budget),
                "size_feasible": cost <= raw(budget)
                and impact <= dec(p.max_price_impact_pct)
                and int(quote.get("slippageBps") or 0) <= p.slippage_bps,
                "screen_spread_at_quote": str(self.estimate(asset, direction))
                if self.estimate(asset, direction) is not None
                else None,
            }
            if book_fill:
                result.update(self.book_fields(asset, book_fill))
            from .direct import DirectRaydium

            if isinstance(self.quote_provider, DirectRaydium):
                result["dex_bank_versions"] = self.quote_provider.bank_versions(quote)
            if local_plan is not None:
                result["backpack_valid_until"] = min(
                    result.get("backpack_valid_until") or result.get("backpack_rfq_valid_until") or 0,
                    result["dex_observed_at"] + p.quote_max_age_s,
                )
            if not coverage:
                self.results[key] = result
                self.volume_search.record(key, result)
            if self.quote_observer and not coverage and result["backpack_quote_confirmed"]:
                self.quote_observer(result)
            if not coverage:
                self.failures.pop(key, None)
                self.failure_causes.pop(key, None)
            pair = price_pair(result)
            if not coverage:
                previous_price = self.price_history.get(key)
                if previous_price and pair["updated_at"] > previous_price["updated_at"]:
                    self.price_repeat_intervals.append(pair["updated_at"] - previous_price["updated_at"])
                self.price_history[key] = pair
            self.completed_quotes += 1
            plan = {**result, "dex_plan": quote}
            if coverage_state:
                coverage_state.result = dict(result)
                coverage_state.plan = plan
                coverage_state.reason = coverage_state.renewal_reason
                coverage_state.retry_at = 0
                coverage_state.failures = 0
                coverage_state.completed = coverage_state.completed or self.coverage_result_fresh(
                    coverage_state
                )
            else:
                self.local_plans[key] = plan
            if rfq_check is not None:
                self.rfq_after.pop((symbol, "Bid" if direction == FORWARD else "Ask"), None)
            if isinstance(
                self.quote_provider, DirectRaydium
            ) and self.quote_provider.bank_versions_are_current(result["dex_bank_versions"]):
                version = self.local_input_version(key, plan)
                minimum_owner = (
                    direction == FORWARD
                    and self.forward_coverage_enabled
                    and (owner := self.forward_coverage.get(symbol)) is not None
                    and owner.plan is not None
                )
                if version is not None and (coverage or not minimum_owner):
                    if plan["backpack_source"] == "indicative":
                        # Record what was calculated, never a newer WS value
                        # that arrived while this exact quote was in flight.
                        version = (version[0], (plan["backpack_timestamp"], plan["backpack_price"]))
                    self.local_inputs[key] = version
            self.local_calculations += local_plan is not None
            if not coverage:
                self.retry_after.pop(key, None)
                self.quote_failures.pop(key, None)
            self.latencies.append((self.clock() - started) * 1000)
            event(
                "quote_complete",
                asset=symbol,
                direction=direction,
                size_usdc=str(budget),
                cost_raw=cost,
                profit_raw=profit,
                edge_bps=str(edge),
                impact_pct=str(impact),
                backpack_price=pair["backpack_price"],
                backpack_source=result["backpack_source"],
                backpack_market=result.get("backpack_market"),
                dex_price=pair["dex_price"],
                quoted_output_raw=quoted_output,
                minimum_output_raw=output,
                quantity_raw=acquired if direction == FORWARD else sellable,
                elapsed_s=self.clock() - started,
                phase="local" if local_plan is not None else "network",
            )
            if coverage:
                return result
            actual = result
            now = self.clock()
            fresh_samples = [
                r
                for r in self.volume_search.samples[key]
                if r["size_feasible"]
                and r["mint"] == asset.mint
                and r["backpack_session"] == actual["backpack_session"]
                and 0 <= now - r["dex_observed_at"] <= self.settings.monitor.max_quote_age_s
                and r.get("backpack_source") == actual.get("backpack_source")
                and (not r["backpack_quote_confirmed"] or now < r["backpack_valid_until"])
                and (
                    not isinstance(self.quote_provider, DirectRaydium)
                    or self.quote_provider.bank_versions_are_current(r.get("dex_bank_versions", []))
                )
            ]
            if book_fill:
                current_samples = []
                for sample in fresh_samples:
                    try:
                        revalued = self.revalue_book(asset, sample)
                        if revalued["size_feasible"]:
                            current_samples.append(revalued)
                    except (ValueError, KeyError, ArithmeticError):
                        continue
                fresh_samples = current_samples
            if fresh_samples and not coverage:
                self.results[key] = max(fresh_samples, key=lambda r: r["profit_raw"])
                self.price_history[key] = price_pair(self.results[key])
            return actual
        except asyncio.CancelledError:
            if not coverage:
                self.results.pop(key, None)
            raise
        except Exception as error:
            if coverage_state:
                coverage_state.reason = error_code(error)
            if local_plan is not None:
                if (
                    coverage_state is not None
                    and coverage
                    and local_plan.get("minimum_coverage")
                    and local_plan.get("acquired_raw") == coverage_state.quantity_raw
                    and error_code(error) == "insufficient_tick_coverage"
                    and (bank_key := getattr(error, "bank_key", None)) is not None
                    and callable(request := getattr(self.quote_provider, "request_minimum_expansion", None))
                ):
                    # Enqueue one owned background bank expansion. Extra local
                    # slots never await RPC or fall back to network routing.
                    request(bank_key)
                event("local_quote_unavailable", asset=symbol, direction=direction, code=error_code(error))
                return None
            code = error_code(error)
            if coverage:
                expected = code in MARKET_BLOCKS or isinstance(error, QuoteUnavailable) and error.expected
                if expected:
                    self.blocked[code] += 1
                else:
                    self.errors["quote"] += 1
                if coverage_state and not expected and not isinstance(error, RfqUnavailable):
                    coverage_state.failures += 1
                    coverage_state.retry_at = self.clock() + min(
                        60, self.settings.monitor.quote_refresh_s * 2 ** min(coverage_state.failures, 3)
                    )
                event("forward_coverage_unavailable", asset=symbol, direction=direction, code=code)
                return None
            if (
                direction == FORWARD
                and code
                in SIZE_REFUSALS
                | {
                    "spot_insufficient_depth",
                    "spot_insufficient_balance",
                    "spot_invalid_quantity",
                    "spot_price_band",
                }
                and "budget" in locals()
            ):
                self.volume_search.refuse(key, budget, self.clock())
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
            if not expected and not isinstance(error, RfqUnavailable):
                self.quote_failures[key] += 1
                delay = min(60, self.settings.monitor.quote_refresh_s * 2 ** min(self.quote_failures[key], 3))
                self.retry_after[key] = self.clock() + delay
        finally:
            self.updated.set()
        return None

    async def screen_forward_sizes(self, state: ForwardCoverage, plan: dict) -> dict | None:
        """Reprice the size grid from the same ready bank, without new route reads."""
        from .direct import DirectRaydium

        provider = self.quote_provider
        if not isinstance(provider, DirectRaydium) or provider.bank_feed is None:
            return None
        versions = provider.bank_versions(plan)
        if not versions or any(
            provider.bank_feed.peek((pool, source, target, 10)) is None
            for pool, source, target, _, _ in versions
        ):
            return None
        asset = state.asset
        try:
            obs = self.coverage_observation(asset)
            cap = self.budget(asset, obs)
            floor = self.size_floor(asset, obs)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            return None
        samples: list[tuple[dict, dict]] = []
        seen: set[int] = set()
        key = asset.symbol, FORWARD
        for budget in sorted(
            {min(cap, max(cap * part, floor)) for part in (Decimal("0.25"), Decimal("0.5"), Decimal(1))}
        ):
            if budget < floor:
                continue
            try:
                quantity, _ = self.sizing(asset, obs, notional=budget)
                if quantity in seen:
                    continue
                seen.add(quantity)
                row = await self.quote(
                    key, notional=budget, quote_pair=partial(provider.cached_plan_quote, plan=plan)
                )
                if row and row["size_feasible"]:
                    samples.append((dict(row), dict(self.local_plans[key])))
            except (ValueError, KeyError, TypeError, ArithmeticError):
                continue
        if not samples:
            return None
        result, owned_plan = max(samples, key=lambda pair: pair[0]["profit_raw"])
        self.results[key] = result
        self.local_plans[key] = owned_plan
        self.price_history[key] = price_pair(result)
        return result

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
        from .direct import DirectRaydium

        if isinstance(self.quote_provider, DirectRaydium):
            # Coalesce reads across stocks, not only across one stock's size
            # grid. Each quote keeps its own validated bank and original age.
            async with self.quote_provider.batch_quotes(continuous=True) as quote_pair:
                await self.schedule_quotes(quote_pair)
        else:
            await self.schedule_quotes()

    async def cached_forward_search_quote(
        self,
        source: str,
        target: str,
        amount: int,
        *,
        plan: dict,
        fallback: Callable[..., Awaitable[dict]] | None = None,
        priority: int = 10,
    ) -> dict:
        from .direct import DirectRaydium

        provider = self.quote_provider
        assert isinstance(provider, DirectRaydium)
        try:
            return await provider.cached_plan_quote(source, target, amount, plan=plan, priority=priority)
        except (QuoteUnavailable, ValueError) as error:
            codes = error.codes if isinstance(error, QuoteUnavailable) else [error_code(error)]
            if not codes or not set(codes) <= {
                "pool_bank_not_ready",
                "stale_pool_quote",
                "pool_changed_during_refresh",
                "insufficient_tick_coverage",
                "insufficient_liquidity",
                "no_tick_liquidity",
            }:
                raise
            # A larger amount may require more ticks or another pool. One
            # residual search slot owns this complete fallback, not the baseline.
            return await (fallback or provider.quote_pair)(source, target, amount, priority=priority)

    def sync_primary_banks(self) -> None:
        """Prioritize current minimum routes even while their DEX banks expire."""
        from .direct import DirectRaydium
        from .raydium import CLMM

        provider = self.quote_provider
        if not self.forward_coverage_enabled or not isinstance(provider, DirectRaydium):
            return
        feed = provider.bank_feed
        if feed is None or not hasattr(feed, "set_primary"):
            return
        primary: Counter[tuple[str, str, str, int]] = Counter()
        minimum_arrays: dict[tuple[str, str, str, int], int] = {}
        unresolved: set[tuple[str, str, str, int]] = set()

        def owned_arrays(route: dict) -> dict[tuple[str, str, str, int], int]:
            requirements: dict[tuple[str, str, str, int], int] = {}
            legs = route.get("_direct_route")
            if legs:
                for leg in legs:
                    for key, limit in owned_arrays(leg).items():
                        requirements[key] = max(requirements.get(key, 0), limit)
            else:
                direct = route.get("_direct", {})
                arrays = direct.get("arrays")
                if direct.get("program") == CLMM and isinstance(arrays, (list, tuple)) and arrays:
                    key = route["pool"], route["inputMint"], route["outputMint"], 10
                    requirements[key] = min(self.settings.direct.max_tick_arrays, max(2, len(arrays) + 1))
            return requirements

        for state in self.forward_coverage.values():
            plan = state.plan
            if plan is None and self.indicative_feed is not None:
                warmed = self.valid_prefetched_route(state)
                if warmed is not None:
                    try:
                        self.sync_indicative_coverage(state)
                        self.coverage_observation(state.asset)
                        keys = {
                            (pool, source, target, 10)
                            for pool, source, target, _, _ in provider.bank_versions(warmed)
                        }
                        primary.update(keys)
                        # No current calculation exists yet to resolve a head
                        # expansion. Pin only the validated topology dependencies.
                        unresolved.update(keys)
                        for key, limit in owned_arrays(warmed).items():
                            minimum_arrays[key] = max(minimum_arrays.get(key, 0), limit)
                        promote = getattr(feed, "promote", None)
                        if promote is not None and any(key not in getattr(feed, "hot", {}) for key in keys):
                            promote(
                                {
                                    "routePlan": [
                                        {
                                            "swapInfo": {
                                                "ammKey": pool,
                                                "inputMint": source,
                                                "outputMint": target,
                                            }
                                        }
                                        for pool, source, target, _ in keys
                                    ]
                                },
                                10,
                            )
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        pass
                continue
            if (
                not plan
                or not plan.get("minimum_coverage")
                or plan.get("mint") != state.asset.mint
                or plan.get("acquired_raw") != state.quantity_raw
            ):
                continue
            try:
                session, rules = self.coverage_rules(state.asset)
                if (
                    session != state.session
                    or raw(self.minimum_quantity(state.asset, rules), state.asset.decimals)
                    != state.quantity_raw
                ):
                    continue
                if self.indicative_feed is not None:
                    self.sync_indicative_coverage(state)
                self.coverage_observation(state.asset)
                if plan["backpack_session"] != state.session:
                    continue
                keys = {
                    (pool, source, target, 10)
                    for pool, source, target, _, _ in provider.bank_versions(plan["dex_plan"])
                }
                primary.update(keys)
                promote = getattr(feed, "promote", None)
                if promote is not None and any(key not in getattr(feed, "hot", {}) for key in keys):
                    # A provider refusal can outlast hot retention. Restoring
                    # ownership must also let known addresses refresh again.
                    promote(
                        {
                            "routePlan": [
                                {"swapInfo": {"ammKey": pool, "inputMint": source, "outputMint": target}}
                                for pool, source, target, _ in keys
                            ]
                        },
                        10,
                    )
                for pool, source, target, slot, observed in provider.bank_versions(plan["dex_plan"]):
                    key = pool, source, target, 10
                    bank = feed.peek(key)
                    if (
                        bank is None
                        or not 0 <= self.clock() - bank["observed"] < self.settings.policy.quote_max_age_s
                        or observed != bank["observed"]
                        or slot != bank["response"]["context"]["slot"]
                    ):
                        unresolved.add(key)
                for key, limit in owned_arrays(plan["dex_plan"]).items():
                    minimum_arrays[key] = max(minimum_arrays.get(key, 0), limit)
            except (ValueError, KeyError, TypeError, ArithmeticError):
                continue
        feed.set_primary(primary)
        set_arrays = getattr(feed, "set_minimum_arrays", None)
        if set_arrays is not None:
            set_arrays(minimum_arrays, resolved=set(primary) - unresolved)

    async def schedule_quotes(self, quote_pair: Callable[..., Awaitable[dict]] | None = None) -> None:
        from .direct import DirectRaydium

        while True:
            provider = self.quote_provider
            bank_changed = (
                getattr(provider.bank_feed, "changed", None)
                if isinstance(provider, DirectRaydium) and provider.bank_feed is not None
                else None
            )
            if bank_changed is not None:
                bank_changed.clear()
            for key, task in list(self.pending.items()):
                if task.done():
                    if not task.cancelled():
                        await task
                    self.pending.pop(key)
                    self.local_pending.discard(key)
                    self.coverage_pending.discard(key)
                    self.search_pending.discard(key)
            for key, task in list(self.minimum_pending.items()):
                if task.done():
                    if not task.cancelled():
                        await task
                    self.minimum_pending.pop(key)
            self.adopt_coverage_routes()
            self.sync_primary_banks()
            concurrency = self.settings.monitor.concurrency
            local_limit = max(1, concurrency // 2)
            collision_limit = min(4, local_limit)
            extra_minimum_lane = (
                self.settings.mode == "paper"
                and self.forward_coverage_enabled
                and self.indicative_feed is not None
                and isinstance(provider, DirectRaydium)
                and provider.bank_feed is not None
            )

            def collision_candidate(key: tuple[str, str], plan: dict, *, enabled=extra_minimum_lane) -> bool:
                owner = self.forward_coverage.get(key[0])
                return bool(
                    enabled
                    and key in self.pending
                    and key in self.search_pending
                    and key not in self.coverage_pending
                    and key[1] == FORWARD
                    and plan.get("minimum_coverage")
                    and plan.get("backpack_source") == "indicative"
                    and owner is not None
                    and owner.plan is plan
                    and owner.asset == self.assets.get(key[0])
                    and owner.quantity_raw == plan.get("acquired_raw")
                )

            # Dispatching this batch has no await. WS, metadata and bank inputs
            # cannot change until we yield, so validate each owner/version once.
            coverage_candidates: list[tuple[bool, float, str]] | None = None
            plans: dict[tuple[str, str], dict] | None = None
            ready_local_inputs: list[tuple[tuple[str, str], tuple]] | None = None
            while not self.reporting and (
                len(self.pending) < concurrency
                or extra_minimum_lane
                and len(self.local_pending) + len(self.minimum_pending) < local_limit
                and len(self.pending) + len(self.minimum_pending) < concurrency + local_limit
            ):
                provider = self.quote_provider
                from .direct import DirectRaydium

                network_room = len(self.pending) < concurrency
                coverage_key = None
                if network_room:
                    if coverage_candidates is None:
                        coverage_candidates = self.forward_coverage_candidates()
                    candidate = min(
                        (
                            row
                            for row in coverage_candidates
                            if (row[2], FORWARD) not in self.pending
                            and (row[2], FORWARD) not in self.minimum_pending
                        ),
                        default=None,
                    )
                    if candidate is not None:
                        coverage_key = candidate[2], FORWARD
                initial_started = self.forward_coverage_enabled and all(
                    self.forward_coverage.get(asset.symbol)
                    and self.forward_coverage[asset.symbol].attempted_at
                    or asset.symbol in self.forward_coverage_blocks
                    for asset in self.assets.values()
                    if self.chain.get(asset.symbol) == "verified_unit_mapping"
                )
                reserve_search = (
                    network_room
                    and initial_started
                    and not self.search_pending
                    and (
                        not self.coverage_turn
                        if self.settings.monitor.concurrency == 1
                        else len(self.pending) >= self.settings.monitor.concurrency - 1
                    )
                )
                reserved_selection = self.choose() if reserve_search else None
                if plans is None:
                    plans = dict(self.local_plans)
                    if self.forward_coverage_enabled:
                        plans.update(
                            {
                                (symbol, FORWARD): state.plan
                                for symbol, state in self.forward_coverage.items()
                                if state.plan is not None
                            }
                        )
                if ready_local_inputs is None:
                    ready_local_inputs = []
                    if (
                        isinstance(provider, DirectRaydium)
                        and provider.bank_feed is not None
                        and len(self.local_pending) + len(self.minimum_pending) < local_limit
                    ):
                        now = self.clock()
                        possible = [
                            key
                            for key, plan in plans.items()
                            if key not in self.minimum_pending
                            and (key not in self.pending or collision_candidate(key, plan))
                            and key[0] in self.assets
                            and now >= self.local_after.get(key, 0)
                            and plan["mint"] == self.assets[key[0]].mint
                            and plan["backpack_source"] in {"rfq", "spot", "indicative"}
                            and ("_direct" in plan["dex_plan"] or "_direct_route" in plan["dex_plan"])
                        ]
                        for key in possible:
                            local_version = self.local_input_version(key, plans[key])
                            if local_version is not None and local_version != self.local_inputs.get(key):
                                ready_local_inputs.append((key, local_version))
                local_keys = (
                    [
                        row
                        for row in ready_local_inputs
                        if row[0] not in self.minimum_pending
                        and (
                            row[0] not in self.pending
                            or len(self.minimum_pending) < collision_limit
                            and collision_candidate(row[0], plans[row[0]])
                        )
                    ]
                    if len(self.local_pending) + len(self.minimum_pending) < local_limit
                    else []
                )
                ready_minimum = [row for row in local_keys if plans[row[0]].get("minimum_coverage")]
                if not network_room:
                    # These extra slots only consume already owned public
                    # inputs and ready banks. The cached minimum path has no
                    # account request, route discovery or network fallback.
                    ready_minimum = [
                        row for row in ready_minimum if plans[row[0]]["backpack_source"] == "indicative"
                    ]
                    if not extra_minimum_lane or not ready_minimum:
                        break
                prefer_minimum = bool(ready_minimum and (self.local_turn or reserved_selection is None))
                if prefer_minimum:
                    local_keys = ready_minimum
                    reserved_selection = None
                elif coverage_key is not None and reserved_selection is None:
                    state = self.forward_coverage[coverage_key[0]]
                    coverage_quote = quote_pair
                    if isinstance(provider, DirectRaydium) and state.plan:
                        coverage_quote = partial(provider.cached_plan_quote, plan=state.plan["dex_plan"])
                    else:
                        state.discovery_at = self.clock()
                        if (
                            self.settings.mode == "paper"
                            and self.indicative_feed is not None
                            and isinstance(provider, DirectRaydium)
                            and provider.bank_feed is not None
                        ):
                            prefetched = getattr(self, "prefetched_coverage_quote", None)
                            coverage_quote = prefetched(state, fallback=quote_pair) if prefetched else None
                            if coverage_quote is None:
                                coverage_quote = partial(quote_pair or provider.quote_pair, minimum=True)
                    self.coverage_turn = False
                    self.coverage_pending.add(coverage_key)
                    self.pending[coverage_key] = asyncio.create_task(
                        self.quote(coverage_key, coverage=True, quote_pair=coverage_quote)
                    )
                    continue
                # Alternate dispatches and bound local in-flight work so cold
                # exploration retains slots even when many banks change at once.
                selected = (
                    None
                    if prefer_minimum
                    else reserved_selection
                    or (self.choose() if network_room and (not local_keys or not self.local_turn) else None)
                )
                if local_keys and selected is None:
                    assert isinstance(provider, DirectRaydium)
                    key, local_version = min(local_keys, key=lambda row: self.local_after.get(row[0], 0))
                    self.local_after[key] = self.clock() + 1
                    self.local_inputs[key] = local_version
                    plan = plans[key]
                    coverage = plan.get("minimum_coverage", False)
                    self.local_turn = False
                    task = asyncio.create_task(
                        self.quote(
                            key,
                            notional=dec(plan["search_budget_usdc"]),
                            local_plan=plan,
                            quote_pair=partial(provider.cached_plan_quote, plan=plan["dex_plan"]),
                            coverage=coverage,
                        )
                    )
                    if key in self.pending:
                        # Only a ready owned public minimum may share the key
                        # with a larger search. It owns no ordinary result row.
                        assert collision_candidate(key, plan)
                        self.minimum_pending[key] = task
                    else:
                        self.local_pending.add(key)
                        if coverage:
                            self.coverage_pending.add(key)
                        self.pending[key] = task
                    continue
                if selected is None:
                    break
                self.local_turn = True
                self.coverage_turn = True
                if self.forward_coverage_enabled and selected[1] == FORWARD:
                    self.search_pending.add(selected)
                search_quote = quote_pair
                coverage = self.forward_coverage.get(selected[0])
                if (
                    self.forward_coverage_enabled
                    and selected[1] == FORWARD
                    and isinstance(provider, DirectRaydium)
                    and coverage
                    and coverage.plan is not None
                ):
                    search_quote = partial(
                        self.cached_forward_search_quote, plan=coverage.plan["dex_plan"], fallback=quote_pair
                    )
                if (
                    self.indicative_feed is not None
                    and selected[1] == FORWARD
                    and coverage
                    and coverage.plan is not None
                    and selected not in self.optimization_requested
                ):
                    self.optimization_requested.discard(selected)
                    self.pending[selected] = asyncio.create_task(
                        self.screen_forward_sizes(coverage, coverage.plan["dex_plan"])
                    )
                elif selected in self.optimization_requested:
                    self.optimization_requested.discard(selected)
                    self.pending[selected] = asyncio.create_task(
                        self.optimize(selected, quote_pair=search_quote)
                    )
                else:
                    self.pending[selected] = asyncio.create_task(
                        self.quote(selected, notional=self.next_budget(selected), quote_pair=search_quote)
                    )
            version = self.backpack.stock_feed.version
            waiters = [asyncio.create_task(self.backpack.stock_feed.wait(version))]
            if self.indicative_feed is not None:
                waiters.append(asyncio.create_task(self.indicative_feed.wait(self.indicative_feed.version)))
            if bank_changed is not None:
                waiters.append(asyncio.create_task(bank_changed.wait()))
            try:
                done, _ = await asyncio.wait(
                    [*waiters, *self.pending.values(), *self.minimum_pending.values()],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=0.25,
                )
                for waiter in waiters:
                    if waiter in done:
                        await waiter
            finally:
                for waiter in waiters:
                    if not waiter.done():
                        waiter.cancel()
                await asyncio.gather(*waiters, return_exceptions=True)

    async def start(self) -> None:
        self.tasks = [
            asyncio.create_task(self.supervise(fn))
            for fn in (self.metadata_loop, self.external_prices_loop, self.prices_loop, self.quotes_loop)
        ]
        if self.settings.monitor.rfq_validation:
            self.tasks.append(asyncio.create_task(self.supervise(self.rfq_loop)))
        if self.forward_coverage_enabled:
            self.tasks.append(asyncio.create_task(self.supervise(self.coverage_routes_loop)))
        if self.settings.monitor.spot_books:
            self.tasks.append(asyncio.create_task(self.supervise(self.books_loop)))

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
        tasks = [*self.tasks, *self.pending.values(), *self.minimum_pending.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self.pending.clear()
        self.minimum_pending.clear()
        if self._owns_quote_provider and self._quote_provider is not None:
            await self._quote_provider.close()

    def model_blocks(self, row: dict) -> list[str]:
        p = self.settings.policy
        return [
            code
            for code, failed in (
                ("backpack_rfq_unconfirmed", row.get("backpack_model") is True),
                ("indicative_requires_rfq", row.get("backpack_source") == "indicative"),
                ("cost_cap", not self.settings.monitor.auto_size and row["cost_raw"] > raw(p.max_cost_usdc)),
                ("profit_below_minimum", row["profit_raw"] < raw(p.min_profit_usdc)),
                ("edge_below_minimum", dec(row["edge_bps"]) < p.min_edge_bps),
                ("price_impact_limit", dec(row["price_impact_pct"]) > dec(p.max_price_impact_pct)),
                ("slippage_limit", row.get("slippage_bps", p.slippage_bps) > p.slippage_bps),
            )
            if failed
        ]

    def session_status(self, now: float) -> dict:
        status = {"rfq_session_state": "unknown", "rfq_session": None, "rfq_next_open_at": None}
        if not self.sessions or not 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2:
            return status
        moment = datetime.fromtimestamp(now, UTC)
        session = self.calendar_session(now)
        opening = None if session else next_session_open(self.sessions, self.holidays, moment)
        return {
            "rfq_session_state": "open" if session else "closed",
            "rfq_session": session,
            "rfq_next_open_at": opening.timestamp() if opening else None,
        }

    def coverage_snapshot(self) -> dict:
        now = self.clock()
        eligible = sum(self.chain.get(symbol) == "verified_unit_mapping" for symbol in self.assets)
        rows = []
        checked = priced = refused = waiting_bank = completed = unattempted = known_blocks = 0
        for asset in self.assets.values():
            state = self.forward_coverage.get(asset.symbol)
            block = self.forward_coverage_blocks.get(asset.symbol)
            if block and block["asset"] != asset:
                block = None
            verified = self.chain.get(asset.symbol) == "verified_unit_mapping"
            current = False
            try:
                current = bool(
                    state
                    and state.asset == asset
                    and state.session == self.coverage_rules(asset)[0]
                    and 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2
                )
            except (ValueError, KeyError, TypeError, ArithmeticError):
                pass
            if state and current and verified and self.indicative_feed is not None:
                self.sync_indicative_coverage(state)
            check = state.check if state and current and verified else None
            own = bool(
                state is not None
                and check is not None
                and check.symbol == asset.symbol
                and check.side == "Bid"
                and check.session == state.session
                and dec(check.quantity) == ui(state.quantity_raw, asset.decimals)
                and check.checked_at <= now
            )
            bid_fresh = bool(own and check is not None and check.usable(now))
            rejected = bool(
                own and check is not None and check.state == "unavailable" and now < check.eligibility_until
            )
            fresh = bool(current and state and self.coverage_result_fresh(state))
            checked += bid_fresh or rejected
            refused += rejected
            priced += fresh
            waiting_bank += bid_fresh and not fresh
            terminal_block = bool(
                verified
                and block
                and block["terminal"]
                and 0 <= now - self.metadata_at <= self.settings.monitor.metadata_s * 2
            )
            completed += terminal_block or bool(verified and current and state and state.completed)
            known_blocks += terminal_block
            unattempted += bool(verified and not block and not (state and state.attempted_at))
            status = (
                "unverified"
                if not verified
                else block["state"]
                if block
                else "fresh_priced"
                if fresh
                else "refused"
                if rejected
                else "waiting_bank"
                if bid_fresh
                else "waiting_bid"
            )
            pair = price_pair(state.result) if state and state.result else None
            if pair:
                pair = {**pair, "fresh": fresh}
            rows.append(
                {
                    "asset": asset.symbol,
                    "quantity": str(ui(state.quantity_raw, asset.decimals)) if state else None,
                    "state": status,
                    "reason": block["reason"]
                    if block
                    else (
                        state.reason if check.source == "indicative" and state is not None else check.reason
                    )
                    if rejected and check is not None
                    else state.reason
                    if state
                    else None,
                    "bid_checked_at": check.checked_at if own and check is not None else None,
                    "bid_valid_until": check.retry_at if own and check is not None else None,
                    "pair": pair,
                    "cost_raw": state.result["cost_raw"] if state and state.result else None,
                    "profit_raw": state.result["profit_raw"] if state and state.result else None,
                    "route_inputs_fresh": fresh,
                    "quote_source": check.source if check else None,
                }
            )
        return {
            "enabled": self.forward_coverage_enabled,
            "quote_source": "indicative" if self.indicative_feed is not None else "rfq",
            "indicative_pricedfresh": priced if self.indicative_feed is not None else 0,
            "rfq_pricedfresh": priced if self.indicative_feed is None else 0,
            "minimum_probe": True,
            "profitable_search_separate": True,
            "universe": len(self.assets),
            "eligible": eligible,
            "checked": checked,
            "pricedfresh": priced,
            "refused": refused,
            "waiting": max(0, eligible - priced - refused),
            "waitingbank": waiting_bank,
            # A failed/stale route probe cannot prove that a current route is absent.
            "currentroute_missing": 0,
            "pending": sum(
                key in self.pending and not self.pending[key].done() for key in self.coverage_pending
            )
            + sum(not task.done() for task in self.minimum_pending.values()),
            "unverified": len(self.assets) - eligible,
            "first_pass_completed": completed,
            "known_blocks": known_blocks,
            "unattempted": unattempted,
            "done_universe": bool(eligible and completed == eligible),
            "rows": rows,
        }

    def snapshot(self, *, include_hidden: bool = False) -> dict:
        now, config = self.clock(), self.settings.monitor
        coverage_report = self.coverage_snapshot()
        coverage_by_asset = {row["asset"]: row for row in coverage_report["rows"]}
        results = dict(self.results)
        if self.forward_coverage_enabled:
            for symbol, state in self.forward_coverage.items():
                if not state.result or state.asset != self.assets.get(symbol):
                    continue
                key = symbol, FORWARD
                primary = results.get(key)
                primary_check = (
                    self.exact_rfq(
                        state.asset,
                        str(ui(primary["acquired_raw"], state.asset.decimals)),
                        primary["backpack_session"],
                    )
                    if primary and primary.get("backpack_source") == "rfq"
                    else None
                )
                primary_current = False
                try:
                    primary_obs = (
                        self.books.observation(symbol)
                        if primary and primary.get("backpack_source") == "spot"
                        else self.observation(state.asset)
                    )
                    primary_current = bool(
                        primary
                        and self.quantity_session(primary_obs) == primary["backpack_session"]
                        and self.sale_quantity(state.asset, primary_obs, primary["acquired_raw"])
                        == primary["acquired_raw"]
                    )
                    if primary_current and primary is not None and primary.get("backpack_source") == "spot":
                        self.current_book_fill(state.asset, primary)
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    primary_current = False
                primary_fresh = bool(
                    primary
                    and primary_current
                    and 0 <= now - primary["dex_observed_at"] <= config.max_quote_age_s
                    and (
                        primary.get("backpack_source") == "spot"
                        or primary.get("backpack_source") == "indicative"
                        and 0 <= now - primary["backpack_timestamp"] < config.max_quote_age_s
                        or primary_check
                        and primary_check.checked_at == primary["backpack_timestamp"]
                        and dec(primary_check.price or "0") == dec(primary["backpack_price"])
                    )
                )
                from .direct import DirectRaydium

                if primary_fresh and primary is not None and isinstance(self.quote_provider, DirectRaydium):
                    primary_fresh = self.quote_provider.bank_versions_are_current(
                        primary.get("dex_bank_versions", [])
                    )
                if not primary or not primary_fresh and self.coverage_result_fresh(state):
                    results[key] = state.result
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
        for key, cached_result in results.items():
            asset = self.assets.get(key[0])
            if (
                asset
                and cached_result.get("backpack_quote_confirmed")
                and cached_result.get("backpack_source") != "spot"
            ):
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
            coverage = self.forward_coverage.get(asset.symbol) if self.forward_coverage_enabled else None
            if coverage and coverage.asset == asset and coverage.check and coverage.check.source == "rfq":
                buy_check = coverage.check
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
                    session = self.current_rfq_session(asset)
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
                side_eligible, side_reason = rfq_eligible, rfq_reason
                if direction == REVERSE and config.rfq_validation:
                    sell_check = self.rfq.latest.get((asset.symbol, "Ask"))
                    side_eligible = False
                    side_reason = "rfq_sell_check_pending"
                    try:
                        session = self.quantity_session(self.observation(asset))
                        side_eligible = bool(
                            sell_check and sell_check.session == session and sell_check.eligible(now)
                        )
                        if sell_check:
                            side_reason = (
                                sell_check.reason if now < sell_check.eligibility_until else "rfq_check_stale"
                            )
                            if sell_check.session != session:
                                side_reason = "rfq_session_unavailable"
                    except (ValueError, KeyError, TypeError):
                        side_reason = "rfq_session_unavailable"
                key = asset.symbol, direction
                result = results.get(key)
                if result and result.get("backpack_source") == "indicative":
                    side_eligible = False
                    side_reason = "indicative_quote_pending"
                    try:
                        exact_public = self.indicative_check(
                            asset, result["acquired_raw"], result["backpack_session"]
                        )
                        side_eligible = exact_public.usable(now)
                        side_reason = exact_public.reason
                    except (ValueError, KeyError, TypeError, ArithmeticError) as error:
                        side_reason = error_code(error)
                if result and result.get("backpack_source") == "rfq" and config.rfq_validation:
                    side_eligible = False
                    side_reason = "rfq_check_stale"
                    quantity = result["acquired_raw"] if direction == FORWARD else result["sellable_raw"]
                    exact = self.exact_rfq(
                        asset,
                        str(ui(quantity, asset.decimals)),
                        result["backpack_session"],
                        "Bid" if direction == FORWARD else "Ask",
                    )
                    try:
                        if (
                            exact
                            and exact.checked_at == result["backpack_timestamp"]
                            and dec(exact.price or "0") == dec(result["backpack_price"])
                            and exact.session == self.current_rfq_session(asset)
                        ):
                            side_eligible = True
                            side_reason = exact.reason
                    except (ValueError, KeyError, TypeError):
                        pass
                row = dict(
                    result
                    or {"asset": asset.symbol, "direction": direction, "mint": asset.mint, "indicative": True}
                )
                age = now - result.get("dex_observed_at", result["quote_started_at"]) if result else None
                metadata_fresh = 0 <= now - self.metadata_at <= config.metadata_s * 2
                fresh = (
                    age is not None
                    and 0 <= age <= config.max_quote_age_s
                    and metadata_fresh
                    and self.chain.get(asset.symbol) == "verified_unit_mapping"
                    and row.get("mint") == asset.mint
                    and (direction == FORWARD or asset.deposit_enabled)
                )
                from .direct import DirectRaydium

                if fresh and isinstance(self.quote_provider, DirectRaydium):
                    fresh = self.quote_provider.bank_versions_are_current(row.get("dex_bank_versions", []))
                if result and fresh:
                    try:
                        p = self.settings.policy
                        if row.get("backpack_source") == "spot":
                            fill = self.current_book_fill(asset, row)
                            observation = self.books.observation(asset.symbol).model_copy(
                                update={"ask" if direction == FORWARD else "bid": fill.price}
                            )
                            self.quantity_session(observation)
                            if self.sale_quantity(
                                asset,
                                observation,
                                row["acquired_raw"] if direction == FORWARD else row["minimum_output_raw"],
                            ) != (row["acquired_raw"] if direction == FORWARD else row["sellable_raw"]):
                                raise BookUnavailable("spot_invalid_quantity")
                            row.update(self.book_fields(asset, fill))
                            row["cost_raw" if direction == FORWARD else "proceeds_raw"] = fill.value_raw
                            row["price_impact_pct"] = str(
                                max(dec(row["dex_price_impact_pct"]), dec(fill.impact_pct))
                            )
                        else:
                            observation = (
                                self.coverage_observation(asset)
                                if row.get("minimum_coverage")
                                else self.observation(asset)
                            )
                            if row.get("backpack_source") == "indicative":
                                public_check = self.indicative_check(
                                    asset, row["acquired_raw"], row["backpack_session"]
                                )
                                observation = self.check_observation(public_check)
                                row["backpack_valid_until"] = public_check.retry_at
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
                            and (self.settings.monitor.auto_size or row["cost_raw"] <= raw(p.max_cost_usdc))
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
                    candidate=fresh
                    and bool(row.get("passes_model_limits"))
                    and row.get("backpack_source") != "indicative",
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
                spot_result = row.get("backpack_source") == "spot"
                source_eligible = self.books.available(asset.symbol) if spot_result else side_eligible
                fresh_calculations += bool(result and fresh and source_eligible)
                direction_eligible = source_eligible and (
                    not config.rfq_validation
                    or result is not None
                    and row.get("backpack_quote_confirmed")
                    and fresh
                )
                display_eligible = bool(
                    direction_eligible
                    or source_eligible
                    and result
                    and fresh
                    and row.get("backpack_source") == "indicative"
                )
                # Informational calls expose every validated positive minimum
                # net. Trading thresholds continue to control candidate only.
                row["signal_eligible"] = bool(
                    direction_eligible
                    and result
                    and fresh
                    and positive
                    and all(
                        code in {"profit_below_minimum", "edge_below_minimum", "price_impact_limit"}
                        for code in row["candidate_blocks"]
                    )
                )
                if (side_eligible or self.books.available(asset.symbol)) and screen is not None:
                    screened_assets.add(asset.symbol)
                if display_eligible and result and fresh:
                    # Price availability is independent of profitability. Publish
                    # only validated prices here, without a hidden negative spread.
                    pair = price_pair(row)
                    pair["quote_age_s"] = max(0, now - pair["updated_at"])
                    prices.append(pair)
                if (display_eligible and positive) or include_hidden:
                    rows.append(row)
                if not direction_eligible or not positive:
                    diagnostics.append(
                        {
                            "asset": asset.symbol,
                            "direction": direction,
                            "retry_in_s": max(0, self.retry_after.get(key, 0) - now),
                            "reasons": [self.chain.get(asset.symbol, "unverified")]
                            if self.chain.get(asset.symbol) != "verified_unit_mapping"
                            else [side_reason]
                            if not side_eligible and not self.books.available(asset.symbol)
                            else ["nonpositive_net"]
                            if fresh and direction_eligible and not positive
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
            current_pair = next(
                (pair for pair in prices if pair["asset"] == symbol and pair["direction"] == FORWARD), None
            )
            if current_pair:
                record.update(current_pair)
            record.update(
                asset=symbol,
                rfq_state=status["state"],
                rfq_reason=status["reason"],
                spot_market=self.books.markets[symbol].symbol if symbol in self.books.markets else None,
                spot_available=self.books.available(symbol),
                chain=self.chain.get(symbol, "unverified"),
                fresh=(symbol, FORWARD) in fresh_keys,
                **self.budget_info(symbol),
            )
            if self.forward_coverage_enabled:
                record["minimum_coverage"] = coverage_by_asset.get(symbol)
            if record.get("dex_price") is not None:
                record["quote_age_s"] = max(0, now - record["updated_at"])
            elif self.books.available(symbol):
                obs = self.books.observation(symbol)
                record.update(
                    backpack_source="spot",
                    backpack_price=obs.ask,
                    backpack_updated_at=obs.timestamp,
                    backpack_fresh=True,
                    buy_quantity="—",
                )
            elif check and check.price:
                record.update(
                    backpack_price=check.price,
                    backpack_updated_at=check.checked_at,
                    backpack_fresh=check.usable(now) and status["state"] == "available",
                    buy_quantity=check.quantity,
                )
            reverse = dict(self.price_history.get((symbol, REVERSE), {}))
            current_reverse = next(
                (pair for pair in prices if pair["asset"] == symbol and pair["direction"] == REVERSE), None
            )
            if current_reverse:
                reverse.update(current_reverse)
            reverse.update(asset=symbol, direction=REVERSE, fresh=(symbol, REVERSE) in fresh_keys)
            reverse["mint"] = self.assets[symbol].mint
            if reverse.get("updated_at") is not None:
                reverse["quote_age_s"] = max(0, now - reverse["updated_at"])
            reverse["reasons"] = next(
                (d["reasons"] for d in diagnostics if d["asset"] == symbol and d["direction"] == REVERSE), []
            )
            sell_check = self.rfq.latest.get((symbol, "Ask"))
            reverse["rfq_state"] = sell_check.public(now)["state"] if sell_check else "unknown"
            if "rfq_session_unavailable" in reverse["reasons"]:
                reverse["rfq_state"] = "unavailable"
            record["reverse"] = reverse
            record["mint"] = self.assets[symbol].mint
            record["reasons"] = next(
                (d["reasons"] for d in diagnostics if d["asset"] == symbol and d["direction"] == FORWARD), []
            )
            catalog.append(record)
        repeat_intervals = sorted(self.price_repeat_intervals)
        latency = sorted(self.latencies)
        return {
            "observed_at": now,
            **self.session_status(now),
            "dex_backend": self.settings.dex_backend,
            "quote_backend": self.quote_backend,
            "quote_source": "jupiter_flow"
            if isinstance(self.quote_provider, JupiterFlowQuotes)
            else self.quote_backend,
            "universe": len(self.assets),
            "notional_usdc": config.notional_usdc,
            "auto_size": config.auto_size,
            "backpack_price_ttl_s": config.max_quote_age_s,
            "indicative_quote_ttl_s": config.max_quote_age_s,
            "dex_bank_ttl_s": self.settings.policy.quote_max_age_s,
            "forward_coverage": coverage_report,
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
            "signal_candidates": sum(r["signal_eligible"] for r in rows),
            "completed_quotes": self.completed_quotes,
            "local_calculations": self.local_calculations,
            "fresh_calculations": fresh_calculations,
            "rfq_confirmed_calculations": sum(pair.get("backpack_source") == "rfq" for pair in prices),
            "stock_value_backend": self.stock_values.backend,
            "quote_retry_waiting": sum(now < deadline for deadline in self.retry_after.values()),
            "pending": len(self.pending) + len(self.minimum_pending),
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
            "indicative_ws": {
                "enabled": self.indicative_feed is not None,
                "connected": self.indicative_feed.socket is not None if self.indicative_feed else False,
                "assets": len(self.indicative_feed.latest) if self.indicative_feed else 0,
                "proxied": self.indicative_feed.proxy is not None if self.indicative_feed else False,
            },
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
            "price_repeat_p95_s": repeat_intervals[
                min(len(repeat_intervals) - 1, int(len(repeat_intervals) * 0.95))
            ]
            if repeat_intervals
            else None,
            "focused_assets": sorted(s for s, end in self.focus_until.items() if end > now),
            "diagnostics": diagnostics,
            "rfq_validation": config.rfq_validation,
            "rfq_cache_ttl_s": config.rfq_cache_s,
            "rfq_status": rfq_status,
            "rfq_sell_status": [
                check.public(now)
                for (symbol, side), check in self.rfq.latest.items()
                if side == "Ask" and symbol in self.assets
            ],
            "spot_markets": len(self.books.markets),
            "spot_available_assets": sum(self.books.available(symbol) for symbol in self.books.markets),
            "spot_supported_assets": sum(
                self.books.available(symbol)
                and self.chain.get(symbol) == "verified_unit_mapping"
                and 0 <= now - self.metadata_at <= config.metadata_s * 2
                for symbol in self.books.markets
            ),
            "spot_refresh_s": config.spot_refresh_s,
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
        # Keep recently observed wallet assets on the fast lane. The bounded
        # focus only affects scheduling, never quote freshness or trade rules.
        self.focus_until[symbol] = now + max(300, self.settings.monitor.exploration_s * 4)
        if len(self.focus_until) > 8:
            self.focus_until.pop(min(self.focus_until, key=lambda s: self.focus_until[s]))
        for direction in (FORWARD, REVERSE):
            if direction == FORWARD and self.budget_info(symbol).get("within_budget") is False:
                continue
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

    def request_optimization(self, symbol: str) -> bool:
        asset = self.assets.get(symbol)
        if (
            asset is None
            or self.chain.get(symbol) != "verified_unit_mapping"
            or self.budget_info(symbol).get("within_budget") is False
            or self.rfq_cooldown(asset, FORWARD)
            and not self.size_refusal(asset)
        ):
            return False
        key = symbol, FORWARD
        if key in self.pending:
            return False
        self.optimization_requested.add(key)
        self.requested.setdefault(key, self.clock())
        return True

    async def optimize(
        self, key: tuple[str, str], *, quote_pair: Callable[..., Awaitable[dict]] | None = None
    ) -> None:
        """Bounded exact-size search occupying one ordinary scheduler slot."""
        asset = self.assets[key[0]]
        cap = self.budget(asset, self.planning_observation(asset))
        samples: list[dict] = []
        probes = 0

        def yield_to_coverage() -> bool:
            if probes and key in self.search_pending and self.choose_forward_coverage(ignore_pending=key):
                self.optimization_requested.add(key)
                self.requested.setdefault(key, self.clock())
                return True
            return False

        try:
            async with asyncio.timeout(35):
                for fraction in VolumeSearch.fractions:
                    if yield_to_coverage():
                        return
                    # Many fractions collapse to the same legal whole-share lot.
                    # Spend the provider slot only once per sized quantity.
                    try:
                        asset = self.assets[key[0]]
                        obs = self.planning_observation(asset)
                        budget = (
                            self.next_budget(key)
                            if self.settings.monitor.auto_size
                            else min(cap, max(cap * fraction, self.size_floor(asset, obs)))
                        )
                        quantity, _ = self.sizing(asset, obs, notional=budget)
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        continue
                    if any(r["acquired_raw"] == quantity for r in samples):
                        continue
                    probes += 1
                    row = await self.quote(key, notional=budget, quote_pair=quote_pair)
                    if row is not None:
                        samples.append(dict(row))
                    if self.rfq_cooldown(self.assets[key[0]], key[1]) and not self.size_refusal(
                        self.assets[key[0]]
                    ):
                        break
                feasible = [r for r in samples if r["size_feasible"]]
                if samples:
                    best = (
                        max(feasible, key=lambda r: r["profit_raw"])
                        if feasible
                        else min(samples, key=lambda r: r["cost_raw"])
                    )
                    ceiling = (
                        self.size_ceiling(asset, self.planning_observation(asset))
                        if self.settings.monitor.auto_size
                        else cap
                    )
                    # Refine the winner inside the provider's legal size range.
                    for budget in (
                        max(cap / 32, dec(best["search_budget_usdc"]) - cap / 16),
                        min(ceiling, dec(best["search_budget_usdc"]) + cap / 16),
                    ):
                        if yield_to_coverage():
                            return
                        if any(dec(r["search_budget_usdc"]) == budget for r in samples):
                            continue
                        probes += 1
                        row = await self.quote(key, notional=budget, quote_pair=quote_pair)
                        if row is not None:
                            samples.append(dict(row))
                            if row["size_feasible"] and (
                                not best["size_feasible"] or row["profit_raw"] > best["profit_raw"]
                            ):
                                best = row
                    # A winning old sample is re-quoted before it becomes the current result.
                    if not yield_to_coverage():
                        await self.quote(key, notional=dec(best["search_budget_usdc"]), quote_pair=quote_pair)
        except TimeoutError:
            event("volume_search_timeout", asset=key[0], direction=key[1])
        finally:
            event("volume_search_complete", asset=key[0], direction=key[1], quotes=len(samples))
            self.updated.set()

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

        prices = [r for r in snapshot.get("prices", []) if selected(r)]
        lines = format_spreads(prices)
        if not lines:
            lines = ["Свежих полных котировок пока нет. Спред и net не оценены."]
            rejected = [
                row
                for row in snapshot.get("rows", [])
                if selected(row)
                and row.get("fresh")
                and row.get("profit_raw", 0) > 0
                and row.get("signal_eligible") is False
                and row.get("candidate_blocks")
            ]
            for row in rejected[:3]:
                lines.append(f"{row['asset']}: {block_text(row['candidate_blocks'])}")
            session_message = session_text(snapshot)
            if session_message:
                lines.append(session_message)
            if symbol:
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
        if symbol is None:
            coverage_lines = compact_coverage(snapshot)
            if coverage_lines:
                lines.extend(["", *coverage_lines])
        lines.extend(["", "Подробнее: /monitor · Все объёмы: /prices" + (f" {symbol}" if symbol else "")])
        return "\n".join(lines)

    def prices_text(
        self, symbols: list[str] | None = None, *, page: int = 1, total: int = 1, snapshot: dict | None = None
    ) -> str:
        snapshot = self.snapshot() if snapshot is None else snapshot
        selected = sorted(self.assets) if symbols is None else symbols
        catalog = {r["asset"]: r for r in snapshot["price_catalog"]}
        interval = snapshot["price_repeat_median_s"]
        cadence = f"повтор цены ~{interval:.0f}с" if interval is not None else "цены по очереди"
        lines = [
            f"Цены · {'автообъём, без потолка USDC' if self.settings.monitor.auto_size else 'до ' + self.settings.monitor.notional_usdc + ' USDC'} · {page}/{total}",
            f"RFQ каждые {self.settings.monitor.rfq_cache_s:g}с; {cadence} "
            f"(запрос ≥{self.quote_provider.interval:g}с).",
            f"Свежими считаются цены до {self.settings.monitor.max_quote_age_s:g}с. Время МСК.",
        ]
        if self.forward_coverage_enabled:
            lines[1:3] = [
                "BP → DEX: минимальная партия по всем акциям; большие объёмы проверяются отдельно.",
                f"Свежесть: RFQ до {self.settings.monitor.max_quote_age_s:g}с; "
                f"DEX до {snapshot.get('dex_bank_ttl_s', self.settings.policy.quote_max_age_s):g}с. Время МСК.",
            ]
        if snapshot.get("spot_markets"):
            lines.insert(
                2,
                f"Стаканы обновляются каждые {self.settings.monitor.spot_refresh_s:g}с; учитывается taker-комиссия.",
            )
        session_message = session_text(snapshot)
        if session_message:
            lines.append(session_message)
        for symbol in selected:
            record = catalog.get(symbol)
            if record is None:
                continue
            rfq = {"available": "доступна", "unavailable": "нет", "unknown": "непроверена"}[
                record["rfq_state"]
            ]
            book = (
                " · стакан "
                + (
                    "доступен"
                    if record["spot_available"] and record["chain"] == "verified_unit_mapping"
                    else "не пригоден для расчёта"
                )
                if record.get("spot_market")
                else ""
            )
            sell_rfq = {"available": "доступна", "unavailable": "нет", "unknown": "непроверена"}[
                record["reverse"]["rfq_state"]
            ]
            lines.append(f"\n{symbol} · RFQ покупка {rfq}, продажа {sell_rfq}{book}")
            coverage = record.get("minimum_coverage")
            if coverage:
                coverage_status = {
                    "fresh_priced": "свежий расчёт",
                    "refused": "отказ Backpack",
                    "waiting_bid": "ожидает цены Backpack",
                    "waiting_bank": "ожидает свежей цены DEX",
                    "waiting_metadata": "ожидает правил рынка",
                    "skipped_current_session": "сессия недоступна",
                    "unsupported_quantity": "нет допустимой партии",
                    "unverified": "соответствие токена не подтверждено",
                }.get(coverage["state"], "ожидает расчёта")
                quantity = f"{coverage['quantity']} шт · " if coverage.get("quantity") else ""
                lines.append(f"Покрытие BP → DEX: {quantity}{coverage_status}.")
            lines.extend(price_report({**record, "direction": FORWARD}))
            lines.extend(price_report(record["reverse"]))
            if record["rfq_state"] != "available" and not record["spot_available"]:
                lines.append(block_text([record["rfq_reason"]]))
            if record["chain"] != "verified_unit_mapping":
                lines.append(block_text([record["chain"]]))
            if record.get("within_budget") is False:
                lines.append(
                    f"Мин. {record['minimum_buy_quantity']} шт ≈ {dec(record['minimum_cost_usdc']):.2f} USDC; лимит {record['budget_usdc']}. Кол исключён по объёму."
                )
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
    bp = "BP стакан" if row.get("backpack_source") == "spot" else "BP"
    return f"{bp} → {label}" if row["direction"] == FORWARD else f"{label} → {bp}"


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
        "mint": row["mint"],
        "direction": row["direction"],
        "quote_backend": row["quote_backend"],
        "backpack_source": row.get("backpack_source", "rfq"),
        "backpack_price": format(dec(row["backpack_price"]), ".6f").rstrip("0").rstrip("."),
        "dex_price": format(dex_price, ".6f"),
        "backpack_updated_at": bp_at,
        "dex_updated_at": dex_at,
        "updated_at": min(bp_at, dex_at),
        "quote_age_s": max(0, row.get("quote_age_s", 0), row.get("observed_at", dex_at) - min(bp_at, dex_at)),
        "buy_quantity": amount(
            row["acquired_raw"] if row["direction"] == FORWARD else row["minimum_output_raw"], decimals
        ),
        "sell_quantity": amount(
            row["delivered_raw"] if row["direction"] == FORWARD else row["sellable_raw"], decimals
        ),
        "cost_raw": row["cost_raw"],
        "quoted_output_raw": row["quoted_output_raw"],
        "minimum_output_raw": row["minimum_output_raw"],
        "proceeds_raw": row["proceeds_raw"],
        "profit_raw": row["profit_raw"],
        "quoted_profit_raw": row["quoted_output_raw"] - row["cost_raw"] - row["gas_raw"]
        if row["direction"] == FORWARD
        else None,
        "gas_raw": row["gas_raw"],
        "deposit_gas_raw": row.get("deposit_gas_raw", 0),
        "backpack_deposit_required": row.get("backpack_deposit_required", row["direction"] == REVERSE),
        "candidate_blocks": row.get("candidate_blocks", []),
        "stock_decimals": decimals,
        "route_markets": row.get("route_markets", []),
    }


def signed_profit(value: int) -> str:
    return f"{ui(value):+.2f}" if abs(value) >= 10000 or value == 0 else f"{ui(value):+.6f}"


def venue_links(row: dict) -> list[str]:
    ticker = url_quote(row["asset"].removesuffix(".US"), safe="")
    links = [f"Backpack: https://backpack.exchange/stocks/{ticker}"]
    if row.get("quote_backend") == "raydium":
        for market in row.get("route_markets", [])[:4]:
            pool = market.get("pool", "")
            try:
                Pubkey.from_string(pool)
            except ValueError:
                continue
            links.append(f"Пул {market.get('label', 'Raydium')}: https://solscan.io/account/{pool}")
    if row.get("mint"):
        source, target = (row["mint"], USDC_MINT) if row["direction"] == FORWARD else (USDC_MINT, row["mint"])
        links.append("Jupiter: https://jup.ag/swap?" + urlencode({"sell": source, "buy": target}))
    return links


def price_report(record: dict) -> list[str]:
    direction = price_direction(record)
    if record.get("dex_price") is None:
        reasons = [code for code in record.get("reasons", []) if code != "nonpositive_net"]
        return [
            f"{direction}: нет актуального расчёта" + (" · " + block_text(reasons) if reasons else ""),
            *venue_links(record),
        ]
    if not record["fresh"]:
        reasons = [
            code
            for code in record.get("reasons", [])
            if code not in {"nonpositive_net", "stale_exact_quote", "no_exact_quote"}
        ]
        return [
            f"{direction}: котировка устарела · {record['quote_age_s']:.0f}с",
            f"Последний расчёт {quote_time(record['updated_at'], zone=False)}; прибыль сейчас не оценена.",
            *(["Сейчас: " + block_text(reasons)] if reasons else []),
            *venue_links(record),
        ]
    profit = record["profit_raw"]
    lines = [
        f"{direction}: {'ПРИБЫЛЬ В РАСЧЁТЕ' if profit > 0 else 'УБЫТОК' if profit < 0 else 'НУЛЕВОЙ РЕЗУЛЬТАТ'} "
        f"{signed_profit(profit)} USDC ({Decimal(profit) * 100 / record['cost_raw']:+.2f}%) с запасом slippage",
        f"Объём: {amount(record['cost_raw'])} USDC",
    ]
    if record["direction"] == FORWARD:
        lines.extend(
            [
                f"{record['buy_quantity']} шт за {amount(record['cost_raw'])} → {amount(record['quoted_output_raw'])} USDC "
                f"(мин {amount(record['proceeds_raw'])})",
                f"По quote: {signed_profit(record['quoted_profit_raw'])} USDC после расходов",
            ]
        )
    else:
        lines.append(
            f"{amount(record['cost_raw'])} USDC → мин {record['buy_quantity']} шт; "
            f"продать {record['sell_quantity']} шт за {amount(record['proceeds_raw'])} USDC"
        )
        lines.append("Продажа после зачисления на BP; цена за время перевода может измениться.")
    lines.append(
        f"BP {record['backpack_price']} / {dex_label(record)} {record['dex_price']} USDC/шт · "
        f"BP {quote_time(record['backpack_updated_at'], zone=False)} / "
        f"DEX {quote_time(record['dex_updated_at'], zone=False)} · {record['quote_age_s']:.0f}с"
    )
    lines.append(f"Комиссии учтены; gas {amount(record['gas_raw'])} USDC по модели.")
    if record.get("backpack_source") == "indicative":
        lines.append("BP: публичная limit-котировка; вход требует RFQ на этот объём и проверки баланса.")
    lines.extend(venue_links(record))
    return lines


def session_text(snapshot: dict) -> str | None:
    if snapshot.get("rfq_session_state") != "closed":
        return None
    text = "RFQ-сессия закрыта."
    if snapshot.get("rfq_next_open_at") is not None:
        opening = datetime.fromtimestamp(snapshot["rfq_next_open_at"], ZoneInfo("Europe/Moscow"))
        text += f" Следующее открытие: {opening:%d.%m %H:%M} МСК."
    return text


def compact_quote(row: dict) -> list[str]:
    lines = [
        f"{row['asset']} · {price_direction(row)}",
        f"Чистый спред: {signed_profit(row['profit_raw'])} USDC ({dec(row['edge_bps']) / 100:+.2f}%) "
        "после расходов и запаса slippage",
        f"Объём: {amount(row['cost_raw'])} USDC",
    ]
    if "quoted_output_raw" in row:
        pair = price_pair(row)
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
        if row["direction"] == REVERSE:
            lines.append("Продажа после зачисления на BP; цена за время перевода может измениться.")
    else:
        lines.append(f"Возраст котировки: {row['quote_age_s']:.0f}с")
    if not row.get("candidate") and row.get("candidate_blocks"):
        lines.append("Автоисполнение не разрешено: " + block_text(row["candidate_blocks"]))
    lines.extend(venue_links(row))
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
        names = {
            USDC_MINT: "USDC",
            "So11111111111111111111111111111111111111112": "SOL",
            row.get("mint"): row.get("asset", "акции").removesuffix(".US"),
        }

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
        f"BP стакан ({row.get('book_levels', 0)} уровней)"
        if row.get("backpack_source") == "spot"
        else "BP RFQ (проверена, отменена)"
        if row.get("backpack_quote_confirmed")
        else "BP оценка (FMV/last, без RFQ)"
        if row.get("backpack_model")
        else "BP"
    )
    gas_text = (
        "gasless уже включён" if row.get("dex_gasless") else f"gas {amount(row['gas_raw'])} USDC (модель)"
    )
    if row.get("dex_gasless") and row.get("deposit_gas_raw"):
        gas_text += f"; перевод в BP {amount(row['deposit_gas_raw'])} USDC (модель)"
    if row.get("backpack_deposit_required"):
        lines.append("Продажа после зачисления на BP; цена за время перевода может измениться.")
    lines.append(
        f"{bp_label} {price_pair(row)['backpack_price']} USDC/акция; сессия {row['backpack_session'].removeprefix('US_EQUITIES_')}; "
        f"fee {dec(str(row['backpack_fee_bps'])) / 100:g}%; slippage {row['slippage_bps'] / 100:g}%; "
        f"{gas_text}"
    )
    return lines
