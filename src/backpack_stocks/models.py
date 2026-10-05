from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

STOCK_MINT = "WENAZ2WyPbmgvUcKfQ8hyMDfBQP9bZ65hsZ5KTFrRGZ"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ATA = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
JUPITER = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"


def dec(value: str | int | Decimal) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError("Use decimal strings or integers, never floats")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("Non-finite amount")
    return result


def raw(value: str | Decimal, decimals: int = 6) -> int:
    scaled = dec(value) * 10**decimals
    if scaled < 0 or scaled != scaled.to_integral_value():
        raise ValueError("Negative amount or precision exceeds mint decimals")
    if scaled >= 2**63:
        raise ValueError("Amount exceeds durable ledger integer range")
    return int(scaled)


def ui(value: int, decimals: int = 6) -> Decimal:
    return Decimal(value) / 10**decimals


def impact_percent(quote: dict) -> Decimal:
    # V2 priceImpact is percentage points; legacy priceImpactPct is a decimal ratio.
    if quote.get("priceImpact") is not None:
        return dec(str(quote["priceImpact"]))
    if quote.get("priceImpactPct") is not None:
        return dec(str(quote["priceImpactPct"])) * 100
    raise ValueError("Price impact is missing; cannot verify impact policy")


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("Nonpositive step")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class Policy(StrictModel):
    quantity: str = "10"
    max_cost_usdc: str = "100"
    min_profit_usdc: str = "0.20"
    min_edge_bps: int = Field(default=30, ge=0, le=10000)
    slippage_bps: int = Field(default=50, ge=1, le=500)
    backpack_fee_bps: int = Field(default=0, ge=0, le=10000)
    withdrawal_fee: str = "0.08"
    withdrawal_min: str = "0.16"
    withdrawal_semantics: Literal["deduct", "additional", "unverified"] = "unverified"
    gas_budget_usdc: str = "0.03"
    max_network_lamports: int = Field(default=300000, ge=5000)
    min_wallet_lamports: int = Field(default=20000000, ge=1000000)
    transfer_timeout_s: int = Field(default=900, ge=1)
    exit_timeout_s: int = Field(default=600, ge=1)
    rfq_timeout_s: int = Field(default=60, ge=1)
    settle_timeout_s: int = Field(default=300, ge=1)
    quote_max_age_s: int = Field(default=8, ge=1)
    max_price_impact_pct: str = "1"
    max_cycles: int = Field(default=1, ge=1, le=1000)
    cooldown_s: int = Field(default=60, ge=0)
    live_fee_verified: bool = False
    live_quantity_mapping_verified: bool = False
    live_rfq_verified: bool = False
    live_withdrawal_verified: bool = False

    @model_validator(mode="after")
    def amounts(self) -> Policy:
        for field in ["quantity", "max_cost_usdc", "withdrawal_min"]:
            if dec(getattr(self, field)) <= 0:
                raise ValueError(f"{field} must be positive")
        for field in ["min_profit_usdc", "withdrawal_fee", "gas_budget_usdc", "max_price_impact_pct"]:
            if dec(getattr(self, field)) < 0:
                raise ValueError(f"{field} cannot be negative")
        raw(self.quantity)
        raw(self.withdrawal_fee)
        return self

    def withdrawal(self, acquired_raw: int) -> tuple[int, int]:
        fee = raw(self.withdrawal_fee)
        if self.withdrawal_semantics == "unverified":
            raise ValueError("Withdrawal fee semantics have not been verified")
        request = acquired_raw if self.withdrawal_semantics == "deduct" else acquired_raw - fee
        delivered = acquired_raw - fee
        if delivered <= 0 or request < raw(self.withdrawal_min):
            raise ValueError("Withdrawal below minimum after fee")
        return request, delivered


class MonitorPolicy(StrictModel):
    enabled: bool = True
    spot_books: bool = True
    spot_refresh_s: float = Field(default=2, ge=1, le=30)
    rfq_validation: bool = True
    rfq_timeout_s: float = Field(default=5, ge=1, le=15)
    rfq_retry_s: float = Field(default=120, ge=10, le=600)
    rfq_cache_s: float = Field(default=60, ge=10, le=300)
    rfq_concurrency: int = Field(default=4, ge=1, le=8)
    quote_backend: Literal["jupiter", "execution"] = "jupiter"
    jupiter_flow: bool = True
    jupiter_profile: Literal["manual", "ultra"] = "manual"
    jupiter_data_dir: str = "data/jupiter"
    jupiter_wallet_env: str = "JUPITER_QUOTE_WALLET"
    notional_usdc: str = "100"
    auto_size: bool = False
    metadata_s: float = Field(default=300, ge=30)
    price_batch_s: float = Field(default=10, ge=2)
    quote_refresh_s: float = Field(default=5, ge=1)
    max_quote_age_s: float = Field(default=8, ge=1)
    exploration_s: float = Field(default=120, ge=5)
    concurrency: int = Field(default=2, ge=1, le=8)

    @model_validator(mode="after")
    def valid(self) -> MonitorPolicy:
        if raw(self.notional_usdc) <= 0:
            raise ValueError("Monitor notional must be positive")
        return self


class DirectPolicy(StrictModel):
    enable_live: bool = False
    via_sol: bool = False
    split_pools: bool = False
    watched_catalog: str | None = None
    catalog: str = "research/pool-pricing/catalog-native.json"
    clmm_worker: str = "local_clmm/worker.cjs"
    max_pools: int = Field(default=4, ge=1, le=12)
    max_tick_arrays: int = Field(default=8, ge=1, le=12)
    concurrency: int = Field(default=2, ge=1, le=4)
    compute_units: int = Field(default=500000, ge=100000, le=1400000)
    priority_micro_lamports: int = Field(default=1000, ge=0, le=10000000)
    journal: str = "state/direct-swaps.sqlite3"


class Settings(StrictModel):
    mode: Literal["paper", "live"] = "paper"
    source: Literal["replay", "public"] = "replay"
    database: str = "state/paper.sqlite3"
    replay_file: str = "examples/replay.json"
    wallet: str | None = None
    dex_backend: Literal["jupiter", "raydium"] = "jupiter"
    direct: DirectPolicy = Field(default_factory=DirectPolicy)
    rpc_url: str = "https://api.mainnet-beta.solana.com"
    jupiter_key_env: str = "JUPITER_API_KEY"
    backpack_seed_env: str = "BACKPACK_ED25519_SEED_BASE64"
    wallet_seed_env: str = "SOLANA_KEYPAIR_BASE64"
    api_token_env: str = "BACKPACK_STOCKS_API_TOKEN"
    poll_s: float = Field(default=3, ge=1)
    jupiter_interval_s: float = Field(default=2.1, ge=0.007)
    jupiter_requests_per_second: float = Field(default=1, gt=0, le=150)
    backpack_interval_s: float = Field(default=0.05, ge=0.031)
    rpc_interval_s: float = Field(default=0.30, ge=0.01)
    paper_delivery_s: int = Field(default=20, ge=0)
    paper_initial_usdc: str = "1000"
    paper_notional_usdc: str | None = None
    log_file: str | None = None
    paper_impact_bps_per_stock: str = "0.1"
    paper_rest_fallback: bool = False
    paper_rfq_markup_bps: int = Field(default=20, ge=0, le=1000)
    backpack_private_reads: bool = False
    paper_auto_start: bool = False
    telegram_digest_s: int = Field(default=0, ge=0, le=3600)
    telegram_spread_alerts: bool = False
    telegram_alert_cooldown_s: int = Field(default=300, ge=30, le=86400)
    enable_live: bool = False
    live_pilot: bool = True
    rebalance: Literal["manual", "each_cycle"] = "manual"
    automation_evidence_file: str | None = None
    policy: Policy = Field(default_factory=Policy)
    monitor: MonitorPolicy = Field(default_factory=MonitorPolicy)

    @model_validator(mode="after")
    def valid(self) -> Settings:
        if self.paper_notional_usdc is not None and (
            self.mode != "paper" or not 0 < dec(self.paper_notional_usdc) <= dec(self.policy.max_cost_usdc)
        ):
            raise ValueError("Paper notional requires paper mode and must fit the cost cap")
        if self.paper_auto_start and self.mode != "paper":
            raise ValueError("Automatic startup is only available for paper")
        if 0 < self.telegram_digest_s < 30:
            raise ValueError("Telegram digest interval must be at least 30 seconds")
        if self.jupiter_interval_s < 1 / self.jupiter_requests_per_second:
            raise ValueError("Jupiter request interval exceeds the configured plan quota")
        from urllib.parse import urlsplit

        public_rpc = {
            "api.mainnet-beta.solana.com",
            "api.mainnet.solana.com",
            "api.devnet.solana.com",
            "api.testnet.solana.com",
        }
        if urlsplit(self.rpc_url).hostname in public_rpc and self.rpc_interval_s < 0.26:
            raise ValueError("Public Solana RPC interval must respect its per-method quota")
        if self.mode == "live":
            p = self.policy
            if not self.enable_live or not self.wallet or self.source != "public":
                raise ValueError("Live requires enable_live, wallet and source=public")
            if not all(
                [
                    p.live_fee_verified,
                    p.live_quantity_mapping_verified,
                    p.live_rfq_verified,
                    p.live_withdrawal_verified,
                ]
            ):
                raise ValueError("Live requires verified RFQ, fee, conversion and withdrawal contracts")
            if p.withdrawal_semantics == "unverified":
                raise ValueError("Live withdrawal semantics missing")
            if self.live_pilot and p.max_cycles != 1:
                raise ValueError("Pilot permits one cycle only")
            if not self.live_pilot and not self.automation_evidence_file:
                raise ValueError("Automated live entry requires audited pilot evidence")
        if not self.rpc_url.startswith("https://"):
            raise ValueError("RPC must use HTTPS")
        raw(self.paper_initial_usdc)
        if dec(self.paper_impact_bps_per_stock) < 0:
            raise ValueError("Invalid paper impact")
        return self


class Phase(StrEnum):
    RFQ_OPEN = "RFQ_OPEN"
    BUY_PENDING = "BUY_PENDING"
    BOUGHT = "BOUGHT"
    WITHDRAW_PENDING = "WITHDRAW_PENDING"
    ARRIVED = "ARRIVED"
    SELL_PENDING = "SELL_PENDING"
    SOLD = "SOLD"
    RETURN_PENDING = "RETURN_PENDING"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    PAUSED = "PAUSED"


class Cycle(StrictModel):
    id: str
    mode: Literal["paper", "live"]
    phase: Phase
    created: float
    updated: float
    quantity_raw: int
    cost_raw: int = 0
    delivered_raw: int = 0
    proceeds_raw: int = 0
    gas_cost_raw: int = 0
    realised_pnl_raw: int = 0
    rfq_id: str | None = None
    quote_id: str | None = None
    client_id: int
    withdrawal_id: str | None = None
    withdrawal_signature: str | None = None
    sell_signature: str | None = None
    deadline: float = 0
    reason: str | None = None
    data: dict = Field(default_factory=dict)


class Observation(StrictModel):
    ask: str
    bid: str
    timestamp: float
    session: str
    source: str


class Spread(StrictModel):
    cost_raw: int
    delivered_raw: int
    output_raw: int
    minimum_output_raw: int
    profit_raw: int
    edge_bps: str
    passes: bool
    source: str


def spread(p: Policy, ask: str, output_raw: int, delivered_raw: int, source: str) -> Spread:
    # Round costs up, proceeds down. Jupiter output is after route fees; do not charge them twice.
    from decimal import ROUND_UP

    if dec(ask) <= 0 or output_raw <= 0 or delivered_raw <= 0:
        raise ValueError("Positive executable prices/quantities required")
    cost = int(
        (dec(p.quantity) * dec(ask) * (1 + Decimal(p.backpack_fee_bps) / 10000) * 10**6).to_integral_value(
            rounding=ROUND_UP
        )
    )
    conservative = output_raw * (10000 - p.slippage_bps) // 10000
    profit = conservative - cost - raw(p.gas_budget_usdc)
    edge = Decimal(profit) * 10000 / cost if cost > 0 else Decimal(0)
    return Spread(
        cost_raw=cost,
        delivered_raw=delivered_raw,
        output_raw=output_raw,
        minimum_output_raw=conservative,
        profit_raw=profit,
        edge_bps=str(edge),
        passes=cost <= raw(p.max_cost_usdc) and profit >= raw(p.min_profit_usdc) and edge >= p.min_edge_bps,
        source=source,
    )
