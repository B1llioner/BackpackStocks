"""Private rotating JSON logs with an explicit public-field allowlist."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOGGER = logging.getLogger("backpack_stocks.operations")
FIELDS = {
    "asset",
    "direction",
    "pool",
    "code",
    "error_type",
    "component",
    "mode",
    "size_usdc",
    "cost_raw",
    "profit_raw",
    "edge_bps",
    "impact_pct",
    "quotes",
    "candidates",
    "blocked",
    "errors",
    "ws_connected",
    "ws_reconnects",
    "cycles",
    "phase",
    "cycle_id",
    "attempt",
    "elapsed_s",
    "responses_429",
    "queued",
    "delivered",
    "status_code",
    "seconds",
    "quantity_raw",
    "qualified",
    "fresh_quotes",
    "rfq_available",
    "side",
    "state",
    "reason",
    "api_code",
    "backpack_price",
    "backpack_source",
    "backpack_market",
    "dex_price",
    "quoted_output_raw",
    "minimum_output_raw",
    "quantity",
    "signature",
}
REASONS = {
    "Pool swaps disabled": "pool_disabled",
    "Pool not open": "pool_not_open",
    "Empty effective reserves": "empty_reserves",
    "No bounded directional CLMM ticks": "no_tick_liquidity",
    "CLMM exhausted price limit": "insufficient_liquidity",
    "Unsafe mint capability/precision": "unsupported_mint",
    "RPC finalized bank is too old": "stale_rpc_bank",
    "Directional tick bitmap changed during refresh": "pool_changed_during_refresh",
    "Pool routing changed during refresh": "pool_changed_during_refresh",
    "Indicative route price impact too high": "price_impact_limit",
    "Direct account owner/executable mismatch": "account_owner_or_missing",
    "Direct account layout/discriminator mismatch": "account_layout_mismatch",
    "Direct vault ownership/control mismatch": "vault_control_mismatch",
    "CLMM config/pool tick spacing mismatch": "tick_spacing_mismatch",
    "Invalid CLMM fees": "invalid_fee_config",
    "Output outside durable amount bounds": "output_bounds",
    "Observation PDA mismatch": "observation_pda_mismatch",
    "Observation belongs to another pool": "observation_pool_mismatch",
    "Tick array PDA/pool mismatch": "tick_array_pda_mismatch",
    "Invalid tick array start": "tick_array_start_invalid",
    "Initialized tick index mismatch": "tick_index_mismatch",
    "Reverse amount below minimum deposit": "below_deposit_minimum",
    "Reverse sale below RFQ session minimum": "below_rfq_minimum",
    "Notional below session minimum quantity": "budget_below_minimum_lot",
    "Below withdrawal minimum after fee": "below_withdrawal_minimum",
    "Missing RFQ sell quantity constraints": "rfq_session_unavailable",
    "No quantity constraints for observed session": "rfq_session_unavailable",
    "No RFQ trading session is open": "rfq_session_unavailable",
    "Reverse deposit/mint mapping unavailable": "reverse_unavailable",
}
MARKET_BLOCKS = {
    "pool_disabled",
    "pool_not_open",
    "empty_reserves",
    "no_tick_liquidity",
    "insufficient_liquidity",
    "insufficient_tick_coverage",
    "unsupported_mint",
    "no_direct_pool",
    "price_impact_limit",
    "pool_changed_during_refresh",
    "below_deposit_minimum",
    "below_rfq_minimum",
    "budget_below_minimum_lot",
    "below_withdrawal_minimum",
    "rfq_session_unavailable",
    "rfq_no_quote",
    "rfq_request_rejected",
    "rfq_insufficient_balance",
    "rfq_invalid_quantity",
    "jupiter_no_route",
    "rfq_auth_required",
    "rfq_budget_changed",
    "reverse_unavailable",
}


def error_code(error: BaseException) -> str:
    code = getattr(error, "code", None)
    if isinstance(code, str) and re.fullmatch(r"[a-z0-9_]{1,64}", code):
        return code
    if isinstance(error, TimeoutError):
        return "timeout"
    return REASONS.get(str(error), type(error).__name__)


def event(name: str, *, level: str = "INFO", **fields) -> None:
    # Raw exception messages, requests, URLs, credentials and transactions are never accepted.
    clean = {
        k: v for k, v in fields.items() if k in FIELDS and isinstance(v, (str, int, float, bool, type(None)))
    }
    LOGGER.log(getattr(logging, level), name, extra={"public_fields": clean})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "event": record.msg,
                **getattr(record, "public_fields", {}),
            },
            allow_nan=False,
        )


class PrivateRotatingHandler(RotatingFileHandler):
    def _open(self):
        fd = os.open(self.baseFilename, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, self.mode, encoding=self.encoding)


def configure(path: str = "state/runtime.jsonl") -> None:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    for handler in LOGGER.handlers[:]:
        LOGGER.removeHandler(handler)
        handler.close()
    handler = PrivateRotatingHandler(target, maxBytes=4_000_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False

    def async_error(loop, context):
        error = context.get("exception")
        event(
            "asyncio_error",
            level="ERROR",
            component="asyncio",
            error_type=type(error).__name__ if error else "CallbackError",
        )

    asyncio.get_running_loop().set_exception_handler(async_error)


class QuoteUnavailable(ValueError):
    def __init__(self, codes: list[str]):
        self.codes = sorted(set(codes))
        self.code = "no_direct_pool" if not codes else "route_unavailable"
        self.expected = not codes or all(code in MARKET_BLOCKS for code in codes)
        super().__init__("No fresh complete direct Raydium route")
