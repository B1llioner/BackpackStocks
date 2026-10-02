"""Quote-only adapter of scripts/jupiter_flow.py. No signing wallet or execution methods."""

from __future__ import annotations

import os
import time
from pathlib import Path

from .jupiter import Jupiter
from .jupiter_proxy import replay_proxy
from .jupiter_replay import JupiterReplay, SwapInput, captured_frontend_key
from .models import Settings


class JupiterFlowQuotes:
    def __init__(self, settings: Settings, *, replay: JupiterReplay | None = None):
        self.policy = settings.monitor
        self.wallet = settings.wallet or os.environ.get(self.policy.jupiter_wallet_env) or None
        self.slippage = settings.policy.slippage_bps
        self.replay = replay or JupiterReplay(
            backend="website",
            website_key=os.environ.get("JUPITER_REPLAY_WEBSITE_KEY")
            or captured_frontend_key(Path(self.policy.jupiter_data_dir)),
            proxy=replay_proxy(),
            transport="curl_cffi",
            interval=settings.jupiter_interval_s,
        )

    @property
    def interval(self) -> float:
        return self.replay.interval

    async def close(self) -> None:
        await self.replay.close()

    def status(self) -> dict:
        return {
            "source": "jupiter_flow",
            "backend": "website",
            "profile": self.policy.jupiter_profile,
            "transport": self.replay.transport,
            "proxied": self.replay.proxied,
            "interval_s": self.interval,
            "responses_429": self.replay.responses_429,
            "queued": self.replay.queued,
            "cooldown_s": max(0, self.replay._defer_until - time.monotonic()),
            "wallet_context": self.wallet is not None,
        }

    async def quote_pair(self, source: str, target: str, amount: int, *, priority: int = 0) -> dict:
        if type(amount) is not int or not 0 < amount < 2**63:
            raise ValueError("Invalid raw quote amount")
        # Zero decimals encodes already-raw units exactly, for every mint's precision.
        request = SwapInput(
            input_mint=source,
            output_mint=target,
            amount=str(amount),
            amount_decimals=0,
            profile=self.policy.jupiter_profile,
            wallet=self.wallet,
            slippage_bps=self.slippage,
        )
        prepared = await self.replay.order(request)
        q = prepared["quote"]
        Jupiter.validate_quote(q, source, target, amount)
        if q.get("swapMode") != "ExactIn" or not 0 < int(q["otherAmountThreshold"]) <= int(q["outAmount"]):
            raise ValueError("Invalid frontend minimum output or swap mode")
        if self.policy.jupiter_profile == "manual" and int(q.get("slippageBps", -1)) != self.slippage:
            raise ValueError("Frontend slippage differs from monitor policy")
        # The order endpoint may return an unsigned transaction. Discard it; the
        # monitor has no execution API, wallet key, signature or transaction journal.
        return {
            **{k: v for k, v in q.items() if k not in {"transaction", "requestId"}},
            "_request_started_at": prepared["requestedAt"],
            "_observed_at": prepared["requestedAt"],
            "quote_source": "jupiter_flow",
            "quote_profile": self.policy.jupiter_profile,
        }
