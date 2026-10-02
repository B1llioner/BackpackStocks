from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from .backpack import Backpack
from .direct import DirectRaydium
from .jupiter import Jupiter
from .models import Observation, Settings, dec, impact_percent, ui
from .store import Store

if TYPE_CHECKING:
    from .monitor import SpreadMonitor


class ReplayMarket:
    def __init__(self, settings: Settings, store: Store, clock=time.time):
        self.settings, self.store, self.clock = settings, store, clock
        document = json.loads(Path(settings.replay_file).read_text())
        self.frames = document["frames"]
        if not self.frames or any(dec(f["ask"]) <= 0 or dec(f["dex_bid"]) <= 0 for f in self.frames):
            raise ValueError("Replay must have positive ask/dex_bid frames")
        offsets = [f["offset_s"] for f in self.frames]
        if offsets != sorted(offsets) or offsets[0] != 0:
            raise ValueError("Replay frames must start at zero and be sorted")
        self.origin = float(store.get_meta("replay_origin") or self.clock())
        store.set_meta("replay_origin", str(self.origin))

    def frame(self) -> dict:
        elapsed = self.clock() - self.origin
        return next((f for f in reversed(self.frames) if f["offset_s"] <= elapsed), self.frames[0])

    async def observation(self) -> Observation:
        f = self.frame()
        return Observation(
            ask=f["ask"],
            bid=f.get("bid", f["ask"]),
            timestamp=self.clock(),
            session=f.get("session", "REPLAY"),
            source="synthetic replay; modeled RFQ",
        )

    async def output(self, amount: int) -> dict:
        f = self.frame()
        sold = int(self.store.get_meta("paper_sold_raw") or "0")
        impact = dec(self.settings.paper_impact_bps_per_stock) * (ui(sold) + ui(amount) / 2) / 10000
        value = int(ui(amount) * dec(f["dex_bid"]) * max(Decimal(0), 1 - impact) * 10**6)
        return {
            "outAmount": str(value),
            "priceImpact": str(impact * 100),
            "source": "synthetic replay with cumulative own impact",
        }

    async def close(self) -> None:
        pass


class PublicMarket:
    def __init__(
        self, settings: Settings, store: Store, backpack: Backpack, jupiter: Jupiter | DirectRaydium
    ):
        self.settings, self.store, self.backpack, self.jupiter = settings, store, backpack, jupiter
        self.latest: Observation | None = None
        self._task: asyncio.Task | None = None
        self.monitor: SpreadMonitor | None = None

    async def start(self) -> None:
        async def receive():
            async for observation in self.backpack.stock_prices():
                self.latest = observation

        self._task = asyncio.create_task(receive())

    async def observation(self) -> Observation:
        if self._task is not None and self._task.done():
            raise ValueError("Public stock feed receiver stopped")
        if self.latest is None:
            if self.settings.mode == "paper" and self.settings.paper_rest_fallback:
                return await self.rest_model()
            raise ValueError("Waiting for a public stock bid/ask")
        age = time.time() - self.latest.timestamp
        if age > self.settings.policy.quote_max_age_s or age < -2:
            if self.settings.mode == "paper" and self.settings.paper_rest_fallback:
                return await self.rest_model()
            raise ValueError("Stock quote stale or future-dated")
        return self.latest

    async def rest_model(self) -> Observation:
        cached = self.monitor.external.get("WEN.US") if self.monitor else None
        cache_age = time.time() - self.monitor.external_at if self.monitor else float("inf")
        ticker = (
            {"lastPrice": cached}
            if cached is not None and 0 <= cache_age <= self.settings.policy.quote_max_age_s
            else await self.backpack.ticker("WEN.US_USDC")
        )
        price = dec(ticker["lastPrice"])
        if price <= 0:
            raise ValueError("Invalid external last price")
        markup = Decimal(self.settings.paper_rfq_markup_bps) / 10000
        return Observation(
            ask=str(price * (1 + markup)),
            bid=str(price * (1 - markup)),
            timestamp=time.time(),
            session="MODEL",
            source="REST external last price + modeled RFQ markup; source age unknown",
        )

    async def output(self, amount: int) -> dict:
        q = (
            await self.jupiter.quote_v1(amount, self.settings.policy.slippage_bps)
            if (self.settings.mode == "live")
            else await self.jupiter.quote(amount)
        )
        value = int(q["outAmount"])
        model_impact = impact_percent(q)
        if self.settings.mode == "paper":
            sold = int(self.store.get_meta("paper_sold_raw") or "0")
            impact = dec(self.settings.paper_impact_bps_per_stock) * (ui(sold) + ui(amount) / 2) / 10000
            value = int(Decimal(value) * max(Decimal(0), 1 - impact))
            model_impact += impact * 100
        return {
            **q,
            "outAmount": str(value),
            "priceImpact": str(model_impact),
            "source": q.get("source", "Jupiter executable-size quote-only"),
        }

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class PaperBroker:
    def __init__(self, settings: Settings, market, clock=time.time):
        self.settings, self.market, self.clock = settings, market, clock

    async def preflight(self) -> dict:
        return {"source": "modeled paper", "ready": True}

    async def open_rfq(self, cycle) -> dict:
        return {
            "rfqId": cycle.id,
            "expiryTime": int((self.clock() + self.settings.policy.rfq_timeout_s) * 1000),
        }

    async def buy_quote(self, cycle) -> dict | None:
        obs = await self.market.observation()
        return {"price": obs.ask, "quoteId": cycle.id + "-q", "source": obs.source}

    async def accept(self, cycle) -> dict:
        return {"accepted": True}

    async def fill(self, cycle) -> dict:
        return {
            "quantity": cycle.quantity_raw,
            "cost": cycle.data["reserved_cost"],
            "source": "modeled paper fill",
        }

    async def withdrawal_baseline(self) -> dict:
        return {}

    async def withdraw(self, cycle, request: int) -> dict:
        return {"id": cycle.id, "status": "pending", "arrival": self.clock() + self.settings.paper_delivery_s}

    async def delivery(self, cycle, intent) -> dict | None:
        if self.clock() < intent["result"]["arrival"]:
            return None
        return {"quantity": cycle.delivered_raw, "signature": "paper-delivery-" + cycle.id}

    async def prepare_sell(self, cycle, minimum: int) -> dict | None:
        q = await self.market.output(cycle.delivered_raw)
        output = int(q["outAmount"]) * (10000 - self.settings.policy.slippage_bps) // 10000
        if output < minimum:
            return None
        return {"minimum": minimum, "output": output, "signature": "paper-swap-" + cycle.id}

    async def send_sell(self, cycle, prepared) -> dict:
        return {"signature": prepared["signature"], "output": prepared["output"]}

    async def sale(self, cycle, intent) -> dict:
        return {
            "output": intent["result"]["output"],
            "signature": intent["result"]["signature"],
            "gas_usdc": int(dec(self.settings.policy.gas_budget_usdc) * 10**6),
            "gas_model": True,
        }

    async def prepare_return(self, cycle) -> dict:
        return {
            "signature": "paper-return-" + cycle.id,
            "quantity": cycle.proceeds_raw,
            "arrival": self.clock() + self.settings.paper_delivery_s,
        }

    async def send_return(self, cycle, prepared) -> dict:
        return {"signature": prepared["signature"]}

    async def return_receipt(self, cycle, intent) -> dict | None:
        if self.clock() < intent["payload"]["arrival"]:
            return None
        return {"quantity": cycle.proceeds_raw, "signature": intent["payload"]["signature"]}

    async def reconcile(self, cycle, action: str, intent: dict) -> dict | None:
        # Paper has no external effects: deterministic reconstruction of a submitted local action.
        if action == "rfq":
            return await self.open_rfq(cycle)
        if action == "buy":
            return await self.accept(cycle)
        if action == "withdraw":
            return {
                "id": cycle.id,
                "status": "pending",
                "arrival": cycle.data["withdraw_started"] + self.settings.paper_delivery_s,
            }
        if action == "sell":
            return {"signature": intent["payload"]["signature"], "output": intent["payload"]["output"]}
        if action == "return":
            return {"signature": intent["payload"]["signature"]}
        return None
