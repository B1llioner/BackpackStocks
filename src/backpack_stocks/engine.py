from __future__ import annotations

import asyncio
import secrets
import time
import uuid
from decimal import ROUND_UP, Decimal
from typing import TYPE_CHECKING

from .models import Cycle, Phase, Settings, Spread, dec, floor_step, impact_percent, raw, spread, ui
from .observability import error_code, event
from .store import Store
from .transport import RemoteError, UnknownOutcome

if TYPE_CHECKING:
    from .monitor import SpreadMonitor


class Engine:
    def __init__(self, settings: Settings, store: Store, market, broker, clock=time.time):
        self.settings, self.store, self.market, self.broker, self.clock = (
            settings,
            store,
            market,
            broker,
            clock,
        )
        self.lock = asyncio.Lock()
        self.last_spread: Spread | None = None
        self.monitor: SpreadMonitor | None = None
        self.last_scan_quantity_raw = raw(settings.policy.quantity)

    def transition(self, c: Cycle, phase: Phase, **updates):
        c.phase, c.updated = phase, self.clock()
        for name, value in updates.items():
            setattr(c, name, value)
        self.store.save(c)
        self.store.event(c.id, "transition", {"phase": phase, "reason": c.reason})
        event("cycle_transition", cycle_id=c.id, phase=phase, quantity_raw=c.quantity_raw)

    async def mutate(self, c: Cycle, action: str, payload: dict, submit) -> dict | None:
        intent = self.store.intent(c, action, payload)
        if intent["status"] == "ACK":
            return intent["result"]
        if intent["status"] != "PREPARED":
            result = await self.broker.reconcile(c, action, intent)
            if result is not None:
                self.store.intent_status(intent["id"], "ACK", result)
            return result
        # Durable SUBMITTING before sending. A restart here reconciles even if send never happened.
        self.store.intent_status(intent["id"], "SUBMITTING")
        try:
            result = await submit()
        except UnknownOutcome:
            self.store.intent_status(intent["id"], "UNKNOWN")
            self.store.event(c.id, "unknown_outcome", {"action": action})
            return None
        except (RemoteError, ValueError):
            self.store.intent_status(intent["id"], "REJECTED")
            raise ValueError(f"{action} rejected; reconcile intent before resuming") from None
        self.store.intent_status(intent["id"], "ACK", result)
        return result

    async def scan(self):
        p = self.settings.policy
        observation = await self.market.observation()
        if self.settings.paper_notional_usdc is not None:
            budget = min(dec(self.settings.paper_notional_usdc), ui(self.store.balance("backpack", "USDC")))
            if self.settings.source == "public":
                if self.monitor is None or "WEN.US" not in self.monitor.assets:
                    raise ValueError("Waiting for public asset sizing constraints")
                acquired, _ = self.monitor.sizing(self.monitor.assets["WEN.US"], observation, notional=budget)
            else:
                acquired = raw(
                    floor_step(
                        budget / (dec(observation.ask) * (1 + dec(p.backpack_fee_bps) / 10000)),
                        dec("0.000001"),
                    )
                )
            p = p.model_copy(update={"quantity": str(ui(acquired))})
        self.last_scan_quantity_raw = raw(p.quantity)
        _, delivered = p.withdrawal(raw(p.quantity))
        quote = await self.market.output(delivered)
        if abs(impact_percent(quote)) > dec(p.max_price_impact_pct):
            raise ValueError("Indicative route price impact too high")
        self.last_spread = spread(p, observation.ask, int(quote["outAmount"]), delivered, observation.source)
        return self.last_spread

    async def tick(self) -> dict:
        async with self.lock:
            if self.store.get_meta("stopped") == "1":
                return self.status()
            c = self.store.active()
            if c and c.phase == Phase.PAUSED:
                return self.status()
            try:
                if c:
                    await self.advance(c)
                else:
                    await self.start_if_profitable()
            except RemoteError:
                event("engine_read_error", level="WARNING", component="engine", code="transport_read_error")
                self.store.event(c.id if c else None, "read_error", {"type": "RemoteError"})
                if c and self.clock() > c.deadline:
                    self.pause(c, "Remote read failed beyond deadline")
            except ValueError as error:
                event(
                    "engine_entry_blocked",
                    component="engine",
                    code=error_code(error),
                    error_type=type(error).__name__,
                )
                # Our own validation errors are safe to store; no transport payloads or secrets.
                if c:
                    self.pause(c, str(error))
                else:
                    self.store.event(None, "entry_blocked", {"reason": str(error)})
            except (KeyError, TypeError, ArithmeticError, IndexError) as error:
                event(
                    "engine_contract_error",
                    level="ERROR",
                    component="engine",
                    error_type=type(error).__name__,
                )
                reason = f"Unexpected API contract ({type(error).__name__}); execution paused"
                if c:
                    self.pause(c, reason)
                else:
                    self.store.event(None, "entry_blocked", {"reason": reason})
            return self.status()

    async def start_if_profitable(self, *, pilot: bool = False):
        p = self.settings.policy
        if self.settings.mode == "live" and self.settings.live_pilot and not pilot:
            return
        if len(self.store.cycles()) >= p.max_cycles:
            return
        last = float(self.store.get_meta("last_entry") or "0")
        if self.clock() - last < p.cooldown_s:
            return
        # Live scouts an AwaitAccept RFQ, then tests the executable quote before reserving/buying.
        # Provider-derived last/mid/ask is never used as an executable Backpack entry price.
        signal = None if self.settings.mode == "live" else await self.scan()
        if signal is not None and not signal.passes:
            return
        readiness = await self.broker.preflight()
        if self.settings.mode == "live":
            if not self.store.get_meta("live_initialized"):
                with self.store.atomic():
                    self.store.credit("backpack", "USDC", "available", readiness["usdc"])
                    self.store.credit("backpack", "WEN.US", "available", readiness["stock"])
                    self.store.set_meta("live_initialized", "1")
            if readiness["usdc"] != self.store.balance("backpack", "USDC") or readiness[
                "stock"
            ] != self.store.balance("backpack", "WEN.US"):
                raise ValueError("External Backpack balance change requires reconciliation")
        required = signal.cost_raw if signal is not None else raw(p.max_cost_usdc)
        if self.store.balance("backpack", "USDC") < required:
            raise ValueError("Tracked Backpack capital depleted; rebalance required")
        signal_data = (
            signal.model_dump()
            if signal is not None
            else {"source": "AwaitAccept RFQ scout; no purchase until executable spread passes"}
        )
        client_id = secrets.randbits(32)
        used = {c["client_id"] for c in self.store.cycles()}
        while client_id in used:
            client_id = secrets.randbits(32)
        now = self.clock()
        c = Cycle(
            id=uuid.uuid4().hex,
            mode=self.settings.mode,
            phase=Phase.RFQ_OPEN,
            created=now,
            updated=now,
            quantity_raw=self.last_scan_quantity_raw if signal is not None else raw(p.quantity),
            client_id=client_id,
            deadline=now + p.rfq_timeout_s,
            data={
                "signal": signal_data,
                "cex_stock_before": self.store.balance("backpack", "WEN.US"),
                "cex_usdc_before": self.store.balance("backpack", "USDC"),
                "wallet": self.settings.wallet,
                "gas_readiness": readiness.get("gas"),
            },
        )
        with self.store.atomic():
            self.store.save(c)
            self.store.set_meta("last_entry", str(now))
            self.store.event(c.id, "entry_signal", signal_data)

    async def advance(self, c: Cycle):
        p, now = self.settings.policy, self.clock()
        if c.phase == Phase.RFQ_OPEN:
            rfq = await self.mutate(
                c,
                "rfq",
                {"quantity": c.quantity_raw, "client_id": c.client_id},
                lambda: self.broker.open_rfq(c),
            )
            if rfq is None:
                if now > c.deadline:
                    self.pause(c, "Unknown RFQ submission; do not resubmit")
                return
            c.rfq_id = rfq["rfqId"]
            self.store.save(c)
            # Wait for an unaccepted RFQ to expire; no cancel mutation needed.
            if now > c.deadline or now * 1000 >= rfq.get("expiryTime", c.deadline * 1000):
                self.transition(c, Phase.CANCELLED, reason="Unaccepted RFQ expired")
                return
            quote = await self.broker.buy_quote(c)
            if quote is None:
                return
            _, delivered = p.withdrawal(c.quantity_raw)
            dex = await self.market.output(delivered)
            if abs(impact_percent(dex)) > dec(p.max_price_impact_pct):
                return
            cycle_policy = p.model_copy(update={"quantity": str(ui(c.quantity_raw))})
            signal = spread(cycle_policy, quote["price"], int(dex["outAmount"]), delivered, quote["source"])
            self.last_spread = signal
            if not signal.passes:
                return
            c.quote_id = quote["quoteId"]
            c.data.update({"reserved_cost": signal.cost_raw, "accepted_price": quote["price"]})
            with self.store.atomic():
                self.store.move("USDC", signal.cost_raw, ("backpack", "available"), ("backpack", "reserved"))
                self.transition(c, Phase.BUY_PENDING, deadline=now + p.settle_timeout_s)
        elif c.phase == Phase.BUY_PENDING:
            accepted = await self.mutate(
                c, "buy", {"rfq_id": c.rfq_id, "quote_id": c.quote_id}, lambda: self.broker.accept(c)
            )
            if accepted is None:
                if now > c.deadline:
                    self.pause(c, "Unknown RFQ acceptance; funds may be locked")
                return
            fill = await self.broker.fill(c)
            if fill is None:
                if now > c.deadline:
                    self.pause(c, "RFQ accepted but no settled/credited fill before deadline")
                return
            if (
                fill["cost"] > c.data["reserved_cost"]
                or fill["quantity"] > c.quantity_raw
                or fill["quantity"] <= 0
            ):
                raise ValueError("Fill exceeds approved amount/cost")
            with self.store.atomic():
                self.store.move("USDC", fill["cost"], ("backpack", "reserved"), ("cost", "spent"))
                refund = c.data["reserved_cost"] - fill["cost"]
                if refund:
                    self.store.move("USDC", refund, ("backpack", "reserved"), ("backpack", "available"))
                self.store.credit("backpack", "WEN.US", "available", fill["quantity"])
                c.quantity_raw, c.cost_raw = fill["quantity"], fill["cost"]
                self.transition(c, Phase.BOUGHT)
        elif c.phase == Phase.BOUGHT:
            request, delivered = p.withdrawal(c.quantity_raw)
            baseline = await self.broker.withdrawal_baseline()
            c.delivered_raw = delivered
            c.data.update({"wallet_before": baseline, "withdraw_request": request, "withdraw_started": now})
            with self.store.atomic():
                self.store.move("WEN.US", c.quantity_raw, ("backpack", "available"), ("transfer", "pending"))
                self.transition(c, Phase.WITHDRAW_PENDING, deadline=now + p.transfer_timeout_s)
        elif c.phase == Phase.WITHDRAW_PENDING:
            result = await self.mutate(
                c,
                "withdraw",
                {"quantity": c.data["withdraw_request"]},
                lambda: self.broker.withdraw(c, c.data["withdraw_request"]),
            )
            if result is None:
                if now > c.deadline:
                    self.pause(c, "Unknown withdrawal; do not repeat")
                return
            intent = self.store.require_intent(c.id, "withdraw")
            receipt = await self.broker.delivery(c, intent)
            if receipt is None:
                if now > c.deadline:
                    self.pause(c, "Withdrawal not delivered/finalized before deadline")
                return
            if receipt["quantity"] != c.delivered_raw:
                raise ValueError("Delivery amount differs from approved expectation")
            with self.store.atomic():
                self.store.move("WEN.US", c.delivered_raw, ("transfer", "pending"), ("wallet", "available"))
                self.store.move(
                    "WEN.US", c.quantity_raw - c.delivered_raw, ("transfer", "pending"), ("fees", "spent")
                )
                self.transition(
                    c,
                    Phase.ARRIVED,
                    withdrawal_signature=receipt["signature"],
                    deadline=now + p.exit_timeout_s,
                )
        elif c.phase == Phase.ARRIVED:
            # Delivery risk is real: reacquire an executable exit quote after arrival.
            margin = max(
                raw(p.min_profit_usdc),
                int((Decimal(c.cost_raw) * p.min_edge_bps / 10000).to_integral_value(rounding=ROUND_UP)),
            )
            minimum = c.cost_raw + raw(p.gas_budget_usdc) + margin
            existing = self.store.find_intent(c.id, "sell")
            prepared = existing["payload"] if existing else await self.broker.prepare_sell(c, minimum)
            if prepared is None:
                if now > c.deadline:
                    self.pause(c, "Exit spread disappeared; stock is held in wallet")
                return
            # Persist signed bytes and signature before any RPC submission.
            self.store.intent(c, "sell", prepared)
            with self.store.atomic():
                self.store.move("WEN.US", c.delivered_raw, ("wallet", "available"), ("wallet", "reserved"))
                self.transition(
                    c,
                    Phase.SELL_PENDING,
                    sell_signature=prepared["signature"],
                    deadline=now + p.settle_timeout_s,
                )
        elif c.phase == Phase.SELL_PENDING:
            intent = self.store.require_intent(c.id, "sell")
            result = await self.mutate(
                c, "sell", intent["payload"], lambda: self.broker.send_sell(c, intent["payload"])
            )
            if result is None:
                if now > c.deadline:
                    self.pause(c, "Unknown swap submission; do not sign/send a replacement")
                return
            receipt = await self.broker.sale(c, self.store.find_intent(c.id, "sell"))
            if receipt is None:
                if now > c.deadline:
                    self.pause(c, "Swap not finalized; reconcile recorded signature")
                return
            c.proceeds_raw, c.gas_cost_raw = receipt["output"], receipt["gas_usdc"]
            c.realised_pnl_raw = c.proceeds_raw - c.cost_raw - c.gas_cost_raw
            c.data["receipt"] = receipt
            c.data["gas_cost_is_model"] = receipt.get("gas_model", False)
            with self.store.atomic():
                self.store.move("WEN.US", c.delivered_raw, ("wallet", "reserved"), ("dex", "sold"))
                self.store.credit("wallet", "USDC", "available", c.proceeds_raw)
                self.store.set_meta(
                    "paper_sold_raw", str(int(self.store.get_meta("paper_sold_raw") or "0") + c.delivered_raw)
                )
                self.transition(c, Phase.SOLD if self.settings.rebalance == "each_cycle" else Phase.COMPLETE)
        elif c.phase == Phase.SOLD:
            existing = self.store.find_intent(c.id, "return")
            prepared = existing["payload"] if existing else await self.broker.prepare_return(c)
            self.store.intent(c, "return", prepared)
            with self.store.atomic():
                self.store.move("USDC", c.proceeds_raw, ("wallet", "available"), ("transfer", "pending"))
                self.transition(c, Phase.RETURN_PENDING, deadline=now + p.transfer_timeout_s)
        elif c.phase == Phase.RETURN_PENDING:
            intent = self.store.require_intent(c.id, "return")
            result = await self.mutate(
                c, "return", intent["payload"], lambda: self.broker.send_return(c, intent["payload"])
            )
            if result is None:
                if now > c.deadline:
                    self.pause(c, "Unknown capital-return submission; do not repeat")
                return
            receipt = await self.broker.return_receipt(c, self.store.find_intent(c.id, "return"))
            if receipt is None:
                if now > c.deadline:
                    self.pause(c, "USDC return not credited on Backpack before deadline")
                return
            if receipt["quantity"] != c.proceeds_raw:
                raise ValueError("Capital-return credit differs from expected USDC")
            with self.store.atomic():
                self.store.move("USDC", c.proceeds_raw, ("transfer", "pending"), ("backpack", "available"))
                c.data["return_receipt"] = receipt
                self.transition(c, Phase.COMPLETE)

    def pause(self, c: Cycle, reason: str):
        c.data["resume_phase"] = c.phase.value
        self.transition(c, Phase.PAUSED, reason=reason)

    async def stop(self):
        async with self.lock:
            self.store.set_meta("stopped", "1")
            self.store.event(None, "stopped", {})
        return self.status()

    async def resume(self):
        async with self.lock:
            c = self.store.active()
            if c and c.phase == Phase.PAUSED:
                phase = Phase(c.data["resume_phase"])
                timeout = (
                    self.settings.policy.exit_timeout_s
                    if phase == Phase.ARRIVED
                    else (self.settings.policy.settle_timeout_s)
                )
                self.transition(c, phase, reason=None, deadline=self.clock() + timeout)
            self.store.set_meta("stopped", "0")
        return self.status()

    def status(self) -> dict:
        active = self.store.active()
        return {
            "mode": self.settings.mode,
            "stopped": self.store.get_meta("stopped") == "1",
            "active": active.model_dump(mode="json") if active else None,
            "cycles": self.store.cycles(),
            "ledger": self.store.ledger(),
            "spread": self.last_spread.model_dump() if self.last_spread else None,
        }
