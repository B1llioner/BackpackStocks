from __future__ import annotations

import asyncio
import json
import time
from decimal import ROUND_UP, Decimal
from pathlib import Path

from .backpack import Backpack
from .direct import DirectRaydium
from .direct_execution import DirectSwapGuard
from .jupiter import Jupiter
from .models import STOCK_MINT, TOKEN, USDC_MINT, Cycle, Settings, dec, raw, ui
from .sessions import active_session, validate_quantity
from .solana import SolanaRPC, associated_address, transaction_deltas
from .swap_guard import SwapGuard
from .transfer import USDCTransfer


class LiveBroker:
    def __init__(
        self,
        settings: Settings,
        backpack: Backpack,
        jupiter: Jupiter | DirectRaydium,
        rpc: SolanaRPC,
        guard: SwapGuard | DirectSwapGuard,
        transfer: USDCTransfer,
    ):
        self.settings, self.backpack, self.jupiter, self.rpc, self.guard = (
            settings,
            backpack,
            jupiter,
            rpc,
            guard,
        )
        self.transfer = transfer
        if settings.wallet is None:
            raise ValueError("Live adapter requires a wallet")
        self.wallet = settings.wallet

    async def check_automation_admission(self) -> None:
        if self.settings.live_pilot:
            return
        if self.settings.automation_evidence_file is None:
            raise ValueError("Missing automation evidence file")
        proof = json.loads(Path(self.settings.automation_evidence_file).read_text())
        if proof.get("version") != 1 or proof.get("audit_passed") is not True:
            raise ValueError("Automation admission requires a successful post-pilot audit")
        cycle = Cycle.model_validate(proof["pilot_cycle"])
        if cycle.mode != "live" or cycle.phase != "COMPLETE" or cycle.data.get("wallet") != self.wallet:
            raise ValueError("Pilot evidence is not a completed live cycle for this wallet")
        returned = cycle.data.get("return_receipt")
        if not returned:
            raise ValueError("Pilot evidence must include credited capital return")
        if cycle.rfq_id is None or cycle.quote_id is None:
            raise ValueError("Pilot evidence lacks RFQ/fill identifiers")
        fills = await self.backpack.rfq_fills(cycle.rfq_id)
        actual = [f for f in fills if f["rfqId"] == cycle.rfq_id and f["quoteId"] == cycle.quote_id]
        actual_quantity = sum(raw(f["fillQuantity"]) for f in actual)
        fill_cost = sum(dec(f["fillQuoteQuantity"]) for f in actual)
        cost = int(
            (
                fill_cost * (1 + Decimal(self.settings.policy.backpack_fee_bps) / 10000) * 10**6
            ).to_integral_value(rounding=ROUND_UP)
        )
        if actual_quantity != cycle.quantity_raw or cost != cycle.cost_raw:
            raise ValueError("Pilot RFQ fills/cost do not match evidence/current fee convention")
        withdrawals = await self.backpack.withdrawals(cycle.id)
        if not any(
            w["status"] == "confirmed"
            and w["symbol"] == "WEN.US"
            and w["toAddress"] == self.wallet
            and (w.get("transactionHash") or w.get("identifier")) == cycle.withdrawal_signature
            for w in withdrawals
        ):
            raise ValueError("Pilot Backpack withdrawal is not confirmed for this wallet/signature")
        signatures = [cycle.withdrawal_signature, cycle.sell_signature, returned["signature"]]
        if not all(signatures):
            raise ValueError("Pilot evidence lacks finalized transaction signatures")
        transactions = await asyncio.gather(*(self.rpc.transaction(sig) for sig in signatures))
        if not all(transactions):
            raise ValueError("Pilot transactions are not available with finalized commitment")
        changes = [transaction_deltas(tx, self.wallet) for tx in transactions if tx is not None]
        if (
            changes[0]["stock"] != cycle.delivered_raw
            or changes[1]["stock"] != -cycle.delivered_raw
            or (changes[1]["usdc"] != cycle.proceeds_raw or changes[2]["usdc"] != -cycle.proceeds_raw)
        ):
            raise ValueError("Pilot onchain deltas do not match the evidence")
        deposits = await self.backpack.deposits(returned["signature"])
        if not any(
            d.get("transactionHash") == returned["signature"]
            and d["status"] == "confirmed"
            and d["symbol"] == "USDC"
            and raw(d["quantity"]) == cycle.proceeds_raw
            for d in deposits
        ):
            raise ValueError("Pilot capital return is not confirmed/credited by Backpack")

    async def preflight(self) -> dict:
        await self.check_automation_admission()
        server_time = await self.backpack.server_time()
        if abs(server_time - int(time.time() * 1000)) > 1000:
            raise ValueError("Local clock differs from Backpack by more than one second")
        assets, securities, sessions, holidays = await asyncio.gather(
            self.backpack.assets(),
            self.backpack.securities(),
            self.backpack.sessions(),
            self.backpack.holidays(),
        )
        balances: dict
        account: dict
        mint_result: dict
        wallet: dict
        balances, account, mint_result, wallet = await asyncio.gather(
            self.backpack.balances(),
            self.backpack.account(),
            self.rpc.check_mint(),
            self.rpc.wallet_snapshot(self.wallet),
        )
        target = next(a for a in assets if a["symbol"] == "WEN.US")
        token = next(t for t in target["tokens"] if t["blockchain"] == "Solana")
        p = self.settings.policy
        if (
            token["contractAddress"] != STOCK_MINT
            or token["nativeDecimals"] != 6
            or not token["withdrawEnabled"]
        ):
            raise ValueError("Backpack target mint/capability changed")
        if dec(token["withdrawalFee"]) != dec(p.withdrawal_fee) or dec(token["minimumWithdrawal"]) != dec(
            p.withdrawal_min
        ):
            raise ValueError("Withdrawal fee/minimum changed")
        security = next(s for s in securities if s["asset"] == "WEN.US")
        validate_quantity(security, active_session(sessions, holidays), p.quantity)
        if (
            account.get("autoLend") is not False
            or account.get("autoBorrowSettlements") is not False
            or (account.get("liquidating") is not False)
        ):
            raise ValueError("Auto lending/borrowing must be disabled")
        if wallet["lamports"] < p.min_wallet_lamports:
            raise ValueError("Insufficient wallet gas reserve")
        if raw(balances["USDC"]["available"]) < raw(p.max_cost_usdc):
            raise ValueError("Insufficient Backpack available USDC")
        gas = await self.gas_readiness(wallet)
        p.withdrawal(raw(p.quantity))
        return {
            "usdc": raw(balances["USDC"]["available"]),
            "stock": raw(balances.get("WEN.US", {}).get("available", "0")),
            "wallet": {k: wallet[k] for k in ["stock", "usdc", "lamports", "slot"]},
            "gas": gas,
        }

    async def gas_readiness(self, wallet: dict) -> dict:
        p = self.settings.policy
        rent = await self.rpc.call("getMinimumBalanceForRentExemption", [165, {"commitment": "finalized"}])
        own_missing = wallet["accounts"][2] is None
        rent_count = int(own_missing)
        transaction_count = 1
        if self.settings.rebalance == "each_cycle":
            address = await self.backpack.deposit_address()
            target_ata = associated_address(address["address"], USDC_MINT, TOKEN)
            target = await self.rpc.accounts([target_ata])
            rent_count += int(target["value"][0] is None)
            transaction_count += 1
        budget_lamports = p.max_network_lamports * transaction_count + rent * rent_count
        if wallet["lamports"] < p.min_wallet_lamports + budget_lamports:
            raise ValueError("Wallet cannot retain gas reserve after maximum fees and ATA rent")
        sol_quote = await self.jupiter.quote_pair(
            "So11111111111111111111111111111111111111112", USDC_MINT, budget_lamports
        )
        value = (int(sol_quote["outAmount"]) * 110 + 99) // 100
        if raw(p.gas_budget_usdc) < value:
            raise ValueError("Configured gas budget is below current fee/rent valuation with 10% cushion")
        return {
            "maximum_lamports": budget_lamports,
            "minimum_budget_usdc_raw": value,
            "valuation": f"{self.settings.dex_backend} exact-in quote plus 10%; realised SOL cost remains separately reported",
        }

    async def open_rfq(self, cycle: Cycle) -> dict:
        return await self.backpack.submit_rfq(str(ui(cycle.quantity_raw)), cycle.client_id)

    async def buy_quote(self, cycle: Cycle) -> dict | None:
        results = await self.backpack.rfqs(cycle.rfq_id)
        matching = [r for r in results if r["rfq"]["rfqId"] == cycle.rfq_id]
        if not matching:
            return None
        rfq = matching[0]["rfq"]
        if time.time() * 1000 >= rfq["expiryTime"] - 1500:
            return None
        quotes = matching[0]["quotes"]
        valid = [q for q in quotes if q["status"] == "New" and dec(q["askPrice"]) > 0]
        if not valid:
            return None
        q = min(valid, key=lambda x: dec(x["askPrice"]))
        return {"price": q["askPrice"], "quoteId": q["quoteId"], "source": "executable Backpack RFQ"}

    async def accept(self, cycle: Cycle) -> dict:
        if cycle.rfq_id is None or cycle.quote_id is None:
            raise ValueError("RFQ acceptance IDs are missing")
        # Re-read current best quote immediately before acceptance, after Jupiter's latency.
        current = await self.buy_quote(cycle)
        if (
            not current
            or current["quoteId"] != cycle.quote_id
            or dec(current["price"]) != dec(cycle.data["accepted_price"])
        ):
            raise ValueError("RFQ best quote changed before acceptance")
        return await self.backpack.accept_rfq(cycle.rfq_id, cycle.quote_id)

    async def fill(self, cycle: Cycle) -> dict | None:
        if cycle.rfq_id is None:
            raise ValueError("RFQ fill ID is missing")
        fills = await self.backpack.rfq_fills(cycle.rfq_id)
        fills = [f for f in fills if f["rfqId"] == cycle.rfq_id and f["quoteId"] == cycle.quote_id]
        if not fills:
            return None
        # Never use requested quantity as actual fill quantity.
        quantity = sum(raw(f["fillQuantity"]) for f in fills)
        quote = sum(dec(f["fillQuoteQuantity"]) for f in fills)
        history = await self.backpack.rfq_history()
        order = next((r for r in history if r["rfqId"] == cycle.rfq_id), None)
        if not order or order["status"] not in {"Filled", "Cancelled", "Expired"}:
            return None
        if quantity <= 0 or quantity > cycle.quantity_raw:
            raise ValueError("Invalid actual RFQ fill quantity")
        cost = int(
            (quote * (1 + Decimal(self.settings.policy.backpack_fee_bps) / 10000) * 10**6).to_integral_value(
                rounding=ROUND_UP
            )
        )
        balances = await self.backpack.balances()
        if raw(balances.get("WEN.US", {}).get("available", "0")) != cycle.data["cex_stock_before"] + quantity:
            raise ValueError("RFQ fill and credited Backpack stock balance do not match")
        if raw(balances["USDC"]["available"]) != cycle.data["cex_usdc_before"] - cost:
            raise ValueError("RFQ cost/fee and Backpack USDC balance do not match")
        return {"quantity": quantity, "cost": cost, "source": "RFQ fills plus credited capital"}

    async def withdrawal_baseline(self) -> dict:
        snapshot = await self.rpc.wallet_snapshot(self.wallet)
        return {k: snapshot[k] for k in ["stock", "usdc", "slot", "lamports"]}

    async def withdraw(self, cycle: Cycle, request: int) -> dict:
        # Revalidate capabilities/fees and mint at the irreversible boundary.
        await self.rpc.check_mint()
        assets = await self.backpack.assets()
        token = next(
            t for a in assets if a["symbol"] == "WEN.US" for t in a["tokens"] if t["blockchain"] == "Solana"
        )
        if not token["withdrawEnabled"] or dec(token["withdrawalFee"]) != dec(
            self.settings.policy.withdrawal_fee
        ):
            raise ValueError("Withdrawal capability/fee changed")
        return await self.backpack.withdraw(str(ui(request)), self.wallet, cycle.id)

    async def delivery(self, cycle: Cycle, intent: dict) -> dict | None:
        history = await self.backpack.withdrawals(cycle.id)
        if len(history) > 1:
            raise ValueError("Multiple withdrawals share one intent ID")
        if not history:
            return None
        withdrawal = history[0]
        if withdrawal["symbol"] != "WEN.US" or withdrawal["toAddress"] != self.wallet:
            raise ValueError("Withdrawal asset/recipient mismatch")
        if raw(withdrawal["fee"]) != raw(self.settings.policy.withdrawal_fee):
            raise ValueError("Actual withdrawal fee differs from policy")
        if withdrawal["status"] == "pending":
            return None
        if withdrawal["status"] != "confirmed":
            raise ValueError("Withdrawal requires attention/unknown status")
        signature = withdrawal.get("transactionHash") or withdrawal.get("identifier")
        if not signature:
            return None
        tx = await self.rpc.transaction(signature)
        if not tx:
            return None
        changes = transaction_deltas(tx, self.wallet)
        if changes["stock"] != cycle.delivered_raw:
            raise ValueError("Withdrawal transaction did not deliver the exact expected stock")
        snapshot = await self.rpc.wallet_snapshot(self.wallet)
        if snapshot["stock"] != cycle.data["wallet_before"]["stock"] + cycle.delivered_raw:
            raise ValueError("External wallet inventory change; reconcile before selling")
        balances = await self.backpack.balances()
        if raw(balances.get("WEN.US", {}).get("available", "0")) != cycle.data["cex_stock_before"]:
            raise ValueError("Withdrawal debit differs from expected Backpack balance")
        return {"quantity": changes["stock"], "signature": signature}

    async def prepare_sell(self, cycle: Cycle, minimum: int) -> dict | None:
        snapshot = await self.rpc.wallet_snapshot(self.wallet)
        if snapshot["stock"] != cycle.data["wallet_before"]["stock"] + cycle.delivered_raw:
            raise ValueError("Wallet stock changed outside this cycle")
        q = (
            await self.jupiter.quote(cycle.delivered_raw)
            if isinstance(self.jupiter, DirectRaydium)
            else await self.jupiter.build_v1(
                cycle.delivered_raw, self.wallet, self.settings.policy.slippage_bps
            )
        )
        if int(q["otherAmountThreshold"]) < minimum:
            return None
        return await self.guard.prepare(q, cycle.delivered_raw, minimum)

    async def send_sell(self, cycle: Cycle, prepared: dict) -> dict:
        if isinstance(self.guard, DirectSwapGuard):
            return {"signature": await self.guard.send(prepared)}
        snapshot = await self.rpc.wallet_snapshot(self.wallet)
        if snapshot["stock"] != prepared["wallet_stock"] or snapshot["usdc"] != prepared["wallet_usdc"]:
            raise ValueError("Wallet inventory changed since swap simulation")
        signature = await self.rpc.send(prepared["transaction"], prepared["min_slot"])
        if signature != prepared["signature"]:
            raise ValueError("RPC returned a different transaction signature")
        return {"signature": signature}

    async def sale(self, cycle: Cycle, intent: dict) -> dict | None:
        signature = intent["payload"]["signature"]
        tx = await self.rpc.transaction(signature)
        if not tx:
            status = await self.rpc.signature_status(signature)
            if status and status["err"]:
                raise ValueError("Swap failed onchain; stock remains exposed")
            return None
        changes = transaction_deltas(tx, self.wallet)
        if changes["stock"] != -cycle.delivered_raw or changes["usdc"] < intent["payload"]["minimum"]:
            raise ValueError("Finalized swap debit/credit mismatch")
        return {
            "output": changes["usdc"],
            "signature": signature,
            "fee_lamports": changes["fee"],
            "gas_usdc": raw(self.settings.policy.gas_budget_usdc),
            "gas_model": True,
        }

    async def prepare_return(self, cycle: Cycle) -> dict:
        balances, address, assets = await asyncio.gather(
            self.backpack.balances(), self.backpack.deposit_address(), self.backpack.assets()
        )
        recipient = address["address"]
        usdc = next(
            t for a in assets if a["symbol"] == "USDC" for t in a["tokens"] if t["blockchain"] == "Solana"
        )
        if not usdc["depositEnabled"] or cycle.proceeds_raw < raw(usdc["minimumDeposit"]):
            raise ValueError("USDC deposit disabled or capital return below minimum")
        before = raw(balances["USDC"]["available"])
        if before != cycle.data["cex_usdc_before"] - cycle.cost_raw:
            raise ValueError("Backpack USDC changed before capital return")
        prepared = await self.transfer.prepare(recipient, cycle.proceeds_raw)
        return {**prepared, "cex_before": before}

    async def send_return(self, cycle: Cycle, prepared: dict) -> dict:
        address = await self.backpack.deposit_address()
        if address["address"] != prepared["recipient"]:
            raise ValueError("Backpack deposit address changed before capital return")
        snapshot = await self.rpc.wallet_snapshot(self.wallet)
        if snapshot["usdc"] != prepared["source_before"]:
            raise ValueError("Wallet USDC changed after capital-return simulation")
        signature = await self.rpc.send(prepared["transaction"], prepared["min_slot"])
        if signature != prepared["signature"]:
            raise ValueError("Capital-return signature mismatch")
        return {"signature": signature}

    async def return_receipt(self, cycle: Cycle, intent: dict) -> dict | None:
        prepared = intent["payload"]
        tx = await self.rpc.transaction(prepared["signature"])
        if not tx:
            return None
        change = transaction_deltas(tx, self.wallet)
        credit = transaction_deltas(tx, prepared["recipient"])
        if change["usdc"] != -cycle.proceeds_raw or credit["usdc"] != cycle.proceeds_raw:
            raise ValueError("Capital-return chain debit/credit mismatch")
        deposits = await self.backpack.deposits(prepared["signature"])
        matches = [
            d for d in deposits if d.get("transactionHash") == prepared["signature"] and d["symbol"] == "USDC"
        ]
        if len(matches) > 1:
            raise ValueError("Multiple Backpack credits for one capital-return transaction")
        if not matches or matches[0]["status"] in {"pending", "initiated"}:
            return None
        if matches[0]["status"] != "confirmed" or raw(matches[0]["quantity"]) != cycle.proceeds_raw:
            raise ValueError("USDC deposit requires attention or has a quantity mismatch")
        balances = await self.backpack.balances()
        if raw(balances["USDC"]["available"]) != prepared["cex_before"] + cycle.proceeds_raw:
            raise ValueError("Deposit confirmed but Backpack available capital is not reconciled")
        return {
            "signature": prepared["signature"],
            "quantity": cycle.proceeds_raw,
            "recipient": prepared["recipient"],
            "fee_lamports": change["fee"],
        }

    async def reconcile(self, cycle: Cycle, action: str, intent: dict) -> dict | None:
        if action == "rfq":
            candidates = [r["rfq"] for r in await self.backpack.rfqs()]
            candidates += await self.backpack.rfq_history()
            matches = {r["rfqId"]: r for r in candidates if r.get("clientId") == cycle.client_id}
            if len(matches) > 1:
                raise ValueError("RFQ client ID collision")
            return next(iter(matches.values()), None)
        if action == "buy":
            if cycle.rfq_id is None:
                raise ValueError("RFQ reconciliation ID is missing")
            rows = await self.backpack.rfq_history()
            fills = await self.backpack.rfq_fills(cycle.rfq_id)
            if any(f["quoteId"] == cycle.quote_id for f in fills):
                return {"accepted": True, "reconciled": True}
            row = next((r for r in rows if r["rfqId"] == cycle.rfq_id), None)
            if row and row.get("deferredSettlementQuoteId") == cycle.quote_id:
                return {"accepted": True, "reconciled": True}
        if action == "withdraw":
            rows = await self.backpack.withdrawals(cycle.id)
            if len(rows) > 1:
                raise ValueError("Duplicate withdrawal records")
            return rows[0] if rows else None
        if action in {"sell", "return"}:
            status = await self.rpc.signature_status(intent["payload"]["signature"])
            if status is not None:
                return {"signature": intent["payload"]["signature"], "reconciled": True}
        # Empty history does not prove failure. Never automatically submit again.
        return None
