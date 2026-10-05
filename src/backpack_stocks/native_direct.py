"""Guarded single-pool native SOL swaps with an atomic temporary WSOL account."""

from __future__ import annotations

import base64
import time

from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import CreateAccountParams, create_account
from solders.transaction import VersionedTransaction

from .direct import WSOL, direct_instructions
from .direct_execution import DirectJournal, DirectSwapGuard, WalletLease, immutable_control
from .models import TOKEN, TOKEN_2022, dec, impact_percent
from .solana import associated_address, decode_token, transaction_deltas
from .swap_guard import SYSTEM


def closed_account(account: dict | None) -> bool:
    # simulateTransaction can return the deallocated account before the bank
    # purges it. A live token account, even with zero tokens, is never closed.
    return account is None or (
        account.get("lamports") == 0
        and account.get("owner") == SYSTEM
        and account.get("data") == ["", "base64"]
        and not account.get("executable")
    )


def token_instruction(
    data: bytes, accounts: list[tuple[str, bool, bool]], program: str = TOKEN
) -> Instruction:
    return Instruction(
        Pubkey.from_string(program),
        data,
        [AccountMeta(Pubkey.from_string(a), signer, writable) for a, signer, writable in accounts],
    )


def native_instructions(
    q: dict, wallet: str, temporary: str, rent: int, minimum: int, settings, *, close_stock: bool = False
) -> list[Instruction]:
    if WSOL not in {q["inputMint"], q["outputMint"]}:
        raise ValueError("Native swap requires a SOL pair")
    buy = q["inputMint"] == WSOL
    state = q["_direct"]["state"]
    side = "A" if state["mintA"] == WSOL else "B"
    if state["tokenProgram" + side] != TOKEN or state["decimals" + side] != 9:
        raise ValueError("Native mint program/precision mismatch")
    canonical = associated_address(wallet, WSOL, TOKEN)
    normal = direct_instructions(q, wallet, minimum, settings)
    swap = normal[-1]
    swap = Instruction(
        swap.program_id,
        swap.data,
        [
            AccountMeta(
                Pubkey.from_string(temporary) if str(a.pubkey) == canonical else a.pubkey,
                a.is_signer,
                a.is_writable,
            )
            for a in swap.accounts
        ],
    )
    result = [
        *normal[:2],
        create_account(
            CreateAccountParams(
                from_pubkey=Pubkey.from_string(wallet),
                to_pubkey=Pubkey.from_string(temporary),
                lamports=rent + (int(q["inAmount"]) if buy else 0),
                space=165,
                owner=Pubkey.from_string(TOKEN),
            )
        ),
        token_instruction(
            b"\x12" + bytes(Pubkey.from_string(wallet)), [(temporary, False, True), (WSOL, False, False)]
        ),
        token_instruction(b"\x11", [(temporary, False, True)]),
    ]
    if buy:
        result.append(normal[2])  # Only the stock ATA; native account is temporary.
    result.extend(
        [
            swap,
            token_instruction(
                b"\x09", [(temporary, False, True), (wallet, False, True), (wallet, True, False)]
            ),
        ]
    )
    if close_stock:
        if buy:
            raise ValueError("Cannot close purchased stock account")
        mint = q["inputMint"]
        side = "A" if state["mintA"] == mint else "B"
        program = state["tokenProgram" + side]
        account = associated_address(wallet, mint, program)
        result.append(
            token_instruction(
                b"\x09", [(account, False, True), (wallet, False, True), (wallet, True, False)], program
            )
        )
    return result


def native_simulation(
    snapshot: dict,
    value: dict,
    *,
    buy: bool,
    amount: int,
    minimum: int,
    wallet: str,
    mint: str,
    program: str,
    fee: int,
    close_stock: bool,
    settings,
) -> tuple[int, int]:
    rows = value.get("accounts")
    if value.get("err") is not None or not isinstance(rows, list) or len(rows) != 3:
        raise ValueError("Native direct simulation failed")
    payer, stock, temporary = rows
    old_payer, old_stock, _ = snapshot["accounts"]
    if (
        not payer
        or payer["owner"] != SYSTEM
        or payer.get("executable")
        or payer.get("data") != old_payer.get("data")
        or not closed_account(temporary)
        or payer["lamports"] < settings.policy.min_wallet_lamports
    ):
        raise ValueError("Native direct payer/cleanup mismatch")
    old = decode_token(old_stock)
    rent_delta = 0
    if close_stock:
        if buy or old["amount"] != amount or not closed_account(stock):
            raise ValueError("Native direct closed stock/input mismatch")
        rent_delta = -old_stock["lamports"]
        stock_change = -amount
    else:
        current = decode_token(stock)
        if (
            not stock
            or current["owner"] != wallet
            or current["mint"] != mint
            or stock["owner"] != program
            or current["delegate"]
            or current["close_authority"]
        ):
            raise ValueError("Native direct token control mismatch")
        if old_stock is not None and immutable_control(stock) != immutable_control(old_stock):
            raise ValueError("Native direct token control changed")
        if old_stock is None:
            if not buy:
                raise ValueError("Native direct missing sale inventory")
            rent_delta = stock["lamports"]
        stock_change = current["amount"] - old["amount"]
    sol_change = payer["lamports"] - old_payer["lamports"]
    pool_sol = sol_change + fee + rent_delta
    if buy:
        if pool_sol != -amount or stock_change < minimum:
            raise ValueError("Native direct simulated debit/credit mismatch")
        output = stock_change
    else:
        if stock_change != -amount or pool_sol < minimum:
            raise ValueError("Native direct simulated debit/credit mismatch")
        output = pool_sol
    return output, rent_delta


class NativeDirectGuard(DirectSwapGuard):
    def __init__(self, *args, close_stock: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.close_stock = close_stock

    async def prepare(self, q: dict, amount: int, minimum: int, *, mint: str | None = None) -> dict:
        settings, rpc = self.settings, self.rpc
        if (
            not settings.direct.enable_live
            or q.get("direct_execution_supported") is False
            or int(q["inAmount"]) != amount
            or WSOL not in {q["inputMint"], q["outputMint"]}
            or abs(impact_percent(q)) > dec(settings.policy.max_price_impact_pct)
        ):
            raise ValueError("Native direct quote outside explicit policy")
        if not 0 <= time.time() - q["_observed_at"] <= settings.policy.quote_max_age_s:
            raise ValueError("Native direct quote expired")
        wallet = str(self._key.pubkey())
        buy = q["inputMint"] == WSOL
        stock_mint = q["outputMint"] if buy else q["inputMint"]
        state = q["_direct"]["state"]
        side = "A" if state["mintA"] == stock_mint else "B"
        program = state["tokenProgram" + side]
        if program not in {TOKEN, TOKEN_2022} or state["decimals" + side] != 6:
            raise ValueError("Native direct stock program/precision mismatch")
        temporary_key = Keypair()
        temporary = str(temporary_key.pubkey())
        refs = [wallet, associated_address(wallet, stock_mint, program), temporary]
        response = await rpc.accounts(refs, min_slot=q["contextSlot"])
        snapshot = {"accounts": response["value"], "addresses": refs}
        payer, stock, unused = response["value"]
        old = decode_token(stock)
        if not payer or payer["owner"] != SYSTEM or payer.get("executable") or unused is not None:
            raise ValueError("Native direct wallet/temporary ownership mismatch")
        if stock and (
            old["owner"] != wallet
            or old["mint"] != stock_mint
            or old["program"] != program
            or old["delegate"]
            or old["close_authority"]
        ):
            raise ValueError("Native direct wallet stock control mismatch")
        if not buy and old["amount"] < amount:
            raise ValueError("Native direct insufficient stock inventory")
        if self.close_stock and (buy or old["amount"] != amount):
            raise ValueError("Native direct close would affect other inventory")
        rent = await rpc.call("getMinimumBalanceForRentExemption", [165, {"commitment": "finalized"}])
        if type(rent) is not int or not 0 < rent <= 10_000_000:
            raise ValueError("Native direct rent outside bounds")
        if payer["lamports"] < (amount if buy else 0) + rent + settings.policy.min_wallet_lamports:
            raise ValueError("Native direct insufficient SOL reserve")
        minimum = max(minimum, int(q["otherAmountThreshold"]))
        instructions = native_instructions(
            q, wallet, temporary, rent, minimum, settings, close_stock=self.close_stock
        )
        latest = await rpc.call(
            "getLatestBlockhash", [{"commitment": "finalized", "minContextSlot": response["context"]["slot"]}]
        )
        message = MessageV0.try_compile(
            self._key.pubkey(), instructions, [], Hash.from_string(latest["value"]["blockhash"])
        )
        fee = (
            await rpc.call(
                "getFeeForMessage", [base64.b64encode(bytes(message)).decode(), {"commitment": "finalized"}]
            )
        )["value"]
        if type(fee) is not int or not 0 <= fee <= settings.policy.max_network_lamports:
            raise ValueError("Native direct network fee exceeds policy")
        unsigned = VersionedTransaction.populate(message, [Signature.default(), Signature.default()])
        if len(bytes(unsigned)) > 1232:
            raise ValueError("Native direct transaction exceeds packet limit")
        min_slot = max(q["contextSlot"], response["context"]["slot"])
        result = await rpc.simulate(base64.b64encode(bytes(unsigned)).decode(), refs, min_slot)
        output, rent_delta = native_simulation(
            snapshot,
            result["value"],
            buy=buy,
            amount=amount,
            minimum=minimum,
            wallet=wallet,
            mint=stock_mint,
            program=program,
            fee=fee,
            close_stock=self.close_stock,
            settings=settings,
        )
        if abs(output - int(q["outAmount"])) * 10000 > int(q["outAmount"]) * settings.policy.slippage_bps:
            raise ValueError("Native direct quote/simulation diverge")
        if not 0 <= time.time() - q["_observed_at"] <= settings.policy.quote_max_age_s:
            raise ValueError("Native direct quote expired before signing")
        signed = VersionedTransaction(message, [self._key, temporary_key])
        return {
            "transaction": base64.b64encode(bytes(signed)).decode(),
            "signature": str(signed.signatures[0]),
            "wallet": wallet,
            "pool": q["pool"],
            "min_slot": min_slot,
            "last_valid_height": latest["value"]["lastValidBlockHeight"],
            "quote_at": q["_observed_at"],
            "addresses": refs,
            "input_mint": q["inputMint"],
            "output_mint": q["outputMint"],
            "input": amount,
            "minimum": minimum,
            "native_buy": buy,
            "stock_mint": stock_mint,
            "stock_before": old["amount"],
            "sol_before": payer["lamports"],
            "rent_delta": rent_delta,
            "fee_lamports": fee,
        }

    async def send(self, prepared: dict) -> str:
        if prepared["wallet"] != str(self._key.pubkey()):
            raise ValueError("Native prepared wallet mismatch")
        current = await self.rpc.accounts(prepared["addresses"], min_slot=prepared["min_slot"])
        payer, stock, temporary = current["value"]
        if (
            not payer
            or payer["lamports"] != prepared["sol_before"]
            or decode_token(stock)["amount"] != prepared["stock_before"]
            or temporary is not None
        ):
            raise ValueError("Native inventory changed after simulation")
        height = await self.rpc.call("getBlockHeight", [{"commitment": "finalized"}])
        if (
            height > prepared["last_valid_height"]
            or not 0 <= time.time() - prepared["quote_at"] <= self.settings.policy.quote_max_age_s
        ):
            raise ValueError("Native prepared quote/blockhash expired")
        signature = await self.rpc.send(prepared["transaction"], prepared["min_slot"])
        if signature != prepared["signature"]:
            raise ValueError("Native RPC signature mismatch; reconcile original")
        return signature


class NativeDirectJournal(DirectJournal):
    async def reconcile(self, identity: str, rpc) -> dict:
        row = self.get(identity)
        p = row["payload"]
        if row["status"] in {"COMPLETE", "FAILED", "CANCELLED", "PREPARING", "PREPARED"}:
            return await super().reconcile(identity, rpc)
        if WalletLease.fence(row["wallet"]).exists():
            import json

            owner = json.loads(WalletLease.fence(row["wallet"]).read_text())
            if owner != {"kind": "direct", "database": self.path, "id": identity}:
                raise ValueError("Native wallet reservation mismatch")
        tx = await rpc.transaction(p["signature"])
        if tx is None:
            return {k: v for k, v in row.items() if k != "payload"}
        if tx["transaction"]["signatures"][0] != p["signature"]:
            raise ValueError("Native finalized signature mismatch")
        meta = tx["meta"]
        if meta["err"] is not None:
            self.update(identity, "FAILED", receipt={"signature": p["signature"], "failed_onchain": True})
        else:
            keys = tx["transaction"]["message"]["accountKeys"]
            keys = [k["pubkey"] if isinstance(k, dict) else k for k in keys]
            wallet_index = keys.index(row["wallet"])
            temporary_index = keys.index(p["addresses"][2])
            delta = transaction_deltas(tx, row["wallet"], p["stock_mint"])
            sol_change = meta["postBalances"][wallet_index] - meta["preBalances"][wallet_index]
            pool_sol = sol_change + meta["fee"] + p["rent_delta"]
            if (
                meta["fee"] != p["fee_lamports"]
                or meta["postBalances"][temporary_index] != 0
                or (p["native_buy"] and (pool_sol != -p["input"] or delta["stock"] < p["minimum"]))
                or (not p["native_buy"] and (delta["stock"] != -p["input"] or pool_sol < p["minimum"]))
            ):
                raise ValueError("Native finalized debit/credit/fee mismatch")
            self.update(
                identity,
                "COMPLETE",
                receipt={
                    "signature": p["signature"],
                    "stock_change_raw": delta["stock"],
                    "sol_change_lamports": sol_change,
                    "pool_sol_lamports": pool_sol,
                    "fee_lamports": meta["fee"],
                    "rent_delta": p["rent_delta"],
                },
            )
        WalletLease.clear(row["wallet"], "direct", self.path, identity)
        row = self.get(identity)
        return {k: v for k, v in row.items() if k != "payload"}
