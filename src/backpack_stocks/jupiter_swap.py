"""Frontend order -> bounded RPC simulation -> partial wallet signing -> execute.

The HTTPS Jupiter deployment and selected Solana RPC are trusted providers. This
validates observable wallet effects, not every router's onchain instruction ABI.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from solders.keypair import Keypair
from solders.message import MessageV0, to_bytes_versioned
from solders.pubkey import Pubkey
from solders.signature import Signature

from .direct_execution import WalletLease, immutable_control
from .jupiter_replay import SOL, USDC, JupiterReplay, ReplayError, SwapInput, raw_amount
from .jupiter_wallet import decode_swap_transaction, sign_swap_transaction, verify_swap_signatures
from .models import STOCK_MINT, TOKEN, TOKEN_2022
from .solana import decode_token

SYSTEM = "11111111111111111111111111111111"
LOOKUP = "AddressLookupTab1e1111111111111111111111111"
DEFAULT_RPC = "https://api.mainnet-beta.solana.com"


def positive_units(value, field: str) -> int:
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise ReplayError(f"Missing/invalid {field}")
    text = str(value)
    if not text.isascii() or not text.isdecimal() or not 0 < int(text) < 2**64:
        raise ReplayError(f"Missing/invalid {field}")
    return int(text)


def order_limits(prepared: dict) -> tuple[int, int]:
    quote = prepared["quote"]
    incoming = positive_units(quote.get("inAmount"), "inAmount")
    outgoing = positive_units(quote.get("outAmount"), "outAmount")
    threshold = positive_units(quote.get("otherAmountThreshold"), "otherAmountThreshold")
    if quote["swapMode"] == "ExactIn":
        if threshold > outgoing:
            raise ReplayError("Minimum output exceeds quoted output")
        return incoming, threshold
    if quote["swapMode"] == "ExactOut":
        if threshold < incoming:
            raise ReplayError("Maximum input is below quoted input")
        return threshold, outgoing
    raise ReplayError("Unsupported swap mode")


def check_fresh_order(prepared: dict) -> None:
    if not 0 <= time.time() - prepared["receivedAt"] <= 30:
        raise ReplayError("Order expired; acquire a new order before signing")
    expire = prepared["quote"].get("expireAt")
    if expire:
        try:
            if isinstance(expire, (int, float)):
                expires = float(expire)
            else:
                dt = datetime.fromisoformat(expire.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    raise ValueError
                expires = dt.timestamp()
            if expires <= time.time():
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ReplayError("RFQ order expiry is invalid/expired") from None


class ReplayRPC:
    def __init__(self, replay: JupiterReplay, url: str = DEFAULT_RPC):
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.fragment
        ):
            raise ReplayError("RPC URL must be HTTPS without userinfo or fragment")
        self.replay, self.url, self.sequence = replay, url, 0

    async def call(self, method: str, params: list):
        self.sequence += 1
        data = await self.replay.request(
            "POST",
            self.url,
            payload={"jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params},
            audit_label="solana.rpc",
            allow_api_error=True,
            interval_s=0.30,
        )
        if data.get("id") != self.sequence or data.get("error") or "result" not in data:
            raise ReplayError(f"Solana RPC {method} failed; no submission")
        return data["result"]

    async def accounts(self, addresses: list[str], *, min_slot: int | None = None) -> dict:
        config: dict = {"encoding": "base64", "commitment": "processed"}
        if min_slot is not None:
            config["minContextSlot"] = min_slot
        result = await self.call("getMultipleAccounts", [addresses, config])
        if not isinstance(result.get("value"), list) or len(result["value"]) != len(addresses):
            raise ReplayError("RPC account snapshot is incomplete")
        return result


async def resolve_transaction(tx, rpc: ReplayRPC) -> tuple[list[str], list[str], list[str]]:
    message = tx.message
    keys = [str(key) for key in message.account_keys]
    header = message.header
    writable = [
        key
        for i, key in enumerate(keys)
        if i < header.num_required_signatures - header.num_readonly_signed_accounts
        or header.num_required_signatures <= i < len(keys) - header.num_readonly_unsigned_accounts
    ]
    if isinstance(message, MessageV0) and message.address_table_lookups:
        tables = await rpc.accounts([str(row.account_key) for row in message.address_table_lookups])
        loaded_writable: list[str] = []
        loaded_readonly: list[str] = []
        for lookup, account in zip(message.address_table_lookups, tables["value"], strict=True):
            if not account or account["owner"] != LOOKUP:
                raise ReplayError("Invalid address lookup table owner")
            raw = base64.b64decode(account["data"][0], validate=True)
            if len(raw) < 56 or (len(raw) - 56) % 32 or struct.unpack_from("<I", raw)[0] != 1:
                raise ReplayError("Malformed address lookup table")
            if struct.unpack_from("<Q", raw, 4)[0] != 2**64 - 1:
                raise ReplayError("Deactivating lookup table is unsupported")
            addresses = [str(Pubkey.from_bytes(raw[i : i + 32])) for i in range(56, len(raw), 32)]
            try:
                loaded_writable.extend(addresses[i] for i in lookup.writable_indexes)
                loaded_readonly.extend(addresses[i] for i in lookup.readonly_indexes)
            except IndexError:
                raise ReplayError("Lookup index exceeds the table") from None
        keys.extend([*loaded_writable, *loaded_readonly])
        writable.extend(loaded_writable)
    programs = []
    for instruction in message.instructions:
        if instruction.program_id_index >= len(keys) or any(i >= len(keys) for i in instruction.accounts):
            raise ReplayError("Instruction account index exceeds the resolved message")
        program = keys[instruction.program_id_index]
        programs.append(program)
        # Never permit top-level token approvals, authority changes or system reassignment.
        if program in {TOKEN, TOKEN_2022} and (not instruction.data or instruction.data[0] in {4, 6, 13}):
            raise ReplayError("Token approval/authority instructions are forbidden")
        if program == SYSTEM and (
            len(instruction.data) < 4 or int.from_bytes(instruction.data[:4], "little") not in {0, 2, 3}
        ):
            raise ReplayError("Unsupported system instruction")
    return keys, list(dict.fromkeys(writable)), list(dict.fromkeys(programs))


def validate_wallet_effects(
    addresses: list[str],
    before: list,
    after: list,
    quote: dict,
    *,
    wallet: str,
    max_input: int,
    min_output: int,
    max_sol_cost: int,
    native_sol: bool,
    estimated_fee: int,
) -> dict:
    if len(before) != len(after) or len(before) != len(addresses) or wallet not in addresses:
        raise ReplayError("Simulation account projection is incomplete")
    deltas: dict[str, int] = defaultdict(int)
    new_rent = 0
    wallet_index = addresses.index(wallet)
    old_wallet, new_wallet = before[wallet_index], after[wallet_index]
    if not old_wallet or not new_wallet or new_wallet["owner"] != SYSTEM or old_wallet["owner"] != SYSTEM:
        raise ReplayError("Wallet must remain a funded system account")
    if old_wallet.get("data") != new_wallet.get("data"):
        raise ReplayError("Wallet account data changed")
    native_delta = new_wallet["lamports"] - old_wallet["lamports"]
    for address, old, new in zip(addresses, before, after, strict=True):
        if address == wallet:
            continue
        old_info, new_info = token_account_info(old), token_account_info(new)
        old_owned = old_info and old_info["owner"] == wallet
        new_owned = new_info and new_info["owner"] == wallet
        if old_owned:
            assert old_info is not None
            if (
                not new_owned
                or new["owner"] != old["owner"]
                or immutable_control(old) != immutable_control(new)
            ):
                raise ReplayError("Wallet token account closed or control/authority changed")
            if old_info["delegate"] or old_info["close_authority"]:
                raise ReplayError("Delegated wallet token accounts are unsupported")
            assert new_info is not None
            change = new_info["amount"] - old_info["amount"]
            deltas[old_info["mint"]] += change
            if old_info["mint"] not in {quote["inputMint"], quote["outputMint"]} and change < 0:
                raise ReplayError("Simulation debits an unrelated wallet token")
        elif new_owned:
            assert new_info is not None
            if new_info["mint"] != quote["outputMint"] or new_info["delegate"] or new_info["close_authority"]:
                raise ReplayError("Unexpected new wallet token account/authority")
            deltas[new_info["mint"]] += new_info["amount"]
            if old is None:
                new_rent += new["lamports"]
    fee = estimated_fee if quote.get("signatureFeePayer", wallet) == wallet else 0
    sol_allowance = fee + new_rent
    # Tip is outside getFeeForMessage; the caller binds its maximum to explicit UI settings.
    tip_allowance = min(max_sol_cost - sol_allowance, quote.get("_requestedJitoTip", 0))
    if sol_allowance > max_sol_cost or tip_allowance < 0:
        raise ReplayError("Network fee and account rent exceed the SOL cost limit")
    sol_allowance += tip_allowance
    input_delta = native_delta if native_sol and quote["inputMint"] == SOL else deltas[quote["inputMint"]]
    output_delta = native_delta if native_sol and quote["outputMint"] == SOL else deltas[quote["outputMint"]]
    if native_sol and quote["inputMint"] == SOL:
        if not 0 < -input_delta <= max_input + sol_allowance:
            raise ReplayError("Simulated SOL debit exceeds the requested amount/fee budget")
    elif not 0 < -input_delta <= max_input:
        raise ReplayError("Simulated token debit exceeds the requested amount")
    minimum_net = min_output - sol_allowance if native_sol and quote["outputMint"] == SOL else min_output
    if minimum_net <= 0 or output_delta < minimum_net:
        raise ReplayError("Simulated output is below the minimum after permitted network costs")
    if (
        not (native_sol and SOL in {quote["inputMint"], quote["outputMint"]})
        and native_delta < -sol_allowance
    ):
        raise ReplayError("Unexpected SOL debit outside the fee/rent budget")
    return {
        "inputDeltaRaw": str(input_delta),
        "outputDeltaRaw": str(output_delta),
        "nativeDeltaLamports": native_delta,
        "networkFeeLamports": fee,
        "newAccountRentLamports": new_rent,
    }


def token_account_info(account: dict | None) -> dict | None:
    if not account or account["owner"] not in {TOKEN, TOKEN_2022}:
        return None
    raw = base64.b64decode(account["data"][0], validate=True)
    # Mint and multisig accounts share the token program owner but have another layout.
    if len(raw) == 165 or (account["owner"] == TOKEN_2022 and len(raw) > 165 and raw[165] == 2):
        return decode_token(account)
    return None


async def prepare_swap(
    replay: JupiterReplay,
    rpc: ReplayRPC,
    swap: SwapInput,
    *,
    max_sol_cost: int = 5_000_000,
    minimum_output: int | None = None,
) -> dict:
    if not swap.wallet or not swap.has_enough_balance:
        raise ReplayError("Swap requires a taker and sufficient-balance mode")
    if type(max_sol_cost) is not int or not 0 <= max_sol_cost <= 2**64 - 1:
        raise ReplayError("Invalid SOL cost limit")
    # Validate human amount precision against the actual mint before obtaining the expiring order.
    mints = await rpc.accounts([swap.input_mint, swap.output_mint])
    side = 0 if swap.swap_mode == "ExactIn" else 1
    account = mints["value"][side]
    if not account or account["owner"] not in {TOKEN, TOKEN_2022}:
        raise ReplayError("Amount mint is not a supported SPL token mint")
    raw = base64.b64decode(account["data"][0], validate=True)
    if len(raw) < 82 or raw[45] != 1 or raw[44] != swap.amount_decimals:
        raise ReplayError("Requested decimals do not match the onchain mint")
    prepared = await replay.order(swap)
    quote = prepared["quote"]
    if quote.get("errorCode") or not quote.get("transaction"):
        raise ReplayError("Order has no executable transaction; inspect balance/errorCode in audit")
    max_input, min_output = order_limits(prepared)
    if minimum_output is not None:
        min_output = max(min_output, positive_units(minimum_output, "minimum output"))
    tx = decode_swap_transaction(quote["transaction"])
    verify_swap_signatures(tx, quote, require_taker=False)
    keys, writable, programs = await resolve_transaction(tx, rpc)
    addresses = list(dict.fromkeys([swap.wallet, *writable]))
    if len(addresses) > 100:
        raise ReplayError("Simulation exceeds the 100-account RPC projection limit")
    snapshot = await rpc.accounts(addresses, min_slot=mints["context"]["slot"])
    fee = await rpc.call(
        "getFeeForMessage",
        [base64.b64encode(to_bytes_versioned(tx.message)).decode(), {"commitment": "processed"}],
    )
    if fee.get("value") is None:
        raise ReplayError("Order blockhash is not valid for fee estimation")
    latest_height = await rpc.call("getBlockHeight", [{"commitment": "processed"}])
    if quote.get("lastValidBlockHeight") and latest_height > int(quote["lastValidBlockHeight"]):
        raise ReplayError("Order block height expired")
    simulated = await rpc.call(
        "simulateTransaction",
        [
            quote["transaction"],
            {
                "encoding": "base64",
                "sigVerify": False,
                "replaceRecentBlockhash": False,
                "commitment": "processed",
                "minContextSlot": snapshot["context"]["slot"],
                "accounts": {"encoding": "base64", "addresses": addresses},
            },
        ],
    )
    value = simulated.get("value", {})
    if value.get("err") is not None or not isinstance(value.get("accounts"), list):
        raise ReplayError("Swap simulation failed; no signing/submission (see sanitized audit)")
    effects = validate_wallet_effects(
        addresses,
        snapshot["value"],
        value["accounts"],
        {**quote, "_requestedJitoTip": swap.jito_tip_lamports or 0},
        wallet=swap.wallet,
        max_input=max_input,
        min_output=min_output,
        max_sol_cost=max_sol_cost,
        native_sol=not swap.use_wsol,
        estimated_fee=fee["value"],
    )
    check_fresh_order(prepared)
    prepared["review"] = {
        "wallet": swap.wallet,
        "router": quote.get("router"),
        "requestId": quote["requestId"],
        "inputMint": swap.input_mint,
        "outputMint": swap.output_mint,
        "maxInputRaw": str(max_input),
        "minOutputRaw": str(min_output),
        "messageSha256": hashlib.sha256(to_bytes_versioned(tx.message)).hexdigest(),
        "signers": [str(key) for key in tx.message.account_keys[: tx.message.header.num_required_signatures]],
        "programs": programs,
        "simulationSlot": simulated["context"]["slot"],
        "recentBlockhash": str(tx.message.recent_blockhash),
        "lastValidBlockHeight": quote.get("lastValidBlockHeight"),
        "simulation": effects,
        "maxSolCostLamports": max_sol_cost,
    }
    return prepared


class SwapJournal:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def path(self, request_id: str) -> Path:
        return self.root / (hashlib.sha256(request_id.encode()).hexdigest() + ".json")

    def write(self, row: dict) -> None:
        target = self.path(row["requestId"])
        temporary = target.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(row, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(target)
        directory = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def read(self, request_id: str) -> dict:
        return json.loads(self.path(request_id).read_text())


async def submit_swap(
    replay: JupiterReplay,
    prepared: dict,
    wallet: Keypair,
    journal: SwapJournal,
    *,
    parent_request_id: str | None = None,
    lease: WalletLease | None = None,
) -> dict:
    owned = WalletLease(str(wallet.pubkey())) if lease is None else None
    try:
        if lease is not None and (lease.file.closed or Path(lease.file.name).stem != str(wallet.pubkey())):
            raise ReplayError("Execution requires the matching active wallet lease")
        return await _submit_swap(replay, prepared, wallet, journal, parent_request_id=parent_request_id)
    finally:
        if owned:
            owned.close()


async def _submit_swap(
    replay: JupiterReplay,
    prepared: dict,
    wallet: Keypair,
    journal: SwapJournal,
    *,
    parent_request_id: str | None = None,
) -> dict:
    check_fresh_order(prepared)
    review = prepared.get("review")
    if not review or review["wallet"] != str(wallet.pubkey()):
        raise ReplayError("A matching simulation review is required before signing")
    tx = decode_swap_transaction(prepared["quote"]["transaction"])
    if hashlib.sha256(to_bytes_versioned(tx.message)).hexdigest() != review["messageSha256"]:
        raise ReplayError("Transaction differs from the simulated message")
    signed = sign_swap_transaction(prepared, wallet)
    signed_tx = decode_swap_transaction(signed)
    index = list(signed_tx.message.account_keys[: signed_tx.message.header.num_required_signatures]).index(
        wallet.pubkey()
    )
    row = {
        **review,
        "backend": replay.backend,
        "phase": "PREPARED",
        "createdAt": datetime.now(UTC).isoformat(),
        "takerTxSignature": str(signed_tx.signatures[index]),
        "txSignature": str(signed_tx.signatures[0])
        if signed_tx.signatures[0] != Signature.default()
        else None,
    }
    # Shared wallet fence prevents another local process from issuing a new order after an ambiguous POST.
    WalletLease.reserve(review["wallet"], "jupiter_frontend", str(journal.root), review["requestId"])
    journal.write(row)
    if parent_request_id:
        parent = journal.read(parent_request_id)
        if parent["phase"] != "CONFIRMED" or parent["wallet"] != row["wallet"]:
            raise ReplayError("Return leg requires the confirmed buy for the same wallet")
        parent["returnRequestId"] = row["requestId"]
        journal.write(parent)
    sent = False

    def on_submit() -> None:
        nonlocal sent
        row["phase"] = "SENDING"
        journal.write(row)
        sent = True

    try:
        result = await replay.execute(prepared, signed, allow_execute=True, on_submit=on_submit)
    except Exception:
        row["phase"] = "UNKNOWN" if sent else "REJECTED"
        journal.write(row)
        if not sent:
            WalletLease.clear(review["wallet"], "jupiter_frontend", str(journal.root), review["requestId"])
            raise ReplayError("Local check stopped the swap before submission; obtain a new order") from None
        raise ReplayError(
            f"Swap outcome unknown; check status for requestId {row['requestId']}; do not repeat"
        ) from None
    row["result"] = {
        key: result.get(key)
        for key in (
            "status",
            "code",
            "totalInputAmount",
            "totalOutputAmount",
            "inputAmountResult",
            "outputAmountResult",
        )
    }
    if result.get("signature"):
        try:
            received_signature = str(Signature.from_string(result["signature"]))
            if received_signature == str(Signature.default()):
                raise ValueError
            if row["txSignature"] and row["txSignature"] != received_signature:
                raise ValueError
            row["txSignature"] = received_signature
        except (ValueError, TypeError):
            row["phase"] = "UNKNOWN"
            journal.write(row)
            raise ReplayError(
                "Execute returned an invalid transaction signature; reconcile journal"
            ) from None
    if result.get("status") == "Success" and result.get("code") == 0 and row["txSignature"]:
        row["phase"] = "CONFIRMED"
    elif result.get("status") == "Failed" and result.get("code") in {
        -1,
        -2,
        -3,
        -1002,
        -1003,
        -1004,
        -2002,
        -2003,
        -2004,
    }:
        row["phase"] = "REJECTED"
    else:
        row["phase"] = "UNKNOWN"
    journal.write(row)
    if row["phase"] in {"CONFIRMED", "REJECTED"}:
        WalletLease.clear(review["wallet"], "jupiter_frontend", str(journal.root), review["requestId"])
    return row


async def reconcile_swap(rpc: ReplayRPC, journal: SwapJournal, request_id: str) -> dict:
    row = journal.read(request_id)
    if row["phase"] == "PREPARED":
        # SENDING is fsynced by before_send before the first network POST can start.
        row["phase"] = "REJECTED"
        row["reason"] = "Process stopped before submission"
        journal.write(row)
        WalletLease.clear(row["wallet"], "jupiter_frontend", str(journal.root), row["requestId"])
        return row
    if row["phase"] in {"CONFIRMED", "REJECTED", "CHAIN_FAILED"}:
        return row
    signature = row.get("txSignature")
    if not signature:
        # RFQ fee payer co-signs later; discover the final tx by its unchanged message.
        recent = await rpc.call(
            "getSignaturesForAddress", [row["wallet"], {"limit": 50, "commitment": "confirmed"}]
        )
        for candidate in recent:
            tx = await rpc.call(
                "getTransaction",
                [
                    candidate["signature"],
                    {"encoding": "base64", "commitment": "confirmed", "maxSupportedTransactionVersion": 0},
                ],
            )
            if tx and tx.get("transaction"):
                decoded = decode_swap_transaction(tx["transaction"][0])
                if hashlib.sha256(to_bytes_versioned(decoded.message)).hexdigest() == row["messageSha256"]:
                    signature = candidate["signature"]
                    row["txSignature"] = signature
                    break
    if signature:
        result = await rpc.call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
        status = result["value"][0]
        if status and status.get("confirmationStatus") in {"confirmed", "finalized"}:
            row["phase"] = "CHAIN_FAILED" if status.get("err") is not None else "CONFIRMED"
            row["chainStatus"] = status
    journal.write(row)
    if row["phase"] in {"CONFIRMED", "CHAIN_FAILED"}:
        WalletLease.clear(row["wallet"], "jupiter_frontend", str(journal.root), row["requestId"])
    # Not found/timeout/expired is never inferred to mean that a new swap is safe.
    return row


async def round_trip(
    replay: JupiterReplay,
    rpc: ReplayRPC,
    template: SwapInput,
    wallet: Keypair,
    state_dir: Path,
    *,
    usdc_value: str = "10",
    max_sol_cost: int = 5_000_000,
    execute: bool = False,
    resume_buy_request_id: str | None = None,
    lease: WalletLease | None = None,
) -> dict:
    owned = WalletLease(str(wallet.pubkey())) if execute and lease is None else None
    try:
        return await _round_trip(
            replay,
            rpc,
            template,
            wallet,
            state_dir,
            usdc_value=usdc_value,
            max_sol_cost=max_sol_cost,
            execute=execute,
            resume_buy_request_id=resume_buy_request_id,
            lease=lease or owned,
        )
    finally:
        if owned:
            owned.close()


async def _round_trip(
    replay: JupiterReplay,
    rpc: ReplayRPC,
    template: SwapInput,
    wallet: Keypair,
    state_dir: Path,
    *,
    usdc_value: str,
    max_sol_cost: int,
    execute: bool,
    resume_buy_request_id: str | None,
    lease: WalletLease | None,
) -> dict:
    from dataclasses import replace
    from decimal import Decimal

    if resume_buy_request_id:
        if not execute:
            raise ReplayError("Resume of the return leg requires --execute")
        journal = SwapJournal(state_dir)
        first_result = journal.read(resume_buy_request_id)
        if (
            first_result["phase"] != "CONFIRMED"
            or first_result["wallet"] != str(wallet.pubkey())
            or first_result["inputMint"] != SOL
            or first_result["outputMint"] != STOCK_MINT
            or first_result["backend"] != replay.backend
        ):
            raise ReplayError("Resume requires the confirmed SOL-to-WEN buy for this wallet/backend")
        existing_return = first_result.get("returnRequestId")
        if existing_return:
            previous = await reconcile_swap(rpc, journal, existing_return)
            if previous["phase"] == "CONFIRMED":
                return {"executed": True, "resumed": True, "buyResult": first_result, "sellResult": previous}
            if previous["phase"] not in {"REJECTED", "CHAIN_FAILED"}:
                raise ReplayError(f"Return leg unresolved; check status for requestId {existing_return}")
        return await finish_round_trip(
            replay,
            rpc,
            template,
            wallet,
            journal,
            first_result,
            {"resumed": True, "stockMint": STOCK_MINT},
            max_sol_cost=max_sol_cost,
            lease=lease,
        )
    reference_raw = raw_amount(usdc_value, 6)
    reference = await replay.order(SwapInput(USDC, SOL, usdc_value, 6))
    sol_units = positive_units(reference["quote"].get("outAmount"), "reference SOL amount")
    sol_amount = format(Decimal(sol_units) / Decimal(10**9), "f")
    buy = replace(
        template,
        input_mint=SOL,
        output_mint=STOCK_MINT,
        amount=sol_amount,
        amount_decimals=9,
        swap_mode="ExactIn",
        wallet=str(wallet.pubkey()),
    )
    first = await prepare_swap(replay, rpc, buy, max_sol_cost=max_sol_cost)
    report = {
        "referenceUsdcRaw": str(reference_raw),
        "solInputRaw": str(sol_units),
        "stockMint": STOCK_MINT,
        "executed": False,
        "buyReview": first["review"],
    }
    if not execute:
        # Without a fill, only preview the reverse: the bought tokens do not yet exist.
        estimate = format(Decimal(first["quote"]["outAmount"]) / Decimal(10**6), "f")
        reverse = await replay.order(
            replace(
                template,
                input_mint=STOCK_MINT,
                output_mint=SOL,
                amount=estimate,
                amount_decimals=6,
                swap_mode="ExactIn",
                wallet=None,
            )
        )
        report["reversePreview"] = {
            "inputRaw": reverse["quote"]["inAmount"],
            "outputRaw": reverse["quote"]["outAmount"],
            "simulated": False,
        }
        return report
    journal = SwapJournal(state_dir)
    first["review"]["roundTripReferenceUsdcRaw"] = str(reference_raw)
    first_result = await submit_swap(replay, first, wallet, journal, lease=lease)
    return await finish_round_trip(
        replay, rpc, template, wallet, journal, first_result, report, max_sol_cost=max_sol_cost, lease=lease
    )


async def finish_round_trip(
    replay: JupiterReplay,
    rpc: ReplayRPC,
    template: SwapInput,
    wallet: Keypair,
    journal: SwapJournal,
    first_result: dict,
    report: dict,
    *,
    max_sol_cost: int,
    lease: WalletLease | None = None,
) -> dict:
    from dataclasses import replace
    from decimal import Decimal

    report["buyResult"] = first_result
    if first_result["phase"] != "CONFIRMED":
        raise ReplayError(f"Round-trip buy not confirmed; reconcile requestId {first_result['requestId']}")
    # Use net tokens actually received, not the earlier quote or the wallet's pre-existing WEN.
    received = positive_units(first_result["result"].get("totalOutputAmount"), "filled WEN amount")
    sell = replace(
        template,
        input_mint=STOCK_MINT,
        output_mint=SOL,
        amount=format(Decimal(received) / Decimal(10**6), "f"),
        amount_decimals=6,
        swap_mode="ExactIn",
        wallet=str(wallet.pubkey()),
    )
    try:
        second = await prepare_swap(replay, rpc, sell, max_sol_cost=max_sol_cost)
        second_result = await submit_swap(
            replay, second, wallet, journal, parent_request_id=first_result["requestId"], lease=lease
        )
    except ReplayError:
        raise ReplayError(
            f"Buy confirmed ({first_result['requestId']}); return leg stopped. Check journal before resuming"
        ) from None
    report.update(executed=True, sellResult=second_result)
    if second_result["phase"] != "CONFIRMED":
        raise ReplayError(f"Round-trip sell not confirmed; reconcile requestId {second_result['requestId']}")
    return report
