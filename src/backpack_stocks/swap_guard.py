from __future__ import annotations

import base64
import json
import struct
from importlib.resources import files

from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.transaction import VersionedTransaction

from .models import ATA, JUPITER, STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, Policy, dec, impact_percent
from .solana import SolanaRPC, associated_address, decode_token

COMPUTE = "ComputeBudget111111111111111111111111111111"
SYSTEM = "11111111111111111111111111111111"
LOOKUP = "AddressLookupTab1e1111111111111111111111111"


def validate_simulation(
    snapshot: dict, value: dict, amount: int, minimum: int, max_lamports: int, wallet: str
) -> None:
    if value.get("err") or not value.get("accounts"):
        raise ValueError("Swap simulation failed")
    payer, stock, usdc = value["accounts"]
    a, b = decode_token(stock), decode_token(usdc)
    old_a, old_b = decode_token(snapshot["accounts"][1]), decode_token(snapshot["accounts"][2])
    if a["amount"] != snapshot["stock"] - amount or b["amount"] - snapshot["usdc"] < minimum:
        raise ValueError("Simulated debit/credit violates policy")
    if a["control"] != old_a["control"]:
        raise ValueError("Input token authority/extensions modified")
    if old_b["owner"] is not None and b["control"] != old_b["control"]:
        raise ValueError("Output token authority/extensions modified")
    if b["owner"] != wallet or b["mint"] != USDC_MINT or b["delegate"] or b["close_authority"]:
        raise ValueError("Wrong simulated output owner/mint/authority")
    if a["delegate"] or a["close_authority"]:
        raise ValueError("Input ATA delegation/close authority unsupported")
    rent = usdc["lamports"] if snapshot["accounts"][2] is None else 0
    if payer["owner"] != SYSTEM or not 0 <= snapshot["lamports"] - payer["lamports"] <= max_lamports + rent:
        raise ValueError("Simulated SOL outflow exceeds limit")
    if payer.get("data") != snapshot["accounts"][0].get("data"):
        raise ValueError("Simulated payer account data changed")


def decode_route(data: bytes) -> tuple[str, dict]:
    idl = json.loads(files("backpack_stocks").joinpath("jupiter_v1_idl.json").read_text())
    instruction = next((i for i in idl["instructions"] if bytes(i["discriminator"]) == data[:8]), None)
    if instruction is None:
        raise ValueError("Unsupported Jupiter instruction discriminator")
    types = {t["name"]: t["type"] for t in idl["types"]}
    position = 8

    def read(t):
        nonlocal position
        if isinstance(t, str):
            formats = {"u8": "B", "u16": "H", "u32": "I", "u64": "Q", "bool": "?"}
            if t not in formats:
                raise ValueError("Unsupported IDL primitive")
            fmt = "<" + formats[t]
            result = struct.unpack_from(fmt, data, position)[0]
            position += struct.calcsize(fmt)
            return result
        if "vec" in t:
            count = read("u32")
            if count > 64:
                raise ValueError("Route vector exceeds bound")
            return [read(t["vec"]) for _ in range(count)]
        if "defined" in t:
            definition = t["defined"]
            name = definition if isinstance(definition, str) else definition["name"]
            spec = types[name]
            if spec["kind"] == "struct":
                return {f["name"]: read(f["type"]) for f in spec["fields"]}
            if spec["kind"] == "enum":
                index = read("u8")
                if index >= len(spec["variants"]):
                    raise ValueError("Unknown swap variant")
                variant = spec["variants"][index]
                fields = variant.get("fields", [])
                return {
                    variant["name"]: [read(f.get("type", f) if isinstance(f, dict) else f) for f in fields]
                }
        raise ValueError("Unsupported IDL type")

    try:
        result = {arg["name"]: read(arg["type"]) for arg in instruction["args"]}
    except (struct.error, KeyError, IndexError):
        raise ValueError("Malformed Jupiter route instruction") from None
    if position != len(data):
        raise ValueError("Trailing bytes in Jupiter route")
    return instruction["name"], result


def validate_instructions(
    q: dict, wallet: str, amount: int, min_output: int, p: Policy, *, mint: str = STOCK_MINT
) -> list[Instruction]:
    from solders.pubkey import Pubkey

    if q["inputMint"] != mint or q["outputMint"] != USDC_MINT or int(q["inAmount"]) != amount:
        raise ValueError("Swap mint/input mismatch")
    if (
        q.get("cleanupInstruction")
        or q.get("otherInstructions")
        or q.get("tipInstruction")
        or q.get("tokenLedgerInstruction")
    ):
        raise ValueError("Extra/cleanup/token-ledger instructions unsupported")
    if q.get("platformFee") and int(q["platformFee"].get("amount", 0)):
        raise ValueError("Platform fee unsupported")
    if abs(impact_percent(q)) > dec(p.max_price_impact_pct):
        raise ValueError("Price impact exceeds policy")
    swap = q["swapInstruction"]
    if swap["programId"] != JUPITER:
        raise ValueError("Wrong swap program")
    name, args = decode_route(base64.b64decode(swap["data"], validate=True))
    threshold = args["quoted_out_amount"] * (10000 - args["slippage_bps"]) // 10000
    if args["in_amount"] != amount or args["slippage_bps"] != p.slippage_bps or args["platform_fee_bps"]:
        raise ValueError("Encoded amount/slippage/fee mismatch")
    if threshold < min_output or int(q["otherAmountThreshold"]) < min_output:
        raise ValueError("Encoded minimum output violates exit policy")
    src = associated_address(wallet, mint, TOKEN_2022)
    dst = associated_address(wallet, USDC_MINT, TOKEN)
    accounts = [a["pubkey"] for a in swap["accounts"]]
    required = (
        {1: wallet, 2: src, 3: dst, 4: JUPITER, 5: USDC_MINT, 6: JUPITER}
        if name == "route"
        else {2: wallet, 3: src, 6: dst, 7: mint, 8: USDC_MINT, 9: JUPITER}
    )
    if any(len(accounts) <= i or accounts[i] != v for i, v in required.items()):
        raise ValueError("Unexpected swap authority/source/destination/fee account")
    result = []
    instructions = [*q.get("computeBudgetInstructions", []), *q.get("setupInstructions", []), swap]
    units, micro_price = 200000, 0
    for ix in instructions:
        program = ix["programId"]
        data = base64.b64decode(ix["data"], validate=True)
        rows = ix["accounts"]
        if any(a["isSigner"] and a["pubkey"] != wallet for a in rows):
            raise ValueError("Additional signer forbidden")
        if program == COMPUTE:
            if rows or not data:
                raise ValueError("Invalid compute instruction")
            if data[0] == 2 and len(data) == 5:
                units = struct.unpack_from("<I", data, 1)[0]
                if not 1 <= units <= 1400000:
                    raise ValueError("Compute limit outside bounds")
            elif data[0] == 3 and len(data) == 9:
                micro_price = struct.unpack_from("<Q", data, 1)[0]
            else:
                raise ValueError("Unsupported compute instruction")
        elif program == ATA:
            a = [r["pubkey"] for r in rows]
            if data != b"\x01" or a != [wallet, dst, wallet, USDC_MINT, SYSTEM, TOKEN]:
                raise ValueError("Only idempotent creation of wallet USDC ATA is allowed")
        elif program != JUPITER:
            raise ValueError("Unapproved top-level program")
        result.append(
            Instruction(
                Pubkey.from_string(program),
                data,
                [AccountMeta(Pubkey.from_string(a["pubkey"]), a["isSigner"], a["isWritable"]) for a in rows],
            )
        )
    if 5000 + (units * micro_price + 999999) // 1000000 > p.max_network_lamports:
        raise ValueError("Priority fee exceeds policy")
    return result


class SwapGuard:
    def __init__(self, rpc: SolanaRPC, key: Keypair, policy: Policy):
        self.rpc, self._key, self.policy = rpc, key, policy

    async def prepare(self, q: dict, amount: int, minimum: int, *, mint: str = STOCK_MINT) -> dict:
        from solders.pubkey import Pubkey

        wallet = str(self._key.pubkey())
        instructions = validate_instructions(q, wallet, amount, minimum, self.policy, mint=mint)
        await self.rpc.check_mint(mint)
        snapshot = await self.rpc.wallet_snapshot(wallet, mint)
        if snapshot["stock"] < amount or snapshot["lamports"] < self.policy.min_wallet_lamports:
            raise ValueError("Wallet lacks stock or gas reserve")
        # All wallet-owned writable token accounts must be the two controlled ATAs.
        writable = sorted({str(a.pubkey) for ix in instructions for a in ix.accounts if a.is_writable})
        if len(writable) > 100:
            raise ValueError("Too many writable accounts")
        observed = await self.rpc.accounts(writable, min_slot=snapshot["slot"])
        for address, account in zip(writable, observed["value"], strict=True):
            if account and account["owner"] in {TOKEN, TOKEN_2022}:
                info = decode_token(account)
                if info["owner"] == wallet and address not in snapshot["addresses"][1:]:
                    raise ValueError("Swap can access an unrelated wallet token account")
        tables = []
        for address in q.get("addressLookupTableAddresses", []):
            response = await self.rpc.accounts([address], min_slot=snapshot["slot"])
            account = response["value"][0]
            if not account or account["owner"] != LOOKUP:
                raise ValueError("Invalid lookup table owner")
            data = base64.b64decode(account["data"][0], validate=True)
            if len(data) < 56 or (len(data) - 56) % 32 or struct.unpack_from("<Q", data, 4)[0] != 2**64 - 1:
                raise ValueError("Malformed/deactivating lookup table")
            tables.append(
                AddressLookupTableAccount(
                    Pubkey.from_string(address),
                    [Pubkey.from_bytes(data[i : i + 32]) for i in range(56, len(data), 32)],
                )
            )
        latest = await self.rpc.call("getLatestBlockhash", [{"commitment": "finalized"}])
        msg = MessageV0.try_compile(
            self._key.pubkey(), instructions, tables, Hash.from_string(latest["value"]["blockhash"])
        )
        # Simulate with an empty signature first; sign only after validating effects.
        from solders.signature import Signature

        unsigned = base64.b64encode(bytes(VersionedTransaction.populate(msg, [Signature.default()]))).decode()
        simulation = await self.rpc.simulate(unsigned, snapshot["addresses"], snapshot["slot"])
        validate_simulation(
            snapshot, simulation["value"], amount, minimum, self.policy.max_network_lamports, wallet
        )
        fee = await self.rpc.call(
            "getFeeForMessage", [base64.b64encode(bytes(msg)).decode(), {"commitment": "finalized"}]
        )
        if fee["value"] is None or fee["value"] > self.policy.max_network_lamports:
            raise ValueError("Network fee unavailable/exceeds limit")
        signed = VersionedTransaction(msg, [self._key])
        return {
            "transaction": base64.b64encode(bytes(signed)).decode(),
            "signature": str(signed.signatures[0]),
            "min_slot": snapshot["slot"],
            "last_valid_height": latest["value"]["lastValidBlockHeight"],
            "minimum": minimum,
            "input": amount,
            "wallet_stock": snapshot["stock"],
            "wallet_usdc": snapshot["usdc"],
        }
