"""Simulation-guarded direct swaps with durable submission and recovery."""

from __future__ import annotations

import base64
import fcntl
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from .direct import DirectRaydium, direct_instructions
from .models import USDC_MINT, Settings, dec, impact_percent
from .solana import SolanaRPC, associated_address, decode_token, transaction_deltas
from .swap_guard import SYSTEM


class WalletLease:
    """One active local execution process per wallet, shared by FSM and direct CLI."""

    def __init__(self, wallet: str):
        from solders.pubkey import Pubkey

        Pubkey.from_string(wallet)
        root = Path("state/wallet-locks")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.file = open(root / (wallet + ".lock"), "a+")
        os.chmod(self.file.name, 0o600)
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.file.close()
            raise ValueError("Wallet already has an active local execution process") from None

    def close(self) -> None:
        fcntl.flock(self.file, fcntl.LOCK_UN)
        self.file.close()

    @staticmethod
    def fence(wallet: str) -> Path:
        from solders.pubkey import Pubkey

        Pubkey.from_string(wallet)
        return Path("state/wallet-locks") / (wallet + ".pending.json")

    @classmethod
    def reserve(cls, wallet: str, kind: str, database: str, identity: str, *, recover: bool = False) -> None:
        path = cls.fence(wallet)
        owner = {"kind": kind, "database": str(Path(database).resolve()), "id": identity}
        if path.exists():
            previous = json.loads(path.read_text())
            if not recover or previous != owner:
                raise ValueError("Wallet has a durable unresolved execution; reconcile its original database")
            return
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(owner, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    @classmethod
    def clear(cls, wallet: str, kind: str, database: str, identity: str) -> None:
        path = cls.fence(wallet)
        if path.exists():
            expected = {"kind": kind, "database": str(Path(database).resolve()), "id": identity}
            if json.loads(path.read_text()) != expected:
                return
            path.unlink()


def immutable_control(account: dict) -> bytes:
    data = bytearray(base64.b64decode(account["data"][0], validate=True))
    # TransferFeeAmount contains mutable withheld fees; every other extension byte stays immutable.
    pos = 166
    while pos + 4 <= len(data):
        kind, size = (
            int.from_bytes(data[pos : pos + 2], "little"),
            int.from_bytes(data[pos + 2 : pos + 4], "little"),
        )
        if pos + 4 + size > len(data):
            raise ValueError("Malformed token account extension")
        if kind == 2 and size == 8:
            data[pos + 4 : pos + 12] = bytes(8)
        pos += 4 + size
    return bytes(data[:64] + data[72:])


def validate_direct_simulation(
    snapshot: dict,
    value: dict,
    amount: int,
    minimum: int,
    source: str,
    target: str,
    wallet: str,
    settings: Settings,
) -> int:
    rows = value.get("accounts")
    if (
        value.get("err") is not None
        or not isinstance(rows, list)
        or len(rows) != 3
        or any(r is None for r in rows)
    ):
        raise ValueError("Direct simulation failed")
    payer, incoming, outgoing = rows
    a, b = decode_token(incoming), decode_token(outgoing)
    old_a, old_b = decode_token(snapshot["accounts"][1]), decode_token(snapshot["accounts"][2])
    if a["amount"] != old_a["amount"] - amount or b["amount"] - old_b["amount"] < minimum:
        raise ValueError("Direct simulated debit/credit mismatch")
    for info, mint, account, program in (
        (a, source, incoming, snapshot["programs"][0]),
        (b, target, outgoing, snapshot["programs"][1]),
    ):
        if (
            info["owner"] != wallet
            or info["mint"] != mint
            or account["owner"] != program
            or info["delegate"]
            or info["close_authority"]
        ):
            raise ValueError("Direct simulated token authority/program mismatch")
    if immutable_control(incoming) != immutable_control(snapshot["accounts"][1]):
        raise ValueError("Direct input account control changed")
    if snapshot["accounts"][2] is not None and immutable_control(outgoing) != immutable_control(
        snapshot["accounts"][2]
    ):
        raise ValueError("Direct output account control changed")
    rent = outgoing["lamports"] if snapshot["accounts"][2] is None else 0
    old_payer = snapshot["accounts"][0]
    if (
        payer["owner"] != SYSTEM
        or payer.get("data") != old_payer.get("data")
        or not 0 <= old_payer["lamports"] - payer["lamports"] <= settings.policy.max_network_lamports + rent
        or payer["lamports"] < settings.policy.min_wallet_lamports
    ):
        raise ValueError("Direct simulated SOL outflow/reserve mismatch")
    return b["amount"] - old_b["amount"]


class DirectSwapGuard:
    def __init__(self, router: DirectRaydium, rpc: SolanaRPC, key: Keypair, settings: Settings):
        self.router, self.rpc, self._key, self.settings = router, rpc, key, settings

    async def prepare(self, q: dict, amount: int, minimum: int, *, mint: str | None = None) -> dict:
        minimum = max(minimum, int(q["otherAmountThreshold"]))
        source, target = q["inputMint"], q["outputMint"]
        if int(q["inAmount"]) != amount or (mint is not None and source != mint):
            raise ValueError("Direct requested input mismatch")
        if abs(impact_percent(q)) > dec(self.settings.policy.max_price_impact_pct):
            raise ValueError("Direct price impact exceeds policy")
        if not 0 <= time.time() - q["_observed_at"] <= self.settings.policy.quote_max_age_s:
            raise ValueError("Direct quote expired")
        wallet = str(self._key.pubkey())
        state = q["_direct"]["state"]
        a, b = ("A", "B") if source == state["mintA"] else ("B", "A")
        programs = [state["tokenProgram" + a], state["tokenProgram" + b]]
        addresses = [
            wallet,
            associated_address(wallet, source, programs[0]),
            associated_address(wallet, target, programs[1]),
        ]
        response = await self.rpc.accounts(addresses, min_slot=q["contextSlot"])
        snapshot = {"addresses": addresses, "accounts": response["value"], "programs": programs}
        payer = snapshot["accounts"][0]
        input_info = decode_token(snapshot["accounts"][1])
        if (
            not payer
            or payer["owner"] != SYSTEM
            or payer.get("executable")
            or input_info["owner"] != wallet
            or input_info["mint"] != source
            or input_info.get("program") != programs[0]
            or input_info["amount"] < amount
            or input_info["delegate"]
            or input_info["close_authority"]
        ):
            raise ValueError("Direct wallet input/gas not ready")
        output_info = decode_token(snapshot["accounts"][2])
        if output_info["owner"] is not None and (
            output_info["owner"] != wallet
            or output_info["mint"] != target
            or output_info.get("program") != programs[1]
        ):
            raise ValueError("Direct wallet output ATA mismatch")
        instructions = direct_instructions(q, wallet, minimum, self.settings)
        latest = await self.rpc.call(
            "getLatestBlockhash", [{"commitment": "finalized", "minContextSlot": response["context"]["slot"]}]
        )
        msg = MessageV0.try_compile(
            self._key.pubkey(), instructions, [], Hash.from_string(latest["value"]["blockhash"])
        )
        unsigned_tx = VersionedTransaction.populate(msg, [Signature.default()])
        if len(bytes(unsigned_tx)) > 1232:
            raise ValueError("Direct swap exceeds Solana transaction size; reduce tick array bound")
        unsigned = base64.b64encode(bytes(unsigned_tx)).decode()
        min_slot = max(q["contextSlot"], response["context"]["slot"])
        simulation = await self.rpc.simulate(unsigned, addresses, min_slot)
        output = validate_direct_simulation(
            snapshot, simulation["value"], amount, minimum, source, target, wallet, self.settings
        )
        expected = int(q["outAmount"])
        if abs(output - expected) * 10000 > expected * self.settings.policy.slippage_bps:
            raise ValueError("Local quote and deployed program simulation diverge")
        fee = await self.rpc.call(
            "getFeeForMessage", [base64.b64encode(bytes(msg)).decode(), {"commitment": "finalized"}]
        )
        if fee["value"] is None or not 0 <= fee["value"] <= self.settings.policy.max_network_lamports:
            raise ValueError("Direct network fee exceeds policy")
        if not 0 <= time.time() - q["_observed_at"] <= self.settings.policy.quote_max_age_s:
            raise ValueError("Direct quote expired before signing")
        signed = VersionedTransaction(msg, [self._key])
        stock = source if source != USDC_MINT else target
        return {
            "transaction": base64.b64encode(bytes(signed)).decode(),
            "signature": str(signed.signatures[0]),
            "min_slot": min_slot,
            "last_valid_height": latest["value"]["lastValidBlockHeight"],
            "minimum": minimum,
            "input": amount,
            "input_mint": source,
            "output_mint": target,
            "input_before": input_info["amount"],
            "output_before": output_info["amount"],
            "addresses": addresses,
            "prepared_at": time.time(),
            "quote_at": q["_observed_at"],
            "pool": q["pool"],
            "wallet": wallet,
            "fee_lamports": fee["value"],
            "wallet_stock": input_info["amount"] if source == stock else output_info["amount"],
            "wallet_usdc": input_info["amount"] if source == USDC_MINT else output_info["amount"],
        }

    async def send(self, prepared: dict) -> str:
        if prepared["wallet"] != str(self._key.pubkey()):
            raise ValueError("Prepared direct wallet mismatch")
        if not 0 <= time.time() - prepared["quote_at"] <= self.settings.policy.quote_max_age_s:
            raise ValueError("Prepared direct quote expired; reconcile existing intent")
        height = await self.rpc.call("getBlockHeight", [{"commitment": "finalized"}])
        if height > prepared["last_valid_height"]:
            raise ValueError("Prepared direct blockhash expired")
        current = await self.rpc.accounts(prepared["addresses"], min_slot=prepared["min_slot"])
        if (
            decode_token(current["value"][1])["amount"] != prepared["input_before"]
            or decode_token(current["value"][2])["amount"] != prepared["output_before"]
        ):
            raise ValueError("Wallet inventory changed after direct simulation")
        if not 0 <= time.time() - prepared["quote_at"] <= self.settings.policy.quote_max_age_s:
            raise ValueError("Direct quote expired during submission checks")
        signature = await self.rpc.send(prepared["transaction"], prepared["min_slot"])
        if signature != prepared["signature"]:
            raise ValueError("Direct RPC signature mismatch; reconcile original signature")
        return signature


class DirectJournal:
    def __init__(self, path: str):
        target = Path(path).resolve()
        self.path = str(target)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(target, isolation_level=None)
        os.chmod(target, 0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS swaps(id TEXT PRIMARY KEY, wallet TEXT NOT NULL,
            status TEXT NOT NULL, created REAL NOT NULL, payload TEXT NOT NULL, receipt TEXT);
          CREATE UNIQUE INDEX IF NOT EXISTS one_active_direct_wallet ON swaps(wallet)
            WHERE status IN ('PREPARING','PREPARED','SENDING','UNKNOWN','SUBMITTED');
        """)

    def close(self) -> None:
        self.db.close()

    def begin(self, wallet: str, request: dict) -> str:
        identity = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO swaps VALUES(?,?,?,?,?,NULL)",
            (identity, wallet, "PREPARING", time.time(), json.dumps(request)),
        )
        return identity

    def get(self, identity: str) -> dict:
        row = self.db.execute(
            "SELECT id,wallet,status,payload,receipt FROM swaps WHERE id=?", (identity,)
        ).fetchone()
        if row is None:
            raise ValueError("Unknown direct swap intent")
        return {
            "id": row[0],
            "wallet": row[1],
            "status": row[2],
            "payload": json.loads(row[3]),
            "receipt": json.loads(row[4]) if row[4] else None,
        }

    def update(
        self, identity: str, status: str, *, payload: dict | None = None, receipt: dict | None = None
    ) -> None:
        self.db.execute(
            "UPDATE swaps SET status=?,payload=coalesce(?,payload),receipt=coalesce(?,receipt) WHERE id=?",
            (
                status,
                json.dumps(payload) if payload else None,
                json.dumps(receipt) if receipt else None,
                identity,
            ),
        )

    async def execute(self, guard: DirectSwapGuard, quote: dict, minimum: int) -> dict:
        wallet = str(guard._key.pubkey())
        # This committed row reserves the wallet before asynchronous preparation.
        identity = self.begin(
            wallet,
            {
                "input_mint": quote["inputMint"],
                "output_mint": quote["outputMint"],
                "input": int(quote["inAmount"]),
                "minimum": minimum,
            },
        )
        try:
            WalletLease.reserve(wallet, "direct", self.path, identity)
            prepared = await guard.prepare(quote, int(quote["inAmount"]), minimum)
        except BaseException:
            self.update(identity, "CANCELLED")
            if WalletLease.fence(wallet).exists():
                owner = json.loads(WalletLease.fence(wallet).read_text())
                if owner.get("id") == identity and owner.get("database") == self.path:
                    WalletLease.clear(wallet, "direct", self.path, identity)
            raise
        self.update(identity, "PREPARED", payload=prepared)
        self.update(identity, "SENDING")  # Commit before the first sendTransaction attempt.
        try:
            await guard.send(prepared)
        except BaseException:
            self.update(identity, "UNKNOWN")
            raise
        self.update(identity, "SUBMITTED")
        return {"id": identity, "status": "SUBMITTED", "signature": prepared["signature"], "minimum": minimum}

    async def reconcile(self, identity: str, rpc: SolanaRPC) -> dict:
        row = self.get(identity)
        p = row["payload"]
        if row["status"] in {"COMPLETE", "FAILED", "CANCELLED"}:
            WalletLease.clear(row["wallet"], "direct", self.path, identity)
            return {k: v for k, v in row.items() if k != "payload"}
        fence = WalletLease.fence(row["wallet"])
        if fence.exists() and json.loads(fence.read_text()) != {
            "kind": "direct",
            "database": self.path,
            "id": identity,
        }:
            raise ValueError("Wallet reservation belongs to another unresolved execution")
        if row["status"] in {"PREPARING", "PREPARED"}:
            # No send can happen before SENDING is durably committed.
            self.update(identity, "CANCELLED")
            WalletLease.clear(row["wallet"], "direct", self.path, identity)
            return {"id": identity, "status": "CANCELLED", "reason": "Never submitted"}
        tx = await rpc.transaction(p["signature"])
        if tx is not None:
            if tx["transaction"]["signatures"][0] != p["signature"]:
                raise ValueError("Finalized direct signature mismatch")
            if tx["meta"]["err"] is not None:
                self.update(identity, "FAILED", receipt={"signature": p["signature"], "failed_onchain": True})
            else:
                stock = p["input_mint"] if p["input_mint"] != USDC_MINT else p["output_mint"]
                delta = transaction_deltas(tx, row["wallet"], stock)
                incoming, outgoing = (
                    (delta["usdc"], delta["stock"])
                    if p["input_mint"] == USDC_MINT
                    else (delta["stock"], delta["usdc"])
                )
                if incoming != -p["input"] or outgoing < p["minimum"] or delta["fee"] > p["fee_lamports"]:
                    raise ValueError("Finalized direct debit/credit/fee mismatch")
                self.update(
                    identity,
                    "COMPLETE",
                    receipt={
                        "signature": p["signature"],
                        "input": -incoming,
                        "output": outgoing,
                        "fee_lamports": delta["fee"],
                    },
                )
        # A missing transaction/status remains unknown; expiry alone is not proof of no landing.
        row = self.get(identity)
        if row["status"] in {"COMPLETE", "FAILED"}:
            WalletLease.clear(row["wallet"], "direct", self.path, identity)
        return {k: v for k, v in row.items() if k != "payload"}
