from __future__ import annotations

import base64
import struct

from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from .models import ATA, TOKEN, USDC_MINT, Policy
from .solana import SolanaRPC, associated_address, decode_token

SYSTEM = "11111111111111111111111111111111"


def usdc_transfer_instructions(wallet: str, recipient: str, amount: int) -> list[Instruction]:
    if wallet == recipient or not 0 < amount < 2**63:
        raise ValueError("Invalid capital-return recipient/amount")
    src, dst = associated_address(wallet, USDC_MINT, TOKEN), associated_address(recipient, USDC_MINT, TOKEN)

    def meta(key, signer=False, writable=False):
        return AccountMeta(Pubkey.from_string(key), signer, writable)

    create = Instruction(
        Pubkey.from_string(ATA),
        b"\x01",
        [
            meta(wallet, True, True),
            meta(dst, False, True),
            meta(recipient),
            meta(USDC_MINT),
            meta(SYSTEM),
            meta(TOKEN),
        ],
    )
    transfer = Instruction(
        Pubkey.from_string(TOKEN),
        b"\x0c" + struct.pack("<Q", amount) + b"\x06",
        [meta(src, False, True), meta(USDC_MINT), meta(dst, False, True), meta(wallet, True)],
    )
    return [create, transfer]


class USDCTransfer:
    def __init__(self, rpc: SolanaRPC, key: Keypair, policy: Policy):
        self.rpc, self._key, self.policy = rpc, key, policy

    async def prepare(self, recipient: str, amount: int) -> dict:
        wallet = str(self._key.pubkey())
        instructions = usdc_transfer_instructions(wallet, recipient, amount)
        src, dst = (
            associated_address(wallet, USDC_MINT, TOKEN),
            associated_address(recipient, USDC_MINT, TOKEN),
        )
        addresses = [wallet, src, dst]
        current = await self.rpc.accounts(addresses)
        payer, source, destination = current["value"]
        before = decode_token(source)
        if before["owner"] != wallet or before["mint"] != USDC_MINT or before["amount"] < amount:
            raise ValueError("Invalid USDC source balance/owner/mint")
        if (
            before["delegate"]
            or before["close_authority"]
            or payer["lamports"] < self.policy.min_wallet_lamports
        ):
            raise ValueError("Source authority or gas reserve invalid")
        dest_before = decode_token(destination)
        if destination and (dest_before["owner"] != recipient or dest_before["mint"] != USDC_MINT):
            raise ValueError("Backpack USDC recipient ATA mismatch")
        latest = await self.rpc.call("getLatestBlockhash", [{"commitment": "finalized"}])
        msg = MessageV0.try_compile(
            self._key.pubkey(), instructions, [], Hash.from_string(latest["value"]["blockhash"])
        )
        unsigned = base64.b64encode(bytes(VersionedTransaction.populate(msg, [Signature.default()]))).decode()
        sim = await self.rpc.simulate(unsigned, addresses, current["context"]["slot"])
        value = sim["value"]
        if value.get("err") or not value.get("accounts"):
            raise ValueError("USDC return simulation failed")
        post_payer, post_source, post_destination = value["accounts"]
        a, b = decode_token(post_source), decode_token(post_destination)
        if a["amount"] != before["amount"] - amount or a["control"] != before["control"]:
            raise ValueError("USDC return simulated debit/authority mismatch")
        if b["owner"] != recipient or b["mint"] != USDC_MINT or b["amount"] - dest_before["amount"] != amount:
            raise ValueError("USDC return simulated recipient/credit mismatch")
        if destination and b["control"] != dest_before["control"]:
            raise ValueError("Recipient control fields changed")
        rent = post_destination["lamports"] if destination is None else 0
        if post_payer["owner"] != SYSTEM or not 0 <= payer["lamports"] - post_payer["lamports"] <= (
            self.policy.max_network_lamports + rent
        ):
            raise ValueError("USDC return simulated SOL outflow exceeds bound")
        if post_payer.get("data") != payer.get("data"):
            raise ValueError("USDC return modified payer account data")
        fee = await self.rpc.call(
            "getFeeForMessage", [base64.b64encode(bytes(msg)).decode(), {"commitment": "finalized"}]
        )
        if fee["value"] is None or fee["value"] > self.policy.max_network_lamports:
            raise ValueError("USDC return network fee unavailable/exceeds bound")
        signed = VersionedTransaction(msg, [self._key])
        return {
            "signature": str(signed.signatures[0]),
            "transaction": base64.b64encode(bytes(signed)).decode(),
            "min_slot": current["context"]["slot"],
            "recipient": recipient,
            "quantity": amount,
            "source_before": before["amount"],
            "last_valid_height": latest["value"]["lastValidBlockHeight"],
        }
