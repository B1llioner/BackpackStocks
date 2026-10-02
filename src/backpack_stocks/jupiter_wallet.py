"""Local wallet loading and partial signing; no key material leaves this process."""

from __future__ import annotations

import base64
import json
import os
import re
from collections.abc import Mapping

from solders.keypair import Keypair
from solders.message import to_bytes_versioned
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from .jupiter_replay import ReplayError

JUPITER_MANAGED_SIGNER = "sighWH8KaiT7QhtV4w29ReVF8kG6D5yG3EQP1KYyGVF"
JUPITER_GAS_SIGNER = "gasTzr94Pmp4Gf8vknQnqxeYxdgwFjbgdJa4msYRpnB"


def wallet_from_environment(
    env_name: str = "SOLANA_WALLET_KEY", *, environ: Mapping[str, str] | None = None
) -> Keypair:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
        raise ReplayError("Invalid wallet environment variable name")
    value = (os.environ if environ is None else environ).get(env_name, "").strip()
    if not value:
        raise ReplayError(f"Missing wallet in {env_name}")
    try:
        if len(value) > 4096:
            raise ValueError
        if value.startswith("["):
            rows = json.loads(value)
            if not isinstance(rows, list) or not all(type(v) is int and 0 <= v <= 255 for v in rows):
                raise ValueError
            raw = bytes(rows)
        elif value.startswith("base64:"):
            raw = base64.b64decode(value[7:], validate=True)
        else:
            # Solana/Phantom base58 export, with the public half validated by solders.
            key = Keypair.from_base58_string(value)
            raw = bytes(key)
        if len(raw) not in {32, 64}:
            raise ValueError
        key = Keypair.from_seed(raw[:32])
        if len(raw) == 64 and bytes(key) != raw:
            raise ValueError
        return key
    except (ValueError, TypeError, OverflowError):
        raise ReplayError(
            "Invalid wallet key; expected base58 keypair, JSON bytes or base64: bytes"
        ) from None


def decode_swap_transaction(encoded: str) -> VersionedTransaction:
    try:
        if not isinstance(encoded, str) or len(encoded) > 100_000:
            raise ValueError
        tx = VersionedTransaction.from_bytes(base64.b64decode(encoded, validate=True))
        tx.sanitize()
        if len(tx.signatures) != tx.message.header.num_required_signatures:
            raise ValueError
        return tx
    except (ValueError, TypeError):
        raise ReplayError("Unsupported/malformed transaction bytes; no signing or submission") from None


def verify_swap_signatures(tx: VersionedTransaction, quote: dict, *, require_taker: bool) -> None:
    taker = quote.get("taker")
    signers = tx.message.account_keys[: tx.message.header.num_required_signatures]
    addresses = [str(key) for key in signers]
    if not isinstance(taker, str) or taker not in addresses:
        raise ReplayError("Wallet is not a required transaction signer")
    fee_payer = quote.get("signatureFeePayer")
    if fee_payer is not None and fee_payer != addresses[0]:
        raise ReplayError("Transaction fee payer differs from order")
    message = to_bytes_versioned(tx.message)
    # JupiterZ's maker adds its one missing signature after the taker's /execute POST.
    external = [
        address for address in addresses if address not in {JUPITER_MANAGED_SIGNER, JUPITER_GAS_SIGNER}
    ]
    rfq = str(quote.get("router", "")).lower() == "jupiterz" and len(external) <= 2
    for index, (address, pubkey, signature) in enumerate(zip(addresses, signers, tx.signatures, strict=True)):
        if signature != Signature.default():
            if not signature.verify(pubkey, message):
                raise ReplayError("Invalid existing transaction signature")
        elif address == taker:
            if require_taker:
                raise ReplayError("Missing taker signature")
        elif address == JUPITER_MANAGED_SIGNER:
            # Observed v2a Metis transaction: Jupiter's extra signer is read-only.
            # UI signs only the taker; the remaining signature is an execute-service responsibility.
            if index < len(signers) - tx.message.header.num_readonly_signed_accounts:
                raise ReplayError("Managed Jupiter signer unexpectedly has writable privileges")
        elif address == JUPITER_GAS_SIGNER and quote.get("gasless") is True and fee_payer == address:
            pass
        elif not rfq:
            raise ReplayError("Missing non-wallet signature for this router")


def sign_swap_transaction(prepared: dict, wallet: Keypair) -> str:
    quote = prepared["quote"]
    if quote.get("taker") != str(wallet.pubkey()):
        raise ReplayError("Order taker differs from the signing wallet")
    if not quote.get("transaction"):
        raise ReplayError("Preview order has no transaction to sign")
    tx = decode_swap_transaction(quote["transaction"])
    verify_swap_signatures(tx, quote, require_taker=False)
    signers = tx.message.account_keys[: tx.message.header.num_required_signatures]
    signatures = list(tx.signatures)
    signatures[list(signers).index(wallet.pubkey())] = wallet.sign_message(to_bytes_versioned(tx.message))
    signed = VersionedTransaction.populate(tx.message, signatures)
    verify_swap_signatures(signed, quote, require_taker=True)
    return base64.b64encode(bytes(signed)).decode("ascii")
