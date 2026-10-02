"""Standalone stock/USDC direct Raydium swaps; quote never loads private keys."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json

from solders.keypair import Keypair

from .cli import load_settings
from .direct import DirectRaydium
from .direct_execution import DirectJournal, DirectSwapGuard, WalletLease
from .environment import load_environment
from .models import USDC_MINT, raw
from .proxy import PROXY_ENV_NAMES, resolve_proxy
from .runtime import required_env
from .solana import SolanaRPC


async def run(args) -> dict:
    settings = load_settings(args.config)
    if settings.dex_backend != "raydium":
        raise ValueError("Direct CLI requires dex_backend=raydium")
    if args.command == "status":
        status_journal = DirectJournal(settings.direct.journal)
        try:
            return {
                "swaps": [
                    {"id": r[0], "wallet": r[1], "status": r[2], "created": r[3]}
                    for r in status_journal.db.execute(
                        "SELECT id,wallet,status,created FROM swaps ORDER BY created DESC LIMIT 100"
                    )
                ]
            }
        finally:
            status_journal.close()
    names = list(PROXY_ENV_NAMES)
    if args.command == "swap":
        if not args.live or not settings.direct.enable_live or not settings.wallet:
            raise ValueError(
                "Standalone real swap requires --live, direct.enable_live=true and a configured wallet"
            )
        names.append(settings.wallet_seed_env)
    load_environment(args.env_file, names)
    rpc = SolanaRPC(settings.rpc_url, interval=settings.rpc_interval_s, proxy=resolve_proxy("SOLANA"))
    router = None
    journal = None
    lease = None
    try:
        if args.command == "reconcile":
            journal = DirectJournal(settings.direct.journal)
            wallet = journal.get(args.id)["wallet"]
            lease = WalletLease(wallet)
            return await journal.reconcile(args.id, rpc)
        router = DirectRaydium(settings, rpc)
        asset = router.by_symbol[args.symbol]
        source, target = (
            (asset["mint"], USDC_MINT) if args.direction == "sell" else (USDC_MINT, asset["mint"])
        )
        amount = raw(args.amount, asset["decimals"] if args.direction == "sell" else 6)
        if args.command == "swap":
            encoded = base64.b64decode(required_env(settings.wallet_seed_env), validate=True)
            key = Keypair.from_seed(encoded) if len(encoded) == 32 else Keypair.from_bytes(encoded)
            if str(key.pubkey()) != settings.wallet:
                raise ValueError("Direct key does not match wallet")
            lease = WalletLease(settings.wallet)
            if WalletLease.fence(settings.wallet).exists():
                raise ValueError("Wallet has unresolved execution; reconcile its original journal first")
            if amount <= 0 or (
                args.direction == "sell" and amount > raw(settings.policy.quantity, asset["decimals"])
            ):
                raise ValueError("Direct stock input exceeds configured quantity limit")
            if args.direction == "buy" and amount > raw(settings.policy.max_cost_usdc):
                raise ValueError("Direct USDC input exceeds cost cap")
        q = await router.quote_pair(source, target, amount)
        public = {k: v for k, v in q.items() if not k.startswith("_")}
        if args.command == "quote":
            return {"symbol": args.symbol, "financial_mutations": 0, "quote": public}
        if args.direction == "sell" and int(q["outAmount"]) > raw(settings.policy.max_cost_usdc):
            raise ValueError("Direct sell notional exceeds configured USDC exposure cap")
        requested_minimum = raw(args.min_out, 6 if args.direction == "sell" else asset["decimals"])
        if requested_minimum <= 0:
            raise ValueError("Explicit positive --min-out is required")
        minimum = max(requested_minimum, int(q["otherAmountThreshold"]))
        journal = DirectJournal(settings.direct.journal)
        result = await journal.execute(DirectSwapGuard(router, rpc, key, settings), q, minimum)
        return {**result, "symbol": args.symbol, "quote": public}
    finally:
        if journal:
            journal.close()
        if lease:
            lease.close()
        if router:
            await router.close()
        await rpc.close()


def main():
    p = argparse.ArgumentParser(description="Direct Raydium stock/USDC, no Jupiter")
    p.add_argument("--config", default="examples/direct-swap.toml")
    p.add_argument("--env-file", default=".env")
    commands = p.add_subparsers(dest="command", required=True)
    for name in ("quote", "swap"):
        command = commands.add_parser(name)
        command.add_argument("--symbol", required=True)
        command.add_argument("--direction", choices=["buy", "sell"], required=True)
        command.add_argument("--amount", required=True, help="Input UI units: stock for sell, USDC for buy")
        if name == "swap":
            command.add_argument("--min-out", required=True, help="Minimum output UI units")
            command.add_argument("--live", action="store_true")
    commands.add_parser("status")
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("id")
    args = p.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args)), indent=2, default=str, allow_nan=False))
    except Exception as error:
        raise SystemExit(
            f"Direct command failed ({type(error).__name__}); use status/reconcile for submission outcomes"
        ) from None


if __name__ == "__main__":
    main()
