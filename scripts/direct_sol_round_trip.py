"""Quote/simulate, or explicitly execute, a single SOL -> stock -> SOL round trip."""

import argparse
import asyncio
import json
import os
import time
from decimal import Decimal
from pathlib import Path

from backpack_stocks.cli import load_settings
from backpack_stocks.direct import WSOL, DirectRaydium
from backpack_stocks.direct_execution import WalletLease
from backpack_stocks.environment import load_environment
from backpack_stocks.jupiter_wallet import wallet_from_environment
from backpack_stocks.models import dec
from backpack_stocks.native_direct import NativeDirectGuard, NativeDirectJournal
from backpack_stocks.observability import configure, error_code
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy
from backpack_stocks.solana import SolanaRPC


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(value, out)
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(path)


async def execute_leg(journal, guard, quote, minimum, path, context, identity_key):
    # If begin() fails, the previous completed leg must never become this leg's
    # receipt. Only associate a newly inserted intent with the saved context.
    last_row = journal.db.execute("SELECT coalesce(max(rowid), 0) FROM swaps").fetchone()[0]
    try:
        return await journal.execute(guard, quote, minimum)
    finally:
        rows = journal.db.execute(
            "SELECT id FROM swaps WHERE wallet=? AND rowid>? ORDER BY rowid",
            (context["wallet"], last_row),
        ).fetchall()
        if len(rows) == 1:
            context[identity_key] = rows[0][0]
            save(path, context)


async def run(args):
    load_environment(args.env_file, [*PROXY_ENV_NAMES, "SOLANA_WALLET_KEY"])
    key = wallet_from_environment()
    s = load_settings(args.config)
    s.wallet = str(key.pubkey())
    s.direct.enable_live = True  # Standalone flag below; paper runtime remains untouched.
    if not 0 < dec(args.usdc_value) <= dec(s.policy.max_cost_usdc):
        raise ValueError("Round-trip value exceeds configured standalone cap")
    s.policy.slippage_bps = 50
    configure("state/direct-sol-round-trip-log.jsonl")
    rpc = SolanaRPC(s.rpc_url, interval=s.rpc_interval_s, proxy=resolve_proxy("SOLANA"))
    router = DirectRaydium(s, rpc)
    journal = None
    lease = None
    path = Path(args.state)
    context = None

    async def pool_quote(source, target, amount):
        mint = target if source == WSOL else source
        await router.ensure_candidates(mint)
        values = await asyncio.gather(
            *(
                router._limited_quote(pool, source, target, amount, 10)
                for pool in router.sol_candidates.get(mint, [])
            ),
            return_exceptions=True,
        )
        valid = [
            q
            for q in values
            if isinstance(q, dict) and 0 <= time.time() - q["_observed_at"] <= s.policy.quote_max_age_s
        ]
        if not valid:
            raise ValueError("No fresh direct stock/SOL pool")
        return max(valid, key=lambda q: int(q["outAmount"]))

    async def settle(identity):
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            result = await journal.reconcile(identity, rpc)
            if result["status"] in ("COMPLETE", "FAILED", "CANCELLED"):
                if result["status"] != "COMPLETE":
                    raise ValueError("Direct transaction did not complete; original intent retained")
                return result["receipt"]
            await asyncio.sleep(4)
        raise TimeoutError("Pending direct transaction; reconcile original, do not resend")

    try:
        asset = router.by_symbol[args.symbol]
        if args.execute:
            lease = WalletLease(s.wallet)
            if path.exists() and not args.resume:
                raise ValueError("Round-trip state exists; inspect/resume it instead of buying again")
            journal = NativeDirectJournal("state/direct-native-swaps.sqlite3")
            if args.resume:
                context = json.loads(path.read_text())
                if context["wallet"] != s.wallet or context["symbol"] != args.symbol:
                    raise ValueError("Round-trip context wallet/asset mismatch")
                if context["phase"] == "COMPLETE":
                    print(json.dumps({k: v for k, v in context.items() if k != "wallet"}), flush=True)
                    return
            elif WalletLease.fence(s.wallet).exists():
                raise ValueError("Wallet has an unresolved execution; reconcile its original database")
        if context is None:
            prices = await router.prices([WSOL])
            # Value is an explicit input budget, distinct from pool output or net.
            price = dec(prices[WSOL]["usdPrice"])
            amount = int(dec(args.usdc_value) / price * 1_000_000_000)
            if amount <= 0:
                raise ValueError("SOL input rounds to zero")
            before = await rpc.wallet_snapshot(s.wallet, asset["mint"])
            q = await pool_quote(WSOL, asset["mint"], amount)
            print(
                json.dumps(
                    {
                        "phase": "QUOTE",
                        "symbol": args.symbol,
                        "pool": q["pool"],
                        "sol_input": str(Decimal(amount) / 10**9),
                        "usdc_value": args.usdc_value,
                        "stock_quote": str(Decimal(q["outAmount"]) / 10**6),
                        "stock_minimum": str(Decimal(q["otherAmountThreshold"]) / 10**6),
                    }
                ),
                flush=True,
            )
            guard = NativeDirectGuard(router, rpc, key, s)
            if not args.execute:
                prepared = await guard.prepare(q, amount, int(q["otherAmountThreshold"]))
                print(
                    json.dumps(
                        {
                            "phase": "SIMULATED",
                            "fee_lamports": prepared["fee_lamports"],
                            "financial_mutations": 0,
                        }
                    ),
                    flush=True,
                )
                return
            context = {
                "phase": "BUYING",
                "wallet": s.wallet,
                "symbol": args.symbol,
                "input_lamports": amount,
                "sol_before": before["lamports"],
                "stock_before": before["stock"],
                "close_stock": before["accounts"][1] is None,
            }
            save(path, context)  # Crash before execute is explicit, never an automatic retry.
            result = await execute_leg(
                journal, guard, q, int(q["otherAmountThreshold"]), path, context, "buy_id"
            )
            print(json.dumps(result), flush=True)
        if context["phase"] == "BUYING":
            receipt = await settle(context["buy_id"])
            context.update(phase="BOUGHT", acquired_raw=receipt["stock_change_raw"], buy_receipt=receipt)
            save(path, context)
            print(json.dumps({"phase": "BOUGHT", "receipt": receipt}), flush=True)
        if context["phase"] == "BOUGHT":
            q = await pool_quote(asset["mint"], WSOL, context["acquired_raw"])
            guard = NativeDirectGuard(router, rpc, key, s, close_stock=context["close_stock"])
            context["phase"] = "SELLING"
            save(path, context)
            result = await execute_leg(
                journal, guard, q, int(q["otherAmountThreshold"]), path, context, "sell_id"
            )
            print(json.dumps(result), flush=True)
        if context["phase"] == "SELLING":
            receipt = await settle(context["sell_id"])
            context.update(
                phase="COMPLETE",
                sell_receipt=receipt,
                sol_balance_change_lamports=context["buy_receipt"]["sol_change_lamports"]
                + receipt["sol_change_lamports"],
                fee_lamports=context["buy_receipt"]["fee_lamports"] + receipt["fee_lamports"],
            )
            save(path, context)
            print(json.dumps({k: v for k, v in context.items() if k != "wallet"}), flush=True)
    finally:
        if journal:
            journal.close()
        if lease:
            lease.close()
        await router.close()
        await rpc.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="examples/telegram-direct-paper-auto.toml")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--symbol", default="WEN.US")
    p.add_argument("--usdc-value", default="10")
    p.add_argument("--state", default="state/direct-sol-round-trip.json")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--resume", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as error:
        reason = (
            str(error)
            if str(error).startswith(("Native direct", "Native prepared", "No fresh direct"))
            else None
        )
        print(
            json.dumps(
                {
                    "phase": "STOPPED",
                    "code": error_code(error),
                    "type": type(error).__name__,
                    "reason": reason,
                    "action": "inspect_saved_intent_before_any_repeat",
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
