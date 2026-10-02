"""Browser-free Jupiter requests and simulation-checked swaps through a required proxy."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path

from solders.keypair import Keypair

from backpack_stocks.direct_execution import WalletLease
from backpack_stocks.environment import load_environment
from backpack_stocks.jupiter_proxy import JUPITER_PROXY_ENV_NAMES, replay_proxy
from backpack_stocks.jupiter_replay import (
    SOL,
    USDC,
    JupiterReplay,
    ReplayError,
    SwapInput,
    captured_frontend_key,
    sanitized,
)
from backpack_stocks.jupiter_swap import (
    DEFAULT_RPC,
    ReplayRPC,
    SwapJournal,
    prepare_swap,
    reconcile_swap,
    round_trip,
    submit_swap,
)
from backpack_stocks.jupiter_wallet import wallet_from_environment


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "command",
        choices=("params", "wallet", "quote", "prepare", "auth", "execute", "swap", "round-trip", "status"),
    )
    p.add_argument("--backend", choices=("website", "developer", "ultra"), default="website")
    p.add_argument("--transport", choices=("httpx", "curl_cffi"))
    p.add_argument("--data-dir", type=Path, default=Path("data/jupiter"))
    p.add_argument("--proxy-env", help="read the proxy from this environment variable only")
    p.add_argument(
        "--env-file", type=Path, help="explicit local file to load this command's required variables"
    )
    p.add_argument("--wallet-env", default="SOLANA_WALLET_KEY")
    p.add_argument("--rpc-env", default="SOLANA_RPC_URL")
    p.add_argument("--state-dir", type=Path, default=Path("state/jupiter-swaps"))
    p.add_argument("--request-id", help="requestId to reconcile with status")
    p.add_argument(
        "--resume-buy-request-id", help="round-trip: resume only the sell after this confirmed buy"
    )
    p.add_argument("--max-sol-cost-lamports", type=int, default=5_000_000)
    p.add_argument("--min-output-raw", type=int, help="additional minimum output in raw token units")
    p.add_argument("--usdc-value", default="10", help="round-trip reference value in USDC")
    p.add_argument(
        "--no-frontend-key", action="store_true", help="diagnostic: omit publishable frontend config"
    )
    p.add_argument("--input-mint", default=SOL)
    p.add_argument("--output-mint", default=USDC)
    p.add_argument("--amount", help="required explicitly for swap; quote/prepare default to 0.01")
    p.add_argument(
        "--decimals", type=int, default=9, help="decimals of input for ExactIn, output for ExactOut"
    )
    p.add_argument("--profile", choices=("ultra", "manual"), default="ultra")
    p.add_argument("--swap-mode", choices=("ExactIn", "ExactOut"), default="ExactIn")
    p.add_argument("--wallet")
    p.add_argument("--insufficient-balance", action="store_true")
    p.add_argument("--transaction-version", type=int, choices=(0, 1), default=0)
    p.add_argument("--slippage-bps", type=int, default=50)
    p.add_argument("--priority-fee-lamports", type=int, default=1_000_000)
    p.add_argument("--jito-tip-lamports", type=int)
    p.add_argument("--broadcast-fee-type", choices=("maxCap", "exactFee"), default="maxCap")
    p.add_argument("--use-wsol", action="store_true")
    p.add_argument("--exclude-dexes", default="")
    p.add_argument("--exclude-routers", default="")
    p.add_argument("--require-durability", action="store_true", default=None)
    p.add_argument(
        "--disposable-wallet", action="store_true", help="generate an in-memory unfunded auth wallet"
    )
    p.add_argument("--check-vault", action="store_true", help="read protected Trigger vault after login")
    p.add_argument("--refresh", action="store_true", help="test refresh and logout in the same auth session")
    p.add_argument("--output", type=Path)
    p.add_argument("--audit", type=Path, default=Path("data/jupiter/replay.jsonl"))
    p.add_argument("--prepared-file", type=Path)
    p.add_argument("--signed-file", type=Path)
    p.add_argument(
        "--execute",
        action="store_true",
        help="allow submitting swap(s); swap/round-trip otherwise only simulate",
    )
    return p


async def run(args: argparse.Namespace) -> None:
    if args.command == "swap" and not args.amount:
        raise ReplayError("swap requires an explicit --amount")
    swap = SwapInput(
        input_mint=args.input_mint,
        output_mint=args.output_mint,
        amount=args.amount or "0.01",
        amount_decimals=args.decimals,
        profile=args.profile,
        swap_mode=args.swap_mode,
        wallet=args.wallet,
        has_enough_balance=not args.insufficient_balance,
        max_transaction_version=args.transaction_version,
        slippage_bps=args.slippage_bps,
        priority_fee_lamports=args.priority_fee_lamports,
        jito_tip_lamports=args.jito_tip_lamports,
        broadcast_fee_type=args.broadcast_fee_type,
        use_wsol=args.use_wsol,
        exclude_dexes=tuple(filter(None, args.exclude_dexes.split(","))),
        exclude_routers=tuple(filter(None, args.exclude_routers.split(","))),
        require_durability=args.require_durability,
    )
    if args.command == "params":
        print(json.dumps(swap.params(), indent=2))
        return
    needs_wallet = (
        args.command in {"wallet", "swap", "round-trip"}
        or (args.command == "prepare" and not args.wallet)
        or (args.command == "auth" and not args.disposable_wallet)
    )
    if args.env_file:
        load_environment(
            args.env_file,
            (
                *JUPITER_PROXY_ENV_NAMES,
                "JUPITER_REPLAY_WEBSITE_KEY",
                "JUPITER_REPLAY_API_KEY",
                "JUPITER_API_KEY",
                *((args.proxy_env,) if args.proxy_env else ()),
                *((args.wallet_env, "JUPITER_REPLAY_KEYPAIR") if needs_wallet else ()),
                *((args.rpc_env,) if args.command in {"swap", "round-trip", "status"} else ()),
            ),
        )
    wallet = None
    if needs_wallet:
        wallet_name = args.wallet_env
        if (
            args.command == "auth"
            and not os.environ.get(wallet_name)
            and os.environ.get("JUPITER_REPLAY_KEYPAIR")
        ):
            wallet_name = "JUPITER_REPLAY_KEYPAIR"
        wallet = wallet_from_environment(wallet_name)
        address = str(wallet.pubkey())
        if args.wallet and args.wallet != address:
            raise ReplayError("--wallet differs from the loaded signing wallet")
        swap = replace(swap, wallet=address)
        if args.command == "wallet":
            print(json.dumps({"wallet": address, "keyLoaded": True}))
            return
    try:
        proxy = replay_proxy(env_name=args.proxy_env)
    except ValueError as error:
        raise ReplayError(str(error)) from None
    website_key = os.environ.get("JUPITER_REPLAY_WEBSITE_KEY")
    if args.backend == "website" and not args.no_frontend_key and not website_key:
        website_key = captured_frontend_key(args.data_dir)
    client = JupiterReplay(
        backend=args.backend,
        api_key=os.environ.get("JUPITER_REPLAY_API_KEY") or os.environ.get("JUPITER_API_KEY"),
        website_key=website_key,
        proxy=proxy,
        audit=args.audit,
        transport=args.transport
        or ("curl_cffi" if args.command in {"swap", "round-trip", "status"} else "httpx"),
    )
    try:
        if args.command in {"quote", "prepare"}:
            if args.command == "prepare" and (not args.wallet or args.insufficient_balance):
                raise ReplayError("prepare requires a wallet and sufficient-balance mode")
            result = await client.order(swap)
            if args.command == "prepare" and not result["quote"].get("transaction"):
                raise ReplayError("Server returned no transaction; inspect balance/error in sanitized audit")
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(sanitized(result), indent=2))
            q = result["quote"]
            print(
                json.dumps(
                    {
                        "backend": args.backend,
                        "proxied": client.proxied,
                        "transport": client.transport,
                        "params": result["params"],
                        "result": {
                            k: q.get(k)
                            for k in (
                                "requestId",
                                "inputMint",
                                "outputMint",
                                "inAmount",
                                "outAmount",
                                "slippageBps",
                                "otherAmountThreshold",
                                "router",
                                "taker",
                                "errorCode",
                            )
                        },
                        "hasTransaction": bool(q.get("transaction")),
                    },
                    indent=2,
                )
            )
        elif args.command in {"swap", "round-trip", "status"}:
            rpc = ReplayRPC(client, os.environ.get(args.rpc_env) or DEFAULT_RPC)
            if args.command == "status":
                if not args.request_id:
                    raise ReplayError("status requires --request-id")
                journal = SwapJournal(args.state_dir)
                row = journal.read(args.request_id)
                lease = WalletLease(row["wallet"])
                try:
                    print(json.dumps(await reconcile_swap(rpc, journal, args.request_id), indent=2))
                finally:
                    lease.close()
            else:
                assert wallet is not None
                lease = WalletLease(str(wallet.pubkey())) if args.execute else None
                try:
                    if args.command == "round-trip":
                        result = await round_trip(
                            client,
                            rpc,
                            swap,
                            wallet,
                            args.state_dir,
                            usdc_value=args.usdc_value,
                            max_sol_cost=args.max_sol_cost_lamports,
                            execute=args.execute,
                            resume_buy_request_id=args.resume_buy_request_id,
                            lease=lease,
                        )
                        if args.output:
                            args.output.parent.mkdir(parents=True, exist_ok=True)
                            args.output.write_text(json.dumps(result, indent=2))
                        print(json.dumps(result, indent=2))
                    else:
                        prepared = await prepare_swap(
                            client,
                            rpc,
                            swap,
                            max_sol_cost=args.max_sol_cost_lamports,
                            minimum_output=args.min_output_raw,
                        )
                        print(
                            json.dumps(
                                {
                                    "executed": False,
                                    "proxied": client.proxied,
                                    "transport": client.transport,
                                    "review": prepared["review"],
                                },
                                indent=2,
                            )
                        )
                        if args.execute:
                            result = await submit_swap(
                                client, prepared, wallet, SwapJournal(args.state_dir), lease=lease
                            )
                            print(json.dumps(result, indent=2))
                            if result["phase"] != "CONFIRMED":
                                raise ReplayError(
                                    "Swap not confirmed; check status using the returned requestId"
                                )
                finally:
                    if lease:
                        lease.close()
        elif args.command == "auth":
            if args.disposable_wallet:
                wallet = Keypair()
            assert wallet is not None
            await client.login(wallet)
            print(
                json.dumps(
                    {
                        "authenticated": True,
                        "proxied": client.proxied,
                        "transport": client.transport,
                        "address": client.session.address,
                        "expiresAt": client.session.expires_at,
                    }
                )
            )
            if args.check_vault:
                vault = await client.trigger_vault()
                print(json.dumps({"protectedVaultRead": True, "phase": vault["phase"]}))
            if args.refresh:
                await client.refresh()
                print(json.dumps({"refreshed": True}))
            await client.logout()
            print(json.dumps({"loggedOut": True}))
        else:
            if not args.execute or not args.prepared_file or not args.signed_file:
                raise ReplayError("execute requires --execute, --prepared-file and --signed-file")
            prepared = json.loads(args.prepared_file.read_text())
            signed = args.signed_file.read_text().strip()
            print(json.dumps(sanitized(await client.execute(prepared, signed, allow_execute=True))))
    finally:
        try:
            if args.command == "auth" and client.session:
                await client.logout()
        finally:
            await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(run(parser().parse_args()))
    except (ReplayError, ValueError, OSError) as error:
        # ValueError from key parsing never includes the provided value.
        message = str(error) if isinstance(error, ReplayError) else type(error).__name__
        raise SystemExit("Jupiter replay stopped: " + message) from None
