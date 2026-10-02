from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path

from .environment import load_environment
from .models import Settings
from .proxy import PROXY_ENV_NAMES, resolve_proxy
from .raydium import Raydium
from .runtime import public_clients, runtime


def load_settings(path: str) -> Settings:
    return Settings.model_validate(tomllib.loads(Path(path).read_text()))


async def public_check(settings: Settings):
    ray_proxy = resolve_proxy("RAYDIUM")
    bp, jup, rpc = public_clients(settings)
    pools = Raydium(rpc, proxy=ray_proxy)
    try:
        assets, securities, vaults, mint, ticker = await asyncio.gather(
            bp.assets(), bp.securities(), bp.vaults(), rpc.check_mint(), bp.ticker("WEN.US_USDC")
        )
        discovered = await pools.pools()
        q = await jup.quote(1000000)
        onchain = await pools.pool_onchain("ArXpohFUAsifyZPHsYqgSTqLyA6sQG8KMocJ6zBLG29D")
        print(
            json.dumps(
                {
                    "observed_at_utc": datetime.now(UTC).isoformat(),
                    "assets": [a for a in assets if a["symbol"] == "WEN.US"],
                    "securities": [s for s in securities if s["asset"] == "WEN.US"],
                    "vaults": vaults,
                    "external_ticker": ticker,
                    "mint_slot": mint["context"]["slot"],
                    "dex_backend": settings.dex_backend,
                    "dex_quote": {k: q.get(k) for k in ["inAmount", "outAmount", "router"]},
                    "raydium_pool_count": len(discovered),
                    "pool": onchain,
                },
                indent=2,
            )
        )
    finally:
        await pools.close()
        await bp.close()
        await jup.close()
        await rpc.close()


async def run(settings: Settings, ticks: int | None, *, pilot: bool = False):
    async with runtime(settings) as engine:
        if pilot:
            # Manual one-shot admission. No scanner-created live entry while waiting for a signal.
            async with engine.lock:
                if engine.store.active() is None:
                    await engine.start_if_profitable(pilot=True)
            if engine.store.active() is None:
                print(json.dumps({"pilot": "Pilot admission/attempt limit blocked entry; no RFQ submitted"}))
                return
        count = 0
        while ticks is None or count < ticks:
            status = await engine.tick()
            count += 1
            active = status["active"]
            print(
                json.dumps(
                    {
                        "mode": status["mode"],
                        "active": active["phase"] if active else None,
                        "cycles": len(status["cycles"]),
                        "spread": status["spread"],
                    }
                ),
                flush=True,
            )
            if active and active["phase"] == "PAUSED":
                print(json.dumps({"paused": active["reason"], "cycle_id": active["id"]}), flush=True)
                return
            if not active and (pilot or len(status["cycles"]) >= settings.policy.max_cycles):
                break
            await asyncio.sleep(settings.poll_s)
        print(json.dumps(engine.status(), indent=2))


def main():
    parser = argparse.ArgumentParser(description="Backpack RFQ → Solana → DEX")
    parser.add_argument("--config", default="examples/paper.toml")
    parser.add_argument(
        "--env-file", default=".env", help="Local credential file; exported variables take precedence"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--ticks", type=int)
    run_parser.add_argument("--live", action="store_true", help="Explicitly enable configured real execution")
    pilot_parser = sub.add_parser("pilot", help="Explicit one-cycle live pilot; no automatic scanner entries")
    pilot_parser.add_argument("--live", action="store_true")
    sub.add_parser("public-check", help="Read-only asset, quote, mint and pool probe; loads no signing keys")
    sub.add_parser("status")
    sub.add_parser("resume")
    sub.add_parser("stop")
    export = sub.add_parser("export-pilot", help="Export live pilot evidence with audit_passed=false")
    export.add_argument("--output", default="state/automation-evidence.json")
    serve = sub.add_parser("serve")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--live", action="store_true")
    telegram = sub.add_parser("telegram", help="Private Telegram controller; starts stopped until /resume")
    telegram.add_argument("--live", action="store_true")
    monitor = sub.add_parser("monitor", help="All-stock spread monitor; non-binding Backpack RFQ checks")
    monitor.add_argument("--seconds", type=float)
    monitor.add_argument("--output", help="Save the final public snapshot as JSON")
    stock_quote = sub.add_parser(
        "stock-quote", help="Backpack frontend FMV estimate or non-binding RFQ quote"
    )
    stock_quote.add_argument("--symbol", required=True)
    stock_quote.add_argument("--quantity", required=True)
    stock_quote.add_argument("--rfq", action="store_true", help="Request an AwaitAccept quote; never buy")
    stock_quote.add_argument("--timeout", type=float, default=8)
    stock_buy = sub.add_parser(
        "stock-buy", help="One bounded Backpack stock purchase with a durable request ID"
    )
    stock_buy.add_argument("--symbol", required=True)
    stock_buy.add_argument("--quantity", required=True)
    stock_buy.add_argument("--max-price", required=True, help="Maximum USDC price per share")
    stock_buy.add_argument(
        "--request-id", required=True, help="Reuse this ID to reconcile, never to buy twice"
    )
    stock_buy.add_argument("--live", action="store_true", help="Submit the real price-limited purchase")
    stock_buy.add_argument("--timeout", type=float, default=30)
    stock_buy.add_argument("--journal-dir", default="state/stock-orders")
    args = parser.parse_args()
    try:
        settings = load_settings(args.config)
        execution = {"run", "serve", "pilot", "telegram"}
        if args.command in execution and settings.mode == "live" and not args.live:
            raise ValueError("Real execution requires the --live flag")
        if args.command == "pilot" and (settings.mode != "live" or not settings.live_pilot):
            raise ValueError("pilot requires mode=live and live_pilot=true")
        if args.command in {"run", "serve", "telegram"} and settings.mode == "live" and settings.live_pilot:
            raise ValueError("Use the explicit pilot command before enabling automated live execution")
        names: list[str] = []
        if args.command in {"stock-quote", "stock-buy"}:
            if not 0 < args.timeout <= 120:
                raise ValueError("Stock command timeout must be between 0 and 120 seconds")
            names.extend(PROXY_ENV_NAMES)
            if args.command == "stock-quote" and args.rfq or args.command == "stock-buy" and args.live:
                names.append(settings.backpack_seed_env)
        if settings.source == "public" and args.command in execution | {"monitor", "public-check"}:
            names.extend(PROXY_ENV_NAMES)
        if args.command == "public-check":
            names.extend(PROXY_ENV_NAMES)
            if settings.dex_backend == "jupiter":
                names.append(settings.jupiter_key_env)
        if args.command in execution:
            if settings.source == "public" and (
                settings.dex_backend == "jupiter"
                or settings.monitor.enabled
                and settings.monitor.quote_backend == "jupiter"
            ):
                names.append(settings.jupiter_key_env)
            if settings.mode == "live":
                names.extend([settings.backpack_seed_env, settings.wallet_seed_env])
            elif settings.backpack_private_reads:
                names.append(settings.backpack_seed_env)
            elif settings.source == "public" and settings.monitor.enabled and settings.monitor.rfq_validation:
                names.append(settings.backpack_seed_env)
        if args.command == "serve":
            names.append(settings.api_token_env)
        if args.command == "telegram":
            names.extend(
                ["TELEGRAM_BOT_TOKEN", "TELEGRAM_ALLOWED_USER_ID", "TELEGRAM_PROXY_URL", "OUTBOUND_PROXY_URL"]
            )
        if args.command == "monitor":
            if settings.source != "public" or (args.seconds is not None and args.seconds <= 0):
                raise ValueError("Monitor requires source=public and a positive duration")
            if settings.dex_backend == "jupiter" or settings.monitor.quote_backend == "jupiter":
                names.append(settings.jupiter_key_env)
            if settings.monitor.rfq_validation:
                names.append(settings.backpack_seed_env)
        if names:
            if (
                settings.source == "public"
                and settings.monitor.enabled
                and settings.monitor.jupiter_flow
                and args.command in execution | {"monitor"}
            ):
                names.extend(
                    [
                        "JUPITER_REPLAY_PROXY",
                        "JUPITER_REPLAY_WEBSITE_KEY",
                        settings.monitor.jupiter_wallet_env,
                    ]
                )
            load_environment(args.env_file, names)
        if args.command in {"stock-quote", "stock-buy"}:
            from .stock_cli import run_stock_command

            print(json.dumps(asyncio.run(run_stock_command(args, settings)), indent=2))
        elif args.command == "public-check":
            asyncio.run(public_check(settings))
        elif args.command == "run":
            asyncio.run(run(settings, args.ticks))
        elif args.command == "pilot":
            asyncio.run(run(settings, None, pilot=True))
        elif args.command == "telegram":
            from .telegram import run_telegram

            async def telegram_control():
                async with runtime(settings) as engine:
                    await run_telegram(engine)

            asyncio.run(telegram_control())
        elif args.command == "monitor":
            from .monitor import SpreadMonitor

            async def monitor_control():
                from .backpack import BackpackAuth

                bp, jup, rpc = public_clients(settings)
                if settings.monitor.rfq_validation and os.environ.get(settings.backpack_seed_env):
                    bp.auth = BackpackAuth(os.environ[settings.backpack_seed_env])
                monitor = SpreadMonitor(settings, bp, jup, rpc)
                await monitor.start()
                started = asyncio.get_running_loop().time()
                try:
                    while args.seconds is None or asyncio.get_running_loop().time() - started < args.seconds:
                        print(monitor.text(snapshot=monitor.report()), flush=True)
                        await asyncio.sleep(min(5, args.seconds or 5))
                    snapshot = monitor.snapshot()
                    if args.output:
                        target = Path(args.output)
                        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                        target.write_text(json.dumps(snapshot, indent=2))
                    print(json.dumps({k: v for k, v in snapshot.items() if k != "rows"}, indent=2))
                finally:
                    await monitor.close()
                    for client in (bp, jup, rpc):
                        await client.close()

            asyncio.run(monitor_control())
        elif args.command == "serve":
            import uvicorn

            from .api import create_app

            uvicorn.run(create_app(settings), host="127.0.0.1", port=args.port)
        else:

            async def local_control():
                # Status/control on a stopped process: no keys are necessary.
                from .engine import Engine
                from .store import Store

                store = Store(settings.database, settings)
                try:
                    engine = Engine(settings, store, None, None)
                    if args.command == "export-pilot":
                        eligible = [
                            c
                            for c in store.cycles()
                            if c["mode"] == "live"
                            and c["phase"] == "COMPLETE"
                            and c["data"].get("return_receipt")
                        ]
                        if not eligible:
                            raise ValueError("No completed live pilot with returned capital")
                        target = Path(args.output)
                        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                        # Exclusive creation prevents silently replacing an audited admission file.
                        with target.open("x") as output:
                            json.dump(
                                {"version": 1, "audit_passed": False, "pilot_cycle": eligible[0]},
                                output,
                                indent=2,
                            )
                        result = {"exported": str(target), "audit_passed": False}
                    else:
                        result = (
                            await engine.resume()
                            if args.command == "resume"
                            else (await engine.stop() if args.command == "stop" else engine.status())
                        )
                    print(json.dumps(result, indent=2))
                finally:
                    store.close()

            asyncio.run(local_control())
    except KeyboardInterrupt:
        print("Stopped; durable state will be reconciled on restart.", file=sys.stderr)
    except Exception as error:
        if args.command in {"stock-quote", "stock-buy"}:
            from .stock_cli import StockCommandError
            from .transport import RemoteError

            if isinstance(error, (StockCommandError, RemoteError)):
                print(f"Command failed: {error}", file=sys.stderr)
                if args.command == "stock-buy" and args.live:
                    print("Keep the journal and reuse the same request-id to reconcile.", file=sys.stderr)
                raise SystemExit(1) from None
        # Pydantic errors include user-supplied config values; never print raw exceptions/tracebacks.
        print(
            f"Command failed ({type(error).__name__}); check configuration and recorded state.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
