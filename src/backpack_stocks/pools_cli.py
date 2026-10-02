"""Public pool research and an independently migrated PostgreSQL catalog."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from .cli import load_settings
from .environment import load_environment
from .jupiter import Jupiter
from .pool_catalog import collect_catalog, quote_cpmm_state, refresh_catalog
from .proxy import PROXY_ENV_NAMES, resolve_proxy
from .raydium import Raydium
from .runtime import public_clients


async def run(args) -> object:
    if args.command == "grid-replay":
        from .models import USDC_MINT, raw
        from .pool_compute import PoolCalculator, PreparedCpmmPools

        catalog = json.loads(Path(args.path).read_text())
        assets = {a["mint"]: a for a in catalog["assets"]}
        pools = [
            p
            for p in catalog["pools"]
            if p.get("snapshot", {}).get("state", {}).get("quoteSupported")
            and USDC_MINT in {p["mintA"], p["mintB"]}
            and any(m in assets for m in (p["mintA"], p["mintB"]))
        ]
        calculator = PoolCalculator(backend=args.backend)
        prepared = PreparedCpmmPools(calculator, [p["snapshot"]["state"] for p in pools])
        seeds, metadata = [], []
        for index, pool in enumerate(pools):
            mint = next(m for m in (pool["mintA"], pool["mintB"]) if m in assets)
            input_mint, output_mint = (mint, USDC_MINT) if args.direction == "sell" else (USDC_MINT, mint)
            seeds.append(
                (
                    index,
                    input_mint == pool["snapshot"]["state"]["mintA"],
                    raw(args.base_amount, assets[mint]["decimals"] if args.direction == "sell" else 6),
                )
            )
            metadata.append(
                {
                    "symbol": assets[mint]["symbol"],
                    "pool": pool["address"],
                    "input_mint": input_mint,
                    "output_mint": output_mint,
                    "slot": pool["snapshot"]["slot"],
                    "observed_at": pool["snapshot"]["observed_at"],
                }
            )
        sizes = [int(s) for s in args.sizes.split(",")]
        results = await asyncio.to_thread(prepared.quote_grid, seeds, sizes)
        rows = [
            {
                **metadata[i // len(sizes)],
                "size_multiplier": sizes[i % len(sizes)],
                "input_raw": amount,
                "output_raw": out,
                "minimum_output_raw": minimum,
                "error_code": error,
            }
            for i, (amount, out, minimum, error) in enumerate(results)
        ]
        summary = {
            "mode": "historical_snapshot_replay",
            "backend": calculator.backend,
            "pools": len(pools),
            "jobs": len(rows),
            "workers": 4,
            "errors": sum(bool(r["error_code"]) for r in rows),
            "mutations": 0,
        }
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({"summary": summary, "rows": rows}, indent=2))
        return {**summary, "output": str(target)}
    # Optional dependency is loaded only for commands that actually use PostgreSQL.
    from .postgres import PoolRepository, migrate_local

    if args.command == "migrate":
        load_environment("state/postgres/migration.env", ["POSTGRES_MIGRATION_DSN"])
        return await migrate_local(os.environ["POSTGRES_MIGRATION_DSN"])
    load_environment(args.env_file, ["POSTGRES_DSN"])
    if args.command in {"sync", "refresh"}:
        settings = load_settings(args.config)
        load_environment(args.env_file, [*PROXY_ENV_NAMES, settings.jupiter_key_env])
        bp, jup, rpc = public_clients(settings)
        ray = Raydium(rpc, proxy=resolve_proxy("RAYDIUM"))
        try:

            def progress(stage, n, total):
                print(f"{stage} {n}/{total}", flush=True)

            if args.command == "sync":
                catalog = await collect_catalog(
                    bp,
                    ray,
                    rpc,
                    jupiter=jup if args.jupiter and isinstance(jup, Jupiter) else None,
                    amount_ui=args.amount,
                    progress=progress,
                )
            else:
                catalog = await refresh_catalog(
                    json.loads(Path(args.path).read_text()), rpc, amount_ui=args.amount, progress=progress
                )
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(catalog, indent=2, allow_nan=False))
        finally:
            for client in (ray, bp, jup, rpc):
                await client.close()
    async with PoolRepository(os.environ["POSTGRES_DSN"]) as repo:
        if args.command == "import":
            catalog = json.loads(Path(args.path).read_text())
        if args.command in {"import", "sync", "refresh"}:
            return await repo.save_catalog(catalog)
        if args.command == "status":
            return await repo.status()
        if args.command == "list":
            return await repo.list_pools(args.symbol)
        if args.command == "quote":
            snapshot = await repo.latest_state(args.pool, max_age_s=args.max_age)
            return {
                **quote_cpmm_state(
                    snapshot["state"], args.input_mint, args.amount_raw, slippage_bps=args.slippage
                ),
                "pool": args.pool,
                "slot": snapshot["slot"],
                "observed_at": snapshot["observed_at"],
            }
    raise ValueError("Unknown pool command")


def main():
    p = argparse.ArgumentParser(description="Read-only stock pools and PostgreSQL market data")
    p.add_argument("--env-file", default=".env")
    commands = p.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Explicit migration of the dedicated local DB only")
    sync = commands.add_parser("sync", help="Discover pools and atomically import a public scan")
    sync.add_argument("--config", default="examples/telegram-paper.toml")
    sync.add_argument(
        "--jupiter", action="store_true", help="Sample quote-only routes; shares provider quotas"
    )
    sync.add_argument("--amount", default="1")
    sync.add_argument("--output", default="research/pool-pricing/catalog.json")
    imp = commands.add_parser("import", help="Idempotently import a recorded public scan")
    imp.add_argument("path")
    refresh = commands.add_parser(
        "refresh", help="Refresh known onchain pools without consuming Jupiter quote quota"
    )
    refresh.add_argument("path")
    refresh.add_argument("--config", default="examples/telegram-paper.toml")
    refresh.add_argument("--amount", default="1")
    refresh.add_argument("--output", default="research/pool-pricing/catalog-refreshed.json")
    commands.add_parser("status")
    listing = commands.add_parser("list")
    listing.add_argument("symbol")
    quote = commands.add_parser(
        "quote", help="Integer local CPMM quote from a fresh snapshot; no transaction"
    )
    quote.add_argument("pool")
    quote.add_argument("input_mint")
    quote.add_argument("amount_raw", type=int)
    quote.add_argument("--slippage", type=int, default=50)
    quote.add_argument("--max-age", type=int, default=10)
    grid = commands.add_parser(
        "grid-replay", help="Historical direct-USDC CPMM sizing grid through PyO3; no network or execution"
    )
    grid.add_argument("path")
    grid.add_argument("--sizes", default="1,2,5,10,20")
    grid.add_argument("--base-amount", default="1")
    grid.add_argument("--direction", choices=["sell", "buy"], default="sell")
    grid.add_argument("--backend", choices=["auto", "rust", "python"], default="auto")
    grid.add_argument("--output", default="research/pool-pricing/grid-replay.json")
    args = p.parse_args()
    try:
        result = asyncio.run(run(args))
    except Exception as error:
        # Connection errors may contain a credential DSN; keep the UI/log sanitized.
        raise SystemExit(
            f"Pool command failed ({type(error).__name__}); check configuration and public scan coverage"
        ) from None
    print(json.dumps(result, indent=2, default=str, allow_nan=False))


if __name__ == "__main__":
    main()
