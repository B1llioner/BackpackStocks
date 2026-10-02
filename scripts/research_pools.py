"""Read-only network inventory. No trading/signing credentials are loaded."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from backpack_stocks.cli import load_settings
from backpack_stocks.environment import load_environment
from backpack_stocks.pool_catalog import collect_catalog
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy
from backpack_stocks.raydium import Raydium
from backpack_stocks.runtime import public_clients


async def run(args):
    settings = load_settings(args.config)
    load_environment(args.env_file, [*PROXY_ENV_NAMES, settings.jupiter_key_env])
    bp, jup, rpc = public_clients(settings)
    ray = Raydium(rpc, proxy=resolve_proxy("RAYDIUM"))
    try:
        catalog = await collect_catalog(
            bp,
            ray,
            rpc,
            jupiter=jup if args.jupiter else None,
            amount_ui=args.amount,
            progress=lambda stage, n, total: print(f"{stage} {n}/{total}", flush=True),
        )
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(catalog, indent=2, allow_nan=False))
        print(json.dumps(catalog["summary"]), flush=True)
    finally:
        for client in (ray, bp, jup, rpc):
            await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="examples/telegram-paper.toml")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--jupiter", action="store_true")
    parser.add_argument("--amount", default="1")
    parser.add_argument("--output", default="research/pool-pricing/catalog.json")
    asyncio.run(run(parser.parse_args()))
