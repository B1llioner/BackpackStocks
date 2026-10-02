"""Mainnet monitor check; creates/cancels non-binding RFQs without buying."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from backpack_stocks.backpack import BackpackAuth
from backpack_stocks.cli import load_settings
from backpack_stocks.environment import load_environment
from backpack_stocks.monitor import SpreadMonitor
from backpack_stocks.proxy import PROXY_ENV_NAMES
from backpack_stocks.runtime import public_clients
from backpack_stocks.stock_cli import stock_symbol


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=stock_symbol)
    args = parser.parse_args()
    settings = load_settings("examples/telegram-direct-paper-1k.toml")
    load_environment(
        ".env",
        [
            *PROXY_ENV_NAMES,
            settings.backpack_seed_env,
            settings.jupiter_key_env,
            "JUPITER_REPLAY_PROXY",
            "JUPITER_REPLAY_WEBSITE_KEY",
            settings.monitor.jupiter_wallet_env,
        ],
    )
    settings.backpack_private_reads = False
    bp, dex, rpc = public_clients(settings)
    if settings.monitor.rfq_validation and os.environ.get(settings.backpack_seed_env):
        bp.auth = BackpackAuth(os.environ[settings.backpack_seed_env])
    monitor = SpreadMonitor(settings, bp, dex, rpc)
    try:
        report = await monitor.refresh_report(args.symbol)
        folder = Path("research/fresh-reports")
        folder.mkdir(exist_ok=True)
        name = args.symbol.removesuffix(".US").lower() if args.symbol else "mainnet"
        (folder / f"{name}.json").write_text(json.dumps(report, indent=2))
        text = monitor.text(args.symbol, snapshot=report)
        (folder / f"{name}.txt").write_text(text)
        print(text, flush=True)
    finally:
        await monitor.close()
        await bp.close()
        await dex.close()
        await rpc.close()


if __name__ == "__main__":
    asyncio.run(main())
