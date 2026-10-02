"""Read public Backpack metadata for diagnosing one stock's size/session constraints."""

import argparse
import asyncio
import json
from pathlib import Path

from backpack_stocks.backpack import Backpack
from backpack_stocks.environment import load_environment
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy


async def check(symbol):
    load_environment(".env", PROXY_ENV_NAMES)
    bp = Backpack(proxy=resolve_proxy("BACKPACK"))
    try:
        assets, securities = await asyncio.gather(bp.assets(), bp.securities())
        report = {
            "symbol": symbol,
            "assets": [a for a in assets if a["symbol"] == symbol],
            "securities": [s for s in securities if s["asset"] == symbol],
        }
        target = Path("research/soak-1k") / (symbol + "-rules.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
    finally:
        await bp.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, choices=["CYPH.US"])
    asyncio.run(check(parser.parse_args().symbol))
