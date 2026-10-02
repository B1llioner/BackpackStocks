"""Inspect one stock RFQ without accepting it or buying any shares."""

import argparse
import asyncio
import json

from backpack_stocks.backpack import Backpack, BackpackAuth
from backpack_stocks.cli import load_settings
from backpack_stocks.environment import load_environment
from backpack_stocks.observability import configure
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy
from backpack_stocks.rfq_validation import RfqValidator
from backpack_stocks.runtime import required_env
from backpack_stocks.sessions import active_session
from backpack_stocks.stock_cli import positive, stock_symbol, validate_session


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="examples/telegram-direct-paper-1k.toml")
    parser.add_argument("--symbol", type=stock_symbol, required=True)
    parser.add_argument("--quantity", type=positive, required=True)
    parser.add_argument("--side", choices=("Bid", "Ask"), default="Bid")
    args = parser.parse_args()
    configure("state/rfq-diagnostic.jsonl")
    settings = load_settings(args.config)
    load_environment(".env", [*PROXY_ENV_NAMES, settings.backpack_seed_env])
    bp = Backpack(
        BackpackAuth(required_env(settings.backpack_seed_env)),
        interval=settings.backpack_interval_s,
        proxy=resolve_proxy("BACKPACK"),
    )
    try:
        quantity = str(args.quantity)
        await validate_session(bp, args.symbol, quantity)
        session = active_session(await bp.sessions(), await bp.holidays())
        if session is None:
            raise ValueError("No RFQ trading session is open")
        validator = RfqValidator(
            bp,
            max_age=settings.monitor.max_quote_age_s,
            retry_s=settings.monitor.rfq_retry_s,
            timeout=settings.monitor.rfq_timeout_s,
            cache_s=settings.monitor.rfq_cache_s,
            concurrency=settings.monitor.rfq_concurrency,
        )
        result = await validator.check(args.symbol, quantity, session, side=args.side)
        print(
            json.dumps(
                {**result.public(validator.clock()), "price": result.price, "accepted": False}, indent=2
            )
        )
    finally:
        await bp.close()


if __name__ == "__main__":
    asyncio.run(main())
