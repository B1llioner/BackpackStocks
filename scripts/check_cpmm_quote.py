"""Public direct CPMM mainnet probe; records reserves/config/Token-2022 capabilities."""

import asyncio
import json
from pathlib import Path

from research_direct import RecordedRPC

from backpack_stocks.cli import load_settings
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.environment import load_environment
from backpack_stocks.models import USDC_MINT
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy


async def main():
    settings = load_settings("examples/direct-swap.toml")
    load_environment(".env", PROXY_ENV_NAMES)
    rpc = RecordedRPC(settings.rpc_url, interval=settings.rpc_interval_s, proxy=resolve_proxy("SOLANA"))
    router = DirectRaydium(settings, rpc)
    try:
        q = await router.pool_quote(
            "4m9qf5WE1QvNMbf13vM232oReRWCryt6TLzKeiay5ZYR",
            router.by_symbol["BB.US"]["mint"],
            USDC_MINT,
            100_000,
        )
        report = {"quote": q, "rpc_records": rpc.records, "financial_mutations": 0}
        Path("research/direct-raydium/cpmm-direct-quote.json").write_text(json.dumps(report, indent=2))
        print(json.dumps({k: v for k, v in q.items() if not k.startswith("_")}, indent=2))
    finally:
        await router.close()
        await rpc.close()


if __name__ == "__main__":
    asyncio.run(main())
