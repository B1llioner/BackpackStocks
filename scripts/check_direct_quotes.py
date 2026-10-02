"""Bounded public mainnet quote probes; no signer and no sendTransaction."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from research_direct import RecordedRPC

from backpack_stocks.cli import load_settings
from backpack_stocks.direct import WSOL, DirectRaydium
from backpack_stocks.environment import load_environment
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy


async def main():
    settings = load_settings("examples/direct-swap.toml")
    settings.direct.max_pools = 2
    load_environment(".env", PROXY_ENV_NAMES)
    rpc = RecordedRPC(settings.rpc_url, interval=settings.rpc_interval_s, proxy=resolve_proxy("SOLANA"))
    router = DirectRaydium(settings, rpc)
    report = {"observed_at": datetime.now(UTC).isoformat(), "financial_mutations": 0, "checks": []}
    try:
        bb = router.by_symbol["BB.US"]["mint"]
        for name, source, target, amount in [
            ("WEN_sell", STOCK_MINT, USDC_MINT, 1_000_000),
            ("WEN_buy", USDC_MINT, STOCK_MINT, 6_000_000),
            ("BB_sell", bb, USDC_MINT, 1_000_000),
            ("gas_valuation", WSOL, USDC_MINT, 300_000),
        ]:
            start = len(rpc.records)
            try:
                q = await router.quote_pair(source, target, amount)
                row = {"name": name, "ok": True, "quote": q}
            except Exception as error:
                row = {"name": name, "ok": False, "error_type": type(error).__name__}
            row["rpc_records"] = rpc.records[start:]
            report["checks"].append(row)
            print(json.dumps({k: v for k, v in row.items() if k not in {"rpc_records", "quote"}}), flush=True)
        Path("research/direct-raydium/direct-readonly-checks.json").write_text(json.dumps(report, indent=2))
    finally:
        await router.close()
        await rpc.close()


if __name__ == "__main__":
    asyncio.run(main())
