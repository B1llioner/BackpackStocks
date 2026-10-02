"""One bounded read-only direct mainnet quote with public account recording."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from backpack_stocks.cli import load_settings
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.environment import load_environment
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy
from backpack_stocks.solana import SolanaRPC


class RecordedRPC(SolanaRPC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records: list[dict] = []

    async def accounts(self, addresses, **kwargs):
        result = await super().accounts(addresses, **kwargs)
        self.records.append({"addresses": addresses, "options": kwargs, "result": result})
        return result


async def main():
    settings = load_settings("examples/direct-swap.toml")
    load_environment(".env", PROXY_ENV_NAMES)
    rpc = RecordedRPC(settings.rpc_url, interval=settings.rpc_interval_s, proxy=resolve_proxy("SOLANA"))
    router = DirectRaydium(settings, rpc)
    try:
        q = await router.pool_quote(
            "ArXpohFUAsifyZPHsYqgSTqLyA6sQG8KMocJ6zBLG29D", STOCK_MINT, USDC_MINT, 1_000_000
        )
        root = Path("research/direct-raydium")
        root.mkdir(parents=True, exist_ok=True)
        (root / "wen-direct-quote.json").write_text(
            json.dumps({"quote": q, "rpc_records": rpc.records, "financial_mutations": 0}, indent=2)
        )
        print(json.dumps({k: v for k, v in q.items() if not k.startswith("_")}, indent=2))
    except Exception as error:
        print(type(error).__name__, str(error))
        raise
    finally:
        await router.close()
        await rpc.close()


if __name__ == "__main__":
    asyncio.run(main())
