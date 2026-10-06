from types import SimpleNamespace
from unittest.mock import AsyncMock

from test_direct import fixture

from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings


async def test_broad_spot_screen_does_not_compete_with_exact_bank_refreshes(monkeypatch):
    saved = fixture()
    quote = saved["quote"]
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: quote["_observed_at"])
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kwargs: {
                "context": final["result"]["context"],
                "value": [rows[address] for address in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    router.candidates = {STOCK_MINT: [quote["pool"]]}
    router.pool_catalog[quote["pool"]] = {
        "program": quote["_direct"]["program"],
        "snapshot": {"state": quote["_direct"]["state"]},
    }
    try:
        prices = await router.prices([USDC_MINT, STOCK_MINT])
        assert STOCK_MINT in prices and float(prices[STOCK_MINT]["sellSpotPrice"]) > 0
        rpc.accounts.assert_awaited_once()
        assert rpc.accounts.await_args.kwargs["priority"] == 10
        assert router.screen_status["assets"] == 1
    finally:
        await router.close()
