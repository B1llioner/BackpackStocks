import json
from pathlib import Path

import httpx
import pytest
from solders.pubkey import Pubkey

from backpack_stocks.jupiter import Jupiter
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Policy
from backpack_stocks.swap_guard import validate_instructions
from backpack_stocks.transport import Transport


async def test_generic_stock_quote_keeps_trace_local_and_pins_provider_mints():
    mint = str(Pubkey.new_unique())
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            assert request.url.params["inputMint"] == mint
            assert request.url.params["outputMint"] == USDC_MINT
            return httpx.Response(
                200,
                json={
                    "inputMint": mint,
                    "outputMint": USDC_MINT,
                    "inAmount": "1000000",
                    "outAmount": "6000000",
                    "priceImpactPct": "0.001",
                    "instructionVersion": "V1",
                    "swapMode": "ExactIn",
                },
            )
        body = json.loads(request.content)
        assert not any(k.startswith("_") for k in body["quoteResponse"])
        return httpx.Response(200, json={"swapInstruction": {}})

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler))
    )
    jup = Jupiter(transport=t)
    jup.limiter.configure(0.001)
    try:
        q = await jup.build_v1_stock(mint, 1000000, "11111111111111111111111111111111", 50)
        assert q["_quote_started_at"] > 0 and len(requests) == 2
        with pytest.raises(ValueError):
            await jup.quote_v1_stock(mint, 1000000, True)
        assert len(requests) == 2
    finally:
        await jup.close()


def test_wen_instruction_cannot_be_reused_for_another_stock():
    path = Path(__file__).resolve().parents[1] / "tests/fixtures/jupiter_v1_direct_sample.json"
    quote = json.loads(path.read_text())
    assert quote["inputMint"] == STOCK_MINT
    with pytest.raises(ValueError):
        validate_instructions(
            quote,
            "11111111111111111111111111111111",
            1000000,
            6000000,
            Policy(),
            mint=str(Pubkey.new_unique()),
        )
