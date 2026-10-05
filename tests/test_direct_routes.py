import base64
import copy
import hashlib
import json
import pathlib
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.pubkey import Pubkey

from backpack_stocks.direct import CLOCK, DirectRaydium, validate_auxiliary
from backpack_stocks.direct_routes import WSOL, combine, composed_quote, family_quote
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.pool_state import CLMM
from backpack_stocks.wallet_pools import ALPHABET, swap_pool_hints


def quote(pool, source, target, amount, output, *, observed=None):
    return {
        "pool": pool,
        "inputMint": source,
        "outputMint": target,
        "inAmount": str(amount),
        "outAmount": str(output),
        "otherAmountThreshold": str(output * 9950 // 10000),
        "priceImpact": "0.1",
        "contextSlot": 123,
        "_observed_at": time.time() if observed is None else observed,
        "routePlan": [
            {
                "percent": 100,
                "swapInfo": {
                    "ammKey": pool,
                    "label": "Raydium CLMM",
                    "inputMint": source,
                    "outputMint": target,
                },
            }
        ],
    }


def test_route_converts_sol_raw_units_only_through_the_second_pool():
    first = quote("stock_pool", STOCK_MINT, WSOL, 1_000_000, 12_345_678)
    second = quote("bridge_pool", WSOL, USDC_MINT, 12_345_678, 1_482_097)
    result = combine([first, second], sequential=True)
    assert result["inAmount"] == "1000000" and result["outAmount"] == "1482097"
    assert result["inputMint"] == STOCK_MINT and result["outputMint"] == USDC_MINT
    assert result["otherAmountThreshold"] == str(1_482_097 * 9950 // 10000)
    assert not result["direct_execution_supported"]
    second["inAmount"] = "12345"
    with pytest.raises(ValueError, match="intermediate amount"):
        combine([first, second], sequential=True)


def test_split_preserves_exact_input_and_each_minimum_and_rejects_reused_state():
    result = combine(
        [quote("a", STOCK_MINT, USDC_MINT, 7, 18), quote("b", STOCK_MINT, USDC_MINT, 4, 11)], sequential=False
    )
    assert result["inAmount"] == "11" and result["outAmount"] == "29"
    assert result["otherAmountThreshold"] == "27"
    with pytest.raises(ValueError, match="reuses mutable"):
        combine(
            [quote("a", STOCK_MINT, USDC_MINT, 7, 18), quote("a", STOCK_MINT, USDC_MINT, 4, 11)],
            sequential=False,
        )


async def test_split_depth_can_beat_every_single_pool_without_dropping_input_remainder():
    async def calculate(pool, source, target, amount, **kwargs):
        return quote(pool, source, target, amount, 3 * amount - amount * amount // 1000)

    router = SimpleNamespace(
        settings=Settings(direct={"split_pools": True}), pool_quote=calculate, failures=0
    )
    results, codes = await family_quote(router, ["a", "b"], STOCK_MINT, USDC_MINT, 1001, 0, None)
    assert not codes
    best = max(results, key=lambda q: int(q["outAmount"]))
    assert best["inAmount"] == "1001"
    assert int(best["outAmount"]) > max(int(q["outAmount"]) for q in results if "pool" in q)


@pytest.mark.parametrize("reverse", [False, True])
async def test_full_path_in_both_directions_uses_exact_intermediate_and_oldest_leg_age(reverse):
    calls = []

    async def calculate(pool, source, target, amount, **kwargs):
        calls.append((pool, source, target, amount))
        output = amount * 1000 if target == WSOL else amount // 1000
        if pool == "direct":
            output = amount // 2
        return quote(pool, source, target, amount, output)

    router = SimpleNamespace(
        settings=Settings(),
        candidates={STOCK_MINT: ["direct"]},
        sol_candidates={STOCK_MINT: ["stock_sol"]},
        bridge_pools=AsyncMock(return_value=["bridge"]),
        pool_quote=calculate,
        failures=0,
        quote_count=0,
    )
    source, target = (USDC_MINT, STOCK_MINT) if reverse else (STOCK_MINT, USDC_MINT)
    result = await composed_quote(router, source, target, 1_000_001, 0, None)
    assert result["inputMint"] == source and result["outputMint"] == target
    assert result["inAmount"] == "1000001" and result["outAmount"] == "1000001"
    assert any(c[1] == WSOL and c[3] == 1_000_001_000 for c in calls)
    assert len(result["routePlan"]) == 2 and not result["direct_execution_supported"]


async def test_expired_leg_cannot_become_a_positive_pool_route():
    async def calculate(pool, source, target, amount, **kwargs):
        return quote(pool, source, target, amount, amount * 2, observed=time.time() - 60)

    router = SimpleNamespace(settings=Settings(), pool_quote=calculate, failures=0)
    results, codes = await family_quote(router, ["a"], STOCK_MINT, USDC_MINT, 100, 0, None)
    assert results == [] and codes == ["stale_pool_quote"]


def encode(data):
    number = int.from_bytes(data, "big")
    output = ""
    while number:
        number, remainder = divmod(number, 58)
        output = ALPHABET[remainder] + output
    return "1" * (len(data) - len(data.lstrip(b"\x00"))) + output


async def test_splitting_sale_between_usdc_and_sol_beats_both_whole_routes():
    async def calculate(pool, source, target, amount, **kwargs):
        if pool == "bridge":
            output = amount // 1000
        else:
            output = 3 * amount - amount * amount // 1000
            if target == WSOL:
                output *= 1000
        return quote(pool, source, target, amount, output)

    router = SimpleNamespace(
        settings=Settings(direct={"split_pools": True}),
        candidates={STOCK_MINT: ["stock_usdc"]},
        sol_candidates={STOCK_MINT: ["stock_sol"]},
        bridge_pools=AsyncMock(return_value=["bridge"]),
        pool_quote=calculate,
        failures=0,
        quote_count=0,
    )
    result = await composed_quote(router, STOCK_MINT, USDC_MINT, 1000, 0, None)
    assert result["inAmount"] == "1000" and result["outAmount"] == "2500"
    assert len(result["routePlan"]) == 3


def test_only_verified_swap_wire_layouts_can_produce_pool_hints():
    address = str(Pubkey.from_bytes(bytes([9]) * 32))
    accounts = [STOCK_MINT] * 13
    accounts[2] = address
    wire = hashlib.sha256(b"global:swap_v2").digest()[:8] + bytes(32) + b"\x01"
    instruction = {"programId": CLMM, "accounts": accounts, "data": encode(wire)}
    assert swap_pool_hints([instruction, instruction]) == [{"pool": address, "program": CLMM}]
    for change in ({"data": "!"}, {"data": encode(bytes(41))}, {"programId": STOCK_MINT}, {"accounts": []}):
        assert swap_pool_hints([{**instruction, **change}]) == []


async def test_new_wallet_pool_is_validated_persisted_and_not_refetched(tmp_path):
    saved = json.loads(
        (pathlib.Path(__file__).parent / "fixtures/direct-raydium/wen-direct-quote.json").read_text()
    )
    pool = saved["quote"]["pool"]
    account = saved["rpc_records"][0]["result"]["value"][0]
    rpc = SimpleNamespace(accounts=AsyncMock(return_value={"value": [account], "context": {"slot": 123}}))
    path = tmp_path / "watched.json"
    router = DirectRaydium(Settings(direct={"watched_catalog": str(path)}), rpc)
    try:
        hints = [{"pool": pool, "program": CLMM}]
        assets = [{"symbol": "WEN.US", "mint": STOCK_MINT, "decimals": 6}]
        assert await router.register_pool_hints(hints, assets) == 1
        assert await router.register_pool_hints(hints, assets) == 0
        assert rpc.accounts.await_count == 1
        assert json.loads(path.read_text())["pools"][0]["address"] == pool
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        await router.close()


@pytest.mark.parametrize("corruption", [None, "owner", "pool_id"])
def test_legacy_clmm_oracle_uses_pool_state_key_but_still_requires_owner_and_pool_binding(corruption):
    pool = str(Pubkey.from_bytes(bytes([17]) * 32))
    oracle = str(Pubkey.from_bytes(bytes([18]) * 32))
    data = bytearray(4483)
    data[:8] = hashlib.sha256(b"account:ObservationState").digest()[:8]
    data[19:51] = bytes(Pubkey.from_string(pool if corruption != "pool_id" else STOCK_MINT))
    accounts = {
        oracle: {
            "owner": CLMM if corruption != "owner" else STOCK_MINT,
            "data": [base64.b64encode(data).decode(), "base64"],
        }
    }
    if corruption:
        with pytest.raises(ValueError):
            validate_auxiliary(pool, CLMM, {"observation": oracle}, accounts, [])
    else:
        validate_auxiliary(pool, CLMM, {"observation": oracle}, accounts, [])


@pytest.mark.parametrize("broken_bridge", [False, True])
async def test_sol_screen_converts_nine_decimal_sol_in_same_bank_and_fails_closed(broken_bridge):
    saved = json.loads(
        (pathlib.Path(__file__).parent / "fixtures/direct-raydium/wen-direct-quote.json").read_text()
    )
    base = saved["rpc_records"][0]["result"]["value"][0]
    stock_pool = saved["quote"]["pool"]
    bridge = str(Pubkey.from_bytes(bytes([19]) * 32))

    def account(mint_a, mint_b, decimals_a, decimals_b, disabled=False):
        result = copy.deepcopy(base)
        data = bytearray(base64.b64decode(result["data"][0]))
        data[73:105] = bytes(Pubkey.from_string(mint_a))
        data[105:137] = bytes(Pubkey.from_string(mint_b))
        data[233] = decimals_a
        data[234] = decimals_b
        data[253:269] = (2**64).to_bytes(16, "little")
        data[389] = 16 if disabled else 0
        result["data"][0] = base64.b64encode(data).decode()
        return result

    bridge_account = account(USDC_MINT, WSOL, 6, 9, broken_bridge)
    stock_account = account(WSOL, STOCK_MINT, 9, 6)
    final = saved["rpc_records"][-1]
    clock = copy.deepcopy(final["result"]["value"][final["addresses"].index(CLOCK)])
    clock["data"]["parsed"]["info"]["unixTimestamp"] = int(time.time())

    async def accounts(addresses, **kwargs):
        values = {bridge: bridge_account, stock_pool: stock_account, CLOCK: clock}
        return {"context": {"slot": 123}, "value": [values[a] for a in addresses]}

    router = DirectRaydium(Settings(direct={"via_sol": True}), SimpleNamespace(accounts=accounts))
    router.candidates = {}
    router.sol_candidates = {STOCK_MINT: [stock_pool]}
    router.bridge_pools = AsyncMock(return_value=[bridge])
    router.pool_catalog[stock_pool] = {"program": CLMM, "snapshot": {"state": {"kind": "clmm"}}}
    try:
        prices = await router.prices([STOCK_MINT])
        if broken_bridge:
            assert STOCK_MINT not in prices
        else:
            assert Decimal(prices[WSOL]["usdPrice"]) == 1000
            assert Decimal(prices[STOCK_MINT]["usdPrice"]) == 1
    finally:
        await router.close()
