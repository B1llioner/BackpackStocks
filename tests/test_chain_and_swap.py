import base64
import copy
import json
import struct
from datetime import UTC, datetime
from pathlib import Path

import pytest
from solders.pubkey import Pubkey

from backpack_stocks.models import STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, Policy
from backpack_stocks.raydium import decode_clmm
from backpack_stocks.sessions import active_session, validate_quantity
from backpack_stocks.solana import assert_stock_mint, decode_token, transaction_deltas
from backpack_stocks.swap_guard import decode_route, validate_instructions, validate_simulation

ROOT = Path(__file__).resolve().parents[1]


def public_fixture():
    r = json.loads((ROOT / "tests/fixtures/public_checks_2026-10-01.json").read_text())["records"]
    return r[-1]["data"]["result"]


def swap_fixture():
    return json.loads((ROOT / "tests/fixtures/jupiter_v1_direct_sample.json").read_text())


def test_real_mint_extensions_and_pool_layout_fixture():
    r = public_fixture()
    mint, pool = r["value"]
    info = assert_stock_mint(mint)
    assert info["decimals"] == 6
    decoded = decode_clmm(pool)
    assert {decoded["mintA"], decoded["mintB"]} == {STOCK_MINT, USDC_MINT}
    assert decoded["tickSpacing"] == 60
    assert not decoded["quoteSupported"]
    assert decoded["dynamicFeeControl"] > 0


@pytest.mark.parametrize("change", ["scale", "paused", "hook", "new_extension", "owner", "decimals"])
def test_changed_issuer_capabilities_block_live(change):
    mint = copy.deepcopy(public_fixture()["value"][0])
    info = mint["data"]["parsed"]["info"]
    ext = {e["extension"]: e["state"] for e in info["extensions"]}
    if change == "scale":
        ext["scaledUiAmountConfig"]["newMultiplier"] = "2"
    elif change == "paused":
        ext["pausableConfig"]["paused"] = True
    elif change == "hook":
        ext["transferHook"]["programId"] = TOKEN
    elif change == "new_extension":
        info["extensions"].append({"extension": "transferFeeConfig", "state": {}})
    elif change == "owner":
        mint["owner"] = TOKEN
    else:
        info["decimals"] = 5
    with pytest.raises(ValueError):
        assert_stock_mint(mint)


def test_pool_owner_and_discriminator_checked():
    pool = copy.deepcopy(public_fixture()["value"][1])
    pool["owner"] = TOKEN
    with pytest.raises(ValueError):
        decode_clmm(pool)


def test_real_v1_direct_instructions_decode_and_validate():
    q = swap_fixture()
    name, args = decode_route(base64.b64decode(q["swapInstruction"]["data"]))
    assert name == "route"
    assert args["in_amount"] == 1000000
    assert args["slippage_bps"] == 50
    assert len(validate_instructions(q, "11111111111111111111111111111111", 1000000, 6000000, Policy())) >= 2


def test_legacy_price_impact_ratio_above_one_percent_blocks_signing():
    q = swap_fixture()
    q["priceImpactPct"] = "0.012"
    with pytest.raises(ValueError, match="impact"):
        validate_instructions(q, "11111111111111111111111111111111", 1000000, 6000000, Policy())


@pytest.mark.parametrize(
    "change",
    [
        "program",
        "authority",
        "recipient",
        "amount",
        "slippage",
        "extra_signer",
        "extra_instruction",
        "fee",
        "minimum",
        "discriminator",
    ],
)
def test_rejects_unsafe_or_mismatched_jupiter_transaction(change):
    q = swap_fixture()
    ix = q["swapInstruction"]
    minimum = 6000000
    if change == "program":
        ix["programId"] = TOKEN
    elif change == "authority":
        ix["accounts"][1]["pubkey"] = TOKEN
    elif change == "recipient":
        ix["accounts"][3]["pubkey"] = TOKEN
    elif change == "amount":
        q["inAmount"] = "2000000"
    elif change == "slippage":
        d = bytearray(base64.b64decode(ix["data"]))
        struct.pack_into("<H", d, len(d) - 3, 100)
        ix["data"] = base64.b64encode(d).decode()
    elif change == "extra_signer":
        ix["accounts"][2]["isSigner"] = True
    elif change == "extra_instruction":
        q["otherInstructions"] = [ix]
    elif change == "fee":
        d = bytearray(base64.b64decode(ix["data"]))
        d[-1] = 1
        ix["data"] = base64.b64encode(d).decode()
    elif change == "minimum":
        minimum = 100000000
    else:
        ix["data"] = base64.b64encode(bytes(32)).decode()
    with pytest.raises(ValueError):
        validate_instructions(q, "11111111111111111111111111111111", 1000000, minimum, Policy())


def token_account(mint, wallet, amount, program, *, delegate=0):
    data = bytearray(165)
    data[:32] = bytes(Pubkey.from_string(mint))
    data[32:64] = bytes(Pubkey.from_string(wallet))
    struct.pack_into("<Q", data, 64, amount)
    struct.pack_into("<I", data, 72, delegate)
    data[108] = 1
    return {"owner": program, "data": [base64.b64encode(data).decode(), "base64"], "lamports": 2039280}


@pytest.mark.parametrize("change", [None, "debit", "credit", "delegate", "payer", "sol_outflow"])
def test_simulation_checks_effects_and_authorities(change):
    wallet = "11111111111111111111111111111111"
    payer = {"owner": wallet, "lamports": 100000000}
    a = token_account(STOCK_MINT, wallet, 2000000, TOKEN_2022)
    b = token_account(USDC_MINT, wallet, 1000000, TOKEN)
    snap = {"accounts": [payer, a, b], "stock": 2000000, "usdc": 1000000, "lamports": 100000000}
    post_payer = {"owner": wallet, "lamports": 99995000}
    a2 = token_account(STOCK_MINT, wallet, 1000000, TOKEN_2022)
    b2 = token_account(USDC_MINT, wallet, 7000000, TOKEN)
    if change == "debit":
        a2 = token_account(STOCK_MINT, wallet, 0, TOKEN_2022)
    elif change == "credit":
        b2 = token_account(USDC_MINT, wallet, 6000000, TOKEN)
    elif change == "delegate":
        b2 = token_account(USDC_MINT, wallet, 7000000, TOKEN, delegate=1)
    elif change == "payer":
        post_payer["owner"] = TOKEN
    elif change == "sol_outflow":
        post_payer["lamports"] = 90000000
    value = {"err": None, "accounts": [post_payer, a2, b2]}
    if change:
        with pytest.raises(ValueError):
            validate_simulation(snap, value, 1000000, 6000000, 300000, wallet)
    else:
        validate_simulation(snap, value, 1000000, 6000000, 300000, wallet)
        assert decode_token(a)["amount"] == 2000000


def test_new_output_ata_must_not_have_delegate():
    wallet = "11111111111111111111111111111111"
    snap = {
        "accounts": [{"owner": wallet}, token_account(STOCK_MINT, wallet, 2000000, TOKEN_2022), None],
        "stock": 2000000,
        "usdc": 0,
        "lamports": 100000000,
    }
    post = {
        "err": None,
        "accounts": [
            {"owner": wallet, "lamports": 97000000},
            token_account(STOCK_MINT, wallet, 1000000, TOKEN_2022),
            token_account(USDC_MINT, wallet, 6000000, TOKEN, delegate=1),
        ],
    }
    with pytest.raises(ValueError):
        validate_simulation(snap, post, 1000000, 6000000, 300000, wallet)


def test_causal_transaction_receipt_uses_raw_wallet_changes():
    wallet = "11111111111111111111111111111111"
    tx = {
        "meta": {
            "err": None,
            "fee": 5000,
            "preTokenBalances": [
                {"owner": wallet, "mint": STOCK_MINT, "uiTokenAmount": {"amount": "1000000"}}
            ],
            "postTokenBalances": [
                {"owner": wallet, "mint": STOCK_MINT, "uiTokenAmount": {"amount": "0"}},
                {"owner": wallet, "mint": USDC_MINT, "uiTokenAmount": {"amount": "6000000"}},
            ],
        }
    }
    assert transaction_deltas(tx, wallet) == {"stock": -1000000, "usdc": 6000000, "fee": 5000}


def test_session_timezone_dst_holiday_and_quantity_rules():
    sessions = [
        {
            "name": "REGULAR",
            "startTime": "09:30:00",
            "endTime": "16:00:00",
            "timezone": "America/New_York",
            "startWeekday": 1,
            "endWeekday": 5,
        }
    ]
    winter = datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
    summer = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
    assert active_session(sessions, [], winter) is None
    assert active_session(sessions, [], summer) == "REGULAR"
    holidays = [{"market": "US_EQUITIES", "date": "2026-07-06", "timezone": "America/New_York"}]
    assert active_session(sessions, holidays, summer) is None
    security = {"sessions": [{"name": "REGULAR", "minQuantity": "1", "maxQuantity": "1000", "stepSize": "1"}]}
    validate_quantity(security, "REGULAR", "1")
    with pytest.raises(ValueError):
        validate_quantity(security, "REGULAR", "1.5")
