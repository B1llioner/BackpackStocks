import base64
import copy
import json
import struct
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from backpack_stocks.models import TOKEN, USDC_MINT
from backpack_stocks.pool_catalog import (
    CLOCK,
    enrich_snapshot,
    mint_capabilities,
    quote_cpmm_state,
    transfer_fee_profile,
)
from backpack_stocks.pool_math import Q64, clmm_interval_amounts, cpmm_exact_in, spot_b_per_a
from backpack_stocks.pool_state import CPMM, decode_fee_config, decode_pool

ROOT = Path(__file__).resolve().parents[1]


def fixture():
    return json.loads((ROOT / "tests/fixtures/pool-pricing/rpc-fixtures.json").read_text())


def pool_state():
    f = fixture()
    address = next(a for a in f["pools"] if f["accounts"][a]["owner"] == CPMM)
    state = decode_pool(f["accounts"][address])
    tokens = {m: mint_capabilities(a) for m, a in f["mints"].items()}
    return enrich_snapshot(state, f["accounts"], tokens)


def test_combined_input_creator_fee_rounding_regression():
    # 101 * .013 rounds to 2; two separate ceilings would incorrectly charge 3.
    q = cpmm_exact_in(101, 10000, 20000, 3000, creator_fee_rate=10000)
    assert (q["trade_fee_raw"], q["creator_fee_raw"], q["output_raw"]) == (1, 1, 196)


def test_output_creator_fee_and_transfer_fee_caps():
    q = cpmm_exact_in(101, 10000, 20000, 3000, creator_fee_rate=10000, creator_on_input=False)
    assert (q["pool_output_raw"], q["creator_fee_raw"], q["output_raw"]) == (198, 2, 196)
    q = cpmm_exact_in(
        101, 10000, 20000, 3000, input_transfer_fee=7, output_transfer_bps=300, output_transfer_maximum=4
    )
    assert (q["received_raw"], q["trade_fee_raw"], q["output_transfer_fee_raw"], q["output_raw"]) == (
        94,
        1,
        4,
        180,
    )


@pytest.mark.parametrize("amount", [1234567, 2**53 + 31, 2**63 + 17])
def test_cpmm_large_integer_curve_and_monotonicity(amount):
    reserve_in, reserve_out = 2**63 + 1234, 2**63 + 2345
    q = cpmm_exact_in(amount, reserve_in, reserve_out, 0)
    exact = Fraction(amount * reserve_out, reserve_in + amount)
    assert q["output_raw"] == exact.numerator // exact.denominator
    assert q["output_raw"] < reserve_out
    assert cpmm_exact_in(amount + 100, reserve_in, reserve_out, 0)["output_raw"] >= q["output_raw"]


@pytest.mark.parametrize("value", [True, 1.5, "100", 0, -1, 2**64])
def test_invalid_raw_amounts_rejected(value):
    with pytest.raises(ValueError):
        cpmm_exact_in(value, 10000, 10000, 3000)


def test_depleted_pool_and_fee_consumed_input():
    for args in [(100, 0, 100, 0), (1, 100, 100, 3000), (100, 100, 100, 999999)]:
        with pytest.raises(ValueError):
            cpmm_exact_in(*args)
    with pytest.raises(ValueError):
        cpmm_exact_in(100, 100, 100, 990000, creator_fee_rate=20000)


def test_spot_decimals_and_clmm_one_interval_rounding():
    assert Decimal(spot_b_per_a(1000000, 2000000000, 6, 9)) == 2
    assert clmm_interval_amounts(100, 2 * Q64, Q64) == {"input_a": 50, "output_b": 100}
    assert clmm_interval_amounts(100, Q64, 2 * Q64) == {"input_b": 100, "output_a": 50}


def test_real_cpmm_fee_reserves_authority_and_direction():
    s = pool_state()
    assert s["kind"] == "cpmm" and s["quoteSupported"]
    for side in ("A", "B"):
        assert int(s["reserve" + side]) == int(s["vaultAmount" + side]) - sum(
            int(s[key + side]) for key in ("protocolFees", "fundFees", "creatorFees")
        )
    s.update(feeOn=1, enableCreatorFee=True)
    s["fees"]["creatorFeeRate"] = 10000
    for side, target in (("A", "B"), ("B", "A")):
        q = quote_cpmm_state(s, s["mint" + side], 1000000)
        fee_in, fee_out = s["transferFee" + side], s["transferFee" + target]
        expected = cpmm_exact_in(
            1000000,
            int(s["reserve" + side]),
            int(s["reserve" + target]),
            s["fees"]["tradeFeeRate"],
            creator_fee_rate=10000,
            creator_on_input=side == "A",
            input_transfer_fee=min(fee_in["maximum"], (1000000 * fee_in["bps"] + 9999) // 10000),
            output_transfer_bps=fee_out["bps"],
            output_transfer_maximum=fee_out["maximum"],
        )
        assert q["output_raw"] == expected["output_raw"]
        assert q["minimum_output_raw"] <= q["output_raw"]


@pytest.mark.parametrize(
    "change", ["mint", "authority", "program", "delegate", "clock", "fees", "underflow", "discriminator"]
)
def test_tampered_real_pool_snapshot_fails(change):
    f = fixture()
    address = next(a for a in f["pools"] if f["accounts"][a]["owner"] == CPMM)
    s = decode_pool(f["accounts"][address])
    tokens = {m: mint_capabilities(a) for m, a in f["mints"].items()}
    account = f["accounts"][s["vaultA"]]
    data = bytearray(base64.b64decode(account["data"][0]))
    if change == "mint":
        data[:32] = bytes(32)
    elif change == "authority":
        data[32:64] = bytes(32)
    elif change == "program":
        account["owner"] = TOKEN if s["tokenProgramA"] != TOKEN else CPMM
    elif change == "delegate":
        struct.pack_into("<I", data, 72, 1)
    elif change == "clock":
        f["accounts"][CLOCK]["owner"] = TOKEN
    elif change == "fees":
        f["accounts"][s["config"]]["owner"] = TOKEN
    elif change == "underflow":
        s["protocolFeesA"] = str(2**64 - 1)
    else:
        other = copy.deepcopy(f["accounts"][address])
        other["data"][0] = base64.b64encode(bytes(637)).decode()
        with pytest.raises(ValueError):
            decode_pool(other)
        return
    account["data"][0] = base64.b64encode(data).decode()
    with pytest.raises(ValueError):
        enrich_snapshot(s, f["accounts"], tokens)


@pytest.mark.parametrize("disabled", [True, False])
def test_closed_cpmm_never_quotes(disabled):
    f = fixture()
    address = next(a for a in f["pools"] if f["accounts"][a]["owner"] == CPMM)
    s = decode_pool(f["accounts"][address])
    s["status"] = 4 if disabled else 0
    s["openTime"] = 0 if disabled else 2**63 - 1
    s = enrich_snapshot(s, f["accounts"], {m: mint_capabilities(a) for m, a in f["mints"].items()})
    assert not s["quoteSupported"]
    with pytest.raises(ValueError):
        quote_cpmm_state(s, s["mintA"], 1000000)


def test_clmm_spot_is_not_vault_ratio_or_amount_quote():
    f = fixture()
    address = next(a for a in f["pools"] if f["accounts"][a]["owner"] != CPMM)
    s = enrich_snapshot(
        decode_pool(f["accounts"][address]),
        f["accounts"],
        {m: mint_capabilities(a) for m, a in f["mints"].items()},
    )
    assert s["kind"] == "clmm" and not s["quoteSupported"]
    with pytest.raises(ValueError):
        quote_cpmm_state(s, s["mintA"], 1000000)


def test_epoch_selects_transfer_fee_and_unknown_extensions_block():
    config = {
        "olderTransferFee": {"epoch": 0, "maximumFee": 4, "transferFeeBasisPoints": 100},
        "newerTransferFee": {"epoch": 5, "maximumFee": 7, "transferFeeBasisPoints": 300},
    }
    assert transfer_fee_profile(config, 4) == {"bps": 100, "maximum": 4}
    assert transfer_fee_profile(config, 5) == {"bps": 300, "maximum": 7}
    mint = copy.deepcopy(fixture()["mints"][USDC_MINT])
    mint["data"]["parsed"]["info"]["extensions"] = [{"extension": "unknown", "state": {}}]
    assert not mint_capabilities(mint)["quote_safe"]
    mint["data"]["parsed"]["type"] = "account"
    assert not mint_capabilities(mint)["verified"]


def test_scaled_ui_and_issuer_pausing_never_pass_native_quote_gate():
    mint = copy.deepcopy(fixture()["mints"][USDC_MINT])
    info = mint["data"]["parsed"]["info"]
    info["extensions"] = [
        {
            "extension": "scaledUiAmountConfig",
            "state": {"multiplier": "1.0001069314314899", "newMultiplier": "1.0001069314314899"},
        }
    ]
    assert mint_capabilities(mint)["reason"] == "scaled_ui_not_one"
    info["extensions"] = [{"extension": "pausableConfig", "state": {"paused": True}}]
    assert mint_capabilities(mint)["reason"] == "mint_paused"
    info["extensions"] = [{"extension": "transferHook", "state": {"programId": TOKEN}}]
    assert mint_capabilities(mint)["reason"] == "active_transfer_hook"


def test_bad_config_owner_rejected():
    f = fixture()
    s = pool_state()
    wrong = copy.deepcopy(f["accounts"][s["config"]])
    wrong["owner"] = TOKEN
    with pytest.raises(ValueError):
        decode_fee_config(wrong, "cpmm")
