import base64
import importlib.util
import json
import struct
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.pubkey import Pubkey

from backpack_stocks.direct_execution import DirectJournal
from backpack_stocks.models import STOCK_MINT, TOKEN_2022, Settings
from backpack_stocks.native_direct import native_simulation
from backpack_stocks.swap_guard import SYSTEM

WALLET = str(Pubkey.from_bytes(bytes([77]) * 32))
RENT = 2_100_000


def stock_account(units, *, owner=WALLET, delegate=0):
    data = bytearray(165)
    data[:32] = bytes(Pubkey.from_string(STOCK_MINT))
    data[32:64] = bytes(Pubkey.from_string(owner))
    struct.pack_into("<Q", data, 64, units)
    struct.pack_into("<I", data, 72, delegate)
    data[108] = 1
    return {"owner": TOKEN_2022, "data": [base64.b64encode(data).decode(), "base64"], "lamports": RENT}


def payer(lamports):
    return {"owner": SYSTEM, "data": ["", "base64"], "lamports": lamports}


@pytest.mark.parametrize("buy", [True, False])
@pytest.mark.parametrize("corruption", [None, "extra_debit", "delegate", "unclosed_native", "wrong_owner"])
def test_native_swap_checks_exact_sol_cost_rent_token_credit_and_cleanup(buy, corruption):
    fee = 10_500
    amount = 80_000_000 if buy else 2_000_000
    minimum = 2_000_000 if buy else 79_000_000
    before = [payer(200_000_000), None if buy else stock_account(amount), None]
    after_stock = stock_account(minimum) if buy else stock_account(0)
    after_sol = 200_000_000 - amount - RENT - fee if buy else 200_000_000 + minimum - fee
    if corruption == "extra_debit":
        after_sol -= 1
    if corruption == "delegate":
        after_stock = stock_account(minimum if buy else 0, delegate=1)
    if corruption == "wrong_owner":
        after_stock = stock_account(minimum if buy else 0, owner=STOCK_MINT)
    after = [payer(after_sol), after_stock, payer(1) if corruption == "unclosed_native" else None]
    settings = Settings()

    def call():
        return native_simulation(
            {"accounts": before},
            {"err": None, "accounts": after},
            buy=buy,
            amount=amount,
            minimum=minimum,
            wallet=WALLET,
            mint=STOCK_MINT,
            program=TOKEN_2022,
            fee=fee,
            close_stock=False,
            settings=settings,
        )

    if corruption:
        with pytest.raises(ValueError):
            call()
    else:
        assert call() == (minimum, RENT if buy else 0)


@pytest.mark.parametrize("existing_extra", [False, True])
def test_close_stock_refunds_rent_but_cannot_close_other_inventory(existing_extra):
    amount = 2_000_000
    before = [payer(200_000_000), stock_account(amount + int(existing_extra)), None]
    after = [payer(200_000_000 + 79_000_000 + RENT - 10_500), None, None]
    if existing_extra:
        with pytest.raises(ValueError):
            native_simulation(
                {"accounts": before},
                {"err": None, "accounts": after},
                buy=False,
                amount=amount,
                minimum=79_000_000,
                wallet=WALLET,
                mint=STOCK_MINT,
                program=TOKEN_2022,
                fee=10_500,
                close_stock=True,
                settings=Settings(),
            )
    else:
        assert native_simulation(
            {"accounts": before},
            {"err": None, "accounts": after},
            buy=False,
            amount=amount,
            minimum=79_000_000,
            wallet=WALLET,
            mint=STOCK_MINT,
            program=TOKEN_2022,
            fee=10_500,
            close_stock=True,
            settings=Settings(),
        ) == (79_000_000, -RENT)


def test_closed_native_simulation_accepts_only_purged_or_empty_system_accounts():
    from backpack_stocks.native_direct import closed_account

    assert closed_account(None) and closed_account(payer(0))
    assert not closed_account(stock_account(0))
    assert not closed_account(payer(1))
    assert not closed_account({**payer(0), "data": ["AA==", "base64"]})


@pytest.mark.parametrize("begin_succeeded", [False, True])
async def test_round_trip_failure_never_associates_previous_buy_with_sale(tmp_path, begin_succeeded):
    script = Path(__file__).resolve().parents[1] / "scripts/direct_sol_round_trip.py"
    spec = importlib.util.spec_from_file_location("direct_sol_round_trip", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    journal = DirectJournal(str(tmp_path / "native.sqlite3"))
    previous = journal.begin(WALLET, {})
    journal.update(previous, "COMPLETE")
    context = {"wallet": WALLET, "phase": "SELLING", "buy_id": previous}
    state = tmp_path / "round.json"
    module.save(state, context)

    async def fail(*args):
        if begin_succeeded:
            current = journal.begin(WALLET, {})
            journal.update(current, "CANCELLED")
        raise ValueError("cannot prepare sale")

    journal.execute = AsyncMock(side_effect=fail)
    try:
        with pytest.raises(ValueError, match="cannot prepare sale"):
            await module.execute_leg(journal, SimpleNamespace(), {}, 1, state, context, "sell_id")
        saved = json.loads(state.read_text())
        assert saved["phase"] == "SELLING" and saved["buy_id"] == previous
        if begin_succeeded:
            assert saved["sell_id"] != previous
            assert journal.get(saved["sell_id"])["status"] == "CANCELLED"
        else:
            assert "sell_id" not in saved
    finally:
        journal.close()
