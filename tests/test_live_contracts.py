from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.engine import Engine
from backpack_stocks.live import LiveBroker
from backpack_stocks.models import Cycle, Phase, Policy, Settings
from backpack_stocks.transfer import usdc_transfer_instructions


def cycle():
    return Cycle(
        id="cycle",
        mode="live",
        phase=Phase.BUY_PENDING,
        created=1,
        updated=1,
        quantity_raw=1000000,
        client_id=1,
        rfq_id="rfq",
        quote_id="quote",
        data={"cex_stock_before": 1000000, "cex_usdc_before": 100000000},
    )


def broker():
    # Use public/paper Settings for adapter unit tests: no credentials and no admission bypass in runtime.
    cfg = Settings(wallet="11111111111111111111111111111111", policy=Policy(withdrawal_semantics="deduct"))
    bp = SimpleNamespace(
        rfq_fills=AsyncMock(
            return_value=[
                {
                    "rfqId": "rfq",
                    "quoteId": "quote",
                    "quantity": "1",
                    "fillQuantity": "0.5",
                    "fillQuoteQuantity": "3",
                }
            ]
        ),
        rfq_history=AsyncMock(return_value=[{"rfqId": "rfq", "status": "Filled"}]),
        balances=AsyncMock(return_value={"WEN.US": {"available": "1.5"}, "USDC": {"available": "97"}}),
    )
    return LiveBroker(cfg, bp, None, None, None, None), bp


async def test_live_partial_fill_uses_fill_fields_not_requested_quantity():
    b, _ = broker()
    fill = await b.fill(cycle())
    assert fill["quantity"] == 500000
    assert fill["cost"] == 3000000


async def test_live_fill_needs_credited_balance_and_exact_fee():
    b, bp = broker()
    bp.balances.return_value["USDC"]["available"] = "96.99"
    with pytest.raises(ValueError, match="cost/fee"):
        await b.fill(cycle())
    bp.balances.return_value["USDC"]["available"] = "97"
    bp.balances.return_value["WEN.US"]["available"] = "1"
    with pytest.raises(ValueError, match="credited"):
        await b.fill(cycle())


async def test_live_fill_waits_for_terminal_rfq_and_missing_fill_quantity_fails_closed():
    b, bp = broker()
    bp.rfq_history.return_value[0]["status"] = "New"
    assert await b.fill(cycle()) is None
    bp.rfq_history.return_value[0]["status"] = "Filled"
    del bp.rfq_fills.return_value[0]["fillQuantity"]
    with pytest.raises(KeyError):
        await b.fill(cycle())


async def test_live_scanner_cannot_create_first_pilot_entry(tmp_path):
    from backpack_stocks.store import Store

    p = Policy(
        withdrawal_semantics="deduct",
        live_fee_verified=True,
        live_rfq_verified=True,
        live_quantity_mapping_verified=True,
        live_withdrawal_verified=True,
    )
    cfg = Settings(
        mode="live",
        source="public",
        enable_live=True,
        wallet="example",
        policy=p,
        database=str(tmp_path / "pilot.sqlite3"),
    )
    store = Store(cfg.database, cfg)
    try:
        e = Engine(cfg, store, None, None)
        await e.start_if_profitable()
        assert not store.cycles()
    finally:
        store.close()


async def test_explicit_pilot_can_scout_rfq_without_promoting_indicative_price(tmp_path):
    from backpack_stocks.store import Store

    p = Policy(
        withdrawal_semantics="deduct",
        live_fee_verified=True,
        live_rfq_verified=True,
        live_quantity_mapping_verified=True,
        live_withdrawal_verified=True,
        cooldown_s=0,
    )
    cfg = Settings(
        mode="live",
        source="public",
        enable_live=True,
        wallet="example",
        policy=p,
        database=str(tmp_path / "scout.sqlite3"),
    )
    store = Store(cfg.database, cfg)
    try:
        e = Engine(
            cfg,
            store,
            None,
            SimpleNamespace(preflight=AsyncMock(return_value={"usdc": 1000000000, "stock": 0})),
        )
        await e.start_if_profitable(pilot=True)
        c = store.active()
        assert c.phase == Phase.RFQ_OPEN
        assert "no purchase" in c.data["signal"]["source"]
        assert store.balance("backpack", "USDC", "reserved") == 0
        assert store.find_intent(c.id, "buy") is None
    finally:
        store.close()


def test_automated_live_requires_post_pilot_evidence():
    p = Policy(
        withdrawal_semantics="deduct",
        live_fee_verified=True,
        live_rfq_verified=True,
        live_quantity_mapping_verified=True,
        live_withdrawal_verified=True,
    )
    with pytest.raises(ValueError, match="evidence"):
        Settings(mode="live", source="public", enable_live=True, wallet="example", policy=p, live_pilot=False)


def test_capital_return_transfer_is_fixed_mint_precision_recipient_and_amount():
    from backpack_stocks.models import ATA, TOKEN, USDC_MINT
    from backpack_stocks.solana import associated_address

    wallet, dest = "11111111111111111111111111111111", TOKEN
    ixs = usdc_transfer_instructions(wallet, dest, 1234567)
    assert str(ixs[0].program_id) == ATA
    assert str(ixs[1].program_id) == TOKEN
    assert ixs[1].data == bytes.fromhex("0c87d612000000000006")
    assert str(ixs[1].accounts[1].pubkey) == USDC_MINT
    assert str(ixs[1].accounts[2].pubkey) == associated_address(dest, USDC_MINT, TOKEN)
    assert ixs[1].accounts[3].is_signer
    with pytest.raises(ValueError):
        usdc_transfer_instructions(wallet, wallet, 1)


async def test_live_gas_budget_accounts_for_two_transactions_and_both_ata_rents():
    b, _ = broker()
    b.settings.rebalance = "each_cycle"
    b.backpack.deposit_address = AsyncMock(return_value={"address": "11111111111111111111111111111111"})
    b.rpc = SimpleNamespace(
        call=AsyncMock(return_value=2039280), accounts=AsyncMock(return_value={"value": [None]})
    )
    b.jupiter = SimpleNamespace(quote_pair=AsyncMock(return_value={"outAmount": "400000"}))
    wallet = {"lamports": 100000000, "accounts": [None, None, None]}
    with pytest.raises(ValueError, match="gas budget"):
        await b.gas_readiness(wallet)
    b.settings.policy.gas_budget_usdc = "0.50"
    result = await b.gas_readiness(wallet)
    assert result["maximum_lamports"] == 600000 + 2 * 2039280
    assert result["minimum_budget_usdc_raw"] == 440000
