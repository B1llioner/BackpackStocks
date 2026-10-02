import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backpack_stocks.models import Settings
from backpack_stocks.stock_cli import OrderJournal, buy, estimate, quote, stock_symbol
from backpack_stocks.transport import RemoteError, UnknownOutcome


def backpack():
    return SimpleNamespace(
        auth=SimpleNamespace(public_key="test-account"),
        public=AsyncMock(return_value=[{"ticker": "PTN", "fmv": 8.886, "lastTrade": {"price": 9.06}}]),
        balances=AsyncMock(return_value={"USDC": {"available": "1000"}}),
        account=AsyncMock(return_value={"liquidating": False, "autoBorrowSettlements": False}),
        private=AsyncMock(),
        submit_rfq=AsyncMock(),
        rfqs=AsyncMock(return_value=[]),
        rfq_history=AsyncMock(return_value=[]),
        rfq_fills=AsyncMock(return_value=[]),
        cancel_rfq=AsyncMock(),
        accept_rfq=AsyncMock(),
    )


def rfq(client_id, *, mode="Immediate", quantity="2"):
    return {
        "rfqId": "123",
        "clientId": client_id,
        "symbol": "PTN.US_USDC_RFQ",
        "side": "Bid",
        "quantity": quantity,
        "executionMode": mode,
        "expiryTime": int(time.time() * 1000) + 10_000,
        "status": "New",
    }


@pytest.fixture(autouse=True)
def session(monkeypatch):
    monkeypatch.setattr("backpack_stocks.stock_cli.validate_session", AsyncMock())


async def test_frontend_estimate_uses_fmv_without_paper_markup_or_venue_trade():
    result = await estimate(backpack(), "PTN.US", "112")
    assert result["estimated_price"] == "8.886"
    assert result["estimated_cost_usdc"] == "995.232"
    assert not result["confirmed"]


@pytest.mark.parametrize(
    "symbol,expected", [("ptn", "PTN.US"), ("BRK.B", "BRK.B.US"), ("FLWS.US", "FLWS.US")]
)
def test_stock_symbol(symbol, expected):
    assert stock_symbol(symbol) == expected


async def test_nonbinding_quote_uses_best_ask_and_never_accepts():
    bp = backpack()
    bp.submit_rfq.side_effect = lambda quantity, client_id, **kw: rfq(client_id, mode="AwaitAccept")

    async def records(*args, **kwargs):
        identity = bp.submit_rfq.call_args.args[1]
        return [
            {
                "rfq": rfq(identity, mode="AwaitAccept"),
                "quotes": [
                    {"rfqId": "123", "quoteId": "a", "status": "New", "askPrice": "9"},
                    {"rfqId": "123", "quoteId": "b", "status": "New", "askPrice": "8.9"},
                    {"rfqId": "other", "status": "New", "askPrice": "1"},
                ],
            }
        ]

    bp.rfqs.side_effect = records
    result = await quote(bp, "PTN.US", "2", 1)
    assert result["confirmed"] and result["rfq_price"] == "8.9"
    assert result["rfq_cost_usdc"] == "17.8"
    bp.cancel_rfq.assert_awaited_once_with("123")
    bp.accept_rfq.assert_not_called()


async def test_no_candidate_is_not_a_confirmed_quote_even_when_submit_returned_200():
    bp = backpack()
    bp.submit_rfq.side_effect = lambda quantity, client_id, **kw: rfq(client_id, mode="AwaitAccept")
    bp.cancel_rfq.side_effect = RemoteError("expired")
    result = await quote(bp, "PTN.US", "2", 0.001)
    assert not result["confirmed"] and result["reason"] == "no_rfq_candidate"
    assert "cleanup" in result
    bp.accept_rfq.assert_not_called()


async def test_quote_inspection_does_not_discard_short_remaining_acceptance_window():
    bp = backpack()
    request = None

    def submit(quantity, client_id, **kwargs):
        nonlocal request
        request = rfq(client_id, mode="AwaitAccept")
        request["expiryTime"] = int(time.time() * 1000) + 1000
        return request

    bp.submit_rfq.side_effect = submit

    async def records(*args, **kwargs):
        return [
            {
                "rfq": request,
                "quotes": [{"rfqId": "123", "quoteId": "a", "status": "New", "askPrice": "8.9"}],
            }
        ]

    bp.rfqs.side_effect = records
    result = await quote(bp, "PTN.US", "2", 1)
    assert result["confirmed"] and result["quote_poll_count"] == 1
    assert result["observed_quote_count"] == 1
    bp.accept_rfq.assert_not_called()
    bp.cancel_rfq.assert_awaited_once_with("123")


async def purchase(bp, folder, *, live=True, **overrides):
    args = {"symbol": "PTN.US", "quantity": "2", "max_price": "9", "request_id": "test", "timeout": 0.001}
    args.update(overrides)
    return await buy(bp, Settings(), live=live, folder=folder, **args)


async def test_dry_run_never_reads_account_or_submits(tmp_path):
    bp = backpack()
    result = await purchase(bp, tmp_path, live=False)
    assert result["state"] == "DRY_RUN" and result["maximum_cost_usdc"] == "18"
    bp.balances.assert_not_called()
    bp.private.assert_not_called()
    assert not list(tmp_path.iterdir())


async def test_purchase_sends_one_price_limited_immediate_rfq_and_verifies_fill(tmp_path):
    bp = backpack()
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    bp.rfq_fills.return_value = [{"rfqId": "123", "fillQuantity": "2", "fillQuoteQuantity": "17.8"}]
    first = await purchase(bp, tmp_path)
    second = await purchase(bp, tmp_path)
    assert first == second and first["state"] == "FILLED" and first["average_price"] == "8.9"
    bp.private.assert_awaited_once()
    params = bp.private.call_args.kwargs
    assert params["executionMode"] == "Immediate" and params["price"] == "9" and params["quantity"] == "2"
    assert all(params[k] is False for k in ["autoBorrow", "autoLend", "autoLendRedeem", "autoBorrowRepay"])


async def test_lost_submit_response_is_never_retried(tmp_path):
    bp = backpack()
    bp.private.side_effect = UnknownOutcome("lost")
    with pytest.raises(UnknownOutcome):
        await purchase(bp, tmp_path)
    result = await purchase(bp, tmp_path)
    assert result["state"] == "UNKNOWN"
    bp.private.assert_awaited_once()


async def test_existing_request_id_cannot_change_quantity(tmp_path):
    bp = backpack()
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    await purchase(bp, tmp_path)
    with pytest.raises(ValueError, match="different purchase"):
        await purchase(bp, tmp_path, quantity="3")
    bp.private.assert_awaited_once()


async def test_insufficient_usdc_does_not_spend_usd_or_borrow(tmp_path):
    bp = backpack()
    bp.balances.return_value = {"USD": {"available": "1000"}, "USDT": {"available": "1000"}}
    with pytest.raises(ValueError, match="Insufficient available USDC"):
        await purchase(bp, tmp_path)
    bp.private.assert_not_called()


async def test_funded_purchase_disables_rfq_borrow_without_changing_collateral_settings(tmp_path):
    bp = backpack()
    bp.account.return_value = {"liquidating": False, "autoBorrowSettlements": True}
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    bp.rfq_fills.return_value = [{"rfqId": "123", "fillQuantity": "2", "fillQuoteQuantity": "17.8"}]
    result = await purchase(bp, tmp_path)
    assert result["state"] == "FILLED"
    bp.private.assert_awaited_once()
    assert bp.private.call_args.args[:2] == ("POST", "/api/v1/rfq")
    assert bp.private.call_args.kwargs["autoBorrow"] is False


async def test_liquidating_account_cannot_submit_purchase(tmp_path):
    bp = backpack()
    bp.account.return_value = {"liquidating": True}
    with pytest.raises(ValueError, match="liquidating"):
        await purchase(bp, tmp_path)
    bp.private.assert_not_called()


async def test_expired_rfq_and_missing_fill_are_not_reported_as_a_purchase(tmp_path):
    bp = backpack()
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    bp.rfq_history.return_value = [{"rfqId": "123", "status": "Expired"}]
    assert (await purchase(bp, tmp_path))["state"] == "EXPIRED"


async def test_fill_beyond_limit_is_not_claimed_successful(tmp_path):
    bp = backpack()
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    bp.rfq_fills.return_value = [{"rfqId": "123", "fillQuantity": "2", "fillQuoteQuantity": "19"}]
    with pytest.raises(ValueError, match="exceed"):
        await purchase(bp, tmp_path)
    assert bp.private.await_count == 1


async def test_cost_cap_prevents_submission_but_does_not_prevent_reconciling_an_old_purchase(tmp_path):
    bp = backpack()
    bp.private.side_effect = lambda *args, **kw: rfq(kw["clientId"])
    await purchase(bp, tmp_path)
    settings = Settings()
    settings.policy.max_cost_usdc = "10"
    common = dict(symbol="PTN.US", quantity="2", max_price="9", timeout=0.001, folder=tmp_path)
    with pytest.raises(ValueError, match="exceeds"):
        await buy(bp, settings, live=True, request_id="new", **common)
    with pytest.raises(ValueError, match="exceeds"):
        await buy(bp, settings, live=False, request_id="dry", **common)
    bp.rfq_fills.return_value = [{"rfqId": "123", "fillQuantity": "2", "fillQuoteQuantity": "17.8"}]
    result = await buy(bp, settings, live=True, request_id="test", **common)
    assert result["state"] == "FILLED"
    bp.private.assert_awaited_once()


def test_request_journal_has_exclusive_lock_and_rejects_traversal(tmp_path):
    first = OrderJournal(tmp_path, "test")
    try:
        with pytest.raises(BlockingIOError):
            OrderJournal(tmp_path, "test")
        with pytest.raises(ValueError):
            OrderJournal(tmp_path, "../other")
    finally:
        first.close()


@pytest.mark.parametrize(
    "command,flags,authenticated",
    [
        ("stock-quote", [], False),
        ("stock-quote", ["--rfq"], True),
        ("stock-buy", [], False),
        ("stock-buy", ["--live"], True),
    ],
)
def test_stock_cli_loads_only_required_environment_before_command_without_opening_engine_db(
    monkeypatch, capsys, command, flags, authenticated
):
    from backpack_stocks import cli

    order = []
    settings = Settings()
    monkeypatch.setattr(cli, "load_settings", lambda _: settings)

    def load(path, names):
        assert (settings.backpack_seed_env in names) is authenticated
        assert settings.wallet_seed_env not in names
        order.append("environment")

    async def execute(args, config):
        assert order == ["environment"]
        order.append("stock")
        return {"state": "TEST"}

    store = Mock(side_effect=AssertionError("Stock CLI must not open the running engine database"))
    monkeypatch.setattr("backpack_stocks.store.Store", store)
    monkeypatch.setattr(cli, "load_environment", load)
    monkeypatch.setattr("backpack_stocks.stock_cli.run_stock_command", execute)
    argv = ["backpack-stocks", command, "--symbol", "PTN", "--quantity", "2", *flags]
    if command == "stock-buy":
        argv += ["--max-price", "9", "--request-id", "test"]
    monkeypatch.setattr("sys.argv", argv)
    cli.main()
    assert order == ["environment", "stock"]
    assert '"TEST"' in capsys.readouterr().out
    store.assert_not_called()
