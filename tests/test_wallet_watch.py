import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.pubkey import Pubkey
from solders.signature import Signature

from backpack_stocks.models import STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT
from backpack_stocks.solana import SolanaRPC
from backpack_stocks.wallet_stream import WalletStream
from backpack_stocks.wallet_watch import TITAN, WalletJournal, WalletWatcher, decode_activity

WALLET = str(Pubkey.from_bytes(bytes([7]) * 32))
OTHER = str(Pubkey.from_bytes(bytes([8]) * 32))


def sig(number=1):
    return str(Signature.from_bytes(bytes([number]) * 64))


async def test_stream_signature_is_durable_deduplicated_and_decoded_without_http_sweep(journal):
    rpc = SimpleNamespace(transaction=AsyncMock(return_value=transaction()))
    monitor = SimpleNamespace(
        assets={"PUSA.US": SimpleNamespace(mint=STOCK_MINT, symbol="PUSA.US")},
        results={},
        request_refresh=lambda s: False,
        budget_info=lambda s: {},
    )
    watcher = WalletWatcher(journal, rpc, monitor, clock=lambda: 1005)
    stream = WalletStream("https://example.test/rpc", None, watcher.queue_signature)
    payload = {
        "method": "logsNotification",
        "params": {"subscription": 42, "result": {"value": {"signature": sig()}}},
    }
    stream.ingest(payload, {42: WALLET})
    stream.ingest(payload, {42: WALLET})
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1
    assert watcher.decode_ready.is_set()
    await watcher.decode_pending()
    assert len(journal.notifications()) == 1
    assert journal.notifications()[0]["correlation"]["refresh_requested"] is False


def test_unknown_stream_subscription_cannot_enqueue_wallet_activity(journal):
    watcher = WalletWatcher(journal, None, None)
    stream = WalletStream("https://example.test", None, watcher.queue_signature)
    stream.ingest(
        {
            "method": "logsNotification",
            "params": {"subscription": 99, "result": {"value": {"signature": sig()}}},
        },
        {42: WALLET},
    )
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def balance(index, mint, owner, units, decimals=6):
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "programId": TOKEN if mint == USDC_MINT else TOKEN_2022,
        "uiTokenAmount": {"amount": str(units), "decimals": decimals},
    }


def transaction(signature=None):
    return {
        "version": 1,
        "slot": 123,
        "blockTime": 1005,
        "transaction": {
            "signatures": [signature or sig()],
            "message": {"accountKeys": [{"pubkey": WALLET}], "instructions": [{"programId": TITAN}]},
        },
        "meta": {
            "err": None,
            "fee": 5000,
            "innerInstructions": None,
            "preTokenBalances": [
                balance(1, STOCK_MINT, WALLET, 1_999_840_000),
                balance(2, USDC_MINT, WALLET, 100_000_000),
                balance(3, USDC_MINT, OTHER, 7_000_000_000),
            ],
            "postTokenBalances": [
                balance(1, STOCK_MINT, WALLET, 0),
                balance(2, USDC_MINT, WALLET, 6_798_224_620),
                balance(3, USDC_MINT, OTHER, 0),
            ],
        },
    }


@pytest.fixture
def journal(tmp_path):
    result = WalletJournal(tmp_path / "watch.sqlite3")
    result.add(WALLET, now=1000)
    yield result
    result.close()


def test_public_trade_uses_owner_integer_deltas_and_never_claims_offchain_profit():
    record = decode_activity(transaction(), WALLET, sig(), {STOCK_MINT: "PUSA.US"})
    assert record["kind"] == "sale"
    assert record["legs"][0]["raw"] == -1_999_840_000
    assert record["usdc_raw"] == 6_698_224_620
    assert record["version"] == 1
    assert not record["acquisition_cost_known"]
    tx = transaction()
    tx["transaction"]["message"]["instructions"] = []
    assert decode_activity(tx, WALLET, sig(), {STOCK_MINT: "PUSA.US"})["kind"] == "paired_transfer"


def test_created_and_closed_token_accounts_and_failed_transactions():
    tx = transaction()
    tx["meta"]["postTokenBalances"][0].pop("owner")
    assert decode_activity(tx, WALLET, sig(), {STOCK_MINT: "PUSA.US"})["legs"][0]["raw"] < 0
    tx["meta"]["err"] = {"InstructionError": [1, "Custom"]}
    record = decode_activity(tx, WALLET, sig(), {STOCK_MINT: "PUSA.US"})
    assert record["kind"] == "failed" and record["legs"] == [] and record["usdc_raw"] == 0
    assert record["fee_lamports"] == 5000


@pytest.mark.parametrize("field", ["signature", "decimals", "owner", "amount"])
def test_inconsistent_rpc_balances_are_rejected(field):
    tx = transaction()
    if field == "signature":
        tx["transaction"]["signatures"][0] = sig(2)
    elif field == "decimals":
        tx["meta"]["postTokenBalances"][1]["uiTokenAmount"]["decimals"] = 9
    elif field == "owner":
        tx["meta"]["postTokenBalances"][0]["owner"] = OTHER
    else:
        tx["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = "-1"
    with pytest.raises(ValueError):
        decode_activity(tx, WALLET, sig(), {STOCK_MINT: "PUSA.US"})


async def test_rpc_reader_accepts_transaction_v1_without_sending():
    transport = SimpleNamespace(request=AsyncMock(return_value={"result": transaction()}))
    rpc = SolanaRPC("https://example.test", transport)
    await rpc.transaction(sig(), priority=20)
    arguments = transport.request.await_args.kwargs
    assert arguments["body"]["method"] == "getTransaction"
    assert arguments["body"]["params"][1]["maxSupportedTransactionVersion"] == 1
    assert arguments["mutation"] is False and arguments["priority"] == 20


async def test_initial_import_is_quiet_but_new_transactions_after_registration_notify(journal):
    rpc = SimpleNamespace(
        call=AsyncMock(
            return_value=[{"signature": sig(2), "blockTime": 1001}, {"signature": sig(), "blockTime": 900}]
        )
    )
    watcher = WalletWatcher(journal, rpc, None, clock=lambda: 1005)
    account = journal.db.execute("SELECT * FROM accounts").fetchone()
    await watcher.poll_account(account)
    rows = journal.db.execute("SELECT signature,historical FROM transactions ORDER BY rowid").fetchall()
    assert [tuple(row) for row in rows] == [(sig(2), 0), (sig(), 1)]
    account = journal.db.execute("SELECT * FROM accounts").fetchone()
    await watcher.poll_account(account)
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2


async def test_pagination_keeps_old_cursor_until_all_signatures_are_durable(journal):
    journal.db.execute("UPDATE accounts SET head=?", (sig(1),))
    rpc = SimpleNamespace(
        call=AsyncMock(return_value=[{"signature": sig(i), "blockTime": 1005} for i in range(100, 50, -1)])
    )
    watcher = WalletWatcher(journal, rpc, None, clock=lambda: 1005)
    await watcher.poll_account(journal.db.execute("SELECT * FROM accounts").fetchone())
    account = journal.db.execute("SELECT * FROM accounts").fetchone()
    assert account["head"] == sig(1) and account["target_head"] == sig(100)
    assert account["crawl_before"] == sig(51)
    rpc.call.return_value = [
        {"signature": sig(50), "blockTime": 1004},
        {"signature": sig(1), "blockTime": 900},
    ]
    await watcher.poll_account(account)
    updated = journal.db.execute("SELECT * FROM accounts").fetchone()
    assert updated["head"] == sig(100) and updated["crawl_before"] is None
    assert journal.db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 51
    assert rpc.call.await_args.args[1][1]["before"] == sig(51)


async def test_null_transaction_is_retried_and_notification_survives_restart(journal):
    journal.db.execute("INSERT INTO transactions(wallet,signature,historical) VALUES(?,?,0)", (WALLET, sig()))
    rpc = SimpleNamespace(transaction=AsyncMock(return_value=None))
    monitor = SimpleNamespace(
        assets={STOCK_MINT: SimpleNamespace(mint=STOCK_MINT, symbol="PUSA.US")},
        results={},
        request_refresh=lambda s: True,
        request_optimization=lambda s: True,
        budget_info=lambda s: {},
    )
    watcher = WalletWatcher(journal, rpc, monitor, clock=lambda: 1005)
    watcher.discover_accounts = AsyncMock()
    watcher.poll_account = AsyncMock()
    await watcher.poll_once()
    assert journal.db.execute("SELECT state FROM transactions").fetchone()[0] == "pending"
    assert journal.notifications() == []
    watcher.clock = lambda: 1030
    rpc.transaction.return_value = transaction()
    await watcher.poll_once()
    notifications = journal.notifications()
    assert len(notifications) == 1
    second = WalletJournal(journal.path)
    try:
        assert len(second.notifications()) == 1
        second.ack(notifications[0])
        assert journal.notifications() == []
    finally:
        second.close()


def test_correlation_rejects_quote_received_after_trade(journal):
    row = {
        "asset": "PUSA.US",
        "direction": "backpack_to_jupiter",
        "observed_at": 1006,
        "dex_observed_at": 1004,
        "backpack_timestamp": 1003,
        "backpack_price": "3.17",
        "backpack_rfq_valid_until": 1020,
        "cost_raw": 100_000_000,
        "profit_raw": 500_000,
        "delivered_raw": 100_000_000,
        "quoted_output_raw": 101_000_000,
        "minimum_output_raw": 100_500_000,
        "search_budget_usdc": "100",
    }
    monitor = SimpleNamespace(
        results={("PUSA.US", "backpack_to_jupiter"): row},
        settings=SimpleNamespace(monitor=SimpleNamespace(max_quote_age_s=15)),
        request_refresh=lambda s: True,
        request_optimization=lambda s: True,
        budget_info=lambda s: {},
    )
    watcher = WalletWatcher(journal, None, monitor, clock=lambda: 1008)
    record = decode_activity(transaction(), WALLET, sig(), {STOCK_MINT: "PUSA.US"})
    assert watcher.correlation(record)["reason"] == "no_contemporaneous_quote"
    before = copy.deepcopy(row)
    before["observed_at"] = 1004
    watcher.capture_quote(before)
    assert watcher.correlation(record)["reason"] == "recent_monitor_quote"


def test_wallet_registration_rejects_invalid_addresses(journal):
    with pytest.raises(ValueError):
        journal.add("../.env")
    assert journal.db.execute("SELECT COUNT(*) FROM wallets").fetchone()[0] == 1


def test_recent_history_is_sorted_by_chain_time_not_late_backfill_insertion(journal):
    latest = decode_activity(transaction(sig(1)), WALLET, sig(1), {STOCK_MINT: "PUSA.US"})
    for number in range(1, 221):
        record = copy.deepcopy(latest)
        record["signature"] = sig(number)
        record["block_time"] = 1005 if number == 1 else 900
        journal.db.execute(
            "INSERT INTO transactions(wallet,signature,historical,state,body,block_time) "
            "VALUES(?,?,1,'ready',?,?)",
            (WALLET, sig(number), json.dumps(record), record["block_time"]),
        )
    assert journal.activities(limit=1)[0]["signature"] == sig(1)


def test_cli_search_queue_waits_until_asset_is_ready_and_deduplicates(journal):
    journal.request_size_search("PUSA")
    journal.request_size_search("pusa")
    assert journal.db.execute("SELECT COUNT(*) FROM size_requests").fetchone()[0] == 1
    monitor = SimpleNamespace(request_optimization=lambda symbol: False)
    watcher = WalletWatcher(journal, None, monitor)
    watcher.apply_size_requests()
    assert journal.db.execute("SELECT COUNT(*) FROM size_requests").fetchone()[0] == 1
    monitor.request_optimization = lambda symbol: symbol == "PUSA.US"
    watcher.apply_size_requests()
    assert journal.db.execute("SELECT COUNT(*) FROM size_requests").fetchone()[0] == 0
