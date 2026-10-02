import base64
import copy
import hashlib
import json
import sqlite3
import struct
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from test_chain_and_swap import token_account

from backpack_stocks.direct import DirectRaydium, direct_instructions
from backpack_stocks.direct_execution import (
    DirectJournal,
    DirectSwapGuard,
    WalletLease,
    validate_direct_simulation,
)
from backpack_stocks.models import STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, Settings
from backpack_stocks.pool_state import CPMM
from backpack_stocks.raydium import CLMM

ROOT = Path(__file__).resolve().parents[1]


def fixture():
    return json.loads((ROOT / "tests/fixtures/direct-raydium/wen-direct-quote.json").read_text())


def fresh_quote():
    q = fixture()["quote"]
    q["_observed_at"] = time.time()
    return q


class ReplayRPC:
    def __init__(self, records):
        self.records = iter(records)

    async def accounts(self, addresses, **kwargs):
        record = next(self.records)
        assert addresses == record["addresses"]
        return record["result"]


@pytest.mark.parametrize(
    "change",
    [None, "stale_clock", "spacing", "fee", "paused", "vault", "mint", "tick", "bitmap", "pool_owner"],
)
async def test_real_clmm_snapshot_full_dynamic_quote_and_fail_closed(monkeypatch, change):
    saved = fixture()
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: saved["quote"]["_observed_at"])
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    state = saved["quote"]["_direct"]["state"]
    if change == "stale_clock":
        clock = next(a for a in rows.values() if a and a["owner"].startswith("Sysvar"))
        clock["data"]["parsed"]["info"]["unixTimestamp"] -= 60
    elif change in {"spacing", "fee", "paused", "tick", "bitmap"}:
        address, offset, replacement = {
            "spacing": (state["config"], 51, struct.pack("<H", 1)),
            "fee": (state["config"], 47, struct.pack("<I", 1_000_000)),
            "paused": (saved["quote"]["pool"], 389, bytes([16])),
            "tick": (saved["quote"]["_direct"]["arrays"][0], 8, bytes(32)),
            "bitmap": (saved["quote"]["_direct"]["bitmap"], 8, bytes(32)),
        }[change]
        raw = bytearray(base64.b64decode(rows[address]["data"][0]))
        raw[offset : offset + len(replacement)] = replacement
        rows[address]["data"][0] = base64.b64encode(raw).decode()
    elif change == "vault":
        rows[state["vaultA"]]["data"]["parsed"]["info"]["owner"] = TOKEN
    elif change == "mint":
        rows[STOCK_MINT]["data"]["parsed"]["info"]["decimals"] = 5
    elif change == "pool_owner":
        rows[saved["quote"]["pool"]]["owner"] = CPMM
    router = DirectRaydium(Settings(dex_backend="raydium"), ReplayRPC(saved["rpc_records"]))
    try:
        call = router.pool_quote(saved["quote"]["pool"], STOCK_MINT, USDC_MINT, 1_000_000)
        if change:
            with pytest.raises(ValueError):
                await call
        else:
            q = await call
            assert q["outAmount"] == "6238893"
            assert q["otherAmountThreshold"] == "6207698"
            assert q["_direct"]["state"]["dynamicFeeControl"] > 0
            assert q["_direct"]["state"]["feeOn"] == 2
    finally:
        await router.close()


async def test_clmm_incomplete_tick_coverage_does_not_return_a_partial_quote():
    saved = fixture()
    q = saved["quote"]
    rows = dict(
        zip(saved["rpc_records"][-1]["addresses"], saved["rpc_records"][-1]["result"]["value"], strict=True)
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), ReplayRPC([]))
    try:
        params = router._worker_params(q["pool"], rows[q["pool"]], rows[q["_direct"]["bitmap"]], STOCK_MINT)
        with pytest.raises(ValueError, match="complete tick"):
            await router.worker.call(
                {
                    **params,
                    "op": "quote",
                    "amount": "1000000",
                    "timestamp": int(q["_observed_at"]),
                    "configData": rows[q["_direct"]["state"]["config"]]["data"][0],
                    "arrays": [],
                }
            )
        assert router.worker.process is not None and router.worker.process.returncode is None
    finally:
        await router.close()


@pytest.mark.parametrize("program,reverse", [(CLMM, False), (CLMM, True), (CPMM, False), (CPMM, True)])
def test_direct_wire_instruction_minimum_and_canonical_account_order(program, reverse):
    q = fresh_quote()
    q["_direct"]["program"] = program
    if reverse:
        q["inputMint"], q["outputMint"] = q["outputMint"], q["inputMint"]
    minimum = int(q["otherAmountThreshold"]) + 1
    wallet = str(Keypair.from_seed(bytes([3]) * 32).pubkey())
    ix = direct_instructions(q, wallet, minimum, Settings())[-1]
    assert str(ix.program_id) == program
    name = b"swap_v2" if program == CLMM else b"swap_base_input"
    assert ix.data[:8] == hashlib.sha256(b"global:" + name).digest()[:8]
    assert struct.unpack_from("<QQ", ix.data, 8) == (1_000_000, minimum)
    assert str(ix.accounts[0].pubkey) == wallet and ix.accounts[0].is_signer
    assert len(ix.accounts) == (14 + len(q["_direct"]["arrays"]) if program == CLMM else 13)
    pool_position = 2 if program == CLMM else 3
    assert str(ix.accounts[pool_position].pubkey) == q["pool"]
    assert ix.accounts[pool_position].is_writable
    assert all(not a.is_signer for a in ix.accounts[1:])


@pytest.mark.parametrize("minimum", [0, 6_000_000, 6_300_000, 2**63])
def test_direct_wire_rejects_weaker_or_impossible_minimum(minimum):
    with pytest.raises(ValueError):
        direct_instructions(fresh_quote(), str(Keypair().pubkey()), minimum, Settings())


def simulation_case(wallet):
    payer = {"owner": "11111111111111111111111111111111", "lamports": 100_000_000, "data": ["", "base64"]}
    a = token_account(STOCK_MINT, wallet, 2_000_000, TOKEN_2022)
    b = token_account(USDC_MINT, wallet, 1_000_000, TOKEN)
    post = [
        copy.deepcopy(payer),
        token_account(STOCK_MINT, wallet, 1_000_000, TOKEN_2022),
        token_account(USDC_MINT, wallet, 7_238_893, TOKEN),
    ]
    post[0]["lamports"] -= 5500
    return {"accounts": [payer, a, b], "programs": [TOKEN_2022, TOKEN]}, {"err": None, "accounts": post}


@pytest.mark.parametrize(
    "change", [None, "debit", "credit", "delegate", "program", "authority", "reserve", "gas", "frozen", "err"]
)
def test_direct_simulation_rejects_changed_authority_and_unbounded_effects(change):
    wallet = str(Keypair().pubkey())
    snap, sim = simulation_case(wallet)
    if change in {"debit", "credit", "delegate", "frozen"}:
        n = 1 if change == "debit" else 2
        raw = bytearray(base64.b64decode(sim["accounts"][n]["data"][0]))
        if change in {"debit", "credit"}:
            struct.pack_into("<Q", raw, 64, 0)
        elif change == "delegate":
            struct.pack_into("<I", raw, 72, 1)
        else:
            raw[108] = 2
        sim["accounts"][n]["data"][0] = base64.b64encode(raw).decode()
    elif change == "program":
        sim["accounts"][2]["owner"] = TOKEN_2022
    elif change == "authority":
        raw = bytearray(base64.b64decode(sim["accounts"][2]["data"][0]))
        raw[32:64] = bytes(32)
        sim["accounts"][2]["data"][0] = base64.b64encode(raw).decode()
    elif change in {"reserve", "gas"}:
        sim["accounts"][0]["lamports"] = 1 if change == "reserve" else 90_000_000
    elif change == "err":
        sim["err"] = {"InstructionError": [3, "Custom"]}

    def call():
        return validate_direct_simulation(
            snap, sim, 1_000_000, 6_207_698, STOCK_MINT, USDC_MINT, wallet, Settings()
        )

    if change:
        with pytest.raises(ValueError):
            call()
    else:
        assert call() == 6_238_893


async def test_guard_simulates_unsigned_before_signing_and_rechecks_before_sending():
    key = Keypair.from_seed(bytes([5]) * 32)
    snap, sim = simulation_case(str(key.pubkey()))
    rpc = SimpleNamespace(
        accounts=AsyncMock(return_value={"value": snap["accounts"], "context": {"slot": 452438938}}),
        send=AsyncMock(),
    )

    async def call(method, params):
        if method == "getLatestBlockhash":
            return {"value": {"blockhash": str(Hash.default()), "lastValidBlockHeight": 99}}
        if method == "getFeeForMessage":
            return {"value": 5500}
        assert method == "getBlockHeight"
        return 90

    rpc.call = call

    async def simulate(encoded, addresses, min_slot):
        tx = VersionedTransaction.from_bytes(base64.b64decode(encoded))
        assert bytes(tx.signatures[0]) == bytes(64)
        return {"value": sim}

    rpc.simulate = simulate
    guard = DirectSwapGuard(None, rpc, key, Settings())
    prepared = await guard.prepare(fresh_quote(), 1_000_000, 6_207_698)
    tx = VersionedTransaction.from_bytes(base64.b64decode(prepared["transaction"]))
    assert tx.verify_with_results() == [True]
    rpc.send.return_value = prepared["signature"]
    assert await guard.send(prepared) == prepared["signature"]
    rpc.send.assert_awaited_once()
    rpc.send.reset_mock()
    rpc.accounts.return_value["value"][1] = token_account(STOCK_MINT, str(key.pubkey()), 1, TOKEN_2022)
    with pytest.raises(ValueError, match="inventory changed"):
        await guard.send(prepared)
    rpc.send.assert_not_awaited()


async def test_guard_rechecks_age_after_async_submission_checks(monkeypatch):
    key = Keypair()
    snap, _ = simulation_case(str(key.pubkey()))
    clock = [100.0]
    monkeypatch.setattr("backpack_stocks.direct_execution.time.time", lambda: clock[0])

    async def accounts(*args, **kwargs):
        clock[0] = 110
        return {"value": snap["accounts"]}

    rpc = SimpleNamespace(call=AsyncMock(return_value=90), accounts=accounts, send=AsyncMock())
    guard = DirectSwapGuard(None, rpc, key, Settings())
    prepared = {
        "wallet": str(key.pubkey()),
        "quote_at": 100,
        "last_valid_height": 99,
        "addresses": [],
        "min_slot": 1,
        "input_before": 2_000_000,
        "output_before": 1_000_000,
    }
    with pytest.raises(ValueError, match="during submission"):
        await guard.send(prepared)
    rpc.send.assert_not_awaited()


async def test_journal_commits_signature_before_send_and_unknown_never_retries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    journal = DirectJournal(str(tmp_path / "swaps.db"))
    guard = SimpleNamespace(_key=Keypair(), prepare=AsyncMock(return_value={"signature": "test-signature"}))

    async def send(prepared):
        # A separate connection sees the signature before any network mutation.
        observer = sqlite3.connect(tmp_path / "swaps.db")
        try:
            row = observer.execute("SELECT status,payload FROM swaps").fetchone()
            assert row[0] == "SENDING" and json.loads(row[1])["signature"] == "test-signature"
        finally:
            observer.close()
        raise TimeoutError()

    guard.send = AsyncMock(side_effect=send)
    try:
        with pytest.raises(TimeoutError):
            await journal.execute(guard, fresh_quote(), 6_207_698)
        identity = journal.db.execute("SELECT id FROM swaps").fetchone()[0]
        assert journal.get(identity)["status"] == "UNKNOWN"
        with pytest.raises(sqlite3.IntegrityError):
            await journal.execute(guard, fresh_quote(), 6_207_698)
        rpc = SimpleNamespace(transaction=AsyncMock(return_value=None))
        assert (await journal.reconcile(identity, rpc))["status"] == "UNKNOWN"
        guard.send.assert_awaited_once()
    finally:
        journal.close()


@pytest.mark.parametrize("status", ["PREPARING", "PREPARED"])
async def test_journal_crash_before_durable_send_is_cancelled_without_sending(tmp_path, status, monkeypatch):
    monkeypatch.chdir(tmp_path)
    journal = DirectJournal(str(tmp_path / "swaps.db"))
    try:
        identity = journal.begin(str(Keypair().pubkey()), {})
        journal.update(identity, status)
        rpc = SimpleNamespace(transaction=AsyncMock())
        assert (await journal.reconcile(identity, rpc))["status"] == "CANCELLED"
        rpc.transaction.assert_not_awaited()
    finally:
        journal.close()


def test_wallet_execution_lease_is_shared_and_exclusive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    wallet = str(Keypair().pubkey())
    first = WalletLease(wallet)
    try:
        with pytest.raises(ValueError, match="active local"):
            WalletLease(wallet)
    finally:
        first.close()
    WalletLease(wallet).close()


def test_live_cannot_autostart_and_paper_digest_has_a_lower_bound():
    with pytest.raises(ValueError):
        Settings(paper_auto_start=True, mode="live")
    with pytest.raises(ValueError):
        Settings(telegram_digest_s=1)


@pytest.mark.parametrize("change", [None, "signature", "debit", "credit", "fee", "failed"])
async def test_finalized_receipt_must_match_original_signature_and_funds(tmp_path, monkeypatch, change):
    monkeypatch.chdir(tmp_path)
    wallet = str(Keypair().pubkey())
    journal = DirectJournal(str(tmp_path / "swap.db"))
    identity = journal.begin(wallet, {})
    prepared = {
        "signature": "original",
        "input_mint": STOCK_MINT,
        "output_mint": USDC_MINT,
        "input": 1_000_000,
        "minimum": 6_207_698,
        "fee_lamports": 5500,
    }
    journal.update(identity, "UNKNOWN", payload=prepared)
    WalletLease.reserve(wallet, "direct", journal.path, identity)

    def row(mint, amount):
        return {"owner": wallet, "mint": mint, "uiTokenAmount": {"amount": str(amount)}}

    tx = {
        "transaction": {"signatures": ["wrong" if change == "signature" else "original"]},
        "meta": {
            "err": "failed" if change == "failed" else None,
            "fee": 5501 if change == "fee" else 5500,
            "preTokenBalances": [row(STOCK_MINT, 2_000_000), row(USDC_MINT, 0)],
            "postTokenBalances": [
                row(STOCK_MINT, 1_000_001 if change == "debit" else 1_000_000),
                row(USDC_MINT, 6_207_697 if change == "credit" else 6_238_893),
            ],
        },
    }
    rpc = SimpleNamespace(transaction=AsyncMock(return_value=tx))
    try:
        if change in {"signature", "debit", "credit", "fee"}:
            with pytest.raises(ValueError):
                await journal.reconcile(identity, rpc)
            assert journal.get(identity)["status"] == "UNKNOWN"
            assert WalletLease.fence(wallet).exists()
        else:
            result = await journal.reconcile(identity, rpc)
            assert result["status"] == ("FAILED" if change == "failed" else "COMPLETE")
            assert not WalletLease.fence(wallet).exists()
    finally:
        journal.close()


def test_durable_wallet_fence_blocks_other_journals_and_allows_own_recovery(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    wallet = str(Keypair().pubkey())
    WalletLease.reserve(wallet, "fsm", "first.db", "runtime")
    WalletLease.reserve(wallet, "fsm", "first.db", "runtime", recover=True)
    with pytest.raises(ValueError, match="durable unresolved"):
        WalletLease.reserve(wallet, "direct", "second.db", "new")
    WalletLease.clear(wallet, "direct", "second.db", "new")
    assert WalletLease.fence(wallet).exists()
    WalletLease.clear(wallet, "fsm", "first.db", "runtime")
    assert not WalletLease.fence(wallet).exists()


async def test_direct_backend_never_constructs_a_jupiter_client(monkeypatch):
    from backpack_stocks.runtime import public_clients

    for name in ("OUTBOUND_PROXY_URL", "BACKPACK_PROXY_URL", "BACKPACK_WS_PROXY_URL", "SOLANA_PROXY_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        "backpack_stocks.runtime.Jupiter", lambda *a, **kw: pytest.fail("Unexpected Jupiter client")
    )
    bp, dex, rpc = public_clients(Settings(dex_backend="raydium"))
    try:
        assert isinstance(dex, DirectRaydium)
    finally:
        await bp.close()
        await dex.close()
        await rpc.close()


@pytest.mark.parametrize("change", [None, "token_program", "reserves", "fee_mode", "observation"])
async def test_mainnet_cpmm_native_quote_and_invariant_failures(monkeypatch, change):
    saved = json.loads((ROOT / "tests/fixtures/direct-raydium/cpmm-direct-quote.json").read_text())
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    final = saved["rpc_records"][-1]
    accounts = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    if change:
        raw = bytearray(base64.b64decode(accounts[q["pool"]]["data"][0]))
        if change == "token_program":
            from solders.pubkey import Pubkey

            side = "A" if q["_direct"]["state"]["mintA"] == q["inputMint"] else "B"
            offset = 232 if side == "A" else 264
            raw[offset : offset + 32] = bytes(Pubkey.from_string(TOKEN))
        elif change == "reserves":
            struct.pack_into("<Q", raw, 341, 2**64 - 1)
        elif change == "fee_mode":
            raw[389] = 3
        else:
            observation = accounts[q["_direct"]["state"]["observation"]]
            observation["owner"] = CLMM
        accounts[q["pool"]]["data"][0] = base64.b64encode(raw).decode()
    router = DirectRaydium(Settings(dex_backend="raydium"), ReplayRPC(saved["rpc_records"]))
    try:
        call = router.pool_quote(q["pool"], q["inputMint"], q["outputMint"], 100_000)
        if change:
            with pytest.raises(ValueError):
                await call
        else:
            result = await call
            assert result["outAmount"] == "871702"
            assert result["otherAmountThreshold"] == "867343"
            assert result["kind"] == "cpmm"
            assert router.worker.process is None
    finally:
        await router.close()
