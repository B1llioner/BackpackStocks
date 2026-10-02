from __future__ import annotations

import base64
import hashlib
import json
import time

import httpx
import pytest
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import MessageV0, to_bytes_versioned
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from backpack_stocks.direct_execution import WalletLease
from backpack_stocks.jupiter_replay import SOL, USDC, JupiterReplay, ReplayError, SwapInput
from backpack_stocks.jupiter_swap import (
    SYSTEM,
    ReplayRPC,
    SwapJournal,
    check_fresh_order,
    order_limits,
    prepare_swap,
    reconcile_swap,
    round_trip,
    submit_swap,
    validate_wallet_effects,
)
from backpack_stocks.jupiter_wallet import (
    JUPITER_MANAGED_SIGNER,
    decode_swap_transaction,
    sign_swap_transaction,
    wallet_from_environment,
)
from backpack_stocks.models import JUPITER, STOCK_MINT, TOKEN


@pytest.mark.parametrize("encoding", ["base58", "json", "base64", "seed"])
def test_wallet_formats_never_expose_private_values(encoding):
    key = Keypair()
    value = {
        "base58": str(key),
        "json": json.dumps(list(bytes(key))),
        "base64": "base64:" + base64.b64encode(bytes(key)).decode(),
        "seed": "base64:" + base64.b64encode(bytes(key)[:32]).decode(),
    }[encoding]
    loaded = wallet_from_environment(environ={"SOLANA_WALLET_KEY": value})
    assert loaded.pubkey() == key.pubkey()
    with pytest.raises(ReplayError) as raised:
        wallet_from_environment(environ={"SOLANA_WALLET_KEY": value + "broken"})
    assert value not in str(raised.value) and raised.value.__suppress_context__


def test_wallet_rejects_mismatched_public_half_and_json_booleans():
    key = Keypair()
    bad = list(bytes(key)[:32] + bytes(Keypair().pubkey()))
    for value in (json.dumps(bad), "[true]", "[]", "private-fixture-not-base58"):
        with pytest.raises(ReplayError):
            wallet_from_environment(environ={"SOLANA_WALLET_KEY": value})


def fixture_transaction(wallet, other=None, other_signed=False, readonly=True):
    accounts = [AccountMeta(wallet.pubkey(), True, True)]
    if other:
        pubkey = other.pubkey() if isinstance(other, Keypair) else other
        accounts.append(AccountMeta(pubkey, True, not readonly))
    message = MessageV0.try_compile(
        wallet.pubkey(),
        [Instruction(Pubkey.from_string(JUPITER), b"fixture", accounts)],
        [],
        Hash.new_unique(),
    )
    signatures = [Signature.default() for _ in range(message.header.num_required_signatures)]
    if isinstance(other, Keypair) and other_signed:
        signatures[list(message.account_keys).index(other.pubkey())] = other.sign_message(
            to_bytes_versioned(message)
        )
    return VersionedTransaction.populate(message, signatures)


def prepared_for(tx, wallet, router="metis"):
    return {
        "backend": "website",
        "receivedAt": time.time(),
        "quote": {
            "transaction": base64.b64encode(bytes(tx)).decode(),
            "taker": str(wallet.pubkey()),
            "requestId": "fixture-request",
            "router": router,
            "signatureFeePayer": str(tx.message.account_keys[0]),
            "inputMint": STOCK_MINT,
            "outputMint": USDC,
            "swapMode": "ExactIn",
            "inAmount": "1000000",
            "outAmount": "2000000",
            "otherAmountThreshold": "1980000",
        },
    }


@pytest.mark.parametrize("mode", ["single", "presigned", "rfq", "frontend-managed"])
def test_partial_signing_keeps_versioned_message_and_other_signatures(mode):
    key, service = Keypair(), Keypair()
    other = (
        Pubkey.from_string(JUPITER_MANAGED_SIGNER)
        if mode == "frontend-managed"
        else service
        if mode != "single"
        else None
    )
    tx = fixture_transaction(key, other, other_signed=mode == "presigned")
    prepared = prepared_for(tx, key, "jupiterz" if mode == "rfq" else "metis")
    signed = decode_swap_transaction(sign_swap_transaction(prepared, key))
    assert to_bytes_versioned(signed.message) == to_bytes_versioned(tx.message)
    assert signed.signatures[0].verify(key.pubkey(), to_bytes_versioned(tx.message))
    assert signed.signatures[1:] == tx.signatures[1:]


def test_signing_rejects_other_wallet_unknown_missing_signer_and_writable_managed_signer():
    key = Keypair()
    with pytest.raises(ReplayError, match="taker"):
        sign_swap_transaction(prepared_for(fixture_transaction(key), key), Keypair())
    with pytest.raises(ReplayError, match="non-wallet"):
        sign_swap_transaction(prepared_for(fixture_transaction(key, Keypair()), key), key)
    tx = fixture_transaction(key, Pubkey.from_string(JUPITER_MANAGED_SIGNER), readonly=False)
    with pytest.raises(ReplayError, match="writable"):
        sign_swap_transaction(prepared_for(tx, key), key)


def account_token(wallet, mint, units, lamports=2_039_280):
    raw = bytearray(165)
    raw[:32] = bytes(Pubkey.from_string(mint))
    raw[32:64] = bytes(Pubkey.from_string(wallet))
    raw[64:72] = units.to_bytes(8, "little")
    raw[108] = 1
    return {"owner": TOKEN, "lamports": lamports, "data": [base64.b64encode(raw).decode(), "base64"]}


def account_wallet(lamports):
    return {"owner": SYSTEM, "lamports": lamports, "data": ["", "base64"]}


def effect_fixture():
    wallet = str(Keypair().pubkey())
    src, dst, unrelated = (str(Keypair().pubkey()) for _ in range(3))
    other_mint = str(Keypair().pubkey())
    before = [
        account_wallet(100_000_000),
        account_token(wallet, STOCK_MINT, 2_000_000),
        account_token(wallet, USDC, 1_000_000),
        account_token(wallet, other_mint, 4),
    ]
    after = [
        account_wallet(99_995_000),
        account_token(wallet, STOCK_MINT, 1_000_000),
        account_token(wallet, USDC, 3_000_000),
        account_token(wallet, other_mint, 4),
    ]
    quote = {"inputMint": STOCK_MINT, "outputMint": USDC, "signatureFeePayer": wallet}
    return [wallet, src, dst, unrelated], before, after, quote


def check_effects(addresses, before, after, quote):
    return validate_wallet_effects(
        addresses,
        before,
        after,
        quote,
        wallet=addresses[0],
        max_input=1_000_000,
        min_output=1_980_000,
        max_sol_cost=5_000_000,
        native_sol=True,
        estimated_fee=5000,
    )


def test_simulation_validates_debit_credit_and_account_control():
    args = effect_fixture()
    assert check_effects(*args)["outputDeltaRaw"] == "2000000"


@pytest.mark.parametrize(
    "mutation", ["overdebit", "low-output", "sol-theft", "unrelated-debit", "authority", "closed", "missing"]
)
def test_simulation_rejects_unsafe_wallet_effects(mutation):
    addresses, before, after, quote = effect_fixture()
    if mutation == "overdebit":
        after[1] = account_token(addresses[0], STOCK_MINT, 1)
    elif mutation == "low-output":
        after[2] = account_token(addresses[0], USDC, 1_500_000)
    elif mutation == "sol-theft":
        after[0]["lamports"] -= 1_000_000
    elif mutation == "unrelated-debit":
        raw = bytearray(base64.b64decode(after[3]["data"][0]))
        raw[64:72] = bytes(8)
        after[3]["data"][0] = base64.b64encode(raw).decode()
    elif mutation == "authority":
        after[2] = account_token(str(Keypair().pubkey()), USDC, 3_000_000)
    elif mutation == "closed":
        after[1] = None
    else:
        after.pop()
    with pytest.raises(ReplayError):
        check_effects(addresses, before, after, quote)


def test_native_sol_amount_bounds_and_output_rent():
    wallet, token_address = str(Keypair().pubkey()), str(Keypair().pubkey())
    before = [account_wallet(100_000_000), None]
    after = [
        account_wallet(100_000_000 - 10_000_000 - 5000 - 2_039_280),
        account_token(wallet, USDC, 2_000_000),
    ]
    quote = {"inputMint": SOL, "outputMint": USDC, "signatureFeePayer": wallet}
    result = validate_wallet_effects(
        [wallet, token_address],
        before,
        after,
        quote,
        wallet=wallet,
        max_input=10_000_000,
        min_output=1_980_000,
        max_sol_cost=3_000_000,
        native_sol=True,
        estimated_fee=5000,
    )
    assert result["newAccountRentLamports"] == 2_039_280


def test_minimum_exactout_and_expiry_checks():
    wallet = Keypair()
    prepared = prepared_for(fixture_transaction(wallet), wallet)
    assert order_limits(prepared) == (1_000_000, 1_980_000)
    q = {**prepared["quote"], "swapMode": "ExactOut", "otherAmountThreshold": "1010000"}
    assert order_limits({"quote": q}) == (1_010_000, 2_000_000)
    with pytest.raises(ReplayError, match="expired"):
        check_fresh_order({**prepared, "receivedAt": time.time() - 31})
    with pytest.raises(ReplayError, match="expiry"):
        check_fresh_order({**prepared, "quote": {**prepared["quote"], "expireAt": "2000-01-01T00:00:00Z"}})


async def test_prepare_rpc_simulation_and_submit_exact_signed_order_with_journal(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    key = Keypair()
    address = str(key.pubkey())
    src, dst = Keypair().pubkey(), Keypair().pubkey()
    message = MessageV0.try_compile(
        key.pubkey(),
        [
            Instruction(
                Pubkey.from_string(JUPITER),
                b"fixture",
                [
                    AccountMeta(key.pubkey(), True, True),
                    AccountMeta(src, False, True),
                    AccountMeta(dst, False, True),
                ],
            )
        ],
        [],
        Hash.new_unique(),
    )
    unsigned = VersionedTransaction.populate(message, [Signature.default()])
    quote = prepared_for(unsigned, key)["quote"]
    calls = []

    def handler(request):
        body = json.loads(request.content) if request.content else {}
        calls.append((request.url.path, body.get("method")))
        if request.url.path.endswith("/order"):
            return httpx.Response(200, json=quote)
        if request.url.path.endswith("/execute"):
            assert body["requestId"] == quote["requestId"]
            signed = decode_swap_transaction(body["signedTransaction"])
            assert signed.verify_with_results() == [True] and signed.message == message
            journal_row = SwapJournal(tmp_path / "journal").read(quote["requestId"])
            assert journal_row["phase"] == "SENDING" and WalletLease.fence(address).exists()
            return httpx.Response(
                200,
                json={
                    "status": "Success",
                    "code": 0,
                    "signature": str(signed.signatures[0]),
                    "totalInputAmount": "1000000",
                    "totalOutputAmount": "2000000",
                },
            )
        method = body["method"]
        if method == "getMultipleAccounts" and body["params"][0] == [STOCK_MINT, USDC]:
            mint = bytearray(82)
            mint[44] = 6
            mint[45] = 1
            values = [{"owner": TOKEN, "data": [base64.b64encode(mint).decode(), "base64"]}] * 2
            result = {"context": {"slot": 10}, "value": values}
        elif method == "getMultipleAccounts":
            result = {
                "context": {"slot": 11},
                "value": [
                    account_wallet(100_000_000),
                    account_token(address, STOCK_MINT, 2_000_000),
                    account_token(address, USDC, 1_000_000),
                ],
            }
        elif method == "getFeeForMessage":
            result = {"value": 5000}
        elif method == "getBlockHeight":
            result = 1
        elif method == "simulateTransaction":
            assert (
                body["params"][0] == quote["transaction"]
            )  # Unsigned exact message; no real wallet signature in RPC.
            assert body["params"][1]["sigVerify"] is False
            result = {
                "context": {"slot": 12},
                "value": {
                    "err": None,
                    "accounts": [
                        account_wallet(99_995_000),
                        account_token(address, STOCK_MINT, 1_000_000),
                        account_token(address, USDC, 3_000_000),
                    ],
                },
            }
        else:
            raise AssertionError(method)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        replay = JupiterReplay(client=http, interval=0, audit=tmp_path / "audit.jsonl")
        rpc = ReplayRPC(replay, "https://rpc.example/private-fixture-key")
        prepared = await prepare_swap(replay, rpc, SwapInput(STOCK_MINT, USDC, "1", 6, wallet=address))
        assert not any(path.endswith("/execute") for path, _ in calls)
        result = await submit_swap(replay, prepared, key, SwapJournal(tmp_path / "journal"))
        assert result["phase"] == "CONFIRMED" and not WalletLease.fence(address).exists()
        audit = (tmp_path / "audit.jsonl").read_text()
        assert "private-fixture-key" not in audit and str(key) not in audit
        assert '"signedTransaction": "[REDACTED]"' in audit
    assert sum(path.endswith("/execute") for path, _ in calls) == 1


async def test_unknown_execute_is_fenced_not_retried_and_reconciles_onchain(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    key = Keypair()
    prepared = prepared_for(fixture_transaction(key), key)
    prepared["review"] = {
        "wallet": str(key.pubkey()),
        "requestId": prepared["quote"]["requestId"],
        "messageSha256": hashlib.sha256(
            to_bytes_versioned(decode_swap_transaction(prepared["quote"]["transaction"]).message)
        ).hexdigest(),
    }
    count = 0

    def handler(request):
        nonlocal count
        body = json.loads(request.content)
        if request.url.path.endswith("/execute"):
            count += 1
            raise httpx.ReadTimeout("private-fixture-proxy-value")
        assert body["method"] == "getSignatureStatuses"
        return httpx.Response(
            200,
            json={
                "id": body["id"],
                "result": {"value": [{"confirmationStatus": "finalized", "err": None, "slot": 123}]},
            },
        )

    journal = SwapJournal(tmp_path / "journal")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        replay = JupiterReplay(client=http, interval=0)
        with pytest.raises(ReplayError, match="unknown"):
            await submit_swap(replay, prepared, key, journal)
        assert journal.read("fixture-request")["phase"] == "UNKNOWN" and count == 1
        with pytest.raises(ValueError, match="unresolved"):
            await submit_swap(replay, prepared, key, journal)
        assert count == 1
        result = await reconcile_swap(ReplayRPC(replay, "https://rpc.example"), journal, "fixture-request")
        assert result["phase"] == "CONFIRMED" and not WalletLease.fence(str(key.pubkey())).exists()


async def test_round_trip_sells_only_actual_net_fill_and_keeps_ui_settings(monkeypatch, tmp_path):
    import backpack_stocks.jupiter_swap as flow

    monkeypatch.chdir(tmp_path)
    key = Keypair()
    swaps = []

    class Replay:
        async def order(self, swap):
            assert swap.input_mint == USDC and swap.output_mint == SOL and swap.amount == "10"
            return {"quote": {"outAmount": "100000000"}}

    async def prepare(replay, rpc, swap, **kwargs):
        swaps.append(swap)
        return {"quote": {"outAmount": "1000000"}, "review": {"requestId": "leg-" + str(len(swaps))}}

    async def submit(replay, prepared, wallet, journal, **kwargs):
        return {
            "phase": "CONFIRMED",
            "requestId": prepared["review"]["requestId"],
            "result": {"totalOutputAmount": "900000"},
        }

    monkeypatch.setattr(flow, "prepare_swap", prepare)
    monkeypatch.setattr(flow, "submit_swap", submit)
    template = SwapInput(
        SOL, USDC, "1", 9, profile="manual", jito_tip_lamports=1000, exclude_routers=("jupiterz",)
    )
    result = await round_trip(Replay(), object(), template, key, tmp_path / "state", execute=True)
    assert result["executed"] is True and len(swaps) == 2
    assert swaps[0].output_mint == swaps[1].input_mint == STOCK_MINT
    assert swaps[0].amount == "0.1" and swaps[1].amount == "0.9"
    assert all(s.jito_tip_lamports == 1000 and s.exclude_routers == ("jupiterz",) for s in swaps)


@pytest.mark.parametrize("router", ["jupiterz", "metis"])
async def test_execute_accepts_partial_service_signature_without_overwriting_it(router):
    key = Keypair()
    other = Keypair() if router == "jupiterz" else Pubkey.from_string(JUPITER_MANAGED_SIGNER)
    prepared = prepared_for(fixture_transaction(key, other), key, router)
    signed = sign_swap_transaction(prepared, key)
    calls = []

    def handler(request):
        calls.append(request)
        tx = decode_swap_transaction(json.loads(request.content)["signedTransaction"])
        assert tx.signatures[0].verify(key.pubkey(), to_bytes_versioned(tx.message))
        assert tx.signatures[1] == Signature.default()
        return httpx.Response(200, json={"status": "Success", "code": 0, "signature": str(tx.signatures[0])})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = JupiterReplay(client=http, interval=0)
        assert (await client.execute(prepared, signed, allow_execute=True))["status"] == "Success"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "response,phase",
    [
        ({"status": "Failed", "code": -2}, "REJECTED"),
        ({"status": "Failed", "code": -1000}, "UNKNOWN"),
        ({"status": "Success", "code": 0, "signature": str(Signature.new_unique())}, "UNKNOWN"),
    ],
)
async def test_http_200_does_not_imply_confirmed_and_signature_cannot_change(
    monkeypatch, tmp_path, response, phase
):
    monkeypatch.chdir(tmp_path)
    key = Keypair()
    prepared = prepared_for(fixture_transaction(key), key)
    tx = decode_swap_transaction(prepared["quote"]["transaction"])
    prepared["review"] = {
        "wallet": str(key.pubkey()),
        "requestId": "fixture-request",
        "messageSha256": hashlib.sha256(to_bytes_versioned(tx.message)).hexdigest(),
    }
    journal = SwapJournal(tmp_path / "state")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=response))
    ) as http:
        replay = JupiterReplay(client=http, interval=0)
        if response.get("signature"):
            with pytest.raises(ReplayError, match="signature"):
                await submit_swap(replay, prepared, key, journal)
        else:
            result = await submit_swap(replay, prepared, key, journal)
            assert result["phase"] == phase
    assert journal.read("fixture-request")["phase"] == phase
    assert WalletLease.fence(str(key.pubkey())).exists() == (phase == "UNKNOWN")


async def test_local_rejection_before_post_releases_wallet_fence(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    key = Keypair()
    prepared = prepared_for(fixture_transaction(key), key)
    tx = decode_swap_transaction(prepared["quote"]["transaction"])
    prepared["review"] = {
        "wallet": str(key.pubkey()),
        "requestId": "fixture-request",
        "messageSha256": hashlib.sha256(to_bytes_versioned(tx.message)).hexdigest(),
    }

    class Replay:
        backend = "website"

        async def execute(self, *args, **kwargs):
            raise ReplayError("Local expiry check")  # before on_submit / before network.

    journal = SwapJournal(tmp_path / "state")
    with pytest.raises(ReplayError, match="before submission"):
        await submit_swap(Replay(), prepared, key, journal)
    assert journal.read("fixture-request")["phase"] == "REJECTED"
    assert not WalletLease.fence(str(key.pubkey())).exists()


async def test_rfq_status_discovers_final_signature_by_unchanged_message(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    key, maker = Keypair(), Keypair()
    message = MessageV0.try_compile(
        maker.pubkey(),
        [Instruction(Pubkey.from_string(JUPITER), b"fixture", [AccountMeta(key.pubkey(), True, True)])],
        [],
        Hash.new_unique(),
    )
    tx = VersionedTransaction(message, [maker, key])
    signature = str(tx.signatures[0])
    journal = SwapJournal(tmp_path / "state")
    row = {
        "phase": "UNKNOWN",
        "requestId": "rfq-id",
        "wallet": str(key.pubkey()),
        "txSignature": None,
        "messageSha256": hashlib.sha256(to_bytes_versioned(message)).hexdigest(),
    }
    journal.write(row)
    WalletLease.reserve(row["wallet"], "jupiter_frontend", str(journal.root), row["requestId"])

    def handler(request):
        body = json.loads(request.content)
        method = body["method"]
        if method == "getSignaturesForAddress":
            result = [{"signature": signature}]
        elif method == "getTransaction":
            result = {"transaction": [base64.b64encode(bytes(tx)).decode(), "base64"]}
        else:
            assert method == "getSignatureStatuses"
            result = {"value": [{"confirmationStatus": "finalized", "err": None}]}
        return httpx.Response(200, json={"id": body["id"], "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await reconcile_swap(
            ReplayRPC(JupiterReplay(client=http, interval=0), "https://rpc.example"), journal, "rfq-id"
        )
    assert result["phase"] == "CONFIRMED" and result["txSignature"] == signature
    assert not WalletLease.fence(str(key.pubkey())).exists()


async def test_resume_of_completed_round_trip_never_rebuys_or_resells(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    key = Keypair()
    journal = SwapJournal(tmp_path / "state")
    first = {
        "phase": "CONFIRMED",
        "requestId": "buy-id",
        "wallet": str(key.pubkey()),
        "backend": "website",
        "inputMint": SOL,
        "outputMint": STOCK_MINT,
        "returnRequestId": "sell-id",
    }
    second = {"phase": "CONFIRMED", "requestId": "sell-id", "wallet": str(key.pubkey())}
    journal.write(first)
    journal.write(second)

    class Replay:
        backend = "website"

        async def order(self, *args, **kwargs):
            raise AssertionError("Completed round-trip must not obtain another order")

    result = await round_trip(
        Replay(),
        object(),
        SwapInput(SOL, USDC, "1", 9),
        key,
        journal.root,
        execute=True,
        resume_buy_request_id="buy-id",
    )
    assert result["executed"] is True and result["sellResult"]["requestId"] == "sell-id"
