from __future__ import annotations

import base64
import hashlib
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import Message
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

from backpack_stocks.jupiter_replay import (
    SOL,
    USDC,
    JupiterReplay,
    ReplayError,
    SwapInput,
    captured_frontend_key,
    raw_amount,
)


def test_ui_amount_and_observed_manual_parameters():
    assert raw_amount("0,01", 9) == raw_amount("0.01", 9) == 10_000_000
    swap = SwapInput(SOL, USDC, "0.01", 9, profile="manual")
    assert swap.params() == {
        "amount": "10000000",
        "inputMint": SOL,
        "outputMint": USDC,
        "swapMode": "ExactIn",
        "maxSupportedTransactionVersion": "0",
        "broadcastFeeType": "maxCap",
        "excludeDexes": "",
        "excludeRouters": "",
        "slippageBps": "50",
        "useWsol": "false",
        "priorityFeeLamports": "1000000",
    }
    wallet = str(Keypair().pubkey())
    assert replace(swap, wallet=wallet).params()["taker"] == wallet
    assert "taker" not in replace(swap, wallet=wallet, has_enough_balance=False).params()
    assert (
        replace(swap, wallet=wallet, has_enough_balance=False, swap_mode="ExactOut").params()["taker"]
        == wallet
    )
    assert "slippageBps" not in replace(swap, profile="ultra").params()


@pytest.mark.parametrize("amount", ["NaN", "-1", "1e3", "0", "1.0000000001", "18446744073709551616"])
def test_amount_rejects_ambiguous_or_overflow_values(amount):
    with pytest.raises(ValueError):
        raw_amount(amount, 9)


@pytest.mark.parametrize(
    "changes",
    [
        {"jito_tip_lamports": 999},
        {"max_transaction_version": 2},
        {"slippage_bps": -1},
        {"priority_fee_lamports": True},
        {"profile": "ultra", "swap_mode": "ExactOut"},
    ],
)
def test_invalid_ui_inputs(changes):
    with pytest.raises(ValueError):
        replace(SwapInput(SOL, USDC, "0.01", 9), **changes).params()


async def test_auth_real_signature_refresh_vault_logout_and_redaction(tmp_path: Path):
    wallet = Keypair()
    address = str(wallet.pubkey())
    message = f"Sign this message to authenticate with Jupiter that you are the owner of {address}."
    expires = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    calls = []

    def handler(request):
        body = json.loads(request.content) if request.content else {}
        calls.append(request.url.path)
        if request.url.path.endswith("challenge"):
            assert body == {"type": "message", "walletPubkey": address}
            return httpx.Response(200, json={"type": "message", "challenge": message})
        if request.url.path.endswith("verify"):
            assert body["authMode"] == "access_refresh"
            assert Signature.from_string(body["signature"]).verify(wallet.pubkey(), message.encode())
            return httpx.Response(
                200,
                json={
                    "authMode": "access_refresh",
                    "tokenType": "Bearer",
                    "accessToken": "fake-access",
                    "refreshToken": "fake-refresh",
                    "expiresAt": expires,
                },
            )
        if request.url.path.endswith("vault"):
            assert request.headers["authorization"] == "Bearer fake-access"
            return httpx.Response(404, json={"error": "Vault not found"})
        if request.url.path.endswith("refresh"):
            assert body == {"refreshToken": "fake-refresh"}
            return httpx.Response(
                200,
                json={
                    "tokenType": "Bearer",
                    "accessToken": "fake-access-2",
                    "refreshToken": "fake-refresh-2",
                    "expiresAt": expires,
                },
            )
        assert request.url.path.endswith("logout")
        assert body == {"refreshToken": "fake-refresh-2"}
        return httpx.Response(200, json={"success": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = JupiterReplay(client=http, interval=0, audit=tmp_path / "audit.jsonl")
        session = await client.login(wallet)
        assert "fake-access" not in repr(session)
        assert await client.trigger_vault() == {"phase": "not_registered", "registered": False}
        await client.refresh()
        await client.logout()
        assert client.session is None
    assert len(calls) == 5
    audit = (tmp_path / "audit.jsonl").read_text()
    assert "fake-access" not in audit and "fake-refresh" not in audit
    assert "[REDACTED]" in audit


async def test_auth_rejects_wrong_wallet_challenge():
    wallet = Keypair()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"type": "message", "challenge": "Jupiter for another wallet"})
        )
    ) as http:
        with pytest.raises(ReplayError, match="requested wallet"):
            await JupiterReplay(client=http, interval=0).login(wallet)


async def test_order_checks_response_and_never_retries_denied_access():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(403, json={"message": "organization access denied"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = JupiterReplay(backend="developer", client=http, interval=0)
        with pytest.raises(ReplayError, match="HTTP 403"):
            await client.order(SwapInput(SOL, USDC, "0.01", 9))
    assert len(calls) == 1


async def test_order_rejects_mint_mismatch():
    def handler(request):
        return httpx.Response(200, json={"inputMint": USDC, "outputMint": SOL, "requestId": "fake"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(ReplayError, match="inputMint"):
            await JupiterReplay(client=http, interval=0).order(SwapInput(SOL, USDC, "0.01", 9))


async def test_execute_requires_flag_exact_message_signature_and_fresh_order():
    # Local synthetic transfer only proves byte/signature gates; never sent to a real service.
    wallet = Keypair()
    receiver = Keypair().pubkey()

    def transaction(lamports):
        msg = Message.new_with_blockhash(
            [transfer(TransferParams(from_pubkey=wallet.pubkey(), to_pubkey=receiver, lamports=lamports))],
            wallet.pubkey(),
            Hash.default(),
        )
        return VersionedTransaction(msg, [wallet])

    tx = transaction(1)
    encoded = base64.b64encode(bytes(tx)).decode()
    prepared = {
        "backend": "developer",
        "receivedAt": time.time(),
        "quote": {
            "transaction": encoded,
            "taker": str(wallet.pubkey()),
            "requestId": "fake-request",
        },
    }
    calls = []

    def handler(request):
        calls.append(request)
        assert json.loads(request.content) == {"signedTransaction": encoded, "requestId": "fake-request"}
        return httpx.Response(200, json={"status": "Success"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = JupiterReplay(backend="developer", client=http, interval=0)
        with pytest.raises(ReplayError, match="Explicit"):
            await client.execute(prepared, encoded)
        with pytest.raises(ReplayError, match="changed"):
            await client.execute(
                prepared, base64.b64encode(bytes(transaction(2))).decode(), allow_execute=True
            )
        with pytest.raises(ReplayError, match="expired"):
            await client.execute({**prepared, "receivedAt": time.time() - 40}, encoded, allow_execute=True)
        unsigned = VersionedTransaction.populate(tx.message, [Signature.default()])
        with pytest.raises(ReplayError, match="Missing"):
            await client.execute(prepared, base64.b64encode(bytes(unsigned)).decode(), allow_execute=True)
        assert await client.execute(prepared, encoded, allow_execute=True) == {"status": "Success"}
    assert len(calls) == 1


def test_publishable_frontend_config_is_read_from_pinned_source(tmp_path):
    source = b"const x={apiKey:cfg.swapApiKey??`public-fixture-value`};"
    (tmp_path / "js").mkdir()
    p = tmp_path / "js/vendor-tanstack-runtime-fixture.js"
    p.write_bytes(source)
    (tmp_path / "js-manifest.json").write_text(
        json.dumps(
            [
                {
                    "file": "js/vendor-tanstack-runtime-fixture.js",
                    "sha256": hashlib.sha256(source).hexdigest(),
                }
            ]
        )
    )
    assert captured_frontend_key(tmp_path) == "public-fixture-value"
    p.write_bytes(source + b" ")
    with pytest.raises(ReplayError, match="digest mismatch"):
        captured_frontend_key(tmp_path)
