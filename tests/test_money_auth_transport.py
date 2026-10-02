import base64
from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from backpack_stocks.backpack import Backpack, BackpackAuth, signing_message, vwap
from backpack_stocks.models import Policy, Settings, dec, impact_percent, raw, spread, ui
from backpack_stocks.transport import RemoteError, Transport, UnknownOutcome


@pytest.mark.parametrize("value", ["0.0000001", "-1", "NaN", "Infinity", 0.1, True])
def test_raw_rejects_invalid_amount(value):
    with pytest.raises((ValueError, TypeError)):
        raw(value)


def test_amounts_and_fee_semantics():
    assert raw("1.000001") == 1000001
    assert ui(1000001) == Decimal("1.000001")
    assert Policy(withdrawal_semantics="deduct").withdrawal(10000000) == (10000000, 9920000)
    assert Policy(withdrawal_semantics="additional").withdrawal(10000000) == (9920000, 9920000)
    with pytest.raises(ValueError):
        Policy().withdrawal(10000000)
    with pytest.raises(ValueError):
        Policy(withdrawal_semantics="deduct").withdrawal(100000)


def test_spread_does_not_ignore_delivery_fee_or_slippage():
    p = Policy(withdrawal_semantics="deduct")
    s = spread(p, "6.00", 62000000, 9920000, "fixture")
    assert s.cost_raw == 60000000
    assert s.minimum_output_raw == 61690000
    assert s.profit_raw == 1660000
    assert s.passes
    assert not spread(p, "6.20", 62000000, 9920000, "fixture").passes


def test_price_impact_ratio_is_not_confused_with_percentage_points():
    assert impact_percent({"priceImpactPct": "0.012"}) == Decimal("1.2")
    assert impact_percent({"priceImpact": "1.2", "priceImpactPct": "0.012"}) == Decimal("1.2")
    with pytest.raises(ValueError):
        impact_percent({})


def test_signing_sorted_bool_absent_recipient_and_verified_signature():
    params = {
        "symbol": "WEN.US",
        "quantity": "1.0",
        "autoBorrow": False,
        "recipientInformation": {"name": "example"},
        "unused": None,
    }
    expected = b"instruction=withdraw&autoBorrow=false&quantity=1.0&symbol=WEN.US&timestamp=123&window=5000"
    assert signing_message("withdraw", params, 123) == expected
    auth = BackpackAuth(base64.b64encode(bytes(range(32))).decode())
    headers = auth.headers("withdraw", params, timestamp=123)
    Ed25519PublicKey.from_public_bytes(base64.b64decode(headers["X-API-Key"])).verify(
        base64.b64decode(headers["X-Signature"]), expected
    )
    with pytest.raises(ValueError):
        signing_message("orderExecute", {"quantity": 1.5}, 123)


def test_limit_order_vwap_checks_full_depth():
    assert vwap([["6", "2"], ["7", "3"]], "4") == Decimal("6.5")
    with pytest.raises(ValueError):
        vwap([["6", "2"]], "4")


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_mutation_http_errors_are_unknown_and_not_retried(status):
    calls = []

    async def respond(request):
        calls.append(request)
        return httpx.Response(status, json={"secret": "never expose"})

    client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(respond))
    t = Transport("", client=client)
    with pytest.raises(UnknownOutcome) as error:
        await t.request("POST", "/withdraw", body={"key": "secret"})
    assert len(calls) == 1
    assert "secret" not in str(error.value)
    await t.close()


async def test_mutation_timeout_and_read_retries():
    calls = []

    async def respond(request):
        calls.append(request)
        raise httpx.ReadTimeout("secret URL", request=request)

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(respond))
    )
    with pytest.raises(UnknownOutcome):
        await t.request("POST", "/mutate")
    assert len(calls) == 1
    with pytest.raises(RemoteError):
        await t.request("GET", "/read")
    assert len(calls) == 4
    await t.close()


async def test_paper_client_cannot_authenticate_or_mutate():
    bp = Backpack()
    try:
        with pytest.raises(ValueError):
            await bp.submit_rfq("1", 1)
        with pytest.raises(ValueError):
            await bp.query_order("WEN.US_USDC", order_id="a", client_id=1)
    finally:
        await bp.close()


def test_live_config_requires_contract_verification_and_pilot_bound():
    with pytest.raises(ValueError):
        Settings(mode="live", source="public", enable_live=True, wallet="example")
    p = Policy(
        withdrawal_semantics="deduct",
        live_fee_verified=True,
        live_rfq_verified=True,
        live_quantity_mapping_verified=True,
        live_withdrawal_verified=True,
        max_cycles=2,
    )
    with pytest.raises(ValueError):
        Settings(mode="live", source="public", enable_live=True, wallet="example", policy=p)
    assert dec("6.00") == Decimal(6)
