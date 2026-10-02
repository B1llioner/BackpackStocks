import asyncio
import base64
import gzip
import time
from contextlib import suppress
from email.utils import formatdate
from unittest.mock import AsyncMock

import httpx
import pytest

from backpack_stocks.backpack import Backpack, BackpackAuth
from backpack_stocks.jupiter import Jupiter
from backpack_stocks.limiter import PriorityLimiter
from backpack_stocks.models import Settings
from backpack_stocks.observability import LOGGER
from backpack_stocks.telegram import TelegramAPI, TelegramError
from backpack_stocks.transport import RemoteError, Transport, UnknownOutcome, retry_after_seconds


async def test_keyless_quota_cannot_be_raised_by_interval_configuration():
    keyless = Jupiter(interval=0.01)
    keyed = Jupiter(api_key="fixture-key", interval=0.105)
    try:
        assert keyless.interval == 2.1 and keyed.interval == 0.105
        assert keyless.execute_limiter.interval == 0.055 and keyed.execute_limiter.interval == 0.021
    finally:
        await keyless.close()
        await keyed.close()


def test_adaptive_backoff_only_slows_down_and_never_reduces_a_slower_budget():
    limiter = PriorityLimiter(2.1)
    limiter.slow_down()
    assert limiter.interval == pytest.approx(3.15)
    assert limiter.status()["adaptive_backoffs"] == 1
    limiter.configure(60)
    limiter.slow_down()
    assert limiter.interval == 60


async def test_jupiter_reset_header_defers_all_main_requests_and_preserves_execute_bucket():
    seen = []

    def handler(request):
        seen.append((request.url.path, time.monotonic()))
        return httpx.Response(
            200,
            headers={
                "x-ratelimit-remaining": "0",
                "x-ratelimit-current": "30",
                "x-ratelimit-reset": str(time.time() + 120),
            },
            json={"ok": True},
        )

    t = Transport(
        "",
        client=httpx.AsyncClient(
            base_url="https://api.jup.ag/swap/v2/", transport=httpx.MockTransport(handler)
        ),
    )
    jup = Jupiter(transport=t)
    try:
        await t.request("GET", "/price")
        assert jup.limiter.status()["cooldown_s"] > 119
        assert t.status()["remote_budgets"]["main"]["remaining"] == 0
        assert await asyncio.wait_for(jup.execute("fixture-unsigned-text", "fixture-request"), 0.2) == {
            "ok": True
        }
        assert jup.execute_limiter.status()["cooldown_s"] > 119
        assert len(seen) == 2 and "execute" in seen[-1][0]
    finally:
        await jup.close()


async def test_429_uses_absolute_reset_as_well_as_retry_after(caplog):
    seen = []

    def handler(request):
        seen.append(time.monotonic())
        if len(seen) == 1:
            return httpx.Response(
                429,
                headers={
                    "Retry-After": "0",
                    "x-ratelimit-remaining": "-1",
                    "x-ratelimit-reset": str(time.time() + 0.07),
                },
                json={},
            )
        return httpx.Response(200, json={})

    t = Transport(
        "",
        client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler)),
        rate_headers=True,
    )
    LOGGER.addHandler(caplog.handler)
    try:
        await t.request("GET", "/price")
        assert seen[1] - seen[0] >= 0.065
        assert t.limiter.adaptive_backoffs == 1 and t.status()["responses_429"] == 1
        records = [r for r in caplog.records if r.msg == "http_backoff"]
        assert len(records) == 1
        assert records[0].public_fields["status_code"] == 429
        assert records[0].public_fields["code"] == "rate_limit"
        assert "api.jup.ag" not in str(records[0].public_fields)
    finally:
        LOGGER.removeHandler(caplog.handler)
        await t.close()


@pytest.mark.parametrize("status,kind", [(401, "authentication"), (403, "permissions_or_firewall")])
async def test_api_access_rejection_is_not_retried_without_credentials(status, kind):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={"message": "fixture-body-secret"})

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler))
    )
    try:
        with pytest.raises(RemoteError) as raised:
            await t.request("GET", "/price", headers={"x-api-key": "fixture-owned-key"})
        assert "fixture" not in str(raised.value)
        assert len(requests) == 1 and requests[0].headers["x-api-key"] == "fixture-owned-key"
        assert t.status()["last_rejection"]["kind"] == kind
    finally:
        await t.close()


@pytest.mark.parametrize("method,error", [("GET", RemoteError), ("POST", UnknownOutcome)])
async def test_interactive_challenge_stops_request_without_replaying(method, error):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            403,
            headers={"cf-mitigated": "challenge", "content-type": "text/html"},
            content=b"<html>fixture-challenge</html>",
        )

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://api.jup.ag", transport=httpx.MockTransport(handler))
    )
    try:
        with pytest.raises(error, match="verification required"):
            await t.request(method, "/route")
        assert len(requests) == 1 and t.status()["last_rejection"]["kind"] == "interactive_check"
    finally:
        await t.close()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("12", 12),
        ("0", 0),
        ("0.05", 0.05),
        ("-1", 0),
        ("NaN", None),
        ("Infinity", None),
        ("invalid", None),
        (None, None),
    ],
)
def test_retry_after_numeric_values_are_finite(value, expected):
    assert retry_after_seconds(value) == expected


def test_retry_after_http_date():
    assert retry_after_seconds(formatdate(2030, usegmt=True), now=2000) == 30


async def test_provider_cooldown_applies_to_sibling_requests_and_read_retries():
    seen = []
    cooldown = asyncio.Event()

    def handler(request):
        seen.append((request.url.path, time.monotonic()))
        if len(seen) == 1:
            return httpx.Response(429, headers={"Retry-After": "0.06"}, json={})
        return httpx.Response(200, json={"ok": True})

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler))
    )
    original = t.limiter.defer

    def defer(delay, category=None):
        original(delay, category)
        cooldown.set()

    t.limiter.defer = defer
    first = asyncio.create_task(t.request("GET", "/first"))
    try:
        await asyncio.wait_for(cooldown.wait(), 1)
        await asyncio.gather(first, t.request("GET", "/sibling"))
        assert len(seen) == 3
        assert all(stamp - seen[0][1] >= 0.055 for _, stamp in seen[1:])
        assert t.responses_429 == 1
    finally:
        await t.close()


async def test_history_budget_does_not_block_ready_standard_requests():
    limiter = PriorityLimiter(0.002, categories={"history": 0.035})
    order = []
    times = []

    async def call(name, category):
        await limiter.acquire(category=category)
        order.append(name)
        if category:
            times.append(time.monotonic())

    try:
        await asyncio.gather(
            call("history1", "history"),
            call("history2", "history"),
            call("history3", "history"),
            call("standard", None),
        )
        assert order == ["history1", "standard", "history2", "history3"]
        assert all(b - a >= 0.030 for a, b in zip(times, times[1:], strict=False))
    finally:
        await limiter.close()


async def test_signature_is_created_after_admission_and_refreshed_on_read_retry():
    auth = BackpackAuth(base64.b64encode(bytes(range(32))).decode())
    signed = []
    original = auth.headers

    def headers(*args, **kwargs):
        signed.append(time.monotonic())
        return original(*args, **kwargs)

    auth.headers = headers
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503 if len(requests) == 1 else 200, headers={"Retry-After": "0.02"}, json=[])

    t = Transport(
        "", client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler))
    )
    bp = Backpack(auth, t)
    start = time.monotonic()
    t.limiter.defer(0.04)
    try:
        await bp.private("GET", "/api/v1/rfqs", "rfqQuery")
        assert len(signed) == 2
        assert signed[0] - start >= 0.035 and signed[1] - signed[0] >= 0.018
        assert requests[0].headers["X-Timestamp"] != requests[1].headers["X-Timestamp"]
        assert t.limiter.categories["historical_market_data"] == 2.05
    finally:
        await bp.close()


async def test_cancelled_priority_request_does_not_waste_next_slot():
    limiter = PriorityLimiter(0.02)
    try:
        await limiter.acquire()
        cancelled = asyncio.create_task(limiter.acquire(0))
        await asyncio.sleep(0)
        cancelled.cancel()
        with suppress(asyncio.CancelledError):
            await cancelled
        await asyncio.wait_for(limiter.acquire(10), 0.2)
        assert limiter.admitted == 2
    finally:
        await limiter.close()


async def test_gzipped_json_is_decoded_once_and_counted_conservatively():
    def handler(request):
        return httpx.Response(
            200, headers={"Content-Encoding": "gzip"}, content=gzip.compress(b'{"ok":true}')
        )

    t = Transport(
        "",
        byte_budget=10000,
        client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler)),
    )
    try:
        assert await t.request("GET", "/gzip") == {"ok": True}
        assert t.status()["bytes_last_30s"] >= 11
    finally:
        await t.close()


@pytest.mark.parametrize("method,error", [("GET", RemoteError), ("POST", UnknownOutcome)])
async def test_oversized_response_closes_and_never_repeats_mutation(method, error):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=b"x" * 500)

    t = Transport(
        "",
        max_response_bytes=64,
        client=httpx.AsyncClient(base_url="https://example.com", transport=httpx.MockTransport(handler)),
    )
    try:
        with pytest.raises(error, match="size limit"):
            await t.request(method, "/response")
        assert len(calls) == 1
    finally:
        await t.close()


async def test_bandwidth_admission_waits_for_rolling_window():
    t = Transport(
        "",
        byte_budget=64,
        client=httpx.AsyncClient(
            base_url="https://example.com",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
        ),
    )
    t.byte_history.append((time.monotonic() - 29.95, 100))
    start = time.monotonic()
    try:
        await t.request("GET", "/response")
        assert time.monotonic() - start >= 0.04
    finally:
        await t.close()


async def test_partial_rpc_response_is_counted_even_when_submission_outcome_is_unknown():
    class BrokenStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * 65536
            raise httpx.ReadError("fixture disconnected after partial response")

    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, stream=BrokenStream())

    t = Transport(
        "",
        byte_budget=1_000_000,
        client=httpx.AsyncClient(base_url="https://rpc.example", transport=httpx.MockTransport(handler)),
    )
    try:
        with pytest.raises(UnknownOutcome):
            await t.request("POST", "/", body={"method": "sendTransaction"})
        assert len(seen) == 1 and t.status()["bytes_last_30s"] >= 65536
    finally:
        await t.close()


async def test_telegram_send_cooldown_honors_full_value_without_blocking_incoming_commands():
    def handler(request):
        if request.url.path.endswith("sendMessage"):
            return httpx.Response(
                429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 120}}
            )
        return httpx.Response(200, json={"ok": True, "result": []})

    api = TelegramAPI("123456:fake-token-used-only-in-unit-tests", transport=httpx.MockTransport(handler))
    api.limiter.interval = 0
    try:
        with pytest.raises(TelegramError) as raised:
            await api.reply(471057654, "fixture")
        assert raised.value.retry_after == 120
        assert api.limiter.status()["category_cooldown_s"]["sendMessage"] > 119
        assert await asyncio.wait_for(api.updates(1, timeout=0), 0.2) == []
    finally:
        await api.close()


def test_jupiter_quota_and_public_rpc_per_method_limit_are_validated():
    with pytest.raises(ValueError, match="plan quota"):
        Settings(jupiter_interval_s=0.1)
    assert Settings(jupiter_requests_per_second=10, jupiter_interval_s=0.105)
    with pytest.raises(ValueError, match="per-method quota"):
        Settings(rpc_interval_s=0.02)
    assert Settings(rpc_url="https://dedicated.example.com", rpc_interval_s=0.02)


@pytest.mark.parametrize("flag", [False, True])
def test_telegram_cannot_bypass_explicit_live_or_manual_pilot(monkeypatch, flag):
    import backpack_stocks.cli as cli

    settings = Settings().model_copy(update={"mode": "live", "source": "public", "live_pilot": True})
    monkeypatch.setattr(cli, "load_settings", lambda _: settings)
    loader = AsyncMock()
    monkeypatch.setattr(cli, "load_environment", loader)
    monkeypatch.setattr("sys.argv", ["backpack-stocks", "telegram", *(["--live"] if flag else [])])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 1 and loader.call_count == 0
