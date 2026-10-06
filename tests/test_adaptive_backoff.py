import asyncio
from types import SimpleNamespace

import httpx
import pytest

from backpack_stocks import limiter as limiter_module
from backpack_stocks import transport as transport_module
from backpack_stocks.limiter import PriorityLimiter
from backpack_stocks.transport import Transport, UnknownOutcome


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=100.0)
    original_sleep = asyncio.sleep

    class VirtualAsyncio:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def sleep(self, delay):
            clock.now += delay
            await original_sleep(0)

    monkeypatch.setattr(limiter_module, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(limiter_module, "asyncio", VirtualAsyncio())
    monkeypatch.setattr(
        transport_module,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: 1_700_000_000 + clock.now),
    )
    monkeypatch.setattr(transport_module, "asyncio", VirtualAsyncio())
    monkeypatch.setattr(transport_module, "random", SimpleNamespace(uniform=lambda _start, _end: 0))
    return clock


async def test_seven_in_flight_429_responses_share_one_slowdown_and_keep_every_counter(clock):
    reached, requests = 0, []
    release = asyncio.Event()

    async def handler(request):
        nonlocal reached
        requests.append(request)
        reached += 1
        if reached == 7:
            release.set()
        await release.wait()
        headers = {"Retry-After": "3"}
        if request.url.path == "/rfq/0":
            headers.update(
                {
                    "Retry-After": "9",
                    "x-ratelimit-remaining": "-1",
                    "x-ratelimit-reset": str(transport_module.time.time() + 12),
                }
            )
        return httpx.Response(429, headers=headers, json={})

    transport = Transport(
        "",
        interval=0.05,
        rate_headers=True,
        client=httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)),
    )
    try:
        results = await asyncio.gather(
            *(transport.request("POST", f"/rfq/{index}", body={}) for index in range(7)),
            return_exceptions=True,
        )
        assert len(requests) == 7 and all(isinstance(result, UnknownOutcome) for result in results)
        status = transport.status()
        assert status["responses_429_all_buckets"] == status["responses_429"] == 7
        assert status["adaptive_backoffs"] == 1 and status["adaptive_recoveries"] == 0
        assert status["interval_s"] == 0.25 and status["base_interval_s"] == 0.05
        assert status["cooldown_s"] == pytest.approx(12)
        assert status["max_connections"] == 8
    finally:
        await transport.close()


def test_recovery_needs_eight_successes_and_thirty_quiet_seconds_per_step(clock):
    limiter = PriorityLimiter(0.05)
    limiter.slow_down()
    clock.now = 130
    for _ in range(7):
        limiter.record_success()
    assert limiter.interval == 0.25
    limiter.record_success()
    assert limiter.interval == pytest.approx(0.25 / 1.5)
    for _ in range(20):
        limiter.record_success()
    assert limiter.adaptive_recoveries == 1
    clock.now = 159.999
    limiter.record_success()
    assert limiter.adaptive_recoveries == 1
    clock.now = 160
    limiter.record_success()
    assert limiter.adaptive_recoveries == 2
    assert limiter.interval == pytest.approx(0.25 / 1.5**2)


def test_successes_before_the_quiet_window_cannot_recover_early(clock):
    limiter = PriorityLimiter(0.05)
    limiter.rate_limit_backoff(1)
    clock.now = 129.999
    for _ in range(20):
        limiter.record_success()
    assert limiter.interval == 0.25 and limiter.adaptive_recoveries == 0
    clock.now = 130
    limiter.record_success()
    assert limiter.adaptive_recoveries == 1


@pytest.mark.parametrize("status", [429, 503, "network"])
async def test_new_429_server_or_network_failure_resets_quiet_success_window(clock, status):
    def handler(request):
        if status == "network":
            raise httpx.ConnectError("fixture network error", request=request)
        return httpx.Response(status, headers={"Retry-After": "1"}, json={})

    transport = Transport(
        "",
        interval=0.05,
        client=httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)),
    )
    try:
        limiter = transport.limiter
        limiter.rate_limit_backoff(1)
        clock.now = 130
        for _ in range(7):
            limiter.record_success()
        with pytest.raises(UnknownOutcome):
            await transport.request("POST", "/rfq", body={})
        failed_at, interval = clock.now, limiter.interval
        assert limiter.adaptive_recoveries == 0
        clock.now = failed_at + 29.999
        for _ in range(8):
            limiter.record_success()
        assert limiter.interval == interval and limiter.adaptive_recoveries == 0
        clock.now = failed_at + 30
        limiter.record_success()
        assert limiter.interval == pytest.approx(interval / 1.5)
    finally:
        await transport.close()


def test_recovery_preserves_mixed_category_budgets_and_never_shortens_cooldowns(clock):
    limiter = PriorityLimiter(0.05, categories={"history": 2.05})
    limiter.rate_limit_backoff(1)
    limiter.defer(100)
    limiter.defer(200, "history")
    before = limiter.blocked_until, dict(limiter.category_blocked)
    clock.now = 200
    for _ in range(20):
        limiter.record_success()
    assert limiter.interval == 0.25 and limiter.adaptive_recoveries == 0
    clock.now = 300
    limiter.last = 299.96
    limiter.category_last["history"] = 299.75
    for _ in range(8):
        limiter.record_success()
    assert limiter.interval == pytest.approx(0.25 / 1.5)
    assert (limiter.blocked_until, limiter.category_blocked) == before
    assert limiter.categories == {"history": 2.05}
    assert limiter.ready_at("history") == pytest.approx(301.8)
    assert limiter.ready_at(None) >= limiter.last + limiter.base_interval


@pytest.mark.parametrize("baseline", [0.05, 2.1, 60])
def test_gradual_recovery_never_exceeds_configured_request_rate(clock, baseline):
    limiter = PriorityLimiter(baseline)
    limiter.slow_down()
    for _ in range(12):
        clock.now += 30
        for _ in range(8):
            limiter.record_success()
        assert limiter.interval >= baseline
    assert limiter.interval == baseline and limiter.base_interval == baseline


async def test_successful_responses_respect_remote_reset_before_recovery(clock):
    capped = True

    def handler(_request):
        headers = (
            {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(transport_module.time.time() + 60)}
            if capped
            else {}
        )
        return httpx.Response(200, headers=headers, json={"ok": True})

    transport = Transport(
        "",
        interval=0.05,
        rate_headers=True,
        client=httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler)),
    )
    try:
        limiter = transport.limiter
        limiter.slow_down()
        clock.now = 130
        assert await transport.request("GET", "/price") == {"ok": True}
        deadline = limiter.blocked_until
        for _ in range(20):
            limiter.record_success()
        assert limiter.interval == 0.25 and limiter.blocked_until == deadline
        capped = False
        clock.now = deadline
        for _ in range(8):
            assert await transport.request("GET", "/price") == {"ok": True}
        assert limiter.adaptive_recoveries == 1 and limiter.interval == pytest.approx(0.25 / 1.5)
        assert limiter.blocked_until == deadline
    finally:
        await transport.close()


async def test_recovery_successes_belong_to_the_selected_limiter_bucket(clock):
    client = httpx.AsyncClient(
        base_url="https://example.test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))
    )
    transport = Transport("", interval=0.05, client=client)
    execution = PriorityLimiter(0.021)
    transport.limiter.slow_down()
    execution.slow_down()
    clock.now = 130
    try:
        for _ in range(8):
            await transport.request("GET", "/execute", limiter=execution, bucket="execute")
        assert execution.adaptive_recoveries == 1
        assert transport.limiter.adaptive_recoveries == 0 and transport.limiter.interval == 0.25
    finally:
        await execution.close()
        await transport.close()


@pytest.mark.parametrize("fallback,expected", [(None, 1), (60, 60)])
async def test_configured_429_fallback_is_scoped_without_changing_other_providers(clock, fallback, expected):
    client = httpx.AsyncClient(
        base_url="https://example.test", transport=httpx.MockTransport(lambda _: httpx.Response(429, json={}))
    )
    transport = Transport("", interval=0.05, client=client, rate_limit_fallback_s=fallback)
    try:
        with pytest.raises(UnknownOutcome):
            await transport.request("POST", "/rfq", body={})
        assert transport.status()["rate_limit_fallback_s"] == fallback
        assert transport.limiter.blocked_until - clock.now == expected
        assert transport.limiter.adaptive_backoffs == 1 and transport.responses_429 == 1
    finally:
        await transport.close()


async def test_repeated_no_header_429_inside_one_minute_do_not_compound_slowdown(clock):
    requests, gates, started = [], [asyncio.Event() for _ in range(8)], asyncio.Event()

    async def handler(request):
        requests.append(request)
        index = int(request.url.path.rsplit("/", 1)[1])
        if len(requests) == 8:
            started.set()
        await gates[index].wait()
        return httpx.Response(429, json={})

    client = httpx.AsyncClient(
        base_url="https://example.test", transport=httpx.MockTransport(handler)
    )
    transport = Transport("", interval=0.05, client=client, rate_limit_fallback_s=60)
    tasks = [
        asyncio.create_task(transport.request("POST", f"/rfq/{index}", body={})) for index in range(8)
    ]
    try:
        # Responses can arrive after sibling requests were already admitted.
        await started.wait()
        began = clock.now
        for index, elapsed in enumerate((0, 1, 3, 7, 11, 15, 20, 25)):
            clock.now = began + elapsed
            gates[index].set()
            with pytest.raises(UnknownOutcome):
                await tasks[index]
            assert transport.limiter.interval == 0.25
        assert transport.limiter.adaptive_backoffs == 1 and transport.responses_429 == 8
        assert transport.limiter.blocked_until == began + 85
        assert len(requests) == transport.limiter.admitted == 8
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await transport.close()


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"Retry-After": "120"}, 120),
        ({"Retry-After": "90", "x-ratelimit-reset": "1700000280"}, 180),
        ({"x-ratelimit-reset": "1700000220"}, 120),
        ({"Retry-After": "10"}, 10),
        ({"x-ratelimit-reset": "1700000120"}, 20),
        ({"Retry-After": "invalid"}, 60),
    ],
)
async def test_fallback_does_not_override_explicit_server_retry_or_reset(clock, headers, expected):
    client = httpx.AsyncClient(
        base_url="https://example.test",
        transport=httpx.MockTransport(lambda _: httpx.Response(429, headers=headers, json={})),
    )
    transport = Transport("", interval=0.05, client=client, rate_headers=True, rate_limit_fallback_s=60)
    try:
        with pytest.raises(UnknownOutcome):
            await transport.request("POST", "/rfq", body={})
        assert transport.limiter.blocked_until - clock.now == expected
    finally:
        await transport.close()


async def test_inspector_deadline_cancels_queued_mutating_rfq_before_http_admission(clock, monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler))
    transport = Transport("", interval=0.05, client=client, rate_limit_fallback_s=60)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "time", lambda: clock.now)
    transport.limiter.defer(60)
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(5):
                await transport.request("POST", "/rfq", body={})
        assert requests == [] and transport.limiter.admitted == 0
        assert 105 <= clock.now < 160
        assert transport.limiter.blocked_until == 160
    finally:
        await transport.close()


@pytest.mark.parametrize("fallback", [0, -1, float("inf"), float("nan")])
def test_configured_429_fallback_requires_a_positive_finite_delay(fallback):
    with pytest.raises(ValueError, match="positive finite delay"):
        Transport("https://example.test", rate_limit_fallback_s=fallback)
