import asyncio
from types import SimpleNamespace

import httpx
import pytest

from backpack_stocks import limiter as limiter_module
from backpack_stocks import transport as transport_module
from backpack_stocks.limiter import PriorityLimiter
from backpack_stocks.transport import Transport


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
    monkeypatch.setattr(transport_module, "time", SimpleNamespace(monotonic=lambda: clock.now))
    return clock


def transport(handler, **kwargs):
    client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler))
    return Transport("", client=client, **kwargs)


async def queued(t, count):
    async with asyncio.timeout(1):
        while len(t.limiter.capacity_waiters) != count:
            await asyncio.sleep(0)


async def test_ready_hot_bank_is_not_hidden_by_background_slots_waiting_for_pacing():
    starts = []
    t = transport(lambda request: (starts.append(request.url.path), httpx.Response(200, json={}))[1])
    t.limiter.defer(0.05)
    tasks = [asyncio.create_task(t.request("GET", f"/discovery/{i}", priority=10)) for i in range(8)]
    try:
        await queued(t, 8)
        assert t.in_flight._value == 8  # Rate waiters have reserved no connections.
        tasks.append(asyncio.create_task(t.request("GET", "/winning-bank", priority=5)))
        await asyncio.gather(*tasks)
        assert starts[0] == "/winning-bank" and len(starts) == 9
        assert t.limiter.admitted == 9 and t.in_flight._value == 8
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await t.close()


async def test_full_connections_keep_later_execution_and_hot_reads_in_the_priority_queue(clock):
    release, full = asyncio.Event(), asyncio.Event()
    starts = []
    active, maximum = 0, 0

    async def handler(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        starts.append((request.url.path, clock.now))
        if len(starts) <= 8:
            if len(starts) == 8:
                full.set()
            await release.wait()
        active -= 1
        return httpx.Response(200, json={})

    t = transport(handler, interval=0.05)
    tasks = [asyncio.create_task(t.request("GET", f"/running/{i}", priority=10)) for i in range(8)]
    cancelled = None
    try:
        await asyncio.wait_for(full.wait(), 1)
        assert maximum == 8 and t.in_flight.locked()
        tasks.extend(asyncio.create_task(t.request("GET", f"/cold/{i}", priority=10)) for i in range(3))
        tasks.append(asyncio.create_task(t.request("GET", "/hot", priority=5)))
        tasks.append(asyncio.create_task(t.request("GET", "/execution", priority=0)))
        cancelled = asyncio.create_task(t.request("POST", "/cancelled-rfq", priority=0, body={}))
        await queued(t, 6)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        clock.now += 10
        release.set()
        await asyncio.gather(*tasks)
        assert [path for path, _ in starts[8:10]] == ["/execution", "/hot"]
        assert "/cancelled-rfq" not in {path for path, _ in starts}
        assert maximum == 8 and t.in_flight._value == 8
        assert t.limiter.admitted == 13
        assert all(b[1] - a[1] >= 0.05 - 1e-9 for a, b in zip(starts, starts[1:], strict=False))
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await t.close()


async def test_transport_keeps_oldest_rpc_turns_and_actual_start_spacing(clock):
    starts = []

    def handler(request):
        starts.append((request.url.path, clock.now))
        return httpx.Response(200, json={})

    t = transport(handler, interval=0.01, oldest_every=4)
    try:
        tasks = [asyncio.create_task(t.request("GET", "/history", priority=20))]
        tasks.extend(asyncio.create_task(t.request("GET", f"/pool/{i}", priority=5)) for i in range(8))
        tasks.append(asyncio.create_task(t.request("GET", "/execution", priority=0)))
        await asyncio.gather(*tasks)
        assert [path for path, _ in starts[:4]] == ["/execution", "/pool/0", "/pool/1", "/history"]
        assert [b[1] - a[1] for a, b in zip(starts, starts[1:], strict=False)] == pytest.approx([0.01] * 9)
    finally:
        await t.close()


async def test_before_attempt_delay_does_not_accumulate_permits_or_create_a_start_burst(clock):
    release, entered = asyncio.Event(), asyncio.Event()
    starts = []

    async def before():
        entered.set()
        await release.wait()

    def handler(request):
        starts.append((request.url.path, clock.now))
        return httpx.Response(200, json={})

    t = transport(handler, interval=0.1)
    first = asyncio.create_task(t.request("GET", "/prepared", before_attempt=before))
    followers = []
    try:
        await asyncio.wait_for(entered.wait(), 1)
        followers = [asyncio.create_task(t.request("GET", f"/follow/{i}")) for i in range(3)]
        await queued(t, 3)
        assert t.limiter.admitted == 0 and not starts
        clock.now += 10
        release.set()
        await asyncio.gather(first, *followers)
        assert [b[1] - a[1] for a, b in zip(starts, starts[1:], strict=False)] == pytest.approx([0.1] * 3)
        assert t.limiter.last == pytest.approx(starts[-1][1])
    finally:
        release.set()
        first.cancel()
        for task in followers:
            task.cancel()
        await asyncio.gather(first, *followers, return_exceptions=True)
        await t.close()


async def test_cancelled_reserved_mutation_sends_nothing_and_releases_capacity():
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def before():
        entered.set()
        await release.wait()

    t = transport(lambda request: (calls.append(request.method), httpx.Response(200, json={}))[1])
    mutation = asyncio.create_task(t.request("POST", "/rfq", body={}, before_attempt=before))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        mutation.cancel()
        await asyncio.gather(mutation, return_exceptions=True)
        assert t.in_flight._value == 8 and t.limiter.admitted == 0 and not calls
        await asyncio.wait_for(t.request("GET", "/metadata"), 1)
        assert calls == ["GET"] and t.in_flight._value == 8 and t.limiter.admitted == 1
    finally:
        mutation.cancel()
        await asyncio.gather(mutation, return_exceptions=True)
        await t.close()


async def test_cooldown_received_during_before_attempt_remains_binding_at_actual_start(clock):
    entered, release = asyncio.Event(), asyncio.Event()
    starts = []

    async def before():
        entered.set()
        await release.wait()

    t = transport(lambda _: (starts.append(clock.now), httpx.Response(200, json={}))[1], interval=0.05)
    task = asyncio.create_task(t.request("POST", "/rfq", body={}, before_attempt=before))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        t.limiter.defer(60)
        release.set()
        await task
        assert starts == [160.0] and t.limiter.last == 160
        assert t.limiter.blocked_until == 160 and t.limiter.interval == 0.05
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await t.close()


async def test_independent_bucket_and_ready_category_can_pass_a_blocked_main_request(clock):
    starts = []
    t = transport(lambda request: (starts.append((request.url.path, clock.now)), httpx.Response(200, json={}))[1])
    t.limiter.categories = {"history": 10}
    t.limiter.category_blocked["history"] = 120
    execution = PriorityLimiter(0.2)
    try:
        tasks = [asyncio.create_task(t.request("GET", "/history", priority=0, category="history"))]
        tasks.append(asyncio.create_task(t.request("GET", "/standard", priority=10)))
        tasks.append(asyncio.create_task(t.request("GET", "/execute", priority=5, limiter=execution)))
        await asyncio.gather(*tasks)
        assert [path for path, _ in starts] == ["/execute", "/standard", "/history"]
        assert starts[-1][1] == 120
        assert execution.admitted == 1 and t.limiter.admitted == 2
        assert t.limiter.category_last["history"] == 120
        assert t.limiter.category_blocked == {"history": 120}
    finally:
        await execution.close()
        await t.close()


async def test_before_attempt_error_releases_unstarted_slot_without_sending_or_retrying():
    calls = []
    t = transport(lambda request: (calls.append(request.method), httpx.Response(200, json={}))[1])

    async def failed_preparation():
        raise ValueError("fixture preparation failed")

    try:
        with pytest.raises(ValueError, match="fixture preparation failed"):
            await t.request("POST", "/rfq", body={}, before_attempt=failed_preparation)
        assert t.in_flight._value == 8 and t.limiter.admitted == 0 and not calls
        await asyncio.wait_for(t.request("GET", "/metadata"), 1)
        assert calls == ["GET"] and t.in_flight._value == 8 and t.limiter.admitted == 1
    finally:
        await t.close()


async def test_close_cancels_rate_waiting_mutation_without_a_request_or_slot_leak():
    calls = []
    t = transport(lambda request: (calls.append(request.method), httpx.Response(200, json={}))[1])
    t.limiter.defer(60)
    mutation = asyncio.create_task(t.request("POST", "/rfq", body={}))
    try:
        await queued(t, 1)
        await t.close()
        result = await asyncio.gather(mutation, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert not calls and t.in_flight._value == 8 and t.limiter.admitted == 0
        assert t.limiter.status()["queued"] == 0
    finally:
        mutation.cancel()
        await asyncio.gather(mutation, return_exceptions=True)
