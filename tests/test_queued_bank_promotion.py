import asyncio
import json
import time

import httpx
import pytest

from backpack_stocks.pool_bank_feed import PoolBankFeed
from backpack_stocks.solana import SolanaRPC
from backpack_stocks.transport import Transport


async def wait_queued(http, count):
    async with asyncio.timeout(2):
        while len(http.limiter.capacity_waiters) != count:
            await asyncio.sleep(0)


@pytest.mark.parametrize(
    "initial_owners, owners, promoted_priority", [(0, 0, 10), (0, 1, 5), (0, 7, 3), (7, 0, 10)]
)
async def test_queued_secondary_coalesced_bank_uses_current_ownership_at_admission(
    initial_owners, owners, promoted_priority
):
    release, full = asyncio.Event(), asyncio.Event()
    starts = []
    start_times = []
    group_config = []

    async def handler(request):
        payload = json.loads(request.content)
        addresses = payload["params"][0]
        starts.append(addresses)
        start_times.append(time.monotonic())
        if "active-bank" in addresses:
            group_config.append(payload["params"][1])
        if addresses[0].startswith("running-"):
            if len(starts) == 8:
                full.set()
            await release.wait()
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {"context": {"slot": 2}, "value": addresses},
            },
        )

    client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handler))
    http = Transport("", client=client)
    rpc = SolanaRPC("https://example.test", transport=http)
    feed = PoolBankFeed(rpc, 8)
    feed.runner.cancel()
    await asyncio.gather(feed.runner, return_exceptions=True)
    active_key = "active-pool", "stock", "USDC", 10
    secondary_key = "secondary-pool", "stock", "USDC", 10
    tasks = [asyncio.create_task(rpc.accounts([f"running-{i}"], priority=10)) for i in range(8)]
    producers_called = []

    def producer(address, min_slot):
        async def read():
            producers_called.append(address)
            response = await feed.accounts([address], priority=10, min_slot=min_slot)
            return {
                "observed": time.time(),
                "_acquired_monotonic": time.monotonic(),
                "response": response,
            }

        return read

    active_producer, secondary_producer = producer("active-bank", 1), producer("secondary-bank", 2)
    try:
        await asyncio.wait_for(full.wait(), 2)
        feed.producers[active_key] = active_producer
        feed.producers[secondary_key] = secondary_producer
        feed.set_primary({active_key: initial_owners, secondary_key: initial_owners})
        tasks.append(asyncio.create_task(rpc.accounts(["older-cold"], priority=10)))
        await wait_queued(http, 1)
        active = asyncio.create_task(feed.pool_snapshot(active_key, active_producer, 8))
        secondary = asyncio.create_task(feed.pool_snapshot(secondary_key, secondary_producer, 8))
        tasks.extend([active, secondary])
        await wait_queued(http, 2)
        bank_task = feed.pool_banks[active_key]
        if not initial_owners:
            assert http.status()["queued_by_priority"] == {"10": 2}
        tasks.append(asyncio.create_task(rpc.accounts(["execution"], priority=0)))
        await wait_queued(http, 3)

        feed.set_primary({active_key: owners})
        # A new minimum caller joins the existing queued bank; promotion must
        # change its ticket rather than cancel it or issue a duplicate RPC.
        joined = asyncio.create_task(feed.pool_snapshot(active_key, active_producer, 8))
        tasks.append(joined)
        await asyncio.sleep(0)
        assert feed.pool_banks[active_key] is bank_task
        assert not bank_task.done() and not bank_task.cancelled()
        # Promotion cannot shorten the transport's existing server cooldown.
        http.limiter.defer(0.03)
        deadline = http.limiter.blocked_until
        queued_tiers = http.status()["queued_by_priority"]
        assert queued_tiers == (
            {"0": 1, "10": 2} if promoted_priority == 10 else {"0": 1, "10": 1, str(promoted_priority): 1}
        )
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 2)

        expected = (
            [["execution"], ["older-cold"], ["active-bank", "secondary-bank"]]
            if promoted_priority == 10
            else [["execution"], ["active-bank", "secondary-bank"], ["older-cold"]]
        )
        assert starts[8:] == expected
        assert len(starts) == 11 and http.limiter.admitted == 11
        assert all(stamp >= deadline for stamp in start_times[8:])
        assert http.limiter.blocked_until == deadline and http.limiter.cooldowns == 1
        assert group_config == [{"encoding": "base64", "commitment": "confirmed", "minContextSlot": 2}]
        assert producers_called == ["active-bank", "secondary-bank"]
        assert active.result() is joined.result()
        assert active.result()["response"] == {"context": {"slot": 2}, "value": ["active-bank"]}
        assert secondary.result()["response"] == {"context": {"slot": 2}, "value": ["secondary-bank"]}
        assert http.in_flight._value == 8
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await feed.close()
        await rpc.close()


@pytest.mark.parametrize("invalid", [True, None, -1, 21, 3.0, "3", RuntimeError("bad resolver")])
async def test_invalid_priority_resolver_keeps_default_and_admission_worker_running(invalid):
    starts = []
    client = httpx.AsyncClient(
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: (starts.append(request.url.path), httpx.Response(200, json={}))[1]
        ),
    )
    http = Transport("", client=client)

    def resolve():
        if isinstance(invalid, Exception):
            raise invalid
        return invalid

    http.limiter.defer(0.01)
    try:
        await asyncio.wait_for(
            asyncio.gather(
                http.request("GET", "/cold", priority=10, priority_resolver=resolve),
                http.request("GET", "/primary", priority=5),
            ),
            2,
        )
        assert starts == ["/primary", "/cold"] and http.limiter.admitted == 2
        assert http.in_flight._value == 8
    finally:
        await http.close()
