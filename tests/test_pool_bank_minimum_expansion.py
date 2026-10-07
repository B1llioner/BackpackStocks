import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.pool_bank_feed import REFRESH_PRIORITY, REFRESHING, PoolBankFeed


def bank(stamp=100.0, limit=2, slot=1, acquired=0.0):
    return {
        "observed": stamp,
        "_acquired_monotonic": acquired,
        "array_limit": limit,
        "response": {"context": {"slot": slot}},
    }


async def stop_polling(feed):
    feed.runner.cancel()
    await asyncio.gather(feed.runner, return_exceptions=True)


@pytest.mark.parametrize("owners,priority", [(1, 5), (20, 3)])
async def test_minimum_expansion_deduplicates_and_publishes_original_head_before_any_math(
    monkeypatch, owners, priority
):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    rpc = SimpleNamespace(
        accounts=AsyncMock(side_effect=lambda refs, **kwargs: {"context": {"slot": 2}, "value": refs})
    )
    feed = PoolBankFeed(rpc, 8, timer=lambda: elapsed[0])
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    entered, release = asyncio.Event(), asyncio.Event()
    published = bank(101, 4, 2, 1)

    async def produce():
        assert REFRESHING.get() and REFRESH_PRIORITY.get() == priority
        entered.set()
        await release.wait()
        await asyncio.gather(feed.accounts(["shared"], priority=10), feed.accounts(["execution"], priority=0))
        return published

    producer = AsyncMock(side_effect=produce)
    old = bank()
    try:
        feed.producers[key] = producer
        feed.remember(key, old)
        feed.hot[key] = 400
        feed.set_primary({key: owners})
        feed.set_minimum_arrays({key: 2})
        assert feed.request_minimum_expansion(key, 4)
        task = feed.expansion_tasks[key]
        await asyncio.wait_for(entered.wait(), 1)
        assert feed.request_minimum_expansion(key, 4)
        assert feed.expansion_tasks[key] is task and feed.pool_banks[key] is task
        assert feed.peek(key) is old and producer.await_count == 1
        assert not feed.changed.is_set()
        # Re-synchronizing unchanged owner requirements must not drop a pending expansion.
        feed.set_minimum_arrays({key: 2})
        assert feed.minimum_expansion_limits == {key: 4}
        wall[0], elapsed[0] = 101.0, 1.0
        release.set()
        assert await asyncio.wait_for(task, 1) is published
        assert feed.peek(key) is published and feed.changed.is_set()
        assert feed.received_at[key] == 1 and feed.hot[key] == 400
        assert {
            ref: call.kwargs["priority"] for call in rpc.accounts.await_args_list for ref in call.args[0]
        } == {
            "shared": priority,
            "execution": 0,
        }
        assert feed.minimum_expansion_limits[key] == 4
        assert key not in feed.expansion_tasks and task not in feed.tasks
        feed.set_minimum_arrays({key: 2}, resolved=[key])
        assert feed.minimum_expansion_limits == {} and feed.minimum_arrays == {key: 2}
        wall[0] = 109.0
        assert feed.peek(key) is None  # Publication cannot restamp the acquired bank.
        assert not REFRESHING.get() and REFRESH_PRIORITY.get() == 5
    finally:
        release.set()
        await feed.close()


async def test_expansion_waits_for_existing_read_and_other_snapshot_joins_without_duplicate(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 0.0)
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    entered, release = asyncio.Event(), asyncio.Event()
    order = []

    async def old_read():
        order.append("old_start")
        entered.set()
        await release.wait()
        current = bank()
        feed.remember(key, current)
        order.append("old_done")
        return current

    async def expand_read():
        order.append("expand_start")
        assert feed.minimum_expansion_limits[key] == 4
        order.append("expand_done")
        return bank(limit=4, slot=2)

    producer = AsyncMock(side_effect=expand_read)
    try:
        feed.producers[key] = producer
        feed.set_primary({key: 5})
        old = asyncio.create_task(old_read())
        feed.pool_banks[key] = old
        feed.tasks.add(old)
        old.add_done_callback(feed.tasks.discard)
        await asyncio.wait_for(entered.wait(), 1)
        assert feed.request_minimum_expansion(key, 4)
        expansion = feed.expansion_tasks[key]
        joined = asyncio.create_task(feed.pool_snapshot(key, producer, 8))
        await asyncio.sleep(0)
        producer.assert_not_awaited()
        assert not old.cancelled()
        release.set()
        result = await asyncio.wait_for(joined, 1)
        assert result is await expansion and result["array_limit"] == 4
        assert order == ["old_start", "old_done", "expand_start", "expand_done"]
        producer.assert_awaited_once()
    finally:
        release.set()
        await feed.close()


async def test_larger_requirement_during_inflight_read_uses_same_task_sequentially(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 0.0)
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    entered, release = asyncio.Event(), asyncio.Event()
    reads = []

    async def produce():
        requested = feed.minimum_expansion_limits[key]
        reads.append(requested)
        if len(reads) == 1:
            entered.set()
            await release.wait()
        return bank(limit=requested, slot=len(reads) + 1)

    try:
        feed.producers[key] = produce
        feed.remember(key, bank())
        feed.set_primary([key])
        assert feed.request_minimum_expansion(key, 4)
        task = feed.expansion_tasks[key]
        await asyncio.wait_for(entered.wait(), 1)
        assert feed.request_minimum_expansion(key, 8) and feed.expansion_tasks[key] is task
        release.set()
        result = await asyncio.wait_for(task, 1)
        assert reads == [4, 8] and result["array_limit"] == 8
        assert feed.minimum_expansion_limits[key] == 8
    finally:
        release.set()
        await feed.close()


async def test_expansion_failure_keeps_old_bank_and_cooldown_without_repeated_requests(monkeypatch):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8, timer=lambda: elapsed[0])
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    producer = AsyncMock(side_effect=[TimeoutError(), bank(105, 6, 2, 5)])
    old = bank()
    try:
        feed.producers[key] = producer
        feed.remember(key, old)
        feed.set_primary([key])
        assert feed.request_minimum_expansion(key, 4)
        with pytest.raises(TimeoutError):
            await feed.expansion_tasks[key]
        assert feed.failure_at[key] == 0 and feed.retry_at[key] == 5
        assert feed.peek(key) is old
        assert not feed.request_minimum_expansion(key, 6)
        elapsed[0] = 4.999
        assert not feed.request_minimum_expansion(key, 6)
        producer.assert_awaited_once()
        assert feed.minimum_expansion_limits[key] == 6
        wall[0], elapsed[0] = 105.0, 5.0
        assert feed.request_minimum_expansion(key, 6)
        await feed.expansion_tasks[key]
        assert producer.await_count == 2 and feed.peek(key)["array_limit"] == 6
        assert key not in feed.retry_at and key not in feed.failure_at
    finally:
        await feed.close()


async def test_older_expansion_failure_preserves_newer_published_bank_and_waiting_snapshot(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 4.0)
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    entered, release = asyncio.Event(), asyncio.Event()

    async def fail():
        entered.set()
        await release.wait()
        raise TimeoutError()

    producer = AsyncMock(side_effect=fail)
    newer = bank(100, 4, 3, 4)
    try:
        feed.producers[key] = producer
        feed.remember(key, bank(96))
        feed.set_primary([key])
        assert feed.request_minimum_expansion(key, 4)
        expansion = feed.expansion_tasks[key]
        await asyncio.wait_for(entered.wait(), 1)
        joined = asyncio.create_task(feed.pool_snapshot(key, producer, 8))
        await asyncio.sleep(0)
        assert feed.publish(key, newer)
        release.set()
        with pytest.raises(TimeoutError):
            await expansion
        assert await asyncio.wait_for(joined, 1) is newer
        assert feed.peek(key) is newer and key not in feed.retry_at and key not in feed.failure_at
        producer.assert_awaited_once()
    finally:
        release.set()
        await feed.close()


@pytest.mark.parametrize("stamp", [92.0, 101.0])
async def test_expansion_never_publishes_stale_or_future_bank(monkeypatch, stamp):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 0.0)
    await stop_polling(feed)
    key = "shared", "SOL", "USDC", 10
    old = bank()
    try:
        feed.producers[key] = AsyncMock(return_value=bank(stamp, 4, 2))
        feed.remember(key, old)
        feed.set_primary([key])
        assert feed.request_minimum_expansion(key, 4)
        with pytest.raises(ValueError, match="not usable"):
            await feed.expansion_tasks[key]
        assert feed.peek(key) is old and feed.received_at[key] == 0
        assert feed.retry_at[key] == 5 and not feed.changed.is_set()
    finally:
        await feed.close()


async def test_expansion_rejects_nonprimary_invalid_keys_and_limits_and_prunes_without_cancel(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 0.0)
    await stop_polling(feed)
    key, other = ("shared", "SOL", "USDC", 10), ("other", "SOL", "USDC", 10)
    entered, release = asyncio.Event(), asyncio.Event()

    async def produce():
        entered.set()
        await release.wait()
        return bank(limit=4, slot=2)

    try:
        feed.producers[key] = produce
        feed.remember(key, bank())
        assert not feed.request_minimum_expansion(key, 4)
        feed.set_primary([key])
        for limit in [0, -1, True, 1.0, float("inf"), float("nan"), 13]:
            assert not feed.request_minimum_expansion(key, limit)
        assert not feed.request_minimum_expansion(other, 4)
        assert feed.minimum_expansion_limits == {} and feed.expansion_tasks == {}
        assert feed.request_minimum_expansion(key, 4)
        task = feed.expansion_tasks[key]
        await asyncio.wait_for(entered.wait(), 1)
        feed.set_primary([])
        assert feed.minimum_expansion_limits == {} and not task.cancelled()
        release.set()
        await asyncio.wait_for(task, 1)
        assert not task.cancelled() and task not in feed.tasks
        feed.set_primary([key])
        assert feed.request_minimum_expansion(key, 8)
        await feed.close()
        assert not feed.expansion_tasks and not feed.tasks
    finally:
        release.set()
        await feed.close()


async def test_failed_secondary_waits_thirty_and_promotion_retains_original_failure_time(monkeypatch):
    wall, elapsed = [130.0], [30.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    transport = SimpleNamespace(blocked_until=999)
    rpc = SimpleNamespace(http=SimpleNamespace(limiter=transport))
    feed = PoolBankFeed(rpc, 8, timer=lambda: elapsed[0])
    key = "secondary", "stock", "USDC", 10
    producer = AsyncMock(side_effect=TimeoutError())
    try:
        feed.producers[key] = producer
        feed.remember(key, bank())
        feed.hot[key] = 400
        feed.set_primary([])
        async with asyncio.timeout(1):
            while key not in feed.retry_at:
                await asyncio.sleep(0.01)
        assert feed.failure_at[key] == 30 and feed.retry_at[key] == 60
        wall[0], elapsed[0] = 134.0, 34.0
        await asyncio.sleep(0.55)
        producer.assert_awaited_once()
        feed.set_primary([key])
        assert feed.failure_at[key] == 30 and feed.retry_at[key] == 35
        assert transport.blocked_until == 999
        assert not feed.request_minimum_expansion(key, 4)
        feed.set_primary([key])
        assert feed.retry_at[key] == 35  # Repeated sync cannot move the original deadline.
    finally:
        await feed.close()
