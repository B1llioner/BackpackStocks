import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.pool_bank_feed import PoolBankFeed

KEY = "pool", "stock", "USDC", 10


def bank(slot, observed, acquired, limit=2):
    return {
        "response": {"context": {"slot": slot}},
        "observed": observed,
        "_acquired_monotonic": acquired,
        "array_limit": limit,
    }


def setup(monkeypatch, wall=101.0, elapsed=1.0):
    clocks = SimpleNamespace(wall=wall, elapsed=elapsed)
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: clocks.wall)
    return PoolBankFeed(None, 8, timer=lambda: clocks.elapsed), clocks


async def test_full_publication_cannot_be_rolled_back_by_completed_fast_task(monkeypatch):
    feed, clocks = setup(monkeypatch)
    fast, full = bank(1, 100, 0), bank(2, 101, 1, 8)
    task = asyncio.create_task(asyncio.sleep(0, result=fast))
    try:
        await task
        feed.pool_banks[KEY] = task
        feed.remember(KEY, fast)
        assert feed.publish(KEY, full) and feed.changed.is_set()
        assert feed.peek(KEY) is full and feed.ready[KEY] is full
        assert feed.received_at[KEY] == 1
        producer = AsyncMock(side_effect=AssertionError("fresh published bank must be reused"))
        assert await feed.pool_snapshot(KEY, producer, 8) is full
        producer.assert_not_awaited()
        assert task.done() and not task.cancelled()
    finally:
        await feed.close()


@pytest.mark.parametrize("failed", [False, True])
async def test_inflight_old_read_cannot_replace_or_clear_new_published_head(monkeypatch, failed):
    feed, clocks = setup(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    fast, full = bank(1, 100, 0), bank(2, 101, 1, 8)

    async def produce():
        entered.set()
        await release.wait()
        if failed:
            raise ValueError("older read failed")
        return fast

    pending = asyncio.create_task(feed.pool_snapshot(KEY, produce, 8))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        underlying = feed.pool_banks[KEY]
        assert feed.publish(KEY, full)
        assert not underlying.done() and not underlying.cancelled()
        release.set()
        assert await pending is full
        assert feed.peek(KEY) is full and feed.received_at[KEY] == 1
        assert not underlying.cancelled()
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await feed.close()


async def test_equal_head_full_coverage_survives_lesser_fast_bank_completion(monkeypatch):
    feed, _ = setup(monkeypatch, wall=100, elapsed=0)
    fast, full = bank(7, 100, 0), bank(7, 100, 0, 8)
    try:
        feed.remember(KEY, fast)
        assert feed.publish(KEY, full)
        feed.remember(KEY, fast)
        assert feed.peek(KEY) is full
        assert not feed.publish(KEY, fast)
    finally:
        await feed.close()


@pytest.mark.parametrize(
    "invalid",
    [
        bank(2, 92, 0),  # Expired exactly at the original eight-second boundary.
        bank(2, 100.1, 0),
        bank(2, float("nan"), 0),
        bank(2, float("inf"), 0),
        bank(2, 100, 2),  # Acquisition time is in the future too.
    ],
)
async def test_stale_future_or_invalid_publication_cannot_replace_a_valid_bank(monkeypatch, invalid):
    feed, _ = setup(monkeypatch, wall=100, elapsed=1)
    current = bank(1, 100, 0)
    try:
        feed.remember(KEY, current)
        assert not feed.publish(KEY, invalid)
        assert feed.peek(KEY) is current and feed.received_at[KEY] == 0
        assert not feed.changed.is_set()
    finally:
        await feed.close()


async def test_context_slot_order_precedes_later_receipt_and_publication_never_restamps(monkeypatch):
    feed, clocks = setup(monkeypatch, wall=101, elapsed=3)
    current = bank(2, 100, 0)
    newer = bank(3, 100.5, 0.5, 8)
    try:
        feed.remember(KEY, current)
        assert not feed.publish(KEY, bank(1, 101, 1, 8))
        assert feed.publish(KEY, newer)
        assert newer["observed"] == 100.5 and feed.received_at[KEY] == 0.5
        clocks.wall, clocks.elapsed = 108.5, 11
        assert feed.peek(KEY) is None
        assert not feed.publish(KEY, newer)
        assert newer["observed"] == 100.5 and feed.received_at[KEY] == 0.5
    finally:
        await feed.close()


async def test_legacy_banks_without_context_slots_use_original_observation_order(monkeypatch):
    feed, _ = setup(monkeypatch)
    old, newer = {"observed": 100}, {"observed": 101}
    try:
        feed.remember(KEY, old)
        assert feed.publish(KEY, newer)
        feed.remember(KEY, old)
        assert feed.peek(KEY) is newer
    finally:
        await feed.close()


async def test_old_background_refresh_failure_does_not_erase_published_head_or_delay_its_refresh(monkeypatch):
    feed, _ = setup(monkeypatch, wall=101, elapsed=5)
    old, full = bank(1, 100, 0), bank(2, 101, 5, 8)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def produce():
        entered.set()
        await release.wait()
        finished.set()
        raise ValueError("older refresh failed")

    try:
        feed.remember(KEY, old)
        feed.hot[KEY] = 400
        feed.producers[KEY] = produce
        await asyncio.wait_for(entered.wait(), 1)
        assert feed.publish(KEY, full)
        release.set()
        await asyncio.wait_for(finished.wait(), 1)
        await asyncio.sleep(0)
        assert feed.peek(KEY) is full
        assert KEY not in feed.retry_at
    finally:
        release.set()
        await feed.close()
