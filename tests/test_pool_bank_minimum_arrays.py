import asyncio
from unittest.mock import AsyncMock

from backpack_stocks.pool_bank_feed import PoolBankFeed


async def test_minimum_array_limits_are_bounded_current_ownership_without_restamping(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100.0)
    feed = PoolBankFeed(None, 8, timer=lambda: 10.0)
    primary = "primary", "stock", "USDC", 10
    shared = "shared", "SOL", "USDC", 10
    secondary = "secondary", "stock", "USDC", 10
    unknown = "unknown", "stock", "USDC", 10
    bad_limits = [0, -1, True, False, 1.0, float("inf"), float("nan"), "2", None, 13, 10**100]
    invalid = {(str(index), "stock", "USDC", 10): value for index, value in enumerate(bad_limits)}
    producer = AsyncMock()
    bank = {"observed": 99.0, "_acquired_monotonic": 9.0, "array_limit": 8}
    try:
        for key in [primary, shared, secondary, *invalid]:
            feed.producers[key] = producer
        feed.remember(primary, bank)
        feed.hot[primary] = 400
        # Unconfigured callers retain the existing array-selection behavior.
        feed.set_minimum_arrays({primary: 2})
        assert feed.minimum_arrays == {}
        feed.set_primary({primary: 1, shared: 20, **dict.fromkeys(invalid, 1)})
        limits = {primary: 1, shared: 12, secondary: 2, unknown: 2, **invalid}
        feed.set_minimum_arrays(limits)
        assert feed.minimum_arrays == {primary: 1, shared: 12}
        limits[primary] = 8
        assert feed.minimum_arrays[primary] == 1
        assert feed.ready[primary] is bank and feed.received_at[primary] == 9
        assert feed.hot[primary] == 400 and not feed.changed.is_set()
        producer.assert_not_called()
        feed.set_primary({shared: 1})
        assert feed.minimum_arrays == {shared: 12}
        feed.set_minimum_arrays({shared: 2})
        assert feed.minimum_arrays == {shared: 2}
        feed.set_primary([])
        assert feed.minimum_arrays == {}
    finally:
        await feed.close()


async def test_secondary_refreshes_at_thirty_seconds_primary_at_four_without_extending_ttl(
    monkeypatch,
):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8, timer=lambda: elapsed[0])
    primary, secondary = ("primary", "stock", "USDC", 10), ("secondary", "stock", "USDC", 10)
    first = {"observed": 100.0, "_acquired_monotonic": 0.0}

    def new_bank():
        return {"observed": wall[0], "_acquired_monotonic": elapsed[0]}

    producers = {key: AsyncMock(side_effect=new_bank) for key in [primary, secondary]}
    try:
        for key in [primary, secondary]:
            feed.producers[key] = producers[key]
            feed.remember(key, first)
            feed.hot[key] = 400
        assert feed.refresh_period(secondary) == 4  # Legacy unclassified caller.
        feed.set_primary([primary])
        assert feed.refresh_period(primary) == 4 and feed.refresh_period(secondary) == 30
        wall[0], elapsed[0] = 104.0, 4.0
        await asyncio.wait_for(feed.changed.wait(), 1)
        producers[primary].assert_awaited_once()
        producers[secondary].assert_not_awaited()
        assert feed.peek(secondary) is first and feed.received_at[secondary] == 0
        wall[0], elapsed[0] = 108.0, 8.0
        assert feed.peek(secondary) is None
        assert feed.received_at[secondary] == 0 and feed.hot[secondary] == 400
        wall[0], elapsed[0] = 129.0, 29.0
        await asyncio.sleep(0.55)
        producers[secondary].assert_not_awaited()
        wall[0], elapsed[0] = 130.0, 30.0
        async with asyncio.timeout(1):
            while not producers[secondary].await_count:
                await asyncio.sleep(0.01)
        producers[secondary].assert_awaited_once()
        assert feed.received_at[secondary] == 30
        assert feed.peek(secondary)["observed"] == 130
    finally:
        await feed.close()


async def test_stale_secondary_on_demand_read_fetches_fresh_before_background_deadline(monkeypatch):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8, timer=lambda: elapsed[0])
    key = "secondary", "stock", "USDC", 10
    producer = AsyncMock(side_effect=lambda: {"observed": wall[0], "_acquired_monotonic": elapsed[0]})
    try:
        first = await feed.pool_snapshot(key, producer, 8)
        feed.set_primary([])
        feed.hot[key] = 400
        wall[0], elapsed[0] = 108.0, 8.0
        await asyncio.sleep(0.55)
        producer.assert_awaited_once()
        assert feed.peek(key) is None and first["observed"] == 100
        second = await feed.pool_snapshot(key, producer, 8)
        assert producer.await_count == 2
        assert second is not first and second["observed"] == 108
        assert feed.peek(key) is second and feed.received_at[key] == 8
        wall[0] = 116.0
        assert feed.peek(key) is None
    finally:
        await feed.close()
