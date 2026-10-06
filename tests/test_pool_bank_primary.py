import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.pool_bank_feed import PoolBankFeed


async def test_primary_refreshes_precede_older_secondary_and_preserve_execution_priority():
    rpc = SimpleNamespace(accounts=AsyncMock(return_value={"context": {"slot": 2}, "value": []}))

    async def read(refs, **kwargs):
        return {"context": {"slot": 2}, "value": refs}

    rpc.accounts.side_effect = read
    feed = PoolBankFeed(rpc, 8, timer=lambda: 10)
    primary, secondary = ("primary", "stock", "USDC", 10), ("secondary", "stock", "USDC", 10)
    produced = []

    async def produce(key):
        produced.append(key[0])
        requests = [feed.accounts([key[0]], priority=10)]
        if key == primary:
            requests.append(feed.accounts(["execution"], priority=0))
        await asyncio.gather(*requests)
        return {"observed": 100, "response": {"context": {"slot": 2}}}

    try:
        for key, age in ((secondary, 0), (primary, 1)):
            feed.producers[key] = lambda key=key: produce(key)
            feed.hot[key] = float("inf")
            feed.received_at[key] = age
        feed.set_primary([primary])
        async with asyncio.timeout(1):
            while feed.refreshed < 2:
                await asyncio.sleep(0.01)
        assert produced == ["primary", "secondary"]
        assert {call.args[0][0]: call.kwargs["priority"] for call in rpc.accounts.await_args_list} == {
            "primary": 5,
            "secondary": 10,
            "execution": 0,
        }
        assert feed.refreshed == 2
    finally:
        await feed.close()


@pytest.mark.parametrize("mode,priority", [("legacy", 5), ("empty", 10), ("primary", 5)])
async def test_primary_selection_is_optional_and_empty_selection_demotes_all_refreshes(mode, priority):
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kwargs: {
                "context": {"slot": 2},
                "value": refs,
            }
        )
    )
    feed = PoolBankFeed(rpc, 8, timer=lambda: 10)
    key = "pool", "stock", "USDC", 10

    async def produce():
        await feed.accounts(["pool"], priority=10)
        return {"observed": 100}

    try:
        feed.producers[key] = produce
        feed.hot[key] = float("inf")
        if mode != "legacy":
            feed.set_primary([key] if mode == "primary" else [])
        await asyncio.wait_for(feed.changed.wait(), 1)
        rpc.accounts.assert_awaited_once()
        assert rpc.accounts.await_args.kwargs["priority"] == priority
    finally:
        await feed.close()


async def test_primary_sync_replaces_ownership_without_refreshing_or_extending_retention(monkeypatch):
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: 100)
    feed = PoolBankFeed(None, 8, timer=lambda: 20)
    first, second, unknown = (
        ("first", "stock", "USDC", 10),
        ("second", "stock", "USDC", 10),
        (
            "unknown",
            "stock",
            "USDC",
            10,
        ),
    )
    producer = AsyncMock()
    expired = {"observed": 92}
    try:
        for key in (first, second):
            feed.producers[key] = producer
            feed.ready[key] = expired
            feed.received_at[key] = 0
            feed.hot[key] = 140
        before_hot, before_received = dict(feed.hot), dict(feed.received_at)
        feed.set_primary([first, unknown])
        assert feed.primary == frozenset([first])
        feed.set_primary([second])
        assert feed.primary == frozenset([second])
        feed.set_primary([])
        assert feed.primary == frozenset()
        assert feed.hot == before_hot and feed.received_at == before_received
        assert feed.peek(first) is None and feed.peek(second) is None
        producer.assert_not_called()
    finally:
        await feed.close()


async def test_sdk_validation_delay_does_not_postpone_acquisition_refresh_or_extend_price_age(monkeypatch):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8, timer=lambda: elapsed[0])
    key = "pool", "stock", "USDC", 10
    producer = AsyncMock(
        side_effect=lambda: {
            "observed": wall[0],
            "_acquired_monotonic": elapsed[0],
        }
    )
    acquired = {"observed": 100.0, "_acquired_monotonic": 0.0}
    try:
        # Complete-bank SDK validation finishes two seconds after acquisition.
        wall[0], elapsed[0] = 102.0, 2.0
        feed.remember(key, acquired)
        feed.producers[key] = producer
        feed.hot[key] = 400
        assert feed.received_at[key] == 0
        assert feed.peek(key) is acquired
        await asyncio.sleep(0.55)
        producer.assert_not_awaited()
        wall[0], elapsed[0] = 104.0, 4.0
        await asyncio.wait_for(feed.changed.wait(), 1.1)
        producer.assert_awaited_once()  # Due at acquisition + 4, not validation + 4.
        assert feed.received_at[key] == 4
        refreshed = feed.peek(key)
        wall[0], elapsed[0] = 111.0, 7.0
        assert feed.peek(key) is refreshed and feed.received_at[key] == 4
        wall[0] = 112.0
        assert feed.peek(key) is None  # Original observed time still expires after eight seconds.
    finally:
        await feed.close()
