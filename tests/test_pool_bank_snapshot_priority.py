import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.pool_bank_feed import REFRESH_PRIORITY, REFRESHING, PoolBankFeed


@pytest.mark.parametrize("owners, expected", [(None, 10), (0, 10), (1, 5), (12, 3)])
async def test_quote_started_refresh_inherits_bank_ownership_and_preserves_execution(
    monkeypatch, owners, expected
):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])

    async def read(refs, **kwargs):
        return {"context": {"slot": 2}, "value": refs}

    rpc = SimpleNamespace(accounts=AsyncMock(side_effect=read))
    feed = PoolBankFeed(rpc, 8, timer=lambda: elapsed[0])
    feed.runner.cancel()
    await asyncio.gather(feed.runner, return_exceptions=True)
    key = "shared", "SOL", "USDC", 10
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def produce():
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        await asyncio.gather(
            feed.accounts(["shared"], priority=10), feed.accounts(["execution"], priority=0)
        )
        return {"observed": wall[0], "_acquired_monotonic": elapsed[0]}

    old = {"observed": 100.0, "_acquired_monotonic": 0.0}
    caller = None
    try:
        feed.producers[key] = produce
        feed.remember(key, old)
        feed.pool_banks[key] = asyncio.create_task(asyncio.sleep(0, result=old))
        await feed.pool_banks[key]
        feed.hot[key] = 400
        if owners is not None:
            feed.set_primary({key: owners})
        wall[0], elapsed[0] = 104.0, 4.0
        # Background quote gets to the due bank before the feed's next poll.
        caller = asyncio.create_task(feed.pool_snapshot(key, produce, 8))
        await asyncio.wait_for(entered.wait(), 1)
        quote_started_bank = feed.pool_banks[key]
        feed.runner = asyncio.create_task(feed.refresh_loop())
        await asyncio.sleep(0.55)
        assert feed.pool_banks[key] is quote_started_bank
        assert calls == 1 and feed.refreshed == 0
        assert feed.peek(key) is old and feed.received_at[key] == 0
        release.set()
        new = await asyncio.wait_for(caller, 1)
        priorities = {
            ref: call.kwargs["priority"]
            for call in rpc.accounts.await_args_list
            for ref in call.args[0]
        }
        assert priorities == {"shared": expected, "execution": 0}
        assert feed.peek(key) is new and new["observed"] == 104.0
        assert feed.received_at[key] == 4.0 and feed.hot[key] == 400
        assert not REFRESHING.get() and REFRESH_PRIORITY.get() == 5
        wall[0] = 112.0
        assert feed.peek(key) is None
    finally:
        release.set()
        if caller is not None:
            await asyncio.gather(caller, return_exceptions=True)
        await feed.close()
