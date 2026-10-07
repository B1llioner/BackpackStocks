import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from backpack_stocks.pool_bank_feed import PoolBankFeed


async def test_shared_dependency_precedes_single_primary_and_secondary_without_changing_execution():
    async def read(refs, **kwargs):
        return {"context": {"slot": 2}, "value": refs}

    rpc = SimpleNamespace(accounts=AsyncMock(side_effect=read))
    feed = PoolBankFeed(rpc, 8, timer=lambda: 20)
    shared_old = "shared_old", "SOL", "USDC", 10
    shared_new = "shared_new", "SOL", "USDC", 10
    single = "single", "stock", "SOL", 10
    secondary = "secondary", "stock", "USDC", 10
    produced = []

    async def produce(key):
        produced.append(key[0])
        jobs = [feed.accounts([key[0]], priority=10)]
        if key == shared_old:
            jobs.append(feed.accounts(["execution"], priority=0))
        await asyncio.gather(*jobs)
        return {"observed": 100}

    try:
        for key, stamp in ((secondary, 0), (single, 0), (shared_new, 2), (shared_old, 1)):
            feed.producers[key] = lambda key=key: produce(key)
            feed.hot[key] = float("inf")
            feed.received_at[key] = stamp
        feed.set_primary({shared_old: 2, shared_new: 30, single: 1})
        async with asyncio.timeout(1):
            while feed.refreshed < 4:
                await asyncio.sleep(0.01)
        assert produced[:4] == ["shared_old", "shared_new", "single", "secondary"]
        priorities = {
            ref: call.kwargs["priority"] for call in rpc.accounts.await_args_list for ref in call.args[0]
        }
        assert priorities == {
            "shared_old": 3,
            "shared_new": 3,
            "single": 5,
            "secondary": 10,
            "execution": 0,
        }
    finally:
        await feed.close()


async def test_owner_count_updates_do_not_extend_age_retention_or_legacy_priority():
    feed = PoolBankFeed(None, 8, timer=lambda: 20)
    shared, unknown = ("shared", "SOL", "USDC", 10), ("unknown", "SOL", "USDC", 10)
    try:
        feed.producers[shared] = AsyncMock()
        feed.hot[shared] = 300
        feed.received_at[shared] = 5
        assert feed.refresh_priority(shared) == 5
        feed.set_primary({shared: 12, unknown: 12})
        assert feed.primary_counts == {shared: 12} and feed.refresh_priority(shared) == 3
        feed.set_primary({shared: 1})
        assert feed.refresh_priority(shared) == 5
        feed.set_primary({shared})
        assert feed.primary_counts == {shared: 1} and feed.refresh_priority(shared) == 5
        feed.set_primary({shared: 0})
        assert feed.primary == frozenset() and feed.refresh_priority(shared) == 10
        assert feed.hot[shared] == 300 and feed.received_at[shared] == 5
    finally:
        await feed.close()
