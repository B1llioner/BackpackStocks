import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from backpack_stocks.pool_bank_feed import PoolBankFeed


async def test_lagging_chain_clock_never_creates_continuous_reads_or_extends_quote_age(monkeypatch):
    wall, elapsed = [100.0], [0.0]
    monkeypatch.setattr("backpack_stocks.pool_bank_feed.time.time", lambda: wall[0])
    feed = PoolBankFeed(None, 8, timer=lambda: elapsed[0])
    key = "pool", "stock", "USDC", 10
    producer = AsyncMock(side_effect=lambda: {"observed": wall[0] - 4})
    try:
        await feed.pool_snapshot(key, producer, 8)
        feed.hot[key] = 400
        await asyncio.sleep(0.6)
        assert producer.await_count == 1  # Four-second Clock lag is not a request deadline.
        wall[0], elapsed[0] = 101.0, 1.0
        assert (await feed.pool_snapshot(key, producer, 8))["observed"] == 96
        assert producer.await_count == 1
        assert feed.received_at[key] == 0  # Reading the cache cannot postpone a real refresh.
        wall[0], elapsed[0] = 102.1, 2.1
        await asyncio.wait_for(feed.changed.wait(), 1.5)
        assert producer.await_count == 2 and feed.peek(key)["observed"] == 98.1
        wall[0], elapsed[0] = 106.2, 2.2
        assert feed.peek(key) is None  # Freshness always uses the original chain time.
        await asyncio.sleep(0.6)
        assert producer.await_count == 2
    finally:
        await feed.close()


async def test_full_universe_round_reaches_rpc_batches_without_serial_pool_waves():
    seen = set()
    entire_round = asyncio.Event()

    async def rpc_read(refs, **kwargs):
        seen.update(ref.split(":")[0] for ref in refs)
        if len(seen) == 84:
            entire_round.set()
        await asyncio.wait_for(entire_round.wait(), 1.5)
        return {"context": {"slot": 200}, "value": refs}

    rpc = SimpleNamespace(accounts=AsyncMock(side_effect=rpc_read))
    feed = PoolBankFeed(rpc, 8, timer=lambda: 3.0)
    try:
        for index in range(84):
            key = str(index), "stock", "USDC", 10

            async def produce(index=index):
                await feed.accounts([f"{index}:{n}" for n in range(10)], priority=10)
                return {"observed": 100.0}

            feed.producers[key] = produce
            feed.ready[key] = {"observed": 100.0}
            feed.received_at[key] = 0
            feed.hot[key] = float("inf")
        await asyncio.wait_for(entire_round.wait(), 2)
        await asyncio.sleep(0.05)
        assert feed.refreshed == 84
        assert rpc.accounts.await_count == 9
        assert all(len(call.args[0]) <= 100 for call in rpc.accounts.call_args_list)
        assert all(call.kwargs["priority"] == 5 for call in rpc.accounts.call_args_list)
    finally:
        await feed.close()
