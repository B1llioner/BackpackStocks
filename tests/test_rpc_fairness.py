import asyncio
import time

from backpack_stocks.limiter import PriorityLimiter
from backpack_stocks.solana import SolanaRPC


async def test_rpc_oldest_turn_prevents_history_starvation_and_keeps_execution_first():
    limiter = PriorityLimiter(0.01, oldest_every=4)
    admitted = []

    async def read(label, priority):
        await limiter.acquire(priority)
        admitted.append((label, time.monotonic()))

    try:
        tasks = [asyncio.create_task(read("history", 20))]
        tasks.extend(asyncio.create_task(read(f"pool{n}", 5)) for n in range(8))
        tasks.append(asyncio.create_task(read("execution", 0)))
        await asyncio.gather(*tasks)
        assert [label for label, _ in admitted[:4]] == ["execution", "pool0", "pool1", "history"]
        assert all(b[1] - a[1] >= 0.009 for a, b in zip(admitted, admitted[1:], strict=False))
        assert limiter.admitted == 10
    finally:
        await limiter.close()


async def test_default_rpc_fairness_does_not_increase_transport_or_bandwidth_limits():
    rpc = SolanaRPC("https://example.test/rpc")
    try:
        status = rpc.http.status()
        assert status["oldest_every"] == 4
        assert status["interval_s"] == 0.3
        assert status["max_connections"] == 8
        assert status["byte_budget_per_30s"] == 75_000_000
    finally:
        await rpc.close()
