"""Continuously refresh complete validated banks for recently used pool routes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar

from .observability import error_code, event
from .quote_batch import QuoteAccountBatch

BankKey = tuple[str, str, str, int]
REFRESHING: ContextVar[bool] = ContextVar("refreshing_pool_bank", default=False)


class PoolBankFeed(QuoteAccountBatch):
    def __init__(self, rpc, max_age: float, *, timer: Callable[[], float] = time.monotonic):
        super().__init__(rpc, commitment="confirmed")
        self.max_age = max_age
        self.producers: dict[BankKey, Callable[[], Awaitable[dict]]] = {}
        self.ready: dict[BankKey, dict] = {}
        self.hot: dict[BankKey, float] = {}
        self.retry_at: dict[BankKey, float] = {}
        self.received_at: dict[BankKey, float] = {}
        self.timer = timer
        # Allow a whole monitoring round to reach the coalescer. RPC traffic
        # remains bounded by its unchanged transport pace and eight connections.
        self.limit = asyncio.Semaphore(128)
        self.changed = asyncio.Event()
        self.refreshed = 0
        self.local_quotes = 0
        self.runner = asyncio.create_task(self.refresh_loop())

    def remember(self, key: BankKey, bank: dict) -> None:
        if self.ready.get(key) is not bank:
            self.ready[key] = bank
            self.received_at[key] = self.timer()

    def peek(self, key: BankKey) -> dict | None:
        task = self.pool_banks.get(key)
        if task and task.done() and not task.cancelled() and task.exception() is None:
            self.remember(key, task.result())
        bank = self.ready.get(key)
        return bank if bank and 0 <= time.time() - bank["observed"] < self.max_age else None

    async def pool_snapshot(self, key, producer, max_age):
        if key not in self.producers and len(self.producers) >= 1024:
            raise ValueError("Pool bank cache exceeds bound")
        self.producers[key] = producer
        bank = self.peek(key)
        if (
            bank is not None
            and time.time() - bank["observed"] < max_age
            and self.timer() - self.received_at.get(key, float("-inf")) < self.max_age / 4
        ):
            return bank
        try:
            bank = await super().pool_snapshot(key, producer, max_age)
        except Exception:
            self.ready.pop(key, None)
            raise
        self.remember(key, bank)
        return bank

    async def accounts(self, addresses, *, parsed=False, min_slot=None, priority=0):
        # Refresh winning routes ahead of new route discovery; execution reads
        # at priority zero remain ahead of both. Cache keys keep quote priority.
        if REFRESHING.get():
            priority = min(priority, 5)
        return await super().accounts(addresses, parsed=parsed, min_slot=min_slot, priority=priority)

    def promote(self, quote: dict, priority: int) -> None:
        for leg in quote["routePlan"]:
            info = leg["swapInfo"]
            key = info["ammKey"], info["inputMint"], info["outputMint"], priority
            if key in self.producers:
                self.hot[key] = time.time() + 300

    async def refresh_loop(self):
        while True:
            now = time.time()
            elapsed = self.timer()
            self.hot = {key: until for key, until in self.hot.items() if until > now}
            due = []
            for key in self.hot:
                task = self.pool_banks.get(key)
                bank = self.ready.get(key)
                if task and not task.done():
                    continue
                if elapsed < self.retry_at.get(key, 0):
                    continue
                # Chain Clock may already lag wall time by several seconds on
                # receipt. It measures price freshness, not read scheduling.
                if bank and elapsed - self.received_at.get(key, float("-inf")) < self.max_age / 4:
                    continue
                due.append(key)
            for key in sorted(due, key=lambda k: self.received_at.get(k, 0)):

                async def refresh(key=key):
                    token = REFRESHING.set(True)
                    try:
                        async with self.limit:
                            bank = await self.producers[key]()
                        self.remember(key, bank)
                        self.refreshed += 1
                        self.changed.set()
                        return bank
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        self.ready.pop(key, None)
                        self.retry_at[key] = self.timer() + 5
                        event("pool_bank_refresh_blocked", pool=key[0], code=error_code(error))
                        raise
                    finally:
                        REFRESHING.reset(token)

                task = asyncio.create_task(refresh())
                # Consume failures even when no quote is waiting for this bank.
                task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
                self.pool_banks[key] = task
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
            # Launch due banks together so both validation stages coalesce.
            await asyncio.sleep(0.5)

    def status(self):
        ages = sorted(time.time() - self.ready[key]["observed"] for key in self.hot if key in self.ready)
        return {
            "hot_banks": len(self.hot),
            "fresh_banks": sum(self.peek(key) is not None for key in self.hot),
            "bank_refreshes": self.refreshed,
            "local_quotes": self.local_quotes,
            "bank_age_median_s": ages[len(ages) // 2] if ages else None,
            "bank_age_max_s": max(ages) if ages else None,
        }

    async def close(self):
        self.runner.cancel()
        await asyncio.gather(self.runner, return_exceptions=True)
        await super().close()
