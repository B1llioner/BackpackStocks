"""Continuously refresh complete validated banks for recently used pool routes."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextvars import ContextVar

from .observability import error_code, event
from .quote_batch import QuoteAccountBatch

BankKey = tuple[str, str, str, int]
REFRESHING: ContextVar[bool] = ContextVar("refreshing_pool_bank", default=False)
REFRESH_PRIORITY: ContextVar[int] = ContextVar("pool_bank_refresh_priority", default=5)


class PoolBankFeed(QuoteAccountBatch):
    def __init__(self, rpc, max_age: float, *, timer: Callable[[], float] = time.monotonic):
        super().__init__(rpc, commitment="confirmed")
        self.max_age = max_age
        self.producers: dict[BankKey, Callable[[], Awaitable[dict]]] = {}
        self.ready: dict[BankKey, dict] = {}
        self.hot: dict[BankKey, float] = {}
        self.primary: frozenset[BankKey] | None = None
        self.primary_counts: dict[BankKey, int] = {}
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

    @staticmethod
    def _stamp(bank: dict, name: str) -> float | None:
        value = bank.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None
        try:
            stamp = float(value)
        except OverflowError:
            return None
        return stamp if math.isfinite(stamp) else None

    @staticmethod
    def _slot(bank: dict) -> int | None:
        response = bank.get("response")
        context = response.get("context") if isinstance(response, dict) else None
        value = context.get("slot") if isinstance(context, dict) else None
        return value if type(value) is int and value >= 0 else None

    @classmethod
    def _newer(cls, bank: dict, current: dict) -> bool:
        slot, previous_slot = cls._slot(bank), cls._slot(current)
        if slot is not None and previous_slot is not None and slot != previous_slot:
            return slot > previous_slot
        stamp, previous_stamp = cls._stamp(bank, "observed"), cls._stamp(current, "observed")
        if stamp != previous_stamp:
            return stamp is not None and (previous_stamp is None or stamp > previous_stamp)
        acquired, previous_acquired = (
            cls._stamp(bank, "_acquired_monotonic"),
            cls._stamp(current, "_acquired_monotonic"),
        )
        if acquired is not None and previous_acquired is not None and acquired != previous_acquired:
            return acquired > previous_acquired
        if slot is not None and previous_slot is None:
            return True
        if slot is None and previous_slot is not None:
            return False
        # At an identical head, retain the stronger complete tick bank. An old
        # fast task must not undo a full fallback fetched at the same instant.
        coverage, previous_coverage = bank.get("array_limit", 0), current.get("array_limit", 0)
        return type(coverage) is int and type(previous_coverage) is int and coverage > previous_coverage

    def _remember(self, key: BankKey, bank: dict) -> bool:
        stamp = self._stamp(bank, "observed")
        acquired = self._stamp(bank, "_acquired_monotonic")
        now = time.time()
        if stamp is None or stamp > now or acquired is not None and acquired > self.timer():
            return False
        current = self.ready.get(key)
        if current is bank or current is not None and not self._newer(bank, current):
            return False
        previous_stamp = self._stamp(current, "observed") if current is not None else None
        if (
            previous_stamp is not None
            and 0 <= now - previous_stamp < self.max_age
            and now - stamp >= self.max_age
        ):
            return False
        self.ready[key] = bank
        self.received_at[key] = acquired if acquired is not None else self.timer()
        return True

    def remember(self, key: BankKey, bank: dict) -> None:
        self._remember(key, bank)

    def publish(self, key: BankKey, bank: dict) -> bool:
        """Publish a complete validated fallback without cancelling older reads."""
        stamp = self._stamp(bank, "observed")
        if stamp is None or not 0 <= time.time() - stamp < self.max_age:
            return False
        accepted = self._remember(key, bank)
        if accepted:
            self.retry_at.pop(key, None)
            self.changed.set()
        return accepted

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
            and self.timer() - self.received_at.get(key, float("-inf")) < self.max_age / 2
        ):
            return bank
        previous = self.ready.get(key)

        async def acquire_bank():
            # A quote can win the half-TTL refresh race before refresh_loop.
            # Give that read the current bank ownership priority too; otherwise
            # a shared dependency remains trapped behind the caller's cold work.
            if self.primary is None:
                return await producer()
            token = REFRESHING.set(True)
            priority_token = REFRESH_PRIORITY.set(self.refresh_priority(key))
            try:
                return await producer()
            finally:
                REFRESH_PRIORITY.reset(priority_token)
                REFRESHING.reset(token)

        try:
            bank = await super().pool_snapshot(key, acquire_bank, max_age)
        except Exception:
            if self.ready.get(key) is previous:
                self.ready.pop(key, None)
            else:
                current = self.peek(key)
                if current is not None and time.time() - current["observed"] < max_age:
                    return current
            raise
        self.remember(key, bank)
        return self.ready.get(key, bank)

    async def accounts(self, addresses, *, parsed=False, min_slot=None, priority=0):
        # Current minimum routes precede other retained plans. Execution reads
        # at priority zero remain first. Cache keys keep their original priority.
        if REFRESHING.get():
            priority = min(priority, REFRESH_PRIORITY.get())
        return await super().accounts(addresses, parsed=parsed, min_slot=min_slot, priority=priority)

    def set_primary(self, keys: Iterable[BankKey] | Mapping[BankKey, int]) -> None:
        """Classify known route banks without changing their age or retention."""
        counts = keys if isinstance(keys, Mapping) else dict.fromkeys(keys, 1)
        self.primary_counts = {
            key: count
            for key, count in counts.items()
            if key in self.producers and type(count) is int and count > 0
        }
        self.primary = frozenset(self.primary_counts)

    def refresh_priority(self, key: BankKey) -> int:
        if self.primary is None:
            return 5
        owners = self.primary_counts.get(key, 0)
        return 3 if owners > 1 else 5 if owners == 1 else 10

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
                # Reserve half the unchanged TTL for the paced RPC round and
                # complete-bank validation. Polling every quarter-TTL doubled
                # that work and could expire the previous bank in its queue.
                if bank and elapsed - self.received_at.get(key, float("-inf")) < self.max_age / 2:
                    continue
                due.append(key)
            for key in sorted(
                due,
                key=lambda k: (self.refresh_priority(k), self.received_at.get(k, 0)),
            ):
                priority = self.refresh_priority(key)

                async def refresh(key=key, priority=priority):
                    token = REFRESHING.set(True)
                    priority_token = REFRESH_PRIORITY.set(priority)
                    previous = self.ready.get(key)
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
                        if self.ready.get(key) is previous:
                            self.ready.pop(key, None)
                            self.retry_at[key] = self.timer() + 5
                        event("pool_bank_refresh_blocked", pool=key[0], code=error_code(error))
                        raise
                    finally:
                        REFRESH_PRIORITY.reset(priority_token)
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
