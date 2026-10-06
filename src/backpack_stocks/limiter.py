from __future__ import annotations

import asyncio
import heapq
import itertools
import time


class PriorityLimiter:
    """Pace request starts; execution requests precede queued background monitoring reads."""

    def __init__(
        self, interval: float, *, categories: dict[str, float] | None = None, oldest_every: int | None = None
    ):
        self.interval, self.last = interval, 0.0
        self.base_interval = interval
        self.adaptive_backoffs = 0
        self.categories = categories or {}
        self.oldest_every = oldest_every
        self.category_last: dict[str, float] = {}
        self.blocked_until = 0.0
        self.category_blocked: dict[str, float] = {}
        self.queue: list[tuple[int, int, asyncio.Future, str | None]] = []
        self.sequence = itertools.count()
        self.task: asyncio.Task | None = None
        self.admitted = 0
        self.responses_429 = 0
        self.cooldowns = 0
        self.wait_s = 0.0

    async def acquire(self, priority: int = 0, category: str | None = None) -> None:
        started = time.monotonic()
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self.queue, (priority, next(self.sequence), future, category))
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.dispatch())
        await future
        self.wait_s += time.monotonic() - started

    def defer(self, delay: float, category: str | None = None) -> None:
        deadline = time.monotonic() + max(0, delay)
        if category is None:
            self.blocked_until = max(self.blocked_until, deadline)
        else:
            self.category_blocked[category] = max(self.category_blocked.get(category, 0), deadline)
        self.cooldowns += 1

    def configure(self, interval: float) -> None:
        self.base_interval = self.interval = interval

    def slow_down(self) -> None:
        self.interval = max(self.interval, min(30, max(0.25, self.interval * 1.5)))
        self.adaptive_backoffs += 1

    def ready_at(self, category: str | None) -> float:
        return max(
            self.last + self.interval,
            self.blocked_until,
            self.category_blocked.get(category or "", 0),
            self.category_last.get(category or "", 0) + self.categories.get(category or "", 0),
        )

    async def dispatch(self) -> None:
        while self.queue:
            self.queue = [item for item in self.queue if not item[2].cancelled()]
            heapq.heapify(self.queue)
            if not self.queue:
                break
            now = time.monotonic()
            ready = [item for item in self.queue if self.ready_at(item[3]) <= now]
            if not ready:
                delay = min(self.ready_at(item[3]) - now for item in self.queue)
                await asyncio.sleep(min(1, max(0, delay)))
                continue
            item = min(ready)
            if (
                self.oldest_every
                and self.admitted % self.oldest_every == self.oldest_every - 1
                and item[0] > 0
            ):
                # Preserve execution priority zero, but reserve bounded turns
                # for older monitoring/history requests under sustained load.
                item = min(ready, key=lambda queued: queued[1])
            self.queue.remove(item)
            heapq.heapify(self.queue)
            _, _, future, category = item
            self.last = now
            if category is not None:
                self.category_last[category] = now
            self.admitted += 1
            future.set_result(None)

    async def close(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        for _, _, future, _ in self.queue:
            future.cancel()
        self.queue.clear()

    def status(self) -> dict:
        return {
            "interval_s": self.interval,
            "base_interval_s": self.base_interval,
            "oldest_every": self.oldest_every,
            "adaptive_backoffs": self.adaptive_backoffs,
            "categories_s": self.categories,
            "cooldown_s": max(0, self.blocked_until - time.monotonic()),
            "category_cooldown_s": {
                k: max(0, v - time.monotonic()) for k, v in self.category_blocked.items()
            },
            "admitted": self.admitted,
            "responses_429": self.responses_429,
            "queued": len(self.queue),
            "wait_s": self.wait_s,
            "cooldowns": self.cooldowns,
        }
