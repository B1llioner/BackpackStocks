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
        self.adaptive_recoveries = 0
        self._rate_limit_until = 0.0
        self._recovery_at = 0.0
        self._successful_responses = 0
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
        self.capacity_waiters: set[asyncio.Future] = set()

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
        self._successful_responses = 0

    def record_failure(self) -> None:
        self._recovery_at = time.monotonic()
        self._successful_responses = 0

    def slow_down(self) -> None:
        self.record_failure()
        self.interval = max(self.interval, min(30, max(0.25, self.interval * 1.5)))
        self.adaptive_backoffs += 1

    def rate_limit_backoff(self, delay: float) -> None:
        """One slowdown per cooldown episode; every response extends its wait."""
        now = time.monotonic()
        if now < self._rate_limit_until:
            self.record_failure()
        else:
            self.slow_down()
        self.defer(delay)
        self._rate_limit_until = max(self._rate_limit_until, self.blocked_until, now + self.interval)

    def record_success(self) -> None:
        """Recover gradually after a quiet window, without relaxing server budgets."""
        if self.interval <= self.base_interval:
            return
        now = time.monotonic()
        if now < max(self.blocked_until, max(self.category_blocked.values(), default=0)):
            return
        self._successful_responses = min(8, self._successful_responses + 1)
        if now - self._recovery_at < 30 or self._successful_responses < 8:
            return
        self.interval = max(self.base_interval, self.interval / 1.5)
        self.adaptive_recoveries += 1
        self._successful_responses = 0
        self._recovery_at = now

    def ready_at(self, category: str | None) -> float:
        return max(
            self.last + self.interval,
            self.blocked_until,
            self.category_blocked.get(category or "", 0),
            self.category_last.get(category or "", 0) + self.categories.get(category or "", 0),
        )

    def admit(self, now: float, category: str | None) -> None:
        self.last = now
        if category is not None:
            self.category_last[category] = now
        self.admitted += 1

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
            self.admit(now, category)
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
        for future in self.capacity_waiters:
            future.cancel()

    def status(self) -> dict:
        return {
            "interval_s": self.interval,
            "base_interval_s": self.base_interval,
            "oldest_every": self.oldest_every,
            "adaptive_backoffs": self.adaptive_backoffs,
            "adaptive_recoveries": self.adaptive_recoveries,
            "categories_s": self.categories,
            "cooldown_s": max(0, self.blocked_until - time.monotonic()),
            "category_cooldown_s": {
                k: max(0, v - time.monotonic()) for k, v in self.category_blocked.items()
            },
            "admitted": self.admitted,
            "responses_429": self.responses_429,
            "queued": len(self.queue) + len(self.capacity_waiters),
            "wait_s": self.wait_s,
            "cooldowns": self.cooldowns,
        }


class RequestStart:
    """A reserved connection whose pacing timestamp is committed at HTTP start."""

    def __init__(self, scheduler: RequestScheduler, limiter: PriorityLimiter, category: str | None):
        self.scheduler, self.limiter, self.category = scheduler, limiter, category
        self.future = asyncio.get_running_loop().create_future()
        self.started = asyncio.Event()
        self.reserved = False
        self.queued_at = time.monotonic()

    async def start(self) -> None:
        # A server cooldown can arrive while before_attempt is suspended.
        # Keep that deadline and pace from the actual start, not the reservation.
        while not self.scheduler.closed:
            now = time.monotonic()
            delay = self.limiter.ready_at(self.category) - now
            if delay <= 0:
                self.limiter.admit(now, self.category)
                self.limiter.wait_s += now - self.queued_at
                self.started.set()
                return
            await asyncio.sleep(min(1, delay))
        raise asyncio.CancelledError

    def release(self) -> None:
        if self.reserved:
            self.reserved = False
            self.scheduler.capacity.release()
        self.started.set()
        self.scheduler.changed.set()


class RequestScheduler:
    """Select priority and rate budget together, before reserving a connection."""

    def __init__(self, capacity: asyncio.Semaphore):
        self.capacity = capacity
        self.queue: list[tuple[int, int, RequestStart]] = []
        self.sequence = itertools.count()
        self.changed = asyncio.Event()
        self.task: asyncio.Task | None = None
        self.closed = False

    async def acquire(
        self, limiter: PriorityLimiter, priority: int = 0, category: str | None = None
    ) -> RequestStart:
        if self.closed:
            raise asyncio.CancelledError
        request = RequestStart(self, limiter, category)
        limiter.capacity_waiters.add(request.future)
        heapq.heappush(self.queue, (priority, next(self.sequence), request))
        self.changed.set()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.dispatch())
        try:
            await request.future
            return request
        except asyncio.CancelledError:
            limiter.capacity_waiters.discard(request.future)
            request.release()
            raise

    async def dispatch(self) -> None:
        while self.queue:
            for _, _, request in self.queue:
                if request.future.cancelled():
                    request.limiter.capacity_waiters.discard(request.future)
            self.queue = [item for item in self.queue if not item[2].future.cancelled()]
            heapq.heapify(self.queue)
            if not self.queue:
                break
            if self.capacity.locked():
                self.changed.clear()
                await self.changed.wait()
                continue
            now = time.monotonic()
            ready = [item for item in self.queue if item[2].limiter.ready_at(item[2].category) <= now]
            if not ready:
                delay = min(item[2].limiter.ready_at(item[2].category) - now for item in self.queue)
                await asyncio.sleep(min(1, max(0, delay)))
                continue
            item = min(ready)
            limiter = item[2].limiter
            if limiter.oldest_every and limiter.admitted % limiter.oldest_every == limiter.oldest_every - 1:
                if item[0] > 0:
                    item = min(
                        (entry for entry in ready if entry[2].limiter is limiter), key=lambda entry: entry[1]
                    )
            self.queue.remove(item)
            heapq.heapify(self.queue)
            request = item[2]
            request.limiter.capacity_waiters.discard(request.future)
            await self.capacity.acquire()
            request.reserved = True
            request.future.set_result(None)
            # Do not accumulate paced permits behind an asynchronous callback.
            # The next permit is paced from this caller's actual HTTP start.
            await request.started.wait()

    async def close(self) -> None:
        self.closed = True
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        for _, _, request in self.queue:
            request.limiter.capacity_waiters.discard(request.future)
            request.future.cancel()
        self.queue.clear()
