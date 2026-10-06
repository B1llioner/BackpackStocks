"""Share account-wide RFQ reads without reusing prices from before a submission."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .backpack import Backpack
from .transport import RemoteError


class RfqQueryBatch:
    def __init__(
        self,
        backpack: Backpack,
        *,
        clock: Callable[[], float] = time.time,
        timer: Callable[[], float] = time.monotonic,
        reuse_s: float = 0.15,
        coalesce_s: float = 0.03,
    ):
        self.backpack, self.clock, self.timer = backpack, clock, timer
        self.reuse_s, self.coalesce_s = reuse_s, coalesce_s
        self.generation = 0
        self.query_generation = 0
        self.completed_at = float("-inf")
        self.task: asyncio.Task[tuple[int, list[dict], float]] | None = None
        self.global_supported = True

    def register(self) -> int:
        """Call after a validated RFQ submission, before its first price read."""
        self.generation += 1
        return self.generation

    async def read_all(self) -> tuple[int, list[dict], float]:
        try:
            await asyncio.sleep(self.coalesce_s)
            generation = self.query_generation = self.generation
            records = await self.backpack.rfqs(symbol=None)
            return generation, records, self.clock()
        except RemoteError as error:
            if error.status_code == 400:
                # Older deployments may require filters. Never retry the
                # unsupported account-wide query for every concurrent stock.
                self.global_supported = False
            raise
        finally:
            self.completed_at = self.timer()

    async def query(self, rfq_id: str, symbol: str, registered: int) -> tuple[list[dict], float]:
        while True:
            if not self.global_supported:
                records = await self.backpack.rfqs(rfq_id, symbol=symbol)
                return records, self.clock()
            task = self.task
            if task is None or task.done() and (
                self.timer() - self.completed_at >= self.reuse_s
                or task.exception() is None and self.query_generation < registered
            ):
                task = self.task = asyncio.create_task(self.read_all())
                # One expired inspector must not cancel the read shared by other
                # stocks. Consume errors if every inspector times out first.
                task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            try:
                generation, records, observed_at = await asyncio.shield(task)
            except RemoteError as error:
                if error.status_code == 400 and not self.global_supported:
                    continue
                raise
            if generation >= registered:
                return records, observed_at
            # The RFQ was submitted while that request was already in flight.
            # Wait for a new batch rather than treating its absence as evidence.
