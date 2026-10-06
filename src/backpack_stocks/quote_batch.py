"""Coalesce concurrent read-only pool reads without caching account state."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .solana import SolanaRPC


@dataclass
class AccountRequest:
    addresses: list[str]
    parsed: bool
    min_slot: int | None
    priority: int
    future: asyncio.Future


class QuoteAccountBatch:
    """Each pool's entire account group stays in one RPC bank, at most 100 addresses."""

    def __init__(self, rpc: SolanaRPC | QuoteAccountBatch):
        self.rpc = rpc
        self.waiting: list[AccountRequest] = []
        self.tasks: set[asyncio.Task] = set()
        self.scheduled = False
        self.pool_banks: dict[tuple[str, str, str, int], asyncio.Task] = {}

    async def pool_snapshot(
        self, key: tuple[str, str, str, int], producer: Callable[[], Awaitable[dict]], max_age: float
    ) -> dict:
        """Reuse one validated bank for a request's size grid, preserving its age."""
        task = self.pool_banks.get(key)
        if task is not None and task.done():
            if task.cancelled() or task.exception() is not None:
                task = None
            elif not 0 <= time.time() - task.result()["observed"] < max_age / 2:
                task = None
        if task is None:

            async def produce():
                return await producer()

            task = asyncio.create_task(produce())
            self.pool_banks[key] = task
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        return await asyncio.shield(task)

    async def accounts(
        self, addresses: list[str], *, parsed: bool = False, min_slot: int | None = None, priority: int = 0
    ) -> dict:
        if not 0 < len(addresses) <= 100:
            raise ValueError("Invalid pool account group size")
        future = asyncio.get_running_loop().create_future()
        self.waiting.append(AccountRequest(list(addresses), parsed, min_slot, priority, future))
        if not self.scheduled:
            self.scheduled = True
            task = asyncio.create_task(self.flush())
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        return await future

    async def flush(self) -> None:
        # Let independent pools reach the same read stage, including local tick discovery.
        await asyncio.sleep(0.01)
        requests, self.waiting = self.waiting, []
        self.scheduled = False
        groups: list[list[AccountRequest]] = []
        for request in requests:
            if request.future.done():
                continue
            group = groups[-1] if groups else []
            refs = {a for r in group for a in r.addresses}
            if group and (
                request.parsed != group[0].parsed
                or request.priority != group[0].priority
                or len(refs | set(request.addresses)) > 100
            ):
                group = []
            if not group:
                groups.append(group)
            group.append(request)
        for group in groups:
            addresses = list(dict.fromkeys(a for r in group for a in r.addresses))
            slots = [r.min_slot for r in group if r.min_slot is not None]
            try:
                response = await self.rpc.accounts(
                    addresses,
                    parsed=group[0].parsed,
                    min_slot=max(slots) if slots else None,
                    priority=group[0].priority,
                )
                accounts = dict(zip(addresses, response["value"], strict=True))
                if slots and response["context"]["slot"] < max(slots):
                    raise ValueError("RPC bank below minimum context slot")
                for request in group:
                    if not request.future.done():
                        request.future.set_result(
                            {
                                "context": response["context"],
                                "value": [accounts[a] for a in request.addresses],
                            }
                        )
            except Exception as error:
                for request in group:
                    if not request.future.done():
                        request.future.set_exception(error)
            except asyncio.CancelledError:
                for remaining in groups:
                    for request in remaining:
                        request.future.cancel()
                raise

    async def close(self) -> None:
        self.pool_banks.clear()
        for request in self.waiting:
            request.future.cancel()
        self.waiting.clear()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
