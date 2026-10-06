"""Persistent official CLMM math worker with public inputs and a sanitized environment."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path


class ClmmCalculationError(ValueError):
    def __init__(self, code: str):
        self.code = (
            code
            if code
            in {
                "insufficient_tick_coverage",
                "insufficient_liquidity",
                "clmm_math_error",
                "clmm_input_mismatch",
            }
            else "clmm_worker_protocol_error"
        )
        super().__init__("Local CLMM quote failed or lacks complete tick coverage")


class ClmmWorker:
    def __init__(self, path: str = "local_clmm/worker.cjs"):
        self.path = Path(path).resolve()
        self.process: asyncio.subprocess.Process | None = None
        self.lock = asyncio.Lock()
        self.sequence = 0

    async def close(self) -> None:
        if self.process is not None:
            p, self.process = self.process, None
            if p.returncode is None:
                p.kill()
            await p.wait()

    async def call(self, body: dict) -> dict:
        async with self.lock:
            if self.process is None or self.process.returncode is not None:
                node = shutil.which("node")
                if node is None or not self.path.is_file():
                    raise ValueError("Install local_clmm dependencies and Node before using CLMM")
                self.process = await asyncio.create_subprocess_exec(
                    node,
                    str(self.path),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    limit=2_000_001,
                    env={"PATH": os.environ.get("PATH", ""), "NODE_NO_WARNINGS": "1"},
                )
            p = self.process
            assert p.stdin and p.stdout
            self.sequence += 1
            payload = json.dumps({**body, "id": self.sequence}).encode() + b"\n"
            if len(payload) > 2_000_000:
                raise ValueError("CLMM worker input exceeds bound")
            try:
                async with asyncio.timeout(10):
                    p.stdin.write(payload)
                    await p.stdin.drain()
                    line = await p.stdout.readline()
                    result = json.loads(line)
                    if result.get("id") != self.sequence:
                        raise ValueError("Local CLMM quote failed or lacks complete tick coverage")
                    if "error" in result:
                        raise ClmmCalculationError(result.get("code", "clmm_worker_protocol_error"))
                    return result["result"]
            except ClmmCalculationError:
                # A matched complete response was consumed; market rejection doesn't desync the pipe.
                raise
            except BaseException:
                # Cancelled calls cannot leave unread replies for the next caller.
                await self.close()
                raise


class ClmmWorkerPool:
    """Bounded independent SDK processes; only pure local math runs in parallel."""

    def __init__(self, path: str = "local_clmm/worker.cjs", *, workers: int = 4):
        if type(workers) is not int or not 1 <= workers <= 4:
            raise ValueError("CLMM worker count must be an integer from one to four")
        self.workers = [ClmmWorker(path) for _ in range(workers)]
        self.pending = [0] * workers
        self.cursor = 0
        self.calls = 0
        self.tasks: set[asyncio.Task] = set()
        self.closed = False

    @property
    def process(self) -> asyncio.subprocess.Process | None:
        # Existing callers inspect whether any local math process has started.
        return next((worker.process for worker in self.workers if worker.process is not None), None)

    async def call(self, body: dict) -> dict:
        if self.closed:
            raise ValueError("CLMM worker pool is closed")
        count = len(self.workers)
        index = min(range(count), key=lambda i: (self.pending[i], (i - self.cursor) % count))
        self.cursor = (index + 1) % count
        self.pending[index] += 1
        self.calls += 1
        task = asyncio.current_task()
        if task is not None:
            self.tasks.add(task)
        try:
            # Each worker retains its own lock, response sequence and restart
            # behavior, so an error/cancellation cannot consume another reply.
            return await self.workers[index].call(body)
        finally:
            self.pending[index] -= 1
            if task is not None:
                self.tasks.discard(task)

    def status(self) -> dict:
        return {
            "workers": len(self.workers),
            "running": sum(
                worker.process is not None and worker.process.returncode is None for worker in self.workers
            ),
            "pending": sum(self.pending),
            "pending_by_worker": list(self.pending),
            "calls": self.calls,
        }

    async def close(self) -> None:
        self.closed = True
        current = asyncio.current_task()
        tasks = [task for task in self.tasks if task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(worker.close() for worker in self.workers))
