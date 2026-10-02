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
