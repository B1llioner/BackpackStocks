import asyncio
import json
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.clmm_worker import ClmmCalculationError, ClmmWorkerPool


class MathProcess:
    def __init__(self, backend, index):
        self.backend, self.index = backend, index
        self.returncode = None
        self.requests = deque()
        self.gate = asyncio.Event()
        self.gate.set()
        self.entered = asyncio.Event()
        self.killed = False
        self.stdin = SimpleNamespace(write=self.write, drain=self.drain)
        self.stdout = SimpleNamespace(readline=self.readline)

    def write(self, payload):
        self.requests.append(json.loads(payload))

    async def drain(self):
        await asyncio.sleep(0)

    async def readline(self):
        request = self.requests.popleft()
        self.backend.active += 1
        self.backend.maximum = max(self.backend.maximum, self.backend.active)
        self.entered.set()
        if self.backend.active == 4:
            self.backend.four_started.set()
        try:
            await self.gate.wait()
            mode = self.backend.failures.pop(self.index, None)
            if mode == "wrong_id":
                response = {"id": request["id"] + 1000, "result": {}}
            elif mode == "market":
                response = {"id": request["id"], "error": "rejected", "code": "insufficient_liquidity"}
            else:
                response = {
                    "id": request["id"],
                    "result": {
                        "processor": self.index,
                        "output": str(int(request["amount"]) * 7),
                        "response_id": request["id"],
                    },
                }
            return json.dumps(response).encode() + b"\n"
        finally:
            self.backend.active -= 1

    def kill(self):
        self.killed = True
        self.returncode = -9
        self.gate.set()

    async def wait(self):
        return self.returncode


@pytest.fixture
def sdk_processes(monkeypatch, tmp_path):
    script = tmp_path / "worker.cjs"
    script.write_text("// Test fake: no process or network is started.\n")
    backend = SimpleNamespace(
        processes=[], active=0, maximum=0, four_started=asyncio.Event(), failures={}, blocked=False
    )

    async def start(*args, **kwargs):
        process = MathProcess(backend, len(backend.processes))
        if backend.blocked:
            process.gate.clear()
        backend.processes.append(process)
        return process

    backend.spawn = AsyncMock(side_effect=start)
    monkeypatch.setattr("backpack_stocks.clmm_worker.shutil.which", lambda name: "/test/node")
    monkeypatch.setattr("backpack_stocks.clmm_worker.asyncio.create_subprocess_exec", backend.spawn)
    return str(script), backend


@pytest.mark.parametrize("count", [0, 5, True, 1.0, "4"])
def test_worker_count_rejects_unbounded_or_noninteger_configuration(count):
    with pytest.raises(ValueError, match="one to four"):
        ClmmWorkerPool(workers=count)


@pytest.mark.parametrize("identical", [False, True])
async def test_parallel_sdk_calls_keep_response_mapping_and_never_exceed_four_workers(
    sdk_processes, identical
):
    path, backend = sdk_processes
    backend.blocked = True
    pool = ClmmWorkerPool(path)
    amounts = [37 if identical else index + 1 for index in range(12)]
    calls = [asyncio.create_task(pool.call({"op": "quote", "amount": str(amount)})) for amount in amounts]
    try:
        await asyncio.wait_for(backend.four_started.wait(), 1)
        assert len(backend.processes) == backend.maximum == 4
        assert pool.status()["pending_by_worker"] == [3, 3, 3, 3]
        assert pool.status()["running"] == 4
        for process in backend.processes:
            process.gate.set()
        results = await asyncio.gather(*calls)
        assert [result["output"] for result in results] == [str(amount * 7) for amount in amounts]
        assert [result["processor"] for result in results] == [0, 1, 2, 3] * 3
        assert [result["response_id"] for result in results] == [1] * 4 + [2] * 4 + [3] * 4
        assert pool.status()["pending"] == 0
        assert len(backend.processes) == backend.maximum == 4
    finally:
        await pool.close()
    assert all(process.killed for process in backend.processes)


async def test_least_pending_worker_precedes_round_robin_tie(sdk_processes):
    path, backend = sdk_processes
    pool = ClmmWorkerPool(path, workers=2)
    first = asyncio.create_task(pool.call({"amount": "10"}))
    await asyncio.sleep(0)
    # Worker zero stays busy while worker one finishes and becomes least loaded.
    backend.processes[0].gate.clear()
    try:
        second = await pool.call({"amount": "20"})
        third = await pool.call({"amount": "30"})
        assert second["processor"] == third["processor"] == 1
        backend.processes[0].gate.set()
        assert (await first)["processor"] == 0
    finally:
        await pool.close()


@pytest.mark.parametrize("failure", ["cancel", "wrong_id", "market"])
async def test_one_worker_failure_or_cancellation_cannot_consume_other_workers_reply(sdk_processes, failure):
    path, backend = sdk_processes
    backend.blocked = True
    pool = ClmmWorkerPool(path, workers=2)
    first = asyncio.create_task(pool.call({"amount": "10"}))
    second = asyncio.create_task(pool.call({"amount": "20"}))
    await asyncio.sleep(0)
    await asyncio.gather(*(process.entered.wait() for process in backend.processes))
    try:
        if failure == "cancel":
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            backend.failures[0] = failure
            backend.processes[0].gate.set()
            expected = ClmmCalculationError if failure == "market" else ValueError
            with pytest.raises(expected):
                await first
        backend.processes[1].gate.set()
        assert await second == {"processor": 1, "output": "140", "response_id": 1}
        assert not backend.processes[1].killed
        assert backend.processes[0].killed is (failure != "market")
        backend.blocked = False
        third = await pool.call({"amount": "30"})
        assert third["output"] == "210" and third["response_id"] == 2
        assert third["processor"] == (0 if failure == "market" else 2)
        assert pool.status()["pending"] == 0
    finally:
        await pool.close()


async def test_close_cancels_pending_local_calls_and_releases_every_process(sdk_processes):
    path, backend = sdk_processes
    backend.blocked = True
    pool = ClmmWorkerPool(path)
    calls = [asyncio.create_task(pool.call({"amount": str(index + 1)})) for index in range(8)]
    await asyncio.wait_for(backend.four_started.wait(), 1)
    await asyncio.wait_for(pool.close(), 1)
    assert all(call.cancelled() for call in calls)
    assert all(process.killed for process in backend.processes)
    assert pool.status()["pending"] == pool.status()["running"] == 0
    assert pool.process is None
    await pool.close()
    with pytest.raises(ValueError, match="pool is closed"):
        await pool.call({"amount": "10"})


async def test_each_worker_preserves_sanitized_environment_and_pipe_bounds(sdk_processes, monkeypatch):
    path, backend = sdk_processes
    monkeypatch.setenv("CLMM_TEST_PRIVATE_TOKEN", "not-forwarded")
    pool = ClmmWorkerPool(path)
    try:
        await asyncio.gather(*(pool.call({"amount": "1"}) for _ in range(4)))
        assert backend.spawn.await_count == 4
        for call in backend.spawn.await_args_list:
            assert call.args == ("/test/node", path)
            assert set(call.kwargs["env"]) == {"PATH", "NODE_NO_WARNINGS"}
            assert call.kwargs["env"]["NODE_NO_WARNINGS"] == "1"
            assert call.kwargs["stdin"] == call.kwargs["stdout"] == asyncio.subprocess.PIPE
            assert call.kwargs["stderr"] == asyncio.subprocess.DEVNULL
            assert call.kwargs["limit"] == 2_000_001
    finally:
        await pool.close()
