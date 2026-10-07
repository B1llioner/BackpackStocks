import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_direct import fixture

from backpack_stocks import direct as direct_module
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.direct_routes import WSOL
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.raydium import CLMM
from backpack_stocks.transport import RemoteError


@pytest.fixture
async def discovery(monkeypatch):
    clock = SimpleNamespace(now=1000.0, elapsed=10.0)
    monkeypatch.setattr(
        direct_module,
        "time",
        SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.elapsed),
    )
    ray = SimpleNamespace(
        pools=AsyncMock(return_value=[]),
        close=AsyncMock(),
        http=SimpleNamespace(limiter=SimpleNamespace(blocked_until=0.0)),
    )
    constructor, proxy = Mock(return_value=ray), Mock(return_value=None)
    monkeypatch.setattr(direct_module, "Raydium", constructor)
    monkeypatch.setattr(direct_module, "resolve_proxy", proxy)
    rpc = SimpleNamespace(accounts=AsyncMock())
    router = DirectRaydium(Settings(dex_backend="raydium", direct={"via_sol": True, "max_pools": 2}), rpc)
    router.candidates = {}
    router.sol_candidates = {}
    try:
        yield router, clock, ray, constructor, proxy, rpc
    finally:
        await router.close()


async def test_successful_empty_lookup_is_cached_from_completion_for_three_hundred_seconds(discovery):
    router, clock, ray, constructor, _, _ = discovery

    async def empty(mint, quote_mint):
        clock.now += 4
        return []

    ray.pools.side_effect = empty
    await router.ensure_candidates(STOCK_MINT)
    assert router.discovery_checked[STOCK_MINT] == 1008
    assert ray.pools.await_args_list[0].args == (STOCK_MINT, USDC_MINT)
    assert ray.pools.await_args_list[1].args == (STOCK_MINT, WSOL)
    clock.now = 1307.999
    await router.ensure_candidates(STOCK_MINT)
    constructor.assert_called_once()
    clock.now = 1308
    await router.ensure_candidates(STOCK_MINT)
    assert constructor.call_count == 2
    assert router.discovery_checked[STOCK_MINT] == 1316
    assert STOCK_MINT not in router.discovery_retry_at
    assert STOCK_MINT not in router.discovery_failures


async def test_cancelled_api_lookup_has_short_retry_and_never_looks_completed(discovery):
    router, clock, ray, constructor, _, _ = discovery
    entered = asyncio.Event()

    async def waiting(*args):
        entered.set()
        await asyncio.Event().wait()

    ray.pools.side_effect = waiting
    task = asyncio.create_task(router.ensure_candidates(STOCK_MINT))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    ray.close.assert_awaited_once()
    assert STOCK_MINT not in router.discovery_checked
    assert router.discovery_retry_at[STOCK_MINT] == 1005
    await router.discovery_lock.acquire()
    try:
        clock.now = 1004
        await asyncio.wait_for(router.ensure_candidates(STOCK_MINT), 0.05)
        constructor.assert_called_once()
    finally:
        router.discovery_lock.release()
    clock.now = 1005
    ray.pools.side_effect = None
    await router.ensure_candidates(STOCK_MINT)
    assert router.discovery_checked[STOCK_MINT] == 1005
    assert STOCK_MINT not in router.discovery_retry_at
    assert STOCK_MINT not in router.discovery_failures


async def test_cancellation_before_the_discovery_lock_has_no_api_attempt_to_cache(discovery):
    router, _, _, constructor, _, _ = discovery
    await router.discovery_lock.acquire()
    try:
        task = asyncio.create_task(router.ensure_candidates(STOCK_MINT))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not router.discovery_checked and not router.discovery_retry_at
        constructor.assert_not_called()
    finally:
        router.discovery_lock.release()


async def test_transient_failures_have_bounded_backoff_instead_of_the_successful_lookup_cache(discovery):
    router, clock, ray, constructor, _, _ = discovery
    ray.pools.side_effect = ValueError("fixture discovery unavailable")
    for delay in (5, 10, 20, 40, 60, 60):
        with pytest.raises(ValueError, match="fixture discovery unavailable"):
            await router.ensure_candidates(STOCK_MINT)
        assert STOCK_MINT not in router.discovery_checked
        deadline = router.discovery_retry_at[STOCK_MINT]
        assert deadline == clock.now + delay
        calls = constructor.call_count
        clock.now = deadline - 0.001
        await router.ensure_candidates(STOCK_MINT)
        assert constructor.call_count == calls
        clock.now = deadline
    assert router.discovery_failures[STOCK_MINT] == 5
    assert ray.close.await_count == 6


async def test_429_short_circuits_other_family_and_preserves_long_provider_cooldown_across_mints(discovery):
    router, clock, ray, constructor, _, _ = discovery

    async def limited(*args):
        ray.http.limiter.blocked_until = clock.elapsed + 120
        raise RemoteError("API rejected request: HTTP 429", status_code=429)

    ray.pools.side_effect = limited
    with pytest.raises(RemoteError) as error:
        await router.ensure_candidates(STOCK_MINT, priority=10)
    assert error.value.status_code == 429
    ray.pools.assert_awaited_once_with(STOCK_MINT, USDC_MINT)
    ray.close.assert_awaited_once()
    assert router.discovery_retry_at[STOCK_MINT] == router.discovery_provider_retry_at == 1120
    assert STOCK_MINT not in router.discovery_checked
    clock.now, clock.elapsed = 1119.999, 129.999
    await router.ensure_candidates(STOCK_MINT)
    await router.ensure_candidates(WSOL)
    constructor.assert_called_once()
    clock.now, clock.elapsed = 1120, 130
    ray.pools.side_effect = None
    await router.ensure_candidates(STOCK_MINT)
    assert router.discovery_checked[STOCK_MINT] == 1120


@pytest.mark.parametrize("priority", [0, 10])
async def test_discovered_pool_account_verification_inherits_the_quote_priority(discovery, priority):
    router, _, ray, _, _, rpc = discovery
    saved = fixture()
    q = saved["quote"]
    final = saved["rpc_records"][-1]
    pool_account = final["result"]["value"][final["addresses"].index(q["pool"])]
    ray.pools.side_effect = [[{"id": q["pool"], "programId": CLMM}], []]
    rpc.accounts.return_value = {"context": {"slot": q["contextSlot"]}, "value": [pool_account]}
    await router.ensure_candidates(STOCK_MINT, priority=priority)
    rpc.accounts.assert_awaited_once_with([q["pool"]], priority=priority)
    assert router.candidates[STOCK_MINT] == [q["pool"]]
    assert router.pool_catalog[q["pool"]]["program"] == CLMM


async def test_failed_proxied_discovery_closes_client_and_never_falls_back_to_direct(discovery, caplog):
    router, clock, ray, constructor, proxy, rpc = discovery
    configured = "http://fixture-user:fixture-secret@proxy.example:8080"
    proxy.return_value = configured
    ray.pools.side_effect = RemoteError("Read request failed")
    with pytest.raises(RemoteError) as error:
        await router.ensure_candidates(STOCK_MINT)
    assert "fixture-secret" not in str(error.value) and "fixture-secret" not in caplog.text
    proxy.assert_called_once_with("RAYDIUM")
    constructor.assert_called_once_with(rpc, proxy=configured)
    ray.close.assert_awaited_once()
    await router.ensure_candidates(STOCK_MINT)
    constructor.assert_called_once()
    clock.now += 5
    ray.pools.side_effect = None
    await router.ensure_candidates(STOCK_MINT)
    assert all(call.kwargs["proxy"] == configured for call in constructor.call_args_list)


async def test_forced_refresh_prioritizes_current_verified_pools_over_filled_static_ranks(discovery, monkeypatch):
    router, _, ray, constructor, _, rpc = discovery
    router.candidates[STOCK_MINT] = ["old-usdc-1", "old-usdc-2"]
    router.sol_candidates[STOCK_MINT] = ["dead-sol-1", "dead-sol-2"]
    await router.ensure_candidates(STOCK_MINT)
    constructor.assert_not_called()
    current_clmm, current_cpmm = "current-sol-clmm", "current-sol-cpmm"
    from backpack_stocks.pool_state import CPMM

    ray.pools.side_effect = [
        [],
        [{"id": current_clmm, "programId": CLMM}, {"id": current_cpmm, "programId": CPMM}],
    ]
    rpc.accounts.return_value = {
        "value": [
            {"owner": CLMM, "state": {"mintA": STOCK_MINT, "mintB": WSOL}},
            {"owner": CPMM, "state": {"mintA": STOCK_MINT, "mintB": WSOL}},
        ]
    }
    monkeypatch.setattr(direct_module, "decode_pool", lambda account: account["state"])
    await router.ensure_candidates(STOCK_MINT, priority=10, refresh=True)
    rpc.accounts.assert_awaited_once_with([current_clmm, current_cpmm], priority=10)
    assert router.sol_candidates[STOCK_MINT] == [current_clmm, current_cpmm]
    assert router.candidates[STOCK_MINT] == ["old-usdc-1", "old-usdc-2"]
    assert len(router.sol_candidates[STOCK_MINT]) == router.settings.direct.max_pools == 2
    constructor.assert_called_once()


async def test_forced_refresh_keeps_old_refs_only_after_current_verified_refs(discovery, monkeypatch):
    router, _, ray, _, _, rpc = discovery
    router.candidates[STOCK_MINT] = ["old-usdc-1", "old-usdc-2"]
    router.sol_candidates[STOCK_MINT] = ["old-sol-1", "old-sol-2"]
    old_sol = copy.copy(router.sol_candidates[STOCK_MINT])
    ray.pools.side_effect = [
        [{"id": "invalid-current", "programId": CLMM}, {"id": "verified-current", "programId": CLMM}],
        [],
    ]
    rpc.accounts.return_value = {
        "value": [
            None,
            {"owner": CLMM, "state": {"mintA": STOCK_MINT, "mintB": USDC_MINT}},
        ]
    }
    monkeypatch.setattr(direct_module, "decode_pool", lambda account: account["state"])
    await router.ensure_candidates(STOCK_MINT, refresh=True)
    assert router.candidates[STOCK_MINT] == ["verified-current", "old-usdc-1"]
    assert router.sol_candidates[STOCK_MINT] == old_sol


async def test_forced_refresh_preserves_completion_and_provider_cooldowns(discovery):
    router, clock, ray, constructor, _, _ = discovery
    router.candidates[STOCK_MINT] = ["old-usdc-1", "old-usdc-2"]
    router.sol_candidates[STOCK_MINT] = ["old-sol-1", "old-sol-2"]
    router.discovery_checked[STOCK_MINT] = clock.now
    await router.ensure_candidates(STOCK_MINT, refresh=True)
    constructor.assert_not_called()
    clock.now += 300
    await router.ensure_candidates(STOCK_MINT, refresh=True)
    constructor.assert_called_once()
    assert ray.pools.await_count == 2
    router.discovery_checked.clear()
    router.discovery_provider_retry_at = clock.now + 60
    await router.ensure_candidates(STOCK_MINT, refresh=True)
    constructor.assert_called_once()
