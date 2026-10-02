import copy
import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from test_direct import ReplayRPC, fixture
from test_monitor import monitor as monitor_fixture

from backpack_stocks.clmm_worker import ClmmCalculationError, ClmmWorker
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import STOCK_MINT, USDC_MINT, Settings
from backpack_stocks.monitor import FORWARD
from backpack_stocks.observability import QuoteUnavailable

monitor = pytest.fixture(monitor_fixture.__wrapped__)
FIXTURES = Path(__file__).parent / "fixtures" / "clmm_coverage.json"


@pytest.mark.parametrize("symbol", ["BA.US", "AMD.US"])
async def test_exhausted_mainnet_liquidity_is_not_a_tick_loading_error(symbol):
    body = json.loads(FIXTURES.read_text())[symbol]
    worker = ClmmWorker()
    try:
        with pytest.raises(ClmmCalculationError) as rejected:
            await worker.call(body)
        assert rejected.value.code == "insufficient_liquidity"
        # Exactly the same bank can fill a small amount, so math is operational.
        quote = await worker.call({**body, "amount": "1000000"})
        assert int(quote["output"]) > 0
    finally:
        await worker.close()


async def test_actual_directional_array_cap_is_distinct_from_exhausted_liquidity():
    body = json.loads(FIXTURES.read_text())["AMD.US"]
    body.update(maxArrays=1, arrays=body["arrays"][:1])
    worker = ClmmWorker()
    try:
        with pytest.raises(ClmmCalculationError) as rejected:
            await worker.call(body)
        assert rejected.value.code == "insufficient_tick_coverage"
    finally:
        await worker.close()


async def test_missing_expected_array_is_an_input_error_not_an_illiquid_market():
    body = json.loads(FIXTURES.read_text())["HTZ.US"]
    body["arrays"] = []
    worker = ClmmWorker()
    try:
        with pytest.raises(ClmmCalculationError) as rejected:
            await worker.call(body)
        assert rejected.value.code == "clmm_input_mismatch"
        assert not QuoteUnavailable([rejected.value.code]).expected
    finally:
        await worker.close()


async def test_direct_quote_carries_only_used_arrays_and_monotonic_read_slot(monkeypatch):
    saved = fixture()
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: saved["quote"]["_observed_at"])
    records = copy.deepcopy(saved["rpc_records"])
    records[0]["result"]["context"]["slot"] = 100
    records[1]["result"]["context"]["slot"] = 200
    records[2]["result"]["context"]["slot"] = 200

    class CheckedRPC(ReplayRPC):
        def __init__(self):
            super().__init__(records)
            self.minimum_slots = []

        async def accounts(self, addresses, **kwargs):
            self.minimum_slots.append(kwargs.get("min_slot"))
            return await super().accounts(addresses, **kwargs)

    rpc = CheckedRPC()
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    actual_call = router.worker.call
    swap_arrays = []

    async def call(body):
        result = await actual_call(body)
        if body["op"] == "quote":
            swap_arrays.extend(result["accounts"])
        return result

    monkeypatch.setattr(router.worker, "call", call)
    try:
        quote = await router.pool_quote(saved["quote"]["pool"], STOCK_MINT, USDC_MINT, 1_000_000)
        assert quote["_direct"]["arrays"] == swap_arrays
        assert rpc.minimum_slots == [None, 100, 200]
    finally:
        await router.close()


async def test_monitor_retains_route_causes_and_clears_them_after_recovery(monitor):
    mon, _ = monitor
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["insufficient_liquidity", "no_tick_liquidity"])
    await mon.quote(("WEN.US", FORWARD))
    snapshot = mon.snapshot()
    row = next(r for r in snapshot["rows"] if r["direction"] == FORWARD)
    assert row["error_causes"] == ["insufficient_liquidity", "no_tick_liquidity"]
    assert snapshot["failed_directions"] == 1
    assert snapshot["route_failure_causes"] == {"insufficient_liquidity": 1, "no_tick_liquidity": 1}
    mon.jupiter.quote_pair.side_effect = None
    mon.jupiter.quote_pair.return_value = {"outAmount": "104495958", "priceImpact": "0.1"}
    await mon.quote(("WEN.US", FORWARD))
    snapshot = mon.snapshot()
    assert snapshot["failed_directions"] == 0 and snapshot["route_failure_causes"] == {}
    assert snapshot["blocked"] == {"route_unavailable": 1}  # Cumulative attempts stay distinct.


async def test_rfq_precheck_replaces_previous_route_failure_causes(monitor):
    mon, clock = monitor
    mon.jupiter.quote_pair.side_effect = QuoteUnavailable(["insufficient_liquidity"])
    await mon.quote(("WEN.US", FORWARD))
    mon.assets["WEN.US"] = replace(mon.assets["WEN.US"], sessions=())
    clock.now += mon.settings.monitor.quote_refresh_s
    mon.backpack.stock_feed.latest["WEN"].timestamp = clock()
    assert mon.choose() is None
    row = next(r for r in mon.snapshot()["rows"] if r["direction"] == FORWARD)
    assert row["error_causes"] == ["rfq_session_unavailable"]


@pytest.mark.parametrize("bad_accounts", [[], [USDC_MINT], [USDC_MINT, STOCK_MINT]])
async def test_direct_rejects_missing_or_unvalidated_worker_swap_accounts(monkeypatch, bad_accounts):
    saved = fixture()
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: saved["quote"]["_observed_at"])
    router = DirectRaydium(Settings(dex_backend="raydium"), ReplayRPC(saved["rpc_records"]))
    arrays = saved["quote"]["_direct"]["arrays"]
    router.worker.call = AsyncMock(
        side_effect=[{"addresses": arrays}, {"addresses": arrays}, {"accounts": bad_accounts}]
    )
    try:
        with pytest.raises(ValueError, match="invalid swap account coverage"):
            await router.pool_quote(saved["quote"]["pool"], STOCK_MINT, USDC_MINT, 1_000_000)
    finally:
        await router.close()
