import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_monitor import Clock

from backpack_stocks.backpack import Backpack
from backpack_stocks.rfq_query_batch import RfqQueryBatch
from backpack_stocks.rfq_validation import RfqValidator
from backpack_stocks.transport import RemoteError


def account(clock):
    bp = object.__new__(Backpack)
    bp.auth = object()
    requests = []

    async def submit(quantity, client_id, *, side, symbol):
        request = {
            "rfqId": str(len(requests) + 1),
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "clientId": client_id,
            "executionMode": "AwaitAccept",
            "expiryTime": int((clock() + 2.5) * 1000),
            "status": "New",
        }
        requests.append(request)
        return request

    async def records(rfq_id=None, *, symbol=None):
        return [
            {
                "rfq": dict(request),
                "quotes": [
                    {"rfqId": request["rfqId"], "status": "New", "askPrice": str(10 + index),
                     "bidPrice": str(9 + index)},
                    {"rfqId": "foreign", "status": "New", "askPrice": ".01", "bidPrice": "999"},
                    {"rfqId": request["rfqId"], "status": "Expired", "askPrice": ".01", "bidPrice": "999"},
                ],
            }
            for index, request in enumerate(requests)
            if rfq_id is None or request["rfqId"] == rfq_id
        ]

    bp.submit_rfq = AsyncMock(side_effect=submit)
    bp.rfqs = AsyncMock(side_effect=records)
    bp.cancel_rfq = AsyncMock()
    bp.accept_rfq = AsyncMock()
    validator = RfqValidator(bp, max_age=8, retry_s=120, timeout=5, concurrency=8, clock=clock)
    return bp, validator


async def test_eight_inspectors_share_one_query_without_cross_request_prices():
    clock = Clock()
    bp, validator = account(clock)
    checks = await asyncio.gather(*(
        validator.check(f"S{index:02d}.US", str(index + 1), "REGULAR", side="Bid" if index % 2 else "Ask")
        for index in range(8)
    ))
    for index, check in enumerate(checks):
        assert check.symbol == f"S{index:02d}.US" and check.quantity == str(index + 1)
        assert check.side == ("Bid" if index % 2 else "Ask")
        assert check.price == str((10 if index % 2 else 9) + index) and check.usable(clock())
        assert check.retry_at == check.checked_at + validator.max_age
    bp.rfqs.assert_awaited_once_with(symbol=None)
    assert bp.submit_rfq.await_count == bp.cancel_rfq.await_count == 8
    bp.accept_rfq.assert_not_called()


@pytest.mark.parametrize("field,value", [
    ("symbol", "FOREIGN.US_USDC_RFQ"),
    ("side", "Ask"),
    ("quantity", "999"),
    ("clientId", -1),
    ("executionMode", "Immediate"),
])
async def test_global_query_still_requires_exact_rfq_request_binding(field, value):
    clock = Clock()
    bp, validator = account(clock)
    original = bp.rfqs.side_effect

    async def mismatch(*args, **kwargs):
        records = await original(*args, **kwargs)
        records[0]["rfq"][field] = value
        return records

    bp.rfqs.side_effect = mismatch
    check = await validator.check("WEN.US", "0.16", "REGULAR")
    assert check.state == "unknown" and check.reason == "rfq_response_mismatch" and check.price is None
    bp.rfqs.assert_awaited_once_with(symbol=None)
    bp.cancel_rfq.assert_awaited_once_with("1")
    bp.accept_rfq.assert_not_called()


async def test_cached_batch_keeps_original_observation_time_and_new_submission_requires_new_read():
    clock = Clock()
    bp = SimpleNamespace(rfqs=AsyncMock(return_value=[{"rfq": {"rfqId": "1"}, "quotes": []}]))
    batch = RfqQueryBatch(bp, clock=clock, timer=clock, coalesce_s=0)
    registered = batch.register()
    records, observed = await batch.query("1", "WEN.US_USDC_RFQ", registered)
    clock.now += .1
    cached_records, cached_observed = await batch.query("1", "WEN.US_USDC_RFQ", registered)
    assert cached_records is records and cached_observed == observed < clock()
    bp.rfqs.assert_awaited_once_with(symbol=None)
    registered_new = batch.register()
    _, new_observed = await batch.query("2", "AMD.US_USDC_RFQ", registered_new)
    assert new_observed == clock() and bp.rfqs.await_count == 2


async def test_submission_during_inflight_query_waits_for_a_new_read():
    clock = Clock()
    entered, release = asyncio.Event(), asyncio.Event()
    queries = 0

    async def read(*args, **kwargs):
        nonlocal queries
        queries += 1
        if queries == 1:
            entered.set()
            await release.wait()
        return [{"rfq": {"rfqId": str(queries)}, "quotes": []}]

    bp = SimpleNamespace(rfqs=AsyncMock(side_effect=read))
    batch = RfqQueryBatch(bp, clock=clock, timer=clock, coalesce_s=0)
    old = asyncio.create_task(batch.query("1", "WEN.US_USDC_RFQ", batch.register()))
    new = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        new = asyncio.create_task(batch.query("2", "AMD.US_USDC_RFQ", batch.register()))
        await asyncio.sleep(0)
        release.set()
        old_result, new_result = await asyncio.gather(old, new)
        assert old_result[0][0]["rfq"]["rfqId"] == "1"
        assert new_result[0][0]["rfq"]["rfqId"] == "2"
        assert bp.rfqs.await_count == 2
    finally:
        release.set()
        for task in (old, new):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (old, new) if task is not None), return_exceptions=True)


async def test_unsupported_global_query_falls_back_once_per_id_without_repeating_global_failure():
    clock = Clock()

    async def read(rfq_id=None, *, symbol=None):
        if rfq_id is None:
            raise RemoteError("API rejected request: HTTP 400", status_code=400)
        return [{"rfq": {"rfqId": rfq_id}, "quotes": []}]

    bp = SimpleNamespace(rfqs=AsyncMock(side_effect=read))
    batch = RfqQueryBatch(bp, clock=clock, timer=clock, coalesce_s=0)
    registrations = [batch.register() for _ in range(8)]
    results = await asyncio.gather(*(
        batch.query(str(index), f"S{index}.US_USDC_RFQ", registered)
        for index, registered in enumerate(registrations)
    ))
    assert [result[0][0]["rfq"]["rfqId"] for result in results] == [str(index) for index in range(8)]
    assert bp.rfqs.await_count == 9
    assert sum(not call.args for call in bp.rfqs.await_args_list) == 1
    await batch.query("8", "NEXT.US_USDC_RFQ", batch.register())
    assert bp.rfqs.await_count == 10 and not batch.global_supported


async def test_global_rate_limit_is_shared_without_per_id_fallback_or_immediate_retry():
    clock = Clock()
    bp, validator = account(clock)
    bp.rfqs.side_effect = RemoteError("API rejected request: HTTP 429", status_code=429)
    checks = await asyncio.gather(*(
        validator.check(f"S{index}.US", "1", "REGULAR") for index in range(8)
    ))
    assert all(check.state == "unknown" and check.price is None for check in checks)
    bp.rfqs.assert_awaited_once_with(symbol=None)
    assert bp.cancel_rfq.await_count == 8
    await validator.check("S0.US", "1", "REGULAR", force_refresh=True)
    bp.rfqs.assert_awaited_once()
    assert validator.query_batch.global_supported
    bp.accept_rfq.assert_not_called()


@pytest.mark.parametrize("reason", ["rfq_check_timeout", "rfq_check_failed", "rfq_response_mismatch"])
async def test_larger_unknown_preserves_transport_pause_without_poisoning_minimum_identity(reason):
    clock = Clock()
    bp, validator = account(clock)
    minimum = await validator.check("WEN.US", "1", "REGULAR")
    clock.now += 1
    larger = validator.result("WEN.US", "Bid", "25", "REGULAR", "unknown", reason)
    validator.cache[("WEN.US", "Bid", "25", "REGULAR")] = larger
    validator.latest[("WEN.US", "Bid")] = larger
    clock.now += 1
    projected = await validator.check("WEN.US", "1.0", "REGULAR", force_refresh=True)
    assert projected.symbol == minimum.symbol and projected.side == minimum.side
    assert projected.quantity == minimum.quantity and projected.session == minimum.session
    assert projected.state == "unknown" and projected.reason == larger.reason and projected.price is None
    assert projected.checked_at == larger.checked_at and projected.retry_at == larger.retry_at
    assert projected.eligibility_until == larger.eligibility_until and not projected.usable(clock())
    assert validator.latest[("WEN.US", "Bid")] is larger
    assert validator.cached_exact("WEN.US", "1", "REGULAR") is minimum
    assert minimum.usable(clock())
    bp.submit_rfq.assert_awaited_once()
    bp.rfqs.assert_awaited_once_with(symbol=None)
    bp.cancel_rfq.assert_awaited_once()
    bp.accept_rfq.assert_not_called()


async def test_unfiltered_query_omits_both_filters_from_request_and_signature():
    bp = object.__new__(Backpack)
    signed = []
    bp.auth = SimpleNamespace(headers=lambda instruction, params: signed.append((instruction, params)) or {})
    bp.http = SimpleNamespace(request=AsyncMock(return_value=[]))
    assert await bp.rfqs(symbol=None) == []
    request = bp.http.request.await_args
    assert request.args == ("GET", "/api/v1/rfqs") and request.kwargs["params"] == {}
    request.kwargs["headers"]()
    assert signed == [("rfqQuery", {})]
