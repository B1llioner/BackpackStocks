from unittest.mock import AsyncMock

from test_engine import build, finish

from backpack_stocks.models import Phase


async def test_crash_after_sell_intent_before_reservation_reuses_signed_payload(tmp_path):
    e, clock = build(tmp_path)
    try:
        for _ in range(10):
            await e.tick()
            clock.advance()
        cycle = e.store.active()
        assert cycle.phase == Phase.ARRIVED
        prepared = await e.broker.prepare_sell(cycle, 60500000)
        e.store.intent(cycle, "sell", prepared)
        e.broker.prepare_sell = AsyncMock(side_effect=AssertionError("must reuse durable transaction"))
        await finish(e, clock)
        assert e.store.cycles()[0]["phase"] == "COMPLETE"
        e.broker.prepare_sell.assert_not_called()
        assert e.store.cycles()[0]["sell_signature"] == prepared["signature"]
    finally:
        e.store.close()


async def test_crash_after_return_intent_before_reservation_reuses_signed_payload(tmp_path):
    e, clock = build(tmp_path)
    e.settings.rebalance = "each_cycle"
    try:
        for _ in range(12):
            await e.tick()
            clock.advance()
        cycle = e.store.active()
        assert cycle.phase == Phase.SOLD
        prepared = await e.broker.prepare_return(cycle)
        e.store.intent(cycle, "return", prepared)
        e.broker.prepare_return = AsyncMock(side_effect=AssertionError("must reuse durable transaction"))
        await finish(e, clock)
        assert e.store.cycles()[0]["phase"] == "COMPLETE"
        e.broker.prepare_return.assert_not_called()
        assert e.store.cycles()[0]["data"]["return_receipt"]["signature"] == prepared["signature"]
    finally:
        e.store.close()


async def test_schema_change_pauses_exposure_without_fabricated_fill(tmp_path):
    e, clock = build(tmp_path)
    e.broker.fill = AsyncMock(side_effect=KeyError("fillQuantity"))
    try:
        await finish(e, clock)
        cycle = e.store.active()
        assert cycle.phase == Phase.PAUSED
        assert "API contract" in cycle.reason
        assert e.store.balance("backpack", "USDC", "reserved") == 60000000
        assert e.store.balance("wallet", "WEN.US") == 0
    finally:
        e.store.close()
