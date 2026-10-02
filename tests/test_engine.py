import json

import pytest

from backpack_stocks.engine import Engine
from backpack_stocks.market import PaperBroker, ReplayMarket
from backpack_stocks.models import Phase, Policy, Settings
from backpack_stocks.store import Store
from backpack_stocks.transport import UnknownOutcome


class Clock:
    def __init__(self):
        self.now = 1000000.0

    def __call__(self):
        return self.now

    def advance(self, seconds=1):
        self.now += seconds


def build(tmp_path, *, falling=False, policy=None, clock=None):
    clock = clock or Clock()
    replay = tmp_path / "replay.json"
    if not replay.exists():
        replay.write_text(
            json.dumps(
                {
                    "frames": [
                        {"offset_s": 0, "ask": "6.00", "dex_bid": "6.30"},
                        {"offset_s": 3, "ask": "6.00", "dex_bid": "5.00" if falling else "6.25"},
                    ]
                }
            )
        )
    cfg = Settings(
        database=str(tmp_path / "state.sqlite3"),
        replay_file=str(replay),
        paper_delivery_s=5,
        policy=policy or Policy(withdrawal_semantics="deduct", cooldown_s=0, exit_timeout_s=5),
    )
    store = Store(cfg.database, cfg)
    market = ReplayMarket(cfg, store, clock)
    broker = PaperBroker(cfg, market, clock)
    return Engine(cfg, store, market, broker, clock), clock


async def finish(engine, clock, ticks=30):
    for _ in range(ticks):
        await engine.tick()
        clock.advance()


async def test_paper_full_delivery_cycle_accounts_cost_fee_location(tmp_path):
    e, clock = build(tmp_path)
    try:
        await finish(e, clock)
        c = e.store.cycles()[0]
        assert c["phase"] == "COMPLETE"
        assert c["quantity_raw"] == 10000000
        assert c["delivered_raw"] == 9920000
        assert c["cost_raw"] == 60000000
        assert c["proceeds_raw"] < 9920000 * 6.30
        assert c["realised_pnl_raw"] == c["proceeds_raw"] - 60000000 - 30000
        assert e.store.balance("backpack", "USDC") == 940000000
        assert e.store.balance("wallet", "USDC") == c["proceeds_raw"]
        assert e.store.balance("transfer", "WEN.US", "pending") == 0
        assert e.store.balance("fees", "WEN.US", "spent") == 80000
        assert e.store.balance("wallet", "WEN.US", "reserved") == 0
    finally:
        e.store.close()


async def test_price_drop_during_delivery_holds_inventory_and_pauses(tmp_path):
    e, clock = build(tmp_path, falling=True)
    try:
        await finish(e, clock)
        c = e.store.active()
        assert c.phase == Phase.PAUSED
        assert "spread disappeared" in c.reason
        assert c.proceeds_raw == 0
        assert e.store.balance("wallet", "WEN.US") == 9920000
        assert not e.store.find_intent(c.id, "sell")
    finally:
        e.store.close()


async def test_crash_restart_with_pending_transfer_does_not_double_debit(tmp_path):
    e, clock = build(tmp_path)
    for _ in range(6):
        await e.tick()
        clock.advance()
    assert e.store.active().phase == Phase.WITHDRAW_PENDING
    e.store.close()
    resumed, _ = build(tmp_path, clock=clock)
    try:
        await finish(resumed, clock)
        assert resumed.store.cycles()[0]["phase"] == "COMPLETE"
        assert resumed.store.balance("cost", "USDC", "spent") == 60000000
        assert resumed.store.balance("fees", "WEN.US", "spent") == 80000
    finally:
        resumed.store.close()


async def test_unknown_withdrawal_is_reconciled_without_second_submission(tmp_path):
    e, clock = build(tmp_path)
    calls = []
    original = e.broker.withdraw

    async def lost(cycle, request):
        calls.append(request)
        await original(cycle, request)
        raise UnknownOutcome("lost response")

    e.broker.withdraw = lost
    try:
        await finish(e, clock)
        assert calls == [10000000]
        assert e.store.cycles()[0]["phase"] == "COMPLETE"
    finally:
        e.store.close()


async def test_submitting_intent_without_receipt_never_resubmits(tmp_path):
    e, clock = build(tmp_path)
    calls = []

    async def lost(cycle, request):
        calls.append(request)
        raise UnknownOutcome("lost")

    async def absent(*args):
        return None

    e.broker.withdraw, e.broker.reconcile = lost, absent
    e.settings.policy.transfer_timeout_s = 3
    try:
        await finish(e, clock)
        assert len(calls) == 1
        assert e.store.active().phase == Phase.PAUSED
        assert e.store.balance("transfer", "WEN.US", "pending") == 10000000
    finally:
        e.store.close()


async def test_partial_buy_uses_actual_fill_cost_and_quantity(tmp_path):
    e, clock = build(tmp_path)

    async def partial(cycle):
        return {"quantity": 5000000, "cost": 30000000}

    e.broker.fill = partial
    try:
        await finish(e, clock)
        c = e.store.cycles()[0]
        assert c["phase"] == "COMPLETE"
        assert c["delivered_raw"] == 4920000
        assert c["cost_raw"] == 30000000
        assert e.store.balance("backpack", "USDC") == 970000000
        assert e.store.balance("backpack", "USDC", "reserved") == 0
    finally:
        e.store.close()


async def test_partial_fill_below_withdrawal_minimum_pauses_with_cex_inventory(tmp_path):
    e, clock = build(tmp_path)

    async def partial(cycle):
        return {"quantity": 100000, "cost": 600000}

    e.broker.fill = partial
    try:
        await finish(e, clock)
        assert e.store.active().phase == Phase.PAUSED
        assert e.store.balance("backpack", "WEN.US") == 100000
        assert not e.store.find_intent(e.store.active().id, "withdraw")
    finally:
        e.store.close()


async def test_multiple_paper_cycles_preserve_own_impact_and_locations(tmp_path):
    p = Policy(withdrawal_semantics="deduct", cooldown_s=0, max_cycles=2)
    e, clock = build(tmp_path, policy=p)
    try:
        await finish(e, clock, 50)
        cycles = list(reversed(e.store.cycles()))
        assert len(cycles) == 2
        assert all(c["phase"] == "COMPLETE" for c in cycles)
        assert cycles[1]["proceeds_raw"] < cycles[0]["proceeds_raw"]
        assert e.store.balance("backpack", "USDC") == 880000000
    finally:
        e.store.close()


async def test_paper_capital_return_waits_for_delivery_before_credit(tmp_path):
    e, clock = build(tmp_path)
    e.settings.rebalance = "each_cycle"
    try:
        for _ in range(16):
            await e.tick()
            clock.advance()
        assert e.store.active().phase == Phase.RETURN_PENDING
        assert e.store.balance("backpack", "USDC") == 940000000
        assert e.store.balance("transfer", "USDC", "pending") > 0
        await finish(e, clock, 20)
        c = e.store.cycles()[0]
        assert c["phase"] == "COMPLETE"
        assert e.store.balance("backpack", "USDC") == 940000000 + c["proceeds_raw"]
        assert e.store.balance("transfer", "USDC", "pending") == 0
        assert e.store.balance("wallet", "USDC") == 0
        assert c["data"]["return_receipt"]["quantity"] == c["proceeds_raw"]
    finally:
        e.store.close()


async def test_stop_resume_and_writer_lock(tmp_path):
    e, clock = build(tmp_path)
    try:
        with pytest.raises(RuntimeError):
            Store(e.settings.database, e.settings)
        await e.stop()
        await e.tick()
        assert not e.store.cycles()
        await e.resume()
        await e.tick()
        assert len(e.store.cycles()) == 1
    finally:
        e.store.close()


def test_config_change_on_restart_is_rejected(tmp_path):
    e, _ = build(tmp_path)
    settings = e.settings.model_copy(deep=True)
    e.store.close()
    settings.policy.quantity = "5"
    with pytest.raises(RuntimeError):
        Store(settings.database, settings)


async def test_paper_runtime_does_not_read_signing_env(tmp_path, monkeypatch):
    import backpack_stocks.runtime as runtime_module

    e, _ = build(tmp_path)
    settings = e.settings
    e.store.close()

    def forbidden(name):
        raise AssertionError("Signing env accessed")

    monkeypatch.setattr(runtime_module, "required_env", forbidden)
    async with runtime_module.runtime(settings) as paper:
        assert paper.settings.mode == "paper"


def test_ledger_rollback_is_atomic(tmp_path):
    e, _ = build(tmp_path)
    try:
        with pytest.raises(ValueError), e.store.atomic():
            e.store.move("USDC", 10, ("backpack", "available"), ("wallet", "available"))
            e.store.move("USDC", 10**20, ("backpack", "available"), ("wallet", "available"))
        assert e.store.balance("backpack", "USDC") == 1000000000
        assert e.store.balance("wallet", "USDC") == 0
    finally:
        e.store.close()
