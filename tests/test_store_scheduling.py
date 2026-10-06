import pytest

from backpack_stocks.models import STOCK_MINT, Settings
from backpack_stocks.store import Store


def test_paper_queue_tuning_preserves_existing_journal(tmp_path):
    settings = Settings(database=str(tmp_path / "paper.sqlite3"))
    store = Store(settings.database, settings)
    store.event(None, "prior_event", {"saved": True})
    store.set_meta("telegram:offset", "123")
    store.close()
    settings.monitor.concurrency = 16
    settings.monitor.exploration_s = 30
    settings.monitor.quote_refresh_s = 5
    store = Store(settings.database, settings)
    try:
        assert store.db.execute("SELECT kind FROM events").fetchone()[0] == "prior_event"
        assert store.get_meta("telegram:offset") == "123"
        assert Settings.model_validate_json(store.get_meta("settings")).monitor.concurrency == 16
    finally:
        store.close()


@pytest.mark.parametrize("field", ["slippage_bps", "gas_budget_usdc", "quote_max_age_s", "max_cost_usdc"])
def test_queue_tuning_cannot_hide_a_changed_trading_policy(tmp_path, field):
    settings = Settings(database=str(tmp_path / "paper.sqlite3"))
    store = Store(settings.database, settings)
    store.close()
    settings.monitor.concurrency = 8
    old = getattr(settings.policy, field)
    setattr(settings.policy, field, old + 1 if isinstance(old, int) else str(float(old) + 1))
    with pytest.raises(RuntimeError, match="Config differs"):
        Store(settings.database, settings)


def test_queue_tuning_cannot_change_monitor_freshness_or_live_settings(tmp_path):
    settings = Settings(database=str(tmp_path / "paper.sqlite3"))
    store = Store(settings.database, settings)
    store.close()
    settings.monitor.concurrency = 8
    settings.monitor.max_quote_age_s += 1
    with pytest.raises(RuntimeError, match="Config differs"):
        Store(settings.database, settings)

    live = Settings(
        mode="live",
        source="public",
        enable_live=True,
        wallet=STOCK_MINT,
        database=str(tmp_path / "live.sqlite3"),
        policy={
            "live_fee_verified": True,
            "live_quantity_mapping_verified": True,
            "live_rfq_verified": True,
            "live_withdrawal_verified": True,
            "withdrawal_semantics": "deduct",
        },
    )
    store = Store(live.database, live)
    store.close()
    live.monitor.concurrency = 8
    with pytest.raises(RuntimeError, match="Config differs"):
        Store(live.database, live)
