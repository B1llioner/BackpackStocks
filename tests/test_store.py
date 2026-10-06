import pytest

from backpack_stocks.models import STOCK_MINT, Settings
from backpack_stocks.store import Store


def test_paper_rfq_concurrency_tuning_reopens_existing_journal_and_ledger(tmp_path):
    settings = Settings(database=str(tmp_path / "paper.sqlite3"))
    store = Store(settings.database, settings)
    try:
        store.event(None, "prior_rfq_check", {"state": "unavailable"})
        store.remember_request("prior-request", {"result": "saved"})
        store.set_meta("telegram:offset", "123")
        store.move("USDC", 250_000_000, ("backpack", "available"), ("wallet", "reserved"))
        events, ledger = store.events(), store.ledger()
    finally:
        store.close()

    settings.monitor.rfq_concurrency = 8
    store = Store(settings.database, settings)
    try:
        assert store.events() == events and store.ledger() == ledger
        assert store.balance("backpack", "USDC") == 750_000_000
        assert store.balance("wallet", "USDC", "reserved") == 250_000_000
        assert store.idempotent("prior-request") == {"result": "saved"}
        assert store.get_meta("telegram:offset") == "123"
        assert Settings.model_validate_json(store.get_meta("settings")).monitor.rfq_concurrency == 8
    finally:
        store.close()


def test_live_rfq_concurrency_change_remains_pinned(tmp_path):
    settings = Settings(
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
    store = Store(settings.database, settings)
    store.close()
    changed = settings.model_copy(deep=True)
    changed.monitor.rfq_concurrency = 8
    with pytest.raises(RuntimeError, match="Config differs"):
        Store(settings.database, changed)
    store = Store(settings.database, settings)
    try:
        assert Settings.model_validate_json(store.get_meta("settings")).monitor.rfq_concurrency == 4
    finally:
        store.close()


@pytest.mark.parametrize("section", ["monitor", "policy"])
def test_paper_rfq_concurrency_tuning_cannot_hide_changed_price_ttl(tmp_path, section):
    settings = Settings(database=str(tmp_path / "paper.sqlite3"))
    store = Store(settings.database, settings)
    store.close()
    changed = settings.model_copy(deep=True)
    changed.monitor.rfq_concurrency = 8
    if section == "monitor":
        changed.monitor.max_quote_age_s += 1
    else:
        changed.policy.quote_max_age_s += 1
    with pytest.raises(RuntimeError, match="Config differs"):
        Store(settings.database, changed)
    store = Store(settings.database, settings)
    try:
        assert store.get_meta("settings") == settings.model_dump_json()
    finally:
        store.close()
