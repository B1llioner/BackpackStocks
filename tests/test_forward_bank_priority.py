from dataclasses import replace
from unittest.mock import Mock

import pytest
from test_forward_grid import public_direct_monitor as direct_public_fixture
from test_indicative_coverage import published

from backpack_stocks.models import STOCK_MINT, USDC_MINT


@pytest.fixture
async def primary_monitor():
    async for result in direct_public_fixture.__wrapped__():
        yield result


async def test_expired_dex_bank_keeps_primary_refresh_when_owned_buy_is_current(primary_monitor):
    mon, clock, feed, router, banks, key = primary_monitor
    router.bank_feed.set_primary = Mock()
    router.bank_feed.peek = lambda key: (
        banks[key]
        if key in banks and 0 <= clock() - banks[key]["observed"] < mon.settings.policy.quote_max_age_s
        else None
    )
    state = mon.forward_coverage[key[0]]
    # An expired bank needs a refresh; numerical freshness must not be a
    # prerequisite for receiving the primary refresh priority.
    clock.now += mon.settings.policy.quote_max_age_s + 1
    await feed.ingest(published(clock))
    assert not mon.coverage_result_fresh(state)
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_once_with({("old", STOCK_MINT, USDC_MINT, 10)})
    assert banks[("old", STOCK_MINT, USDC_MINT, 10)]["response"]["context"]["slot"] == 1


@pytest.mark.parametrize(
    "boundary", ["expired_buy", "unavailable", "metadata", "mint", "mapping", "session", "quantity", "grid"]
)
async def test_primary_bank_ownership_cannot_survive_invalid_buy_or_minimum(primary_monitor, boundary):
    mon, clock, feed, router, _, key = primary_monitor
    router.bank_feed.set_primary = Mock()
    state = mon.forward_coverage[key[0]]
    mon.sync_primary_banks()
    assert router.bank_feed.set_primary.call_args.args[0]
    if boundary == "expired_buy":
        clock.now += mon.settings.monitor.max_quote_age_s + 1
    elif boundary == "unavailable":
        clock.now += 1
        await feed.ingest(published(clock, reason="stalePrice", ask=None))
    elif boundary == "metadata":
        mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    elif boundary == "mint":
        mon.assets[key[0]] = replace(state.asset, mint=USDC_MINT)
    elif boundary == "mapping":
        mon.chain[key[0]] = "unsupported_mint_or_unit_mapping"
    elif boundary == "session":
        state.plan["backpack_session"] = "US_EQUITIES_OVERNIGHT"
    elif boundary == "quantity":
        state.plan["acquired_raw"] += 1
    elif boundary == "grid":
        state.plan["minimum_coverage"] = False
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_with(set())


async def test_primary_priority_covers_every_leg_of_a_proven_minimum_route(primary_monitor):
    mon, _, _, router, _, key = primary_monitor
    router.bank_feed.set_primary = Mock()
    state = mon.forward_coverage[key[0]]
    leaf = state.plan["dex_plan"]
    bridge = {**leaf, "pool": "bridge", "inputMint": "SOL"}
    state.plan["dex_plan"] = {**leaf, "_direct_route": [leaf, bridge]}
    mon.sync_primary_banks()
    router.bank_feed.set_primary.assert_called_once_with(
        {("old", STOCK_MINT, USDC_MINT, 10), ("bridge", "SOL", USDC_MINT, 10)}
    )
