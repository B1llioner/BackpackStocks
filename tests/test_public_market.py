import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backpack_stocks.market import PublicMarket
from backpack_stocks.models import Observation, Settings


async def test_stale_quotes_are_not_used_as_fresh_executable_prices():
    settings = Settings(source="public")
    market = PublicMarket(settings, None, None, None)
    market.latest = Observation(
        ask="6", bid="5.9", timestamp=time.time() - 100, session="REGULAR", source="fixture"
    )
    with pytest.raises(ValueError, match="stale"):
        await market.observation()


async def test_rest_fallback_is_explicitly_modeled_and_never_for_live():
    settings = Settings(source="public", paper_rest_fallback=True, paper_rfq_markup_bps=20)
    backpack = SimpleNamespace(ticker=AsyncMock(return_value={"lastPrice": "6.375"}))
    market = PublicMarket(settings, None, backpack, None)
    result = await market.observation()
    assert result.ask == "6.387750"
    assert "modeled" in result.source and "age unknown" in result.source
    # model_copy is used solely to test the no-live-fallback invariant; runtime validates live admission.
    market.settings = settings.model_copy(update={"mode": "live"})
    with pytest.raises(ValueError, match="Waiting"):
        await market.observation()
