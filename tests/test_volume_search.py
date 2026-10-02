from decimal import Decimal

from backpack_stocks.volume_search import VolumeSearch


def test_unlimited_search_expands_past_seed_and_keeps_provider_ceiling():
    search = VolumeSearch()
    key = "BLK.US", "backpack_to_jupiter"
    kwargs = dict(auto_size=True, minimum=Decimal("1055"), maximum=Decimal("1055000"))
    assert search.next(key, Decimal("1000"), 10, **kwargs) == 1055
    search.record(
        key, {"search_budget_usdc": "1055", "observed_at": 10, "size_feasible": True, "profit_raw": 1}
    )
    assert search.next(key, Decimal("1000"), 11, **kwargs) == 2110
    search.record(
        key, {"search_budget_usdc": "2110", "observed_at": 11, "size_feasible": True, "profit_raw": 10}
    )
    assert search.next(key, Decimal("1000"), 12, **kwargs) == 2110
    search.next(key, Decimal("1000"), 13, **kwargs)
    assert search.next(key, Decimal("1000"), 14, **kwargs) == 4220
    search.refuse(key, Decimal("4220"), 14)
    for stamp in range(15, 24):
        assert 1055 <= search.next(key, Decimal("1000"), stamp, **kwargs) <= Decimal("4009")


def test_small_provider_maximum_does_not_exceed_its_limit():
    search = VolumeSearch()
    for stamp in range(5):
        assert (
            search.next(
                ("WEN.US", "forward"),
                Decimal("1000"),
                stamp,
                auto_size=True,
                minimum=Decimal("10"),
                maximum=Decimal("100"),
            )
            <= 100
        )
