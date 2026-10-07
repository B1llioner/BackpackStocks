from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_forward_coverage import coverage_monitor
from test_monitor import observed

from backpack_stocks.sessions import ActiveSessionCache, active_session
from backpack_stocks.stock_books import SPOT_SESSION, BookUnavailable


def calendar(**changes):
    result = {
        "name": "US_EQUITIES_REGULAR",
        "timezone": "America/New_York",
        "startTime": "09:30:00",
        "endTime": "16:00:00",
        "startWeekday": 1,
        "endWeekday": 5,
    }
    result.update(changes)
    return [result]


def test_same_second_reuses_calendar_decision_without_hiding_metadata_changes(monkeypatch):
    sessions, holidays = calendar(), []
    cache = ActiveSessionCache()
    now = datetime(2026, 10, 6, 16, tzinfo=UTC)
    evaluate = Mock(wraps=active_session)
    monkeypatch.setattr("backpack_stocks.sessions.active_session", evaluate)
    for offset in (0, 100, 200, 900000):
        assert cache.get(sessions, holidays, now + timedelta(microseconds=offset)) == "US_EQUITIES_REGULAR"
    evaluate.assert_called_once()
    sessions[0]["name"] = "RENAMED"
    assert cache.get(sessions, holidays, now + timedelta(microseconds=950000)) == "RENAMED"
    assert evaluate.call_count == 2
    holidays.append({"market": "US_EQUITIES", "timezone": "America/New_York", "date": "2026-10-06"})
    assert cache.get(sessions, holidays, now + timedelta(microseconds=960000)) is None
    assert evaluate.call_count == 3
    holidays[0]["date"] = "2026-10-07"
    assert cache.get(sessions, holidays, now + timedelta(microseconds=970000)) == "RENAMED"
    assert evaluate.call_count == 4


@pytest.mark.parametrize("boundary", ["2026-10-06T13:30:00+00:00", "2026-10-06T20:00:00+00:00"])
def test_exact_integer_open_and_close_boundaries_are_never_cached_across(boundary):
    sessions = calendar()
    cache = ActiveSessionCache()
    edge = datetime.fromisoformat(boundary)
    for moment in (edge - timedelta(microseconds=1), edge, edge + timedelta(microseconds=1)):
        assert cache.get(sessions, [], moment) == active_session(sessions, [], moment)


def test_fractional_session_boundaries_take_exact_path_even_inside_same_second(monkeypatch):
    sessions = calendar(startTime="09:30:00.500000", endTime="09:30:00.750000")
    cache = ActiveSessionCache()
    evaluate = Mock(wraps=active_session)
    monkeypatch.setattr("backpack_stocks.sessions.active_session", evaluate)
    base = datetime(2026, 10, 6, 13, 30, tzinfo=UTC)
    for micros, expected in (
        (499999, None),
        (500000, "US_EQUITIES_REGULAR"),
        (749999, "US_EQUITIES_REGULAR"),
        (750000, None),
    ):
        assert cache.get(sessions, [], base + timedelta(microseconds=micros)) == expected
    assert evaluate.call_count == 4


def test_fractional_partial_holiday_ends_at_actual_boundary():
    sessions = calendar()
    holidays = [
        {
            "market": "US_EQUITIES",
            "timezone": "America/New_York",
            "date": "2026-10-06",
            "startTime": "10:00:00.250000",
            "endTime": "10:00:00.750000",
        }
    ]
    cache = ActiveSessionCache()
    base = datetime(2026, 10, 6, 14, tzinfo=UTC)
    for micros, expected in (
        (249999, "US_EQUITIES_REGULAR"),
        (250000, None),
        (749999, None),
        (750000, "US_EQUITIES_REGULAR"),
    ):
        assert cache.get(sessions, holidays, base + timedelta(microseconds=micros)) == expected


def test_clock_rewind_inside_same_second_forces_recompute(monkeypatch):
    cache = ActiveSessionCache()
    sessions = calendar()
    evaluate = Mock(wraps=active_session)
    monkeypatch.setattr("backpack_stocks.sessions.active_session", evaluate)
    now = datetime(2026, 10, 6, 16, tzinfo=UTC)
    assert cache.get(sessions, [], now + timedelta(microseconds=800000)) == "US_EQUITIES_REGULAR"
    assert cache.get(sessions, [], now + timedelta(microseconds=400000)) == "US_EQUITIES_REGULAR"
    assert evaluate.call_count == 2


def test_dst_weekday_wrap_and_full_holiday_midnight_match_original_calendar():
    sessions = calendar(
        name="US_EQUITIES_OVERNIGHT", startTime="20:00:00", endTime="04:00:00", startWeekday=7, endWeekday=4
    )
    holidays = [{"market": "US_EQUITIES", "timezone": "America/New_York", "date": "2026-10-06"}]
    cache = ActiveSessionCache()
    for stamp in (
        "2026-10-05T23:59:59.999999+00:00",
        "2026-10-06T00:00:00+00:00",
        "2026-10-06T03:59:59.999999+00:00",
        "2026-10-06T04:00:00+00:00",
        "2026-11-01T05:59:59.999999+00:00",
        "2026-11-01T06:00:00+00:00",
        "2026-10-10T07:59:59.999999+00:00",
        "2026-10-10T08:00:00+00:00",
    ):
        now = datetime.fromisoformat(stamp)
        assert cache.get(sessions, holidays, now) == active_session(sessions, holidays, now)


@pytest.mark.parametrize(
    "field,value",
    [
        ("timezone", "UTC"),
        ("startTime", "18:00:00"),
        ("endTime", "11:00:00"),
        ("startWeekday", 7),
        ("endWeekday", 1),
    ],
)
def test_in_place_session_time_zone_and_weekday_edits_are_seen_immediately(field, value):
    cache = ActiveSessionCache()
    sessions = calendar()
    now = datetime(2026, 10, 6, 16, tzinfo=UTC)
    assert cache.get(sessions, [], now) == "US_EQUITIES_REGULAR"
    sessions[0][field] = value
    assert cache.get(sessions, [], now) == active_session(sessions, [], now)


def test_monitor_rechecks_metadata_freshness_and_live_asset_rules_even_on_calendar_hit():
    mon, clock = coverage_monitor()
    item = mon.assets["WEN.US"]
    session, rules = mon.coverage_rules(item)
    rules["stepSize"] = "1"
    assert mon.coverage_rules(item)[1]["stepSize"] == "1"
    mon.sessions[0]["name"] = "CHANGED"
    with pytest.raises(ValueError, match="quantity constraints"):
        mon.coverage_rules(item)
    mon.sessions[0]["name"] = session
    mon.metadata_at = clock() - mon.settings.monitor.metadata_s * 2 - 1
    with pytest.raises(ValueError, match="session is open"):
        mon.coverage_rules(item)


def test_invalid_changed_calendar_never_reuses_a_previously_open_decision():
    sessions = calendar()
    cache = ActiveSessionCache()
    now = datetime(2026, 10, 6, 16, tzinfo=UTC)
    assert cache.get(sessions, [], now) == "US_EQUITIES_REGULAR"
    sessions[0]["startTime"] = "invalid"
    for _ in range(2):
        with pytest.raises(ValueError):
            cache.get(sessions, [], now)


def test_nested_in_place_metadata_changes_invalidate_content_snapshot(monkeypatch):
    sessions = calendar()
    sessions[0]["providerMetadata"] = {"revision": [1]}
    cache = ActiveSessionCache()
    now = datetime(2026, 10, 6, 16, tzinfo=UTC)
    evaluate = Mock(wraps=active_session)
    monkeypatch.setattr("backpack_stocks.sessions.active_session", evaluate)
    cache.get(sessions, [], now)
    sessions[0]["providerMetadata"]["revision"].append(2)
    cache.get(sessions, [], now)
    assert evaluate.call_count == 2


def test_all_monitor_calendar_callers_share_one_current_content_checked_decision(monkeypatch):
    mon, clock = coverage_monitor()
    item = mon.assets["WEN.US"]
    clock.now = datetime(2026, 10, 6, 16, tzinfo=UTC).timestamp()
    mon.metadata_at = clock()
    mon.sessions = calendar()
    obs = observed(clock)
    evaluate = Mock(wraps=active_session)
    monkeypatch.setattr("backpack_stocks.sessions.active_session", evaluate)
    for _ in range(72):
        assert mon.quantity_session(obs) == mon.coverage_rules(item)[0] == "US_EQUITIES_REGULAR"
        assert mon.session_status(clock())["rfq_session"] == "US_EQUITIES_REGULAR"
    evaluate.assert_called_once()
    item.sessions[0]["stepSize"] = "1"
    assert mon.quantity_rules(item, obs)["stepSize"] == mon.coverage_rules(item)[1]["stepSize"] == "1"
    mon.sessions[0]["name"] = "CHANGED"
    assert mon.quantity_session(obs) == mon.session_status(clock())["rfq_session"] == "CHANGED"
    with pytest.raises(ValueError, match="quantity constraints"):
        mon.quantity_rules(item, obs)
    with pytest.raises(ValueError, match="quantity constraints"):
        mon.coverage_rules(item)


@pytest.mark.parametrize("empty_calendar", [False, True])
def test_future_metadata_rejected_by_every_monitor_caller_even_after_a_calendar_hit(empty_calendar):
    mon, clock = coverage_monitor()
    item = mon.assets["WEN.US"]
    obs = observed(clock)
    assert mon.quantity_session(obs) == mon.coverage_rules(item)[0]
    mon.metadata_at = clock() + 0.1
    if empty_calendar:
        mon.sessions.clear()
    with pytest.raises(ValueError, match="session is open"):
        mon.quantity_session(obs)
    with pytest.raises(ValueError, match="session is open"):
        mon.coverage_rules(item)
    assert mon.session_status(clock())["rfq_session_state"] == "unknown"


def test_fractional_monitor_calendar_boundaries_apply_to_every_caller():
    mon, clock = coverage_monitor()
    item = mon.assets["WEN.US"]
    base = datetime(2026, 10, 6, 13, 30, tzinfo=UTC).timestamp()
    clock.now = base
    mon.metadata_at = base
    mon.sessions = calendar(startTime="09:30:00.500000", endTime="09:30:00.750000")
    obs = observed(clock)
    for fraction, opened in ((0.499999, False), (0.5, True), (0.749999, True), (0.75, False)):
        clock.now = base + fraction
        if opened:
            assert mon.quantity_session(obs) == mon.coverage_rules(item)[0] == "US_EQUITIES_REGULAR"
        else:
            with pytest.raises(ValueError):
                mon.quantity_session(obs)
            with pytest.raises(ValueError):
                mon.coverage_rules(item)
        assert mon.session_status(clock())["rfq_session_state"] == ("open" if opened else "closed")


def test_spot_calendar_bypass_keeps_current_market_rules_and_metadata_bounds():
    mon, clock = coverage_monitor()
    item = mon.assets["WEN.US"]
    mon.sessions[0].update(startTime="12:00:00", endTime="13:00:00")
    obs = observed(clock).model_copy(update={"session": SPOT_SESSION})
    market = SimpleNamespace(step=Decimal("0.01"), minimum=Decimal("0.01"), maximum=Decimal("100"))
    mon.books.markets[item.symbol] = market
    mon.books.quantity_ceiling = Mock(return_value=Decimal("100"))
    assert mon.quantity_session(obs) == SPOT_SESSION
    assert mon.quantity_rules(item, obs)["stepSize"] == "0.01"
    market.step = Decimal("1")
    assert mon.quantity_rules(item, obs)["stepSize"] == "1"
    mon.metadata_at = clock() + 0.1
    with pytest.raises(BookUnavailable):
        mon.quantity_session(obs)
