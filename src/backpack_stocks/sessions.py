from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from .models import dec


def active_session(sessions: list[dict], holidays: list[dict], now: datetime | None = None) -> str | None:
    now = now or datetime.now(UTC)
    for holiday in holidays:
        if holiday["market"] != "US_EQUITIES":
            continue
        local = now.astimezone(ZoneInfo(holiday["timezone"]))
        if local.date().isoformat() != holiday["date"]:
            continue
        start, end = holiday.get("startTime"), holiday.get("endTime")
        if start is None or end is None or start <= local.strftime("%H:%M:%S") < end:
            return None
    for session in sessions:
        local = now.astimezone(ZoneInfo(session["timezone"]))
        start_time, end_time = session["startTime"], session["endTime"]
        for offset in [0, 1]:
            day = local.date() - timedelta(days=offset)
            weekday = day.isoweekday()
            start_day, end_day = session["startWeekday"], session["endWeekday"]
            allowed = (
                start_day <= weekday <= end_day
                if start_day <= end_day
                else (weekday >= start_day or weekday <= end_day)
            )
            start = datetime.fromisoformat(f"{day}T{start_time}").replace(tzinfo=local.tzinfo)
            end = datetime.fromisoformat(f"{day}T{end_time}").replace(tzinfo=local.tzinfo)
            if end <= start:
                end += timedelta(days=1)
            if allowed and start <= local < end:
                return session["name"]
    return None


def next_session_open(
    sessions: list[dict], holidays: list[dict], now: datetime | None = None
) -> datetime | None:
    """Find the next opening in the published schedule, including holiday resumes.

    An already open market returns now. Search at most two weeks; missing or
    fully blocked schedules return None rather than a guessed opening.
    """
    now = now or datetime.now(UTC)
    if active_session(sessions, holidays, now) is not None:
        return now
    horizon = now + timedelta(days=14)
    boundaries: set[datetime] = set()
    for session in sessions:
        zone = ZoneInfo(session["timezone"])
        day = now.astimezone(zone).date()
        for offset in range(15):
            start = datetime.fromisoformat(f"{day + timedelta(days=offset)}T{session['startTime']}")
            boundaries.add(start.replace(tzinfo=zone).astimezone(UTC))
    for holiday in holidays:
        if holiday["market"] != "US_EQUITIES":
            continue
        zone = ZoneInfo(holiday["timezone"])
        if holiday.get("startTime") is not None and holiday.get("endTime") is not None:
            end = datetime.fromisoformat(f"{holiday['date']}T{holiday['endTime']}")
        else:
            end = datetime.fromisoformat(holiday["date"]) + timedelta(days=1)
        boundaries.add(end.replace(tzinfo=zone).astimezone(UTC))
    return next(
        (
            boundary
            for boundary in sorted(boundaries)
            if now < boundary <= horizon and active_session(sessions, holidays, boundary) is not None
        ),
        None,
    )


def validate_quantity(security: dict, session: str | None, quantity: str) -> None:
    rules = next((s for s in security["sessions"] if s["name"] == session), None)
    if not rules:
        raise ValueError("No RFQ trading session is open")
    q = dec(quantity)
    if q < dec(rules["minQuantity"]) or (rules.get("maxQuantity") and q > dec(rules["maxQuantity"])):
        raise ValueError("Quantity outside session limits")
    if q % dec(rules["stepSize"]) != 0:
        raise ValueError("Quantity incompatible with session step size")
