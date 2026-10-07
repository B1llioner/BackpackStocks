from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
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
        if (
            start is None
            or end is None
            or time.fromisoformat(start) <= local.time() < time.fromisoformat(end)
        ):
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


class ActiveSessionCache:
    """Reuse a calendar decision within a second, never across changed inputs or boundaries."""

    def __init__(self):
        self.sessions: list[dict] | None = None
        self.holidays: list[dict] | None = None
        self.bucket: tuple | None = None
        self.last_now: datetime | None = None
        self.session: str | None = None
        self.fractional = False

    def get(self, sessions: list[dict], holidays: list[dict], now: datetime) -> str | None:
        # C-level content comparisons avoid reparsing every calendar per asset.
        # Deep snapshots also detect nested in-place changes; metadata freshness
        # and asset quantity rules remain the caller's live checks.
        unchanged = sessions == self.sessions and holidays == self.holidays
        if not unchanged:
            fractional = any(
                isinstance(value, str) and ("." in value or "," in value)
                for row in [*sessions, *holidays]
                for value in (row.get("startTime"), row.get("endTime"))
            )
        else:
            fractional = self.fractional
        utc = now if now.tzinfo is UTC else now.astimezone(UTC)
        bucket = utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second
        if (
            unchanged
            and bucket == self.bucket
            and self.last_now is not None
            and utc >= self.last_now
            and not fractional
            and now.tzinfo is not None
        ):
            self.last_now = utc
            return self.session
        # Fractional schedules always take the exact original path. Cache state
        # is committed only after successful parsing/evaluation, including closed.
        session = active_session(sessions, holidays, now)
        if not unchanged:
            self.sessions, self.holidays = deepcopy(sessions), deepcopy(holidays)
        self.bucket, self.last_now = bucket, utc
        self.session, self.fractional = session, fractional
        return session


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
