"""Utilities for session handling and 5-minute bucketization."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Sequence

from zoneinfo import ZoneInfo


_US_EASTERN = ZoneInfo("America/New_York")


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    count = 0
    while True:
        if current.weekday() == weekday:
            count += 1
            if count == n:
                return current
        current += timedelta(days=1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    current = next_month - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _normalize_holidays(year: int) -> Sequence[date]:
    """Return a conservative list of US market holidays for the supplied year."""

    fixed = [date(year, 1, 1), date(year, 7, 4), date(year, 12, 25)]
    observed: List[date] = []
    for day in fixed:
        if day.weekday() == 5:
            observed.append(day - timedelta(days=1))
        elif day.weekday() == 6:
            observed.append(day + timedelta(days=1))
        else:
            observed.append(day)

    observed.extend(
        [
            _nth_weekday(year, 1, 0, 3),  # MLK Day
            _nth_weekday(year, 2, 0, 3),  # Presidents' Day
            _last_weekday(year, 5, 0),  # Memorial Day
            _nth_weekday(year, 9, 0, 1),  # Labor Day
            _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        ]
    )
    return observed


@dataclass
class SessionCalendar:
    """Session calendar for US equities RTH with 5-minute buckets."""

    rth_open: time = time(9, 30)
    rth_close: time = time(16, 0)
    bucket_minutes: int = 5

    def __post_init__(self) -> None:
        if self.bucket_minutes <= 0:
            raise ValueError("bucket_minutes must be positive")
        self._holidays_cache: dict[int, Sequence[date]] = {}

    def is_trading_day(self, session_day: date) -> bool:
        if session_day.weekday() >= 5:
            return False
        holidays = self._holidays_cache.setdefault(
            session_day.year, _normalize_holidays(session_day.year)
        )
        return session_day not in holidays

    def session_open(self, session_day: date) -> datetime:
        naive = datetime.combine(session_day, self.rth_open)
        eastern = naive.replace(tzinfo=_US_EASTERN)
        return eastern.astimezone(timezone.utc)

    def session_close(self, session_day: date) -> datetime:
        naive = datetime.combine(session_day, self.rth_close)
        eastern = naive.replace(tzinfo=_US_EASTERN)
        return eastern.astimezone(timezone.utc)

    def buckets(self, session_day: date) -> List[datetime]:
        open_utc = self.session_open(session_day)
        close_utc = self.session_close(session_day)
        delta = timedelta(minutes=self.bucket_minutes)
        buckets: List[datetime] = []
        current = open_utc
        while current < close_utc:
            buckets.append(current)
            current += delta
        return buckets

    def bucketize(self, ts: datetime) -> datetime:
        ts_utc = _ensure_utc(ts)
        session_day = self.session_date_from_ts(ts_utc)
        if session_day is None:
            return ts_utc
        open_utc = self.session_open(session_day)
        delta = ts_utc - open_utc
        minutes = (delta.total_seconds() // (self.bucket_minutes * 60))
        floored = open_utc + timedelta(minutes=int(minutes * self.bucket_minutes))
        return floored

    def in_rth(self, ts: datetime) -> bool:
        ts_utc = _ensure_utc(ts)
        local = ts_utc.astimezone(_US_EASTERN)
        session_day = local.date()
        if not self.is_trading_day(session_day):
            return False
        open_local = datetime.combine(session_day, self.rth_open, tzinfo=_US_EASTERN)
        close_local = datetime.combine(session_day, self.rth_close, tzinfo=_US_EASTERN)
        return open_local <= local < close_local

    def session_date_from_ts(self, ts: datetime) -> date | None:
        ts_utc = _ensure_utc(ts)
        local = ts_utc.astimezone(_US_EASTERN)
        session_day = local.date()
        if self.in_rth(ts_utc):
            return session_day
        if local.time() >= self.rth_close and self.is_trading_day(session_day):
            return session_day
        if local.time() < self.rth_open:
            previous = session_day - timedelta(days=1)
            while not self.is_trading_day(previous):
                previous -= timedelta(days=1)
            return previous
        return None


DEFAULT_CALENDAR = SessionCalendar()

__all__ = ["SessionCalendar", "DEFAULT_CALENDAR"]
