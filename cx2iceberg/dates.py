"""Dates and times: read without guessing.

Text only in the SAMM schema's format (`xsd:dateTime`: `2022-01-01T10:00:00`, optional fraction, `Z` or `±hh:mm`
up to ±14:00) or a date (`2022-01-01`, with an optional time zone). Python's `fromisoformat` accepted much more, and read
`2022-01-01+02:00` (a date with a time zone) as 02:00 on that day. Other formats: `to_timestamp: {format: ...}`.

* `timestamp` is an instant: with a time zone it is converted to UTC. Without one the format is valid in the schema,
  but the instant is unknown -> error, unless `input.naive_timestamps: utc` or an IANA time zone
  (`Europe/Berlin`); with a time zone, ambiguous or non-existent times at a clock change are errors. Each assumed
  value is counted. A date without a time used as a timestamp also needs that declaration (midnight).
* `24:00:00` (end of the day, valid in the schema) is the instant 00:00 of the next day. For `date/year/month/
  date_format` it is ambiguous (end of day D or start of D+1: `2022-12-31T24:00:00` would change year) -> error.
* `date`, `year`, `month`, `date_format` use the date/time as it was written (in the document's time zone):
  `2023-07-11T01:00:00+02:00` is the 11th (in UTC it would be the 10th). They need no time zone.
* A number is only a date with the unit declared: `to_timestamp: {unit: s}` or `{unit: ms}`.
* Fractions below the microsecond are truncated and counted (an Iceberg timestamp stores microseconds).
* Years outside 0001–9999 (valid in the schema) are not supported: error.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache

from .notes import note, setting

NAIVE_POLICIES = ("fail", "utc")          # or the name of an IANA time zone (e.g. Europe/Berlin)
UNITS = {"s": 1, "ms": 1000}
DATETIME_RE = re.compile(r"(-?\d{4,})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?", re.ASCII)
DATE_RE = re.compile(r"(-?\d{4,})-(\d{2})-(\d{2})(Z|[+-]\d{2}:\d{2})?", re.ASCII)
FORMAT_HINT = ("accepted format (the schema's xsd:dateTime): 2022-01-01T10:00:00Z, 2022-01-01T10:00:00.5+02:00 or "
               "2022-01-01; for another format declare to_timestamp: {format: ...}")


class DateError(ValueError):
    pass


@lru_cache(maxsize=512)
def zone(name: str):
    """IANA time zone (or None for 'utc'/'UTC'); ValueError if it does not exist."""
    if name in ("utc", "UTC"):
        return None
    import zoneinfo  # noqa: PLC0415

    try:
        return zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"unknown time zone: {name!r} (use fail, utc or an IANA name such as Europe/Berlin)") from None


def check_policy(policy: str) -> None:
    if policy != "fail":
        zone(policy)


def _offset(text: str | None, v: str) -> timezone | None:
    if text is None:
        return None
    if text == "Z":
        return timezone.utc
    hours, minutes = int(text[1:3]), int(text[4:6])
    if minutes > 59 or hours > 14 or (hours == 14 and minutes):
        raise DateError(f"'{v}': time zone {text} outside the schema's range (-14:00 to +14:00)")
    delta = timedelta(hours=hours, minutes=minutes)
    return timezone(-delta if text[0] == "-" else delta)


def _year(text: str, v: str) -> int:
    y = int(text)
    if not 1 <= y <= 9999:
        raise DateError(f"'{v}': year {y} is valid in the schema but not supported (0001 to 9999)")
    return y


def parse_text(v: str) -> tuple[datetime, str, bool]:
    """Text -> (datetime, 'datetime' | 'date', end_of_day). Only the schema's format."""
    s = v.strip()
    m = DATETIME_RE.fullmatch(s)
    if m:
        y, mo, d, hh, mi, ss, frac, tz = m.groups()
        end_of_day = hh == "24"
        try:
            if end_of_day:
                if mi != "00" or ss != "00" or (frac and frac.strip("0")):
                    raise DateError(f"'{v}': with hour 24 the time must be 24:00:00 (end of the day)")
                dt = datetime(_year(y, v), int(mo), int(d)) + timedelta(days=1)
            else:
                us = int((frac or "")[:6].ljust(6, "0"))
                dt = datetime(_year(y, v), int(mo), int(d), int(hh), int(mi), int(ss), us)
        except DateError:
            raise
        except (ValueError, OverflowError) as e:
            raise DateError(f"'{v}' is not a valid date/time ({e})") from None
        if frac and len(frac) > 6 and frac[6:].strip("0"):
            note("warning", "timestamp_precision", "truncated",
                 f"'{v}': fraction below the microsecond truncated (a timestamp stores microseconds)")
        tzinfo = _offset(tz, v)
        return (dt.replace(tzinfo=tzinfo) if tzinfo else dt), "datetime", end_of_day
    m = DATE_RE.fullmatch(s)
    if m:
        y, mo, d, tz = m.groups()
        try:
            dt = datetime(_year(y, v), int(mo), int(d))
        except DateError:
            raise
        except ValueError as e:
            raise DateError(f"'{v}' is not a valid date ({e})") from None
        tzinfo = _offset(tz, v)
        return (dt.replace(tzinfo=tzinfo) if tzinfo else dt), "date", False
    raise DateError(f"'{v}' is not a date/time in the schema's format; {FORMAT_HINT}")


def _parse(v, fmt: str | None = None, unit: str | None = None) -> tuple[datetime, str, bool]:
    """-> (datetime with or without a time zone, source: 'object' | 'date' | 'datetime' | 'number', end_of_day)."""
    if isinstance(v, bool):
        raise DateError(f"the boolean {str(v).lower()} is not a date")
    if isinstance(v, datetime):
        return v, "object", False
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day), "date", False
    if isinstance(v, (int, float, Decimal)):
        if unit is None:
            raise DateError(f"the number {v!r} is not a date: declare the unit with to_timestamp: {{unit: s}} or {{unit: ms}}")
        try:
            return datetime.fromtimestamp(float(v) / UNITS[unit], tz=timezone.utc), "number", False
        except (OverflowError, OSError, ValueError) as e:
            raise DateError(f"{v!r} ({unit}) is outside the range of dates: {e}") from None
    if not isinstance(v, str):
        raise DateError(f"{v!r} is not a date")
    if fmt:
        try:
            return datetime.strptime(v.strip(), fmt), "datetime", False
        except ValueError:
            raise DateError(f"'{v}' is not a date/time in the format {fmt!r}") from None
    return parse_text(v)


def _assume_zone(v, dt: datetime, what: str) -> datetime:
    """Date/time without a time zone -> instant, according to input.naive_timestamps (fail | utc | IANA time zone)."""
    policy = setting("naive_timestamps", "fail")
    if policy == "fail":
        raise DateError(f"'{v}': {what} has no time zone (it is valid in the schema, but the instant is unknown); "
                        f"declare input.naive_timestamps: utc or the sender's time zone (e.g. Europe/Berlin)")
    tz = zone(policy)
    if tz is None:
        note("declared", "naive_timestamp", "assumed_utc", f"'{v}': {what} without a time zone, read as UTC")
        return dt
    first, second = dt.replace(tzinfo=tz, fold=0), dt.replace(tzinfo=tz, fold=1)
    if first.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != dt:
        raise DateError(f"'{v}': this time does not exist in {policy} (clock change)")
    if first.utcoffset() != second.utcoffset():
        raise DateError(f"'{v}': this time is ambiguous in {policy} (it happens twice at the clock change)")
    note("declared", "naive_timestamp", "assumed_zone", f"'{v}': {what} without a time zone, read in time zone {policy}")
    return first.astimezone(timezone.utc).replace(tzinfo=None)


def to_timestamp(v, fmt: str | None = None, unit: str | None = None) -> datetime:
    """Instant in UTC (datetime without tzinfo, as Iceberg stores it)."""
    dt, origin, _ = _parse(v, fmt, unit)
    if origin == "date":
        # a date has no time: as an instant it becomes midnight, which must also be declared
        if dt.tzinfo is None:
            return _assume_zone(v, dt, "the date (no time, becomes midnight)")
        if setting("naive_timestamps", "fail") == "fail":
            raise DateError(f"'{v}': it is a date without a time; as an instant it would be midnight (in time zone {dt.tzinfo}); "
                            f"declare input.naive_timestamps (utc or a time zone) to accept it")
        note("declared", "naive_timestamp", "assumed_midnight", f"'{v}': date without a time, read as midnight")
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    if origin == "object":
        return dt                                     # already converted by an earlier transformation (UTC)
    return _assume_zone(v, dt, "the date/time")


def wall(v, fmt: str | None = None, unit: str | None = None) -> datetime:
    """Date/time as it was written (in the document's time zone), without tzinfo."""
    dt, _, end_of_day = _parse(v, fmt, unit)
    if end_of_day:
        raise DateError(f"'{v}': 24:00:00 is the end of that day and the start of the next, so the date is ambiguous; "
                        f"for the instant use to_timestamp (it becomes 00:00 of the next day)")
    return dt.replace(tzinfo=None)


def to_date(v, fmt: str | None = None) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    return wall(v, fmt).date()
