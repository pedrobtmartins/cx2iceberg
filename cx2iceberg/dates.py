"""Datas e horas: leitura sem adivinhar.

Texto só no formato do schema SAMM (`xsd:dateTime`: `2022-01-01T10:00:00`, fracção opcional, `Z` ou `±hh:mm`
até ±14:00) ou data (`2022-01-01`, com fuso opcional). O `fromisoformat` do Python aceitava muito mais, e lia
`2022-01-01+02:00` (uma data com fuso) como as 02:00 desse dia. Outros formatos: `to_timestamp: {format: ...}`.

* `timestamp` é um instante: com fuso é convertido para UTC. Sem fuso o formato é válido no schema, mas o
  instante é desconhecido -> erro, a não ser com `input.naive_timestamps: utc` ou um fuso IANA
  (`Europe/Berlin`); com um fuso, horas ambíguas ou inexistentes na mudança de hora são erro. Cada valor
  assumido fica contado. Uma data sem hora usada como timestamp também precisa dessa declaração (meia-noite).
* `24:00:00` (fim do dia, válido no schema) é o instante 00:00 do dia seguinte. Para `date/year/month/
  date_format` é ambíguo (fim do dia D ou início de D+1: `2022-12-31T24:00:00` mudaria de ano) -> erro.
* `date`, `year`, `month`, `date_format` usam a data/hora tal como foi escrita (no fuso do documento):
  `2023-07-11T01:00:00+02:00` é o dia 11 (em UTC seria o dia 10). Não precisam de fuso.
* Um número só é data com a unidade declarada: `to_timestamp: {unit: s}` ou `{unit: ms}`.
* Fracções abaixo do microssegundo ficam cortadas e contadas (um timestamp Iceberg guarda microssegundos).
* Anos fora de 0001–9999 (válidos no schema) não são suportados: erro.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache

from .notes import note, setting

NAIVE_POLICIES = ("fail", "utc")          # ou o nome de um fuso IANA (ex.: Europe/Berlin)
UNITS = {"s": 1, "ms": 1000}
DATETIME_RE = re.compile(r"(-?\d{4,})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?", re.ASCII)
DATE_RE = re.compile(r"(-?\d{4,})-(\d{2})-(\d{2})(Z|[+-]\d{2}:\d{2})?", re.ASCII)
FORMAT_HINT = ("formato aceite (xsd:dateTime do schema): 2022-01-01T10:00:00Z, 2022-01-01T10:00:00.5+02:00 ou "
               "2022-01-01; para outro formato declare to_timestamp: {format: ...}")


class DateError(ValueError):
    pass


@lru_cache(maxsize=512)
def zone(name: str):
    """Fuso IANA (ou None para 'utc'/'UTC'); ValueError se não existir."""
    if name in ("utc", "UTC"):
        return None
    import zoneinfo  # noqa: PLC0415

    try:
        return zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"fuso horário desconhecido: {name!r} (use fail, utc ou um nome IANA como Europe/Berlin)") from None


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
        raise DateError(f"'{v}': fuso {text} fora do intervalo do schema (-14:00 a +14:00)")
    delta = timedelta(hours=hours, minutes=minutes)
    return timezone(-delta if text[0] == "-" else delta)


def _year(text: str, v: str) -> int:
    y = int(text)
    if not 1 <= y <= 9999:
        raise DateError(f"'{v}': o ano {y} é válido no schema mas não é suportado (0001 a 9999)")
    return y


def parse_text(v: str) -> tuple[datetime, str, bool]:
    """Texto -> (datetime, 'datetime' | 'date', fim_do_dia). Só o formato do schema."""
    s = v.strip()
    m = DATETIME_RE.fullmatch(s)
    if m:
        y, mo, d, hh, mi, ss, frac, tz = m.groups()
        end_of_day = hh == "24"
        try:
            if end_of_day:
                if mi != "00" or ss != "00" or (frac and frac.strip("0")):
                    raise DateError(f"'{v}': com 24 as horas têm de ser 24:00:00 (fim do dia)")
                dt = datetime(_year(y, v), int(mo), int(d)) + timedelta(days=1)
            else:
                us = int((frac or "")[:6].ljust(6, "0"))
                dt = datetime(_year(y, v), int(mo), int(d), int(hh), int(mi), int(ss), us)
        except DateError:
            raise
        except (ValueError, OverflowError) as e:
            raise DateError(f"'{v}' não é uma data/hora válida ({e})") from None
        if frac and len(frac) > 6 and frac[6:].strip("0"):
            note("warning", "timestamp_precision", "truncated",
                 f"'{v}': fracção abaixo do microssegundo cortada (um timestamp guarda microssegundos)")
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
            raise DateError(f"'{v}' não é uma data válida ({e})") from None
        tzinfo = _offset(tz, v)
        return (dt.replace(tzinfo=tzinfo) if tzinfo else dt), "date", False
    raise DateError(f"'{v}' não é uma data/hora no formato do schema; {FORMAT_HINT}")


def _parse(v, fmt: str | None = None, unit: str | None = None) -> tuple[datetime, str, bool]:
    """-> (datetime com ou sem fuso, origem: 'object' | 'date' | 'datetime' | 'number', fim_do_dia)."""
    if isinstance(v, bool):
        raise DateError(f"o booleano {str(v).lower()} não é uma data")
    if isinstance(v, datetime):
        return v, "object", False
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day), "date", False
    if isinstance(v, (int, float, Decimal)):
        if unit is None:
            raise DateError(f"o número {v!r} não é uma data: declare a unidade com to_timestamp: {{unit: s}} ou {{unit: ms}}")
        try:
            return datetime.fromtimestamp(float(v) / UNITS[unit], tz=timezone.utc), "number", False
        except (OverflowError, OSError, ValueError) as e:
            raise DateError(f"{v!r} ({unit}) está fora do intervalo de datas: {e}") from None
    if not isinstance(v, str):
        raise DateError(f"{v!r} não é uma data")
    if fmt:
        try:
            return datetime.strptime(v.strip(), fmt), "datetime", False
        except ValueError:
            raise DateError(f"'{v}' não é uma data/hora no formato {fmt!r}") from None
    return parse_text(v)


def _assume_zone(v, dt: datetime, what: str) -> datetime:
    """Data/hora sem fuso -> instante, segundo input.naive_timestamps (fail | utc | fuso IANA)."""
    policy = setting("naive_timestamps", "fail")
    if policy == "fail":
        raise DateError(f"'{v}': {what} não tem fuso horário (é válido no schema, mas o instante é desconhecido); "
                        f"declare input.naive_timestamps: utc ou o fuso do emissor (ex.: Europe/Berlin)")
    tz = zone(policy)
    if tz is None:
        note("declared", "naive_timestamp", "assumed_utc", f"'{v}': {what} sem fuso, lida como UTC")
        return dt
    first, second = dt.replace(tzinfo=tz, fold=0), dt.replace(tzinfo=tz, fold=1)
    if first.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != dt:
        raise DateError(f"'{v}': esta hora não existe em {policy} (mudança de hora)")
    if first.utcoffset() != second.utcoffset():
        raise DateError(f"'{v}': esta hora é ambígua em {policy} (acontece duas vezes na mudança de hora)")
    note("declared", "naive_timestamp", "assumed_zone", f"'{v}': {what} sem fuso, lida no fuso {policy}")
    return first.astimezone(timezone.utc).replace(tzinfo=None)


def to_timestamp(v, fmt: str | None = None, unit: str | None = None) -> datetime:
    """Instante em UTC (datetime sem tzinfo, como o Iceberg o guarda)."""
    dt, origin, _ = _parse(v, fmt, unit)
    if origin == "date":
        # uma data não tem hora: como instante fica a meia-noite, o que também tem de ser declarado
        if dt.tzinfo is None:
            return _assume_zone(v, dt, "a data (sem hora, fica a meia-noite)")
        if setting("naive_timestamps", "fail") == "fail":
            raise DateError(f"'{v}': é uma data sem hora; como instante ficaria a meia-noite (no fuso {dt.tzinfo}); "
                            f"declare input.naive_timestamps (utc ou um fuso) para o aceitar")
        note("declared", "naive_timestamp", "assumed_midnight", f"'{v}': data sem hora, lida como meia-noite")
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    if origin == "object":
        return dt                                     # já convertido por uma transformação anterior (UTC)
    return _assume_zone(v, dt, "a data/hora")


def wall(v, fmt: str | None = None, unit: str | None = None) -> datetime:
    """Data/hora tal como foi escrita (no fuso do documento), sem tzinfo."""
    dt, _, end_of_day = _parse(v, fmt, unit)
    if end_of_day:
        raise DateError(f"'{v}': 24:00:00 é o fim desse dia e o início do seguinte, por isso a data é ambígua; "
                        f"para o instante use to_timestamp (fica 00:00 do dia seguinte)")
    return dt.replace(tzinfo=None)


def to_date(v, fmt: str | None = None) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    return wall(v, fmt).date()
