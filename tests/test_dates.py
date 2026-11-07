"""Datas: só o formato do schema (xsd:dateTime), 24:00:00, datas com fuso e fusos IANA para horas sem fuso."""
import json
import re
from datetime import date, datetime
from pathlib import Path

import pytest

from cx2iceberg import ConfigError, Document, Engine, from_dict
from cx2iceberg import athena_types as T
from cx2iceberg import notes as N
from cx2iceberg.transforms import apply_chain

EX = Path(__file__).parent.parent / "examples"
SCHEMA_TIMESTAMP = re.compile(json.loads((EX / "schemas" / "pcf_9.0.0-schema.json").read_text())
                              ["components"]["schemas"]["Timestamp"]["pattern"])


def ts(value, **settings):
    with N.scope(settings) as notes:
        out = T.cast(value, "timestamp")
    return out, [n[1:3] for n in notes]


def refuses(value, fragment, t="timestamp", **settings):
    with N.scope(settings), pytest.raises(ValueError) as e:
        T.cast(value, t)
    assert fragment in str(e.value), str(e.value)


# --- o formato aceite é o do schema ---------------------------------------------------
@pytest.mark.parametrize("text,expected", [
    ("2022-01-01T10:00:00Z", datetime(2022, 1, 1, 10)),
    ("2022-01-01T10:00:00.5+02:00", datetime(2022, 1, 1, 8, 0, 0, 500000)),
    ("2022-01-01T10:00:00-05:30", datetime(2022, 1, 1, 15, 30)),
    ("2022-01-01T10:00:00+14:00", datetime(2021, 12, 31, 20)), ("2022-01-01T10:00:00-14:00", datetime(2022, 1, 2, 0)),
    ("2022-01-01T24:00:00Z", datetime(2022, 1, 2)),                       # fim do dia = 00:00 do dia seguinte
    ("2022-12-31T24:00:00.000+01:00", datetime(2022, 12, 31, 23)),          # antes: rejeitado (hour must be in 0..23)
    ("2024-02-29T12:00:00Z", datetime(2024, 2, 29, 12)),
])
def test_schema_format_is_accepted(text, expected):
    assert SCHEMA_TIMESTAMP.fullmatch(text)
    assert ts(text) == (expected, [])


@pytest.mark.parametrize("text", [
    "2022-01-01 10:00:00Z", "2022-01-01t10:00:00z", "2022-01-01T10:00Z", "2022-01-01T10Z", "20220101T100000Z",
    "2022-01-01T10:00:00+0200", "2022-01-01T10:00:00+02", "2022-01-01T10:00:00,5Z", "2022-W01-1T10:00:00Z",
    "2022-001T10:00:00Z", "03/02/2022",
])
def test_what_the_schema_refuses_is_refused(text):
    assert not SCHEMA_TIMESTAMP.fullmatch(text)               # o fromisoformat do Python aceitava quase todos
    refuses(text, "não é uma data/hora no formato do schema")


@pytest.mark.parametrize("text,fragment", [
    ("2022-01-01T10:00:00+15:00", "fuso +15:00 fora do intervalo"), ("2022-01-01T10:00:00+14:30", "fora do intervalo"),
    ("2022-01-01T23:59:60Z", "não é uma data/hora válida"), ("2022-02-30T10:00:00Z", "não é uma data/hora válida"),
    ("2022-01-01T24:00:01Z", "24:00:00 (fim do dia)"), ("2022-01-01T24:30:00Z", "24:00:00"),
    ("0000-01-01T00:00:00Z", "o ano 0 é válido no schema mas não é suportado"),
    ("10000-01-01T00:00:00Z", "o ano 10000 é válido no schema mas não é suportado"),
])
def test_invalid_or_unsupported_values(text, fragment):
    refuses(text, fragment)


def test_date_with_timezone_is_no_longer_misread_as_a_time():
    # antes: fromisoformat('2022-01-01+02:00') -> 2022-01-01 02:00 (o fuso lido como hora)
    assert T.cast("2022-01-01+02:00", "date") == date(2022, 1, 1)
    assert apply_chain("2022-01-01-05:00", "year") == 2022
    refuses("2022-01-01+02:00", "é uma data sem hora")          # como instante: meia-noite, tem de ser declarado
    assert ts("2022-01-01+02:00", naive_timestamps="utc") == (datetime(2021, 12, 31, 22), [("naive_timestamp", "assumed_midnight")])


# --- 24:00:00 -----------------------------------------------------------------------------
def test_end_of_day_is_an_instant_but_an_ambiguous_date():
    assert ts("2022-12-31T24:00:00Z")[0] == datetime(2023, 1, 1)
    for chain in ("year", "to_date", {"date_format": "%Y"}):
        with pytest.raises(ValueError, match="24:00:00 é o fim desse dia e o início do seguinte"):
            apply_chain("2022-12-31T24:00:00Z", chain)               # 2022 ou 2023? não se adivinha
    refuses("2022-12-31T24:00:00Z", "ambígua", t="date")
    assert ts("2022-12-31T24:00:00", naive_timestamps="utc")[0] == datetime(2023, 1, 1)


# --- sem fuso: válido no schema, instante desconhecido ----------------------------------------------
def test_naive_timestamp_message_says_it_is_schema_valid():
    refuses("2022-01-01T10:00:00", "é válido no schema, mas o instante é desconhecido")
    refuses("2022-01-01T10:00:00", "input.naive_timestamps: utc ou o fuso do emissor")


@pytest.mark.parametrize("text,zone,expected", [
    ("2022-06-01T12:00:00", "Europe/Berlin", datetime(2022, 6, 1, 10)),      # verão: +02:00
    ("2022-01-15T12:00:00", "Europe/Berlin", datetime(2022, 1, 15, 11)),     # inverno: +01:00
    ("2022-06-01T12:00:00", "America/New_York", datetime(2022, 6, 1, 16)),
    ("2022-06-01T12:00:00", "UTC", datetime(2022, 6, 1, 12)),
])
def test_naive_timestamps_in_a_declared_zone(text, zone, expected):
    out, notes = ts(text, naive_timestamps=zone)
    assert out == expected and notes == [("naive_timestamp", "assumed_utc" if zone == "UTC" else "assumed_zone")]


@pytest.mark.parametrize("text,fragment", [
    ("2022-10-30T02:30:00", "ambígua"),          # Berlim: 03:00 -> 02:00, as 02:30 acontecem duas vezes
    ("2022-03-27T02:30:00", "não existe"),       # Berlim: 02:00 -> 03:00, as 02:30 não existem
])
def test_daylight_saving_gaps_and_overlaps_are_errors(text, fragment):
    refuses(text, fragment, naive_timestamps="Europe/Berlin")


def test_zone_policy_is_validated_when_loading():
    from_dict({"input": {"naive_timestamps": "Europe/Lisbon"}, "submodels": [{"tables": [{"name": "t", "auto": True}]}]})
    for bad in ("Europe/Lisboa", "local", "+02:00"):
        with pytest.raises(ConfigError, match="fuso horário desconhecido"):
            from_dict({"input": {"naive_timestamps": bad}, "submodels": [{"tables": [{"name": "t", "auto": True}]}]})


def test_zone_policy_in_a_load_is_counted_per_value():
    eng = Engine(from_dict({"input": {"naive_timestamps": "Europe/Berlin"},
                            "submodels": [{"name": "s", "tables": [{"name": "t", "columns": {"at": "at ::timestamp"}}]}]}))
    rows = eng.run([Document({"at": "2022-06-01T12:00:00"}, "a"), Document({"at": "2022-06-01T12:00:00Z"}, "b")])["t"].rows
    assert rows == [{"at": datetime(2022, 6, 1, 10)}, {"at": datetime(2022, 6, 1, 12)}]
    assert eng.report.count(action="assumed_zone") == 1              # só o que não trazia fuso


# --- modo auto: só o formato do schema é detectado como data -------------------------------
def test_auto_detects_only_schema_dates():
    eng = Engine(from_dict({"input": {"naive_timestamps": "utc"}, "submodels": [{"name": "s", "tables": [{"name": "t", "auto": True}]}]}))
    row = eng.run([Document({"a": "2022-01-01T10:00:00Z", "b": "2022-01-01 10:00", "c": "2022-01-01"}, "x")])["t"].rows[0]
    assert row == {"a": datetime(2022, 1, 1, 10), "b": "2022-01-01 10:00", "c": "2022-01-01"}   # b e c ficam texto


def test_unquoted_dates_in_yaml_stay_text():
    from cx2iceberg.config import load_yaml

    assert load_yaml("a: 2025-01-01\nb: 2022-01-01T10:00:00Z") == {"a": "2025-01-01", "b": "2022-01-01T10:00:00Z"}
