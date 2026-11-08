"""Step 2: numbers — parse_number, JsonFloat, casts and numeric transformations."""
import copy
import json
import pickle
import random
from decimal import Decimal

import pytest

from cx2iceberg import ConfigError, Document, Engine, from_dict
from cx2iceberg import athena_types as T
from cx2iceberg.numbers import DBL_DIG, JsonFloat, NumberError, json_float, parse_number, to_decimal, to_float
from cx2iceberg.reader import loads
from cx2iceberg.transforms import apply_chain


# --- parse_number: JSON grammar, no guessing ---------------------------------
@pytest.mark.parametrize("text,expected", [
    ("0", "0"), ("-0", "-0"), ("12", "12"), ("-1.5", "-1.5"), ("2e3", "2E+3"), ("1E-2", "0.01"), ("1.5e+2", "150"),
    ("0.125", "0.125"), ("1.2345", "1.2345"), ("1234.567", "1234.567"), (" 2.5 ", "2.5"), ("\t7\n", "7"),
    ("9" * 60, "9" * 60),                                   # no limit on digits (Decimal)
    ("0.12345678901234567890123", "0.12345678901234567890123"),
])
def test_parse_number_accepts_json_numbers_exactly(text, expected):
    assert parse_number(text) == Decimal(expected)


@pytest.mark.parametrize("text,fragment", [
    ("1.234", "ambiguous"), ("12.345", "ambiguous"), ("-1.234", "ambiguous"), ("1,234", "ambiguous"), ("1.234.567", "ambiguous"),
    ("1,5", "comma"), ("1.234,5", "comma"), ("1,234.5", "comma"),
    ("1_000", "is not a number"), ("+5", "is not a number"), ("007", "is not a number"), ("0x10", "is not a number"),
    (".5", "is not a number"), ("5.", "is not a number"), ("1e", "is not a number"), ("5 kg", "is not a number"),
    ("\u0661\u0662\u0663", "digits 0-9 only"),              # Arabic digits: float() accepted them
    ("\uff11\uff12\uff13", "digits 0-9 only"),              # full width: int()/Decimal() accepted them
    ("1\u0662\u0663", "digits 0-9 only"),                    # 1st digit ASCII, the others Arabic
    ("1.5\uff15", "digits 0-9 only"),                        # full width after the point
    ("\u22125", "is not a number"),                        # Unicode minus sign
    ("\u00a01.5", "is not a number"),                      # non-breaking space (Python's strip() accepted it)
    ("NaN", "is not a finite number"), ("-inf", "is not a finite number"), ("Infinity", "is not a finite number"),
    ("", "empty"), ("   ", "empty"),
])
def test_parse_number_refuses_ambiguous_or_non_numbers(text, fragment):
    with pytest.raises(NumberError, match=fragment):
        parse_number(text)


@pytest.mark.parametrize("text,fmt,expected", [
    ("1.234,5", {"decimal": ",", "thousands": "."}, "1234.5"),
    ("12.345.678", {"decimal": ",", "thousands": "."}, "12345678"),
    ("-1.234", {"decimal": ",", "thousands": "."}, "-1234"),
    ("1,5", {"decimal": ","}, "1.5"),
    ("1 234,5", {"decimal": ",", "thousands": " "}, "1234.5"),
    ("1\u00a0234,5", {"decimal": ",", "thousands": "\u00a0"}, "1234.5"),
    ("1'234.5", {"decimal": ".", "thousands": "'"}, "1234.5"),
    ("1,234.5", {"decimal": ".", "thousands": ","}, "1234.5"),
    ("1.234", {"decimal": "."}, "1.234"),                  # declaring the decimal resolves the ambiguity
    ("1,5e3", {"decimal": ","}, "1500"),
])
def test_parse_number_with_declared_format(text, fmt, expected):
    assert parse_number(text, **fmt) == Decimal(expected)


@pytest.mark.parametrize("text,fmt,fragment", [
    ("1.23,4", {"decimal": ",", "thousands": "."}, "groups of 3"),
    ("1234.5", {"decimal": ",", "thousands": "."}, "groups of 3"),   # '.' declared as thousands: is '1234.5' 12345?
    ("1,2,3", {"decimal": ","}, "more than one decimal separator"),
    ("1.234,5", {"decimal": ","}, "was not declared"),
    ("1,234", {"decimal": "."}, "was not declared"),
    (",5", {"decimal": ","}, "is not a number"),
])
def test_parse_number_format_errors(text, fmt, fragment):
    with pytest.raises(NumberError, match=fragment):
        parse_number(text, **fmt)


def test_to_float_refuses_overflow_underflow_and_non_finite():
    assert to_float("1e308") == 1e308 and to_float(" -2.5 ") == -2.5
    for bad in ("1e400", "-1e400", "1e-400", "0." + "0" * 400 + "1"):
        with pytest.raises(NumberError):
            to_float(bad)
    assert to_float("0e-400") == 0.0                        # zero written as zero is not a loss
    for bad in (float("inf"), float("nan"), 1e308 * 10):
        with pytest.raises(NumberError, match="finite"):
            to_float(bad)


# --- JsonFloat: text kept only when the double may have lost digits -----------
def test_json_float_keeps_text_only_beyond_15_significant_digits():
    assert type(json_float("0.1")) is float and type(json_float("123456789012345")) is float
    assert type(json_float("1.000000000000000000")) is float   # trailing zeros do not count
    x = json_float("0.12345678901234567890123")
    assert isinstance(x, JsonFloat) and x.text == "0.12345678901234567890123" and x == 0.12345678901234568
    with pytest.raises(NumberError, match="out of range"):
        json_float("1e400")
    with pytest.raises(NumberError, match="too small"):
        json_float("1e-400")


def test_dbl_dig_guarantee_behind_the_shortcut():
    # the shortcut's justification: with <= 15 significant digits, repr(float(text)) has the same value
    rnd = random.Random(7)
    for _ in range(20000):
        digits = str(rnd.randint(1, 10 ** rnd.randint(1, DBL_DIG) - 1))
        text = f"{digits[:1]}.{digits[1:] or '0'}e{rnd.randint(-300, 300)}"
        assert Decimal(repr(float(text))) == Decimal(text), text


def test_json_float_behaves_like_a_float():
    x = json_float("12345678901234567890.5")
    assert x + 1 == float(x) + 1 and isinstance(x * 2, float) and hash(x) == hash(float(x))
    assert json.dumps({"v": x}) == json.dumps({"v": float(x)})       # content UUIDs and record_hash do not change
    for clone in (copy.deepcopy(x), pickle.loads(pickle.dumps(x))):
        assert isinstance(clone, JsonFloat) and clone.text == x.text and clone == x


# --- casts ------------------------------------------------------------------------
@pytest.mark.parametrize("raw,t,expected", [
    ('{"v": 1234567890123456789.0}', "bigint", 1234567890123456789),       # before: 1234567890123456768
    ('{"v": 0.12345678901234567890123}', "decimal(38,23)", Decimal("0.12345678901234567890123")),
    ('{"v": 1E+2}', "decimal(5,1)", Decimal("100.0")),
    ('{"v": "1e3"}', "int", 1000),
    ('{"v": " 12 "}', "int", 12),
    ('{"v": "2.50"}', "decimal(10,2)", Decimal("2.50")),
    ('{"v": "-0"}', "double", -0.0),
])
def test_casts_are_exact(raw, t, expected):
    v = T.cast(loads(raw)["v"], t)
    assert v == expected and type(v) is type(expected)


@pytest.mark.parametrize("value,t,fragment", [
    ("12_000", "bigint", "is not a number"), ("\uff11\uff12", "int", "0-9"), ("x", "int", "is not a number"),
    ("1.5", "int", "is not an integer"), ("NaN", "double", "finite"), ("1,5", "double", "comma"),
    ("1e-400", "double", "small"), ("1e400", "decimal(38,2)", "does not fit"), ([1], "double", "is not a number"),
    ({"value": 1}, "int", "is not a number"),
])
def test_casts_refuse_with_a_reason(value, t, fragment):
    with pytest.raises((ValueError, TypeError), match=fragment):
        T.cast(value, t)


def test_blank_text_is_null_only_for_numeric_types():
    assert T.cast("", "double") is None and T.cast("  ", "int") is None and T.cast(" ", "decimal(5,2)") is None
    assert T.cast("", "timestamp") is None                  # as before
    with pytest.raises(ValueError):
        T.cast("   ", "timestamp")                          # only spaces in a date: still an error


# --- transformations -----------------------------------------------------------------
def test_numeric_transforms_use_the_same_rules():
    assert apply_chain("12", "to_number") == 12 and apply_chain("1.50", "to_number") == Decimal("1.50")
    assert apply_chain("1.234,5", {"to_number": {"decimal": ",", "thousands": "."}}) == Decimal("1234.5")
    assert apply_chain({"value": "2500", "unit": "unit:gram"}, {"unit_convert": {"to": "kilogram"}}) == 2.5
    for chain in ("to_number", {"multiply": 2}, {"add": 1}, {"round": {"digits": 1, "mode": "half_up"}}, "abs", "sum",
                  {"clamp": {"min": 0}}):
        with pytest.raises(NumberError):                    # before: '1,5' silently became 1.5
            apply_chain(["1,5"] if chain == "sum" else "1,5", chain)


@pytest.mark.parametrize("transform,fragment", [
    ("uper", "transformation: 'uper'"),
    ({"to_number": {"decimal": ";"}}, "decimal must be"),
    ({"to_number": {"decimal": ",", "thousands": ","}}, "same separator"),
    ({"to_number": {"sep": ","}}, "to_number accepts"),
    ([{"to_number": None}, "lowr"], "transformation: 'lowr'"),
])
def test_transform_names_and_arguments_are_checked_when_loading(transform, fragment):
    cols = {"v": {"path": "v", "transform": transform, "on_error": "null"}}   # before: silently set everything to null
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": cols}]}]})
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": {"cases": [{"path": "v", "transform": transform}]}}}]}]})


# --- report: numbers as text and empty text ----------------------------------------
def test_numbers_as_text_and_empty_text_are_reported():
    cfg = from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {
        "a": "a ::double", "b": "b ::int", "c": {"path": "c", "transform": {"to_number": {"decimal": ","}}, "type": "double"}}}]}]})
    eng = Engine(cfg)
    rows = eng.run([Document({"a": " 2.5 ", "b": "", "c": "1,5"}, "f.json")])["t"].rows
    assert rows == [{"a": 2.5, "b": None, "c": 1.5}]
    rep = eng.report
    assert rep.count(reason="number_as_text", column="a") == 1 and rep.count(reason="empty_text", column="b") == 1
    assert rep.count(column="c") == 0                       # conversion declared with to_number: not a warning


def test_numbers_as_text_inside_arrays_are_reported():
    cfg = from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {"xs": "xs ::array<double>"}}]}]})
    eng = Engine(cfg)
    assert eng.run([Document({"xs": [1, "2.5", ""]}, "f.json")])["t"].rows == [{"xs": [1.0, 2.5, None]}]
    assert eng.report.count(reason="number_as_text") == 1 and eng.report.count(reason="empty_text") == 1
