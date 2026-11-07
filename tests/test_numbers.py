"""Passo 2: números — parse_number, JsonFloat, casts e transformações numéricas."""
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


# --- parse_number: gramática JSON, sem adivinhar ---------------------------------
@pytest.mark.parametrize("text,expected", [
    ("0", "0"), ("-0", "-0"), ("12", "12"), ("-1.5", "-1.5"), ("2e3", "2E+3"), ("1E-2", "0.01"), ("1.5e+2", "150"),
    ("0.125", "0.125"), ("1.2345", "1.2345"), ("1234.567", "1234.567"), (" 2.5 ", "2.5"), ("\t7\n", "7"),
    ("9" * 60, "9" * 60),                                   # sem limite de dígitos (Decimal)
    ("0.12345678901234567890123", "0.12345678901234567890123"),
])
def test_parse_number_accepts_json_numbers_exactly(text, expected):
    assert parse_number(text) == Decimal(expected)


@pytest.mark.parametrize("text,fragment", [
    ("1.234", "ambíguo"), ("12.345", "ambíguo"), ("-1.234", "ambíguo"), ("1,234", "ambíguo"), ("1.234.567", "ambíguo"),
    ("1,5", "vírgula"), ("1.234,5", "vírgula"), ("1,234.5", "vírgula"),
    ("1_000", "não é um número"), ("+5", "não é um número"), ("007", "não é um número"), ("0x10", "não é um número"),
    (".5", "não é um número"), ("5.", "não é um número"), ("1e", "não é um número"), ("5 kg", "não é um número"),
    ("\u0661\u0662\u0663", "só dígitos 0-9"),              # dígitos árabes: float() aceitava
    ("\uff11\uff12\uff13", "só dígitos 0-9"),              # largura total: int()/Decimal() aceitavam
    ("1\u0662\u0663", "só dígitos 0-9"),                    # 1.º dígito ASCII, os outros árabes
    ("1.5\uff15", "só dígitos 0-9"),                        # largura total depois do ponto
    ("\u22125", "não é um número"),                        # sinal menos Unicode
    ("\u00a01.5", "não é um número"),                      # espaço não separável (strip() do Python aceitava)
    ("NaN", "não é um número finito"), ("-inf", "não é um número finito"), ("Infinity", "não é um número finito"),
    ("", "vazio"), ("   ", "vazio"),
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
    ("1.234", {"decimal": "."}, "1.234"),                  # declarar o decimal resolve a ambiguidade
    ("1,5e3", {"decimal": ","}, "1500"),
])
def test_parse_number_with_declared_format(text, fmt, expected):
    assert parse_number(text, **fmt) == Decimal(expected)


@pytest.mark.parametrize("text,fmt,fragment", [
    ("1.23,4", {"decimal": ",", "thousands": "."}, "grupos de 3"),
    ("1234.5", {"decimal": ",", "thousands": "."}, "grupos de 3"),   # '.' declarado como milhares: '1234.5' é 12345?
    ("1,2,3", {"decimal": ","}, "mais de um separador decimal"),
    ("1.234,5", {"decimal": ","}, "não foi declarado"),
    ("1,234", {"decimal": "."}, "não foi declarado"),
    (",5", {"decimal": ","}, "não é um número"),
])
def test_parse_number_format_errors(text, fmt, fragment):
    with pytest.raises(NumberError, match=fragment):
        parse_number(text, **fmt)


def test_to_float_refuses_overflow_underflow_and_non_finite():
    assert to_float("1e308") == 1e308 and to_float(" -2.5 ") == -2.5
    for bad in ("1e400", "-1e400", "1e-400", "0." + "0" * 400 + "1"):
        with pytest.raises(NumberError):
            to_float(bad)
    assert to_float("0e-400") == 0.0                        # zero escrito como zero não é perda
    for bad in (float("inf"), float("nan"), 1e308 * 10):
        with pytest.raises(NumberError, match="finito"):
            to_float(bad)


# --- JsonFloat: texto guardado só quando o double pode ter perdido algarismos -----------
def test_json_float_keeps_text_only_beyond_15_significant_digits():
    assert type(json_float("0.1")) is float and type(json_float("123456789012345")) is float
    assert type(json_float("1.000000000000000000")) is float   # zeros à direita não contam
    x = json_float("0.12345678901234567890123")
    assert isinstance(x, JsonFloat) and x.text == "0.12345678901234567890123" and x == 0.12345678901234568
    with pytest.raises(NumberError, match="alcance"):
        json_float("1e400")
    with pytest.raises(NumberError, match="pequeno"):
        json_float("1e-400")


def test_dbl_dig_guarantee_behind_the_shortcut():
    # a justificação do atalho: com <= 15 algarismos significativos, repr(float(texto)) tem o mesmo valor
    rnd = random.Random(7)
    for _ in range(20000):
        digits = str(rnd.randint(1, 10 ** rnd.randint(1, DBL_DIG) - 1))
        text = f"{digits[:1]}.{digits[1:] or '0'}e{rnd.randint(-300, 300)}"
        assert Decimal(repr(float(text))) == Decimal(text), text


def test_json_float_behaves_like_a_float():
    x = json_float("12345678901234567890.5")
    assert x + 1 == float(x) + 1 and isinstance(x * 2, float) and hash(x) == hash(float(x))
    assert json.dumps({"v": x}) == json.dumps({"v": float(x)})       # UUIDs de conteúdo e record_hash não mudam
    for clone in (copy.deepcopy(x), pickle.loads(pickle.dumps(x))):
        assert isinstance(clone, JsonFloat) and clone.text == x.text and clone == x


# --- casts ------------------------------------------------------------------------
@pytest.mark.parametrize("raw,t,expected", [
    ('{"v": 1234567890123456789.0}', "bigint", 1234567890123456789),       # antes: 1234567890123456768
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
    ("12_000", "bigint", "não é um número"), ("\uff11\uff12", "int", "0-9"), ("x", "int", "não é um número"),
    ("1.5", "int", "não é inteiro"), ("NaN", "double", "finito"), ("1,5", "double", "vírgula"),
    ("1e-400", "double", "pequeno"), ("1e400", "decimal(38,2)", "não cabe"), ([1], "double", "não é um número"),
    ({"value": 1}, "int", "não é um número"),
])
def test_casts_refuse_with_a_reason(value, t, fragment):
    with pytest.raises((ValueError, TypeError), match=fragment):
        T.cast(value, t)


def test_blank_text_is_null_only_for_numeric_types():
    assert T.cast("", "double") is None and T.cast("  ", "int") is None and T.cast(" ", "decimal(5,2)") is None
    assert T.cast("", "timestamp") is None                  # como antes
    with pytest.raises(ValueError):
        T.cast("   ", "timestamp")                          # só espaços numa data: continua a ser erro


# --- transformações -----------------------------------------------------------------
def test_numeric_transforms_use_the_same_rules():
    assert apply_chain("12", "to_number") == 12 and apply_chain("1.50", "to_number") == Decimal("1.50")
    assert apply_chain("1.234,5", {"to_number": {"decimal": ",", "thousands": "."}}) == Decimal("1234.5")
    assert apply_chain({"value": "2500", "unit": "unit:gram"}, {"unit_convert": {"to": "kilogram"}}) == 2.5
    for chain in ("to_number", {"multiply": 2}, {"add": 1}, {"round": {"digits": 1, "mode": "half_up"}}, "abs", "sum",
                  {"clamp": {"min": 0}}):
        with pytest.raises(NumberError):                    # antes: '1,5' virava 1.5 em silêncio
            apply_chain(["1,5"] if chain == "sum" else "1,5", chain)


@pytest.mark.parametrize("transform,fragment", [
    ("uper", "desconhecida: 'uper'"),
    ({"to_number": {"decimal": ";"}}, "decimal deve ser"),
    ({"to_number": {"decimal": ",", "thousands": ","}}, "mesmo separador"),
    ({"to_number": {"sep": ","}}, "to_number aceita"),
    ([{"to_number": None}, "lowr"], "desconhecida: 'lowr'"),
])
def test_transform_names_and_arguments_are_checked_when_loading(transform, fragment):
    cols = {"v": {"path": "v", "transform": transform, "on_error": "null"}}   # antes: anulava tudo em silêncio
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": cols}]}]})
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": {"cases": [{"path": "v", "transform": transform}]}}}]}]})


# --- relatório: números em texto e texto vazio ----------------------------------------
def test_numbers_as_text_and_empty_text_are_reported():
    cfg = from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {
        "a": "a ::double", "b": "b ::int", "c": {"path": "c", "transform": {"to_number": {"decimal": ","}}, "type": "double"}}}]}]})
    eng = Engine(cfg)
    rows = eng.run([Document({"a": " 2.5 ", "b": "", "c": "1,5"}, "f.json")])["t"].rows
    assert rows == [{"a": 2.5, "b": None, "c": 1.5}]
    rep = eng.report
    assert rep.count(reason="number_as_text", column="a") == 1 and rep.count(reason="empty_text", column="b") == 1
    assert rep.count(column="c") == 0                       # conversão declarada com to_number: não é aviso


def test_numbers_as_text_inside_arrays_are_reported():
    cfg = from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {"xs": "xs ::array<double>"}}]}]})
    eng = Engine(cfg)
    assert eng.run([Document({"xs": [1, "2.5", ""]}, "f.json")])["t"].rows == [{"xs": [1.0, 2.5, None]}]
    assert eng.report.count(reason="number_as_text") == 1 and eng.report.count(reason="empty_text") == 1
