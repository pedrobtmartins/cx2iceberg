"""Passo 4: transformações — argumentos validados na carga, sem perdas silenciosas, perdas declaradas contadas."""
from datetime import date, datetime
from decimal import Decimal

import pytest

from cx2iceberg import ConfigError, Document, Engine, from_dict
from cx2iceberg import notes as N
from cx2iceberg.numbers import ExactFloat, JsonFloat, NumberError
from cx2iceberg.transforms import apply_chain


def t(value, chain):
    return apply_chain(value, chain)


def fails(value, chain, fragment):
    with pytest.raises(ValueError) as e:
        apply_chain(value, chain)
    assert fragment in str(e.value), str(e.value)


def with_notes(value, chain, **settings):
    with N.scope(settings) as notes:
        out = apply_chain(value, chain)
    return out, [n[1:3] for n in notes]


def run(cols, docs, input=None, **sub):
    eng = Engine(from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": cols}], **sub}],
                            **({"input": input} if input else {})}))
    return eng.run([Document(d, f"d{i}.json") for i, d in enumerate(docs)], partial=True)["t"].rows, eng.report


# --- argumentos: validados ao carregar a configuração --------------------------------
@pytest.mark.parametrize("transform,fragment", [
    ("prefix", "precisa de um texto"), ({"suffix": 1}, "precisa de um texto"),
    ({"replace": "x"}, "[antigo, novo]"), ({"regex_replace": ["(", ""]}, "regex_replace"),
    ({"regex_extract": "("}, "regex_extract"), ({"regex_extract": {"pattern": "x", "no_match": "zero"}}, "no_match"),
    ({"substring": "a"}, "substring"), ({"split": 1}, "precisa de um texto"), ({"join": 1}, "precisa de um texto"),
    ({"semantic_id_parts": "x"}, "namespace, version ou name"),
    ({"map": "x"}, "map precisa de um objecto"), ({"map": {"_x": 1}}, "chave especial desconhecida"),
    ({"map": {"A": 1, "a": 2, "_ignore_case": True}}, "repete outra chave"),
    ({"map": {"_ignore_case": "yes"}}, "_ignore_case tem de ser true ou false"),
    ({"multiply": "2"}, "precisa de um número"), ({"multiply": True}, "precisa de um número"),
    ({"divide": 0}, "divisão por zero"), ({"add": None}, "precisa de um número"),
    ({"round": 2}, "indique as casas e o modo"), ({"round": {"digits": 2}}, "indique as casas e o modo"),
    ({"round": {"digits": 2, "mode": "bankers"}}, "modo 'bankers' desconhecido"),
    ({"round": {"digits": 1.5, "mode": "half_up"}}, "digits tem de ser um inteiro"),
    ({"clamp": {}}, "clamp"), ({"clamp": {"min": 1, "max": 0}}, "min maior do que max"),
    ({"clamp": {"min": "0"}}, "precisa de um número"),
    ({"unit_convert": {"to": "kilogramz"}}, "unidade desconhecida"),
    ({"unit_convert": {"to": "kilogram", "from": "metre"}}, "não são da mesma grandeza"),
    ({"unit_convert": {}}, "use unit_convert"),
    ({"to_timestamp": {"unit": "us"}}, "unit deve ser"), ({"to_timestamp": 5}, "use to_timestamp"),
    ("date_format", "precisa de um texto"), ({"year": "%Y"}, "não leva argumento"),
    ({"sum": {"nulls": "zero"}}, "nulls: skip"), ({"sort": "yes"}, "true ou false"), ({"to_json": "x"}, "true ou false"),
    ({"hash": "md55"}, "algoritmo desconhecido"), ({"upper": 1}, "não leva argumento"), ({"first": "x"}, "não leva argumento"),
    ("pluck", "precisa de um texto"),
])
def test_transform_arguments_are_checked_when_loading(transform, fragment):
    with pytest.raises(ConfigError) as e:
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": {"path": "v", "transform": transform}}}]}]})
    assert fragment in str(e.value)


# --- texto e JSON: uma só maneira ------------------------------------------------------
def test_text_is_the_same_everywhere():
    assert t(True, "to_string") == "true" and t(True, "upper") == "TRUE"      # antes: 'True'
    assert t(JsonFloat("0.12345678901234567890123"), "to_string") == "0.12345678901234567890123"
    assert t({"k": True, "d": Decimal("1.5")}, "to_string") == '{"k": true, "d": 1.5}'
    assert t(["a", {"k": 1}, True], {"join": "|"}) == 'a|{"k": 1}|true'       # antes: "a|{'k': 1}|True"
    assert t(datetime(2022, 1, 1, 10), "to_string") == "2022-01-01T10:00:00"
    assert t({"d": Decimal("1.5"), "t": datetime(2022, 1, 1)}, "to_json") == '{"d": 1.5, "t": "2022-01-01T00:00:00"}'
    fails(["a", None], "join", "a lista tem nulos")                              # antes: 'a,'
    fails({"d": Decimal("0.12345678901234567890123")}, "to_json", "algarismos a mais para um número JSON")
    fails(JsonFloat("0.12345678901234567890123"), "to_json", "algarismos a mais")
    assert t(JsonFloat("69.14590545404381"), "to_json") == "69.14590545404381"  # 16 algarismos que o double reproduz


# --- números: contas exactas sobre o valor escrito ---------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    (0.1, {"multiply": 3}, 0.3),                            # antes: 0.30000000000000004
    ("0.1", {"add": 0.2}, 0.3),
    ([0.1, 0.2], "sum", 0.3),
    ([1, 2, 3], "sum", 6.0),
    (["1.5", 2], "sum", 3.5),
    (0, {"multiply": -1}, 0.0),                             # sem -0.0
    (-3, "abs", 3), (JsonFloat("-0.12345678901234567890123"), "abs", 0.12345678901234568),
    (2500, {"divide": 1000}, 2.5),
    (-5, {"clamp": {"min": 0}}, 0.0), (5, {"clamp": {"min": 0}}, 5.0),
])
def test_arithmetic_is_exact(value, chain, expected):
    out = t(value, chain)
    assert out == expected and str(out) != "-0.0"


def test_division_keeps_the_exact_result_for_decimal_columns():
    third = t(1, {"divide": 3})
    assert isinstance(third, ExactFloat) and third.text.startswith("0.333333333333333333333333")


@pytest.mark.parametrize("value,digits,mode,expected", [
    (2.675, 2, "half_up", 2.68), (2.675, 2, "half_even", 2.68),    # antes (float): 2.67
    (0.125, 2, "half_even", 0.12), (0.125, 2, "half_up", 0.13), (0.125, 2, "half_down", 0.12),
    (-0.125, 2, "down", -0.12), (-0.125, 2, "floor", -0.13), (-0.125, 2, "ceiling", -0.12), (0.121, 2, "up", 0.13),
    (125, -1, "half_up", 130.0), (125, -1, "half_even", 120.0),
])
def test_round_needs_a_mode_and_uses_it(value, digits, mode, expected):
    assert t(value, {"round": {"digits": digits, "mode": mode}}) == expected


@pytest.mark.parametrize("value,chain,fragment", [
    (True, {"multiply": 2}, "não é um número"), ("1,5", {"add": 1}, "vírgula"), ({"a": 1}, "abs", "não é um número"),
    (1e308, {"multiply": 10}, "fora do alcance de um double"), (1e-300, {"divide": 1e300}, "demasiado pequeno"),
    ([1, True], "sum", "booleano"),
])
def test_arithmetic_refuses(value, chain, fragment):
    fails(value, chain, fragment)


def test_sum_min_max_with_nulls_fail_unless_skipped():
    for name in ("sum", "min", "max"):
        fails([1.5, None, 2.0], name, f"{name}: 1 de 3 valor(es) nulo(s): o resultado seria parcial")
    out, notes = with_notes([1.5, None, 2.0], {"sum": {"nulls": "skip"}})
    assert out == 3.5 and notes == [("nulls_skipped", "skipped")]
    assert t([None, None], {"max": {"nulls": "skip"}}) is None and t([], "sum") is None


# --- unit_convert -------------------------------------------------------------------
@pytest.mark.parametrize("value,arg,expected", [
    ({"value": 2500, "unit": "unit:gram"}, {"to": "kilogram"}, 2.5),
    ({"value": 0.1, "unit": "unit:pound"}, {"to": "kilogram"}, 0.045359237),         # em float: 0.045359237000000004
    ({"value": 0.3, "unit": "unit:inch"}, {"to": "metre"}, 0.00762),                  # em float: 0.007619999999999999
    ({"value": 1, "unit": "unit:pound"}, {"to": "gram"}, 453.59237),
    ({"value": 1, "unit": "unit:kilowattHour"}, {"to": "megajoule"}, 3.6),
    ({"value": 5, "unit": "liter"}, {"to": "cubicMetre"}, 0.005),                   # nome da unidade declarada do PCF
    ({"quantityNumber": 5, "measurementUnit": {"lexicalValue": "gram"}}, {"to": "kilogram"}, 0.005),   # BOM antigo
    ({"value": 5, "unit": "urn:samm:org.eclipse.esmf.samm:unit:2.1.0#gram"}, {"to": "kilogram"}, 0.005),
    ({"value": None, "quantityNumber": 5, "unit": "unit:gram"}, {"to": "kilogram"}, 0.005),          # antes: nulo
    ({"value": 5, "quantityNumber": 5, "unit": "unit:gram", "measurementUnit": "gram"}, {"to": "kilogram"}, 0.005),
    ({"value": "2500", "unit": "unit:gram"}, {"to": "kilogram"}, 2.5),
    (3, {"from": "hourUnitOfTime", "to": "minuteUnitOfTime"}, 180.0),
    ({"unit": "unit:gram"}, {"to": "kilogram"}, None),                               # quantidade sem número: nulo
])
def test_unit_convert(value, arg, expected):
    assert t(value, {"unit_convert": arg}) == expected


@pytest.mark.parametrize("value,arg,fragment", [
    ({"value": 5}, {"to": "kilogram"}, "não tem unidade; indique from"),                 # antes: nulo em silêncio
    (5, {"to": "kilogram"}, "não tem unidade"),
    ({"value": 5, "quantityNumber": 6, "unit": "unit:gram"}, {"to": "kilogram"}, "valores em conflito"),
    ({"value": 5, "unit": "unit:gram", "measurementUnit": "unit:kilogram"}, {"to": "kilogram"}, "unidades em conflito"),
    ({"value": 4, "unit": "unit:piece"}, {"to": "kilogram"}, "não se converte"),
    ({"value": 4, "unit": "unit:Gram"}, {"to": "kilogram"}, "unidade desconhecida"),
    ({"value": True, "unit": "unit:gram"}, {"to": "kilogram"}, "booleano"),
])
def test_unit_convert_refuses(value, arg, fragment):
    fails(value, {"unit_convert": arg}, fragment)


def test_unit_convert_fallback_is_counted():
    out, notes = with_notes({"value": 5}, {"unit_convert": {"to": "kilogram", "from": "gram"}})
    assert out == 0.005 and notes == [("unit_fallback", "assumed_unit")]
    out, notes = with_notes({"value": 5, "unit": "unit:kilogram"}, {"unit_convert": {"to": "kilogram", "from": "gram"}})
    assert out == 5.0 and notes == []                                                    # a unidade do documento manda


# --- listas: tipos e ordem -------------------------------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    ([3, 1.5, 2], "max", 3), ([3, 1.5, 2], "min", 1.5), (["b", "a"], "sort", ["a", "b"]),
    (["2022-01-01T10:00:00+02:00", "2022-01-01T09:00:00Z"], "max", "2022-01-01T09:00:00Z"),   # antes: o 1.º (texto)
    (["2022-01-02", "2021-12-31"], "min", "2021-12-31"),
    ([True, 1, 1.0, "1", None, None], "distinct", [True, 1, "1", None]),                 # antes: [True] (perdia 1)
    ([{"a": 1}, {"a": 1}], "distinct", [{"a": 1}]),                                       # antes: ['{"a": 1}'] (texto)
    (["X", "X"], "single", "X"), ([], "single", None), ("X", "single", "X"),
    ([1, 2], "first", 1), ([1, 2], "last", 2), ([1, None], "count", 2),
])
def test_lists(value, chain, expected):
    out = t(value, chain)
    assert out == expected and [type(x) for x in _list(out)] == [type(x) for x in _list(expected)]


def _list(v):
    return v if isinstance(v, list) else [v]


@pytest.mark.parametrize("value,chain,fragment", [
    (["9", "10"], "max", "números em texto"), (["10", "9"], "sort", "números em texto"),       # antes: '9', ['10','9']
    ([True, 0.5], "min", "tipos diferentes"), (["a", 1], "sort", "tipos diferentes"),
    (["2022-01-01T10:00:00Z", "2022-01-01T11:00:00"], "max", "com e sem fuso"),
    ([1, None], "sort", "a lista tem nulos"),
    (["X", "Y"], "single", "single: 2 valores diferentes"), ([1, "1"], "single", "2 valores diferentes"),
    ([{"a": 1}, {"b": 2}], {"pluck": "a"}, "não tem o campo 'a'"), ([1], {"get": "a"}, "não é um objecto"),
    ("x", "keys", "não é um objecto"), (5, "length", "não é texto nem lista"),
])
def test_lists_refuse(value, chain, fragment):
    fails(value, chain, fragment)


# --- map / null_if / to_bool / regex ------------------------------------------------
@pytest.mark.parametrize("value,arg,expected", [
    ("cut off", {"cut-off": "X"}, "cut off"),                                         # sem tradução: fica o original
    ("CUT-OFF", {"cut-off": "X", "_ignore_case": True}, "X"),
    (1, {1: "um", "_default": "?"}, "um"), (1.0, {1: "um"}, "um"), (True, {True: "sim"}, "sim"), (1, {True: "sim"}, 1),
    (2, {1: "um", "_default": "?"}, "?"), (None, {"_null": "vazio"}, "vazio"), (["a", "b"], {"a": 1}, [1, "b"]),
])
def test_map_compares_with_types(value, arg, expected):
    assert t(value, {"map": arg}) == expected


@pytest.mark.parametrize("value,arg", [(1, {"1": "a"}), ("1", {1: "a"}), ("true", {True: "sim"}), (False, {"false": 0})])
def test_map_same_text_other_type_is_an_error(value, arg):
    fails(value, {"map": arg}, "só corresponde à chave")                                   # antes: traduzia


def test_map_keys_that_python_would_merge_are_refused_in_yaml():
    from cx2iceberg.config import load_yaml

    # 1 e 1.0 (ou true e 1) são a mesma chave num dict do Python: o YAML antigo ficava só com a última tradução
    for text in ("map: {1: a, 1.0: b}", "map: {true: sim, 1: um}", "map: {a: 1, a: 2}"):
        with pytest.raises(Exception, match="chave repetida"):
            load_yaml(text)


def test_null_if_and_to_bool():
    assert t(0, {"null_if": False}) == 0 and t(False, {"null_if": False}) is None            # antes: 0 virava nulo
    assert t(0.0, {"null_if": 0}) is None and t("", {"null_if": ""}) is None
    for v, out in ((True, True), ("yes", True), ("Não", False), (1, True), (0, False), (" TRUE ", True)):
        assert t(v, "to_bool") is out
    for v in ("maybe", 2, 1.0, ""):
        fails(v, "to_bool", "não é um booleano reconhecido")                               # antes: nulo


def test_regex_extract_and_semantic_id_parts():
    assert t("urn:uuid:abc", {"regex_extract": "^urn:uuid:(.*)$"}) == "abc"
    fails("BPN-X", {"regex_extract": r"BPNL\d+"}, "não corresponde")                      # antes: nulo
    out, notes = with_notes("BPN-X", {"regex_extract": {"pattern": r"BPNL\d+", "no_match": None}})
    assert out is None and notes == [("no_match", "nulled")]
    fails("abc", "semantic_id_parts", "não é um semantic id")
    fails('{"v": NaN}', "from_json", "NaN não é JSON válido")


# --- datas ----------------------------------------------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    ("2022-02-04T14:48:54+02:00", "to_timestamp", datetime(2022, 2, 4, 12, 48, 54)),
    ("2023-07-11T01:00:00+02:00", "to_date", date(2023, 7, 11)),                             # antes: dia 10 (UTC)
    ("2022-12-31T23:30:00-02:00", "year", 2022),                                             # antes: 2023
    ("2022-01-31T23:30:00-02:00", "month", 1),
    ("2022-01-01T23:30:00-02:00", {"date_format": "%Y-%m-%d %H:%M"}, "2022-01-01 23:30"),     # antes: 2022-01-02 01:30
    ("2022-01-01", "to_date", date(2022, 1, 1)), ("2022-01-01", "year", 2022),               # data sem hora: não precisa de fuso
    (1641031200, {"to_timestamp": {"unit": "s"}}, datetime(2022, 1, 1, 10)),
    (1641031200000, {"to_timestamp": {"unit": "ms"}}, datetime(2022, 1, 1, 10)),
    ("04/02/2022 14:48 +0100", {"to_timestamp": {"format": "%d/%m/%Y %H:%M %z"}}, datetime(2022, 2, 4, 13, 48)),
])
def test_dates(value, chain, expected):
    assert t(value, chain) == expected


@pytest.mark.parametrize("value,chain,fragment", [
    ("2022-01-01T10:00:00", "to_timestamp", "não tem fuso horário"),                         # antes: UTC sem dizer
    ("2022-01-01", "to_timestamp", "a data (sem hora, fica a meia-noite) não tem fuso horário"),
    ("04/02/2022", {"to_timestamp": "%d/%m/%Y"}, "não tem fuso horário"),
    (2022, "year", "declare a unidade"), (86400000, "to_timestamp", "declare a unidade"),     # antes: 1970 / 1972
    (True, "to_date", "booleano"), ("03/02/2022", "to_timestamp", "não é uma data/hora no formato do schema"),
    (10 ** 20, {"to_timestamp": {"unit": "s"}}, "fora do intervalo"),
])
def test_dates_refuse(value, chain, fragment):
    fails(value, chain, fragment)


def test_naive_timestamps_can_be_declared_utc_and_are_counted():
    out, notes = with_notes("2022-01-01T10:00:00", "to_timestamp", naive_timestamps="utc")
    assert out == datetime(2022, 1, 1, 10) and notes == [("naive_timestamp", "assumed_utc")]
    out, notes = with_notes("2022-01-01T10:00:00.123456789Z", "to_timestamp")
    assert out == datetime(2022, 1, 1, 10, 0, 0, 123456) and notes == [("timestamp_precision", "truncated")]
    assert with_notes("2022-01-01T10:00:00.123456000Z", "to_timestamp")[1] == []             # zeros: nada se perde


def test_today_is_utc_like_now():
    from datetime import timezone

    assert t(None, "today") == datetime.now(timezone.utc).date()


# --- no motor: cada nota chega ao relatório com tabela, coluna e ficheiro -------------------
def test_declared_transform_losses_reach_the_report():
    cols = {"total": {"path": "xs", "transform": {"sum": {"nulls": "skip"}}, "type": "double"},
            "lim": {"path": "x", "transform": {"clamp": {"min": 0}}, "type": "double"},
            "kg": {"path": "q", "transform": {"unit_convert": {"to": "kilogram", "from": "gram"}}, "type": "double"},
            "at": {"path": "at", "type": "timestamp"}}
    rows, rep = run(cols, [{"xs": [1, None], "x": -2, "q": {"value": 500}, "at": "2022-01-01T10:00:00"}],
                    input={"naive_timestamps": "utc"})
    assert rows == [{"total": 1.0, "lim": 0.0, "kg": 0.5, "at": datetime(2022, 1, 1, 10)}]
    got = {(e["column"], e["reason"]) for e in rep.events("declared")}
    assert got == {("total", "nulls_skipped"), ("lim", "clamped"), ("kg", "unit_fallback"), ("at", "naive_timestamp")}
    assert all(e["file"] == "d0.json" for e in rep.events("declared"))


def test_a_failed_value_does_not_leave_its_notes_behind():
    cols = {"v": {"path": "xs", "transform": [{"sum": {"nulls": "skip"}}, {"divide": 1}, "to_bool"], "on_error": "null"}}
    rows, rep = run(cols, [{"xs": [1, None]}])
    assert rep.count(reason="nulls_skipped") == 0 and rep.count(action="nulled") == 1
