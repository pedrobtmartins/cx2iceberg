"""Step 4: transformations — arguments validated at load time, no silent losses, declared losses counted."""
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


# --- arguments: validated when the configuration is loaded --------------------------------
@pytest.mark.parametrize("transform,fragment", [
    ("prefix", "needs a text"), ({"suffix": 1}, "needs a text"),
    ({"replace": "x"}, "[old, new]"), ({"regex_replace": ["(", ""]}, "regex_replace"),
    ({"regex_extract": "("}, "regex_extract"), ({"regex_extract": {"pattern": "x", "no_match": "zero"}}, "no_match"),
    ({"substring": "a"}, "substring"), ({"split": 1}, "needs a text"), ({"join": 1}, "needs a text"),
    ({"semantic_id_parts": "x"}, "namespace, version or name"),
    ({"map": "x"}, "map needs an object"), ({"map": {"_x": 1}}, "unknown special key"),
    ({"map": {"A": 1, "a": 2, "_ignore_case": True}}, "repeats another key"),
    ({"map": {"_ignore_case": "yes"}}, "_ignore_case must be true or false"),
    ({"multiply": "2"}, "needs a number"), ({"multiply": True}, "needs a number"),
    ({"divide": 0}, "division by zero"), ({"add": None}, "needs a number"),
    ({"round": 2}, "give the digits and the mode"), ({"round": {"digits": 2}}, "give the digits and the mode"),
    ({"round": {"digits": 2, "mode": "bankers"}}, "unknown mode 'bankers'"),
    ({"round": {"digits": 1.5, "mode": "half_up"}}, "digits must be an integer"),
    ({"clamp": {}}, "clamp"), ({"clamp": {"min": 1, "max": 0}}, "min greater than max"),
    ({"clamp": {"min": "0"}}, "needs a number"),
    ({"unit_convert": {"to": "kilogramz"}}, "unknown unit"),
    ({"unit_convert": {"to": "kilogram", "from": "metre"}}, "are not the same kind of quantity"),
    ({"unit_convert": {}}, "use unit_convert"),
    ({"to_timestamp": {"unit": "us"}}, "unit must be"), ({"to_timestamp": 5}, "use to_timestamp"),
    ("date_format", "needs a text"), ({"year": "%Y"}, "takes no argument"),
    ({"sum": {"nulls": "zero"}}, "nulls: skip"), ({"sort": "yes"}, "true or false"), ({"to_json": "x"}, "true or false"),
    ({"hash": "md55"}, "unknown algorithm"), ({"upper": 1}, "takes no argument"), ({"first": "x"}, "takes no argument"),
    ("pluck", "needs a text"),
])
def test_transform_arguments_are_checked_when_loading(transform, fragment):
    with pytest.raises(ConfigError) as e:
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": {"path": "v", "transform": transform}}}]}]})
    assert fragment in str(e.value)


# --- text and JSON: one single way ------------------------------------------------------
def test_text_is_the_same_everywhere():
    assert t(True, "to_string") == "true" and t(True, "upper") == "TRUE"      # before: 'True'
    assert t(JsonFloat("0.12345678901234567890123"), "to_string") == "0.12345678901234567890123"
    assert t({"k": True, "d": Decimal("1.5")}, "to_string") == '{"k": true, "d": 1.5}'
    assert t(["a", {"k": 1}, True], {"join": "|"}) == 'a|{"k": 1}|true'       # before: "a|{'k': 1}|True"
    assert t(datetime(2022, 1, 1, 10), "to_string") == "2022-01-01T10:00:00"
    assert t({"d": Decimal("1.5"), "t": datetime(2022, 1, 1)}, "to_json") == '{"d": 1.5, "t": "2022-01-01T00:00:00"}'
    fails(["a", None], "join", "the list has nulls")                              # before: 'a,'
    fails({"d": Decimal("0.12345678901234567890123")}, "to_json", "too many digits for a JSON number")
    fails(JsonFloat("0.12345678901234567890123"), "to_json", "too many digits")
    assert t(JsonFloat("69.14590545404381"), "to_json") == "69.14590545404381"  # 16 digits the double reproduces


# --- numbers: exact arithmetic on the written value ---------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    (0.1, {"multiply": 3}, 0.3),                            # before: 0.30000000000000004
    ("0.1", {"add": 0.2}, 0.3),
    ([0.1, 0.2], "sum", 0.3),
    ([1, 2, 3], "sum", 6.0),
    (["1.5", 2], "sum", 3.5),
    (0, {"multiply": -1}, 0.0),                             # no -0.0
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
    (2.675, 2, "half_up", 2.68), (2.675, 2, "half_even", 2.68),    # before (float): 2.67
    (0.125, 2, "half_even", 0.12), (0.125, 2, "half_up", 0.13), (0.125, 2, "half_down", 0.12),
    (-0.125, 2, "down", -0.12), (-0.125, 2, "floor", -0.13), (-0.125, 2, "ceiling", -0.12), (0.121, 2, "up", 0.13),
    (125, -1, "half_up", 130.0), (125, -1, "half_even", 120.0),
])
def test_round_needs_a_mode_and_uses_it(value, digits, mode, expected):
    assert t(value, {"round": {"digits": digits, "mode": mode}}) == expected


@pytest.mark.parametrize("value,chain,fragment", [
    (True, {"multiply": 2}, "is not a number"), ("1,5", {"add": 1}, "comma"), ({"a": 1}, "abs", "is not a number"),
    (1e308, {"multiply": 10}, "out of range for a double"), (1e-300, {"divide": 1e300}, "too small"),
    ([1, True], "sum", "boolean"),
])
def test_arithmetic_refuses(value, chain, fragment):
    fails(value, chain, fragment)


def test_sum_min_max_with_nulls_fail_unless_skipped():
    for name in ("sum", "min", "max"):
        fails([1.5, None, 2.0], name, f"{name}: 1 of 3 value(s) null: the result would be partial")
    out, notes = with_notes([1.5, None, 2.0], {"sum": {"nulls": "skip"}})
    assert out == 3.5 and notes == [("nulls_skipped", "skipped")]
    assert t([None, None], {"max": {"nulls": "skip"}}) is None and t([], "sum") is None


# --- unit_convert -------------------------------------------------------------------
@pytest.mark.parametrize("value,arg,expected", [
    ({"value": 2500, "unit": "unit:gram"}, {"to": "kilogram"}, 2.5),
    ({"value": 0.1, "unit": "unit:pound"}, {"to": "kilogram"}, 0.045359237),         # as float: 0.045359237000000004
    ({"value": 0.3, "unit": "unit:inch"}, {"to": "metre"}, 0.00762),                  # as float: 0.007619999999999999
    ({"value": 1, "unit": "unit:pound"}, {"to": "gram"}, 453.59237),
    ({"value": 1, "unit": "unit:kilowattHour"}, {"to": "megajoule"}, 3.6),
    ({"value": 5, "unit": "liter"}, {"to": "cubicMetre"}, 0.005),                   # name of the PCF's declared unit
    ({"quantityNumber": 5, "measurementUnit": {"lexicalValue": "gram"}}, {"to": "kilogram"}, 0.005),   # old BOM
    ({"value": 5, "unit": "urn:samm:org.eclipse.esmf.samm:unit:2.1.0#gram"}, {"to": "kilogram"}, 0.005),
    ({"value": None, "quantityNumber": 5, "unit": "unit:gram"}, {"to": "kilogram"}, 0.005),          # before: null
    ({"value": 5, "quantityNumber": 5, "unit": "unit:gram", "measurementUnit": "gram"}, {"to": "kilogram"}, 0.005),
    ({"value": "2500", "unit": "unit:gram"}, {"to": "kilogram"}, 2.5),
    (3, {"from": "hourUnitOfTime", "to": "minuteUnitOfTime"}, 180.0),
    ({"unit": "unit:gram"}, {"to": "kilogram"}, None),                               # quantity without a number: null
])
def test_unit_convert(value, arg, expected):
    assert t(value, {"unit_convert": arg}) == expected


@pytest.mark.parametrize("value,arg,fragment", [
    ({"value": 5}, {"to": "kilogram"}, "has no unit; give from"),                 # before: silently null
    (5, {"to": "kilogram"}, "has no unit"),
    ({"value": 5, "quantityNumber": 6, "unit": "unit:gram"}, {"to": "kilogram"}, "conflicting values"),
    ({"value": 5, "unit": "unit:gram", "measurementUnit": "unit:kilogram"}, {"to": "kilogram"}, "conflicting units"),
    ({"value": 4, "unit": "unit:piece"}, {"to": "kilogram"}, "does not convert"),
    ({"value": 4, "unit": "unit:Gram"}, {"to": "kilogram"}, "unknown unit"),
    ({"value": True, "unit": "unit:gram"}, {"to": "kilogram"}, "boolean"),
])
def test_unit_convert_refuses(value, arg, fragment):
    fails(value, {"unit_convert": arg}, fragment)


def test_unit_convert_fallback_is_counted():
    out, notes = with_notes({"value": 5}, {"unit_convert": {"to": "kilogram", "from": "gram"}})
    assert out == 0.005 and notes == [("unit_fallback", "assumed_unit")]
    out, notes = with_notes({"value": 5, "unit": "unit:kilogram"}, {"unit_convert": {"to": "kilogram", "from": "gram"}})
    assert out == 5.0 and notes == []                                                    # the document's unit wins


# --- lists: types and order -------------------------------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    ([3, 1.5, 2], "max", 3), ([3, 1.5, 2], "min", 1.5), (["b", "a"], "sort", ["a", "b"]),
    (["2022-01-01T10:00:00+02:00", "2022-01-01T09:00:00Z"], "max", "2022-01-01T09:00:00Z"),   # before: the 1st (text)
    (["2022-01-02", "2021-12-31"], "min", "2021-12-31"),
    ([True, 1, 1.0, "1", None, None], "distinct", [True, 1, "1", None]),                 # before: [True] (lost 1)
    ([{"a": 1}, {"a": 1}], "distinct", [{"a": 1}]),                                       # before: ['{"a": 1}'] (text)
    (["X", "X"], "single", "X"), ([], "single", None), ("X", "single", "X"),
    ([1, 2], "first", 1), ([1, 2], "last", 2), ([1, None], "count", 2),
])
def test_lists(value, chain, expected):
    out = t(value, chain)
    assert out == expected and [type(x) for x in _list(out)] == [type(x) for x in _list(expected)]


def _list(v):
    return v if isinstance(v, list) else [v]


@pytest.mark.parametrize("value,chain,fragment", [
    (["9", "10"], "max", "numbers as text"), (["10", "9"], "sort", "numbers as text"),       # before: '9', ['10','9']
    ([True, 0.5], "min", "different or incomparable types"), (["a", 1], "sort", "different or incomparable types"),
    (["2022-01-01T10:00:00Z", "2022-01-01T11:00:00"], "max", "with and without a time zone"),
    ([1, None], "sort", "the list has nulls"),
    (["X", "Y"], "single", "single: 2 different values"), ([1, "1"], "single", "2 different values"),
    ([{"a": 1}, {"b": 2}], {"pluck": "a"}, "has no field 'a'"), ([1], {"get": "a"}, "is not an object"),
    ("x", "keys", "is not an object"), (5, "length", "is neither text nor a list"),
])
def test_lists_refuse(value, chain, fragment):
    fails(value, chain, fragment)


# --- map / null_if / to_bool / regex ------------------------------------------------
@pytest.mark.parametrize("value,arg,expected", [
    ("cut off", {"cut-off": "X"}, "cut off"),                                         # no translation: the original stays
    ("CUT-OFF", {"cut-off": "X", "_ignore_case": True}, "X"),
    (1, {1: "one", "_default": "?"}, "one"), (1.0, {1: "one"}, "one"), (True, {True: "yes"}, "yes"), (1, {True: "yes"}, 1),
    (2, {1: "one", "_default": "?"}, "?"), (None, {"_null": "empty"}, "empty"), (["a", "b"], {"a": 1}, [1, "b"]),
])
def test_map_compares_with_types(value, arg, expected):
    assert t(value, {"map": arg}) == expected


@pytest.mark.parametrize("value,arg", [(1, {"1": "a"}), ("1", {1: "a"}), ("true", {True: "yes"}), (False, {"false": 0})])
def test_map_same_text_other_type_is_an_error(value, arg):
    fails(value, {"map": arg}, "only matches key")                                   # before: it translated


def test_map_keys_that_python_would_merge_are_refused_in_yaml():
    from cx2iceberg.config import load_yaml

    # 1 and 1.0 (or true and 1) are the same key in a Python dict: the old YAML kept only the last translation
    for text in ("map: {1: a, 1.0: b}", "map: {true: yes, 1: one}", "map: {a: 1, a: 2}"):
        with pytest.raises(Exception, match="repeated key"):
            load_yaml(text)


def test_null_if_and_to_bool():
    assert t(0, {"null_if": False}) == 0 and t(False, {"null_if": False}) is None            # before: 0 became null
    assert t(0.0, {"null_if": 0}) is None and t("", {"null_if": ""}) is None
    for v, out in ((True, True), ("yes", True), ("Não", False), (1, True), (0, False), (" TRUE ", True)):
        assert t(v, "to_bool") is out
    for v in ("maybe", 2, 1.0, ""):
        fails(v, "to_bool", "is not a recognised boolean")                               # before: null


def test_regex_extract_and_semantic_id_parts():
    assert t("urn:uuid:abc", {"regex_extract": "^urn:uuid:(.*)$"}) == "abc"
    fails("BPN-X", {"regex_extract": r"BPNL\d+"}, "does not match")                      # before: null
    out, notes = with_notes("BPN-X", {"regex_extract": {"pattern": r"BPNL\d+", "no_match": None}})
    assert out is None and notes == [("no_match", "nulled")]
    fails("abc", "semantic_id_parts", "is not a semantic id")
    fails('{"v": NaN}', "from_json", "NaN is not valid JSON")


# --- dates ----------------------------------------------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    ("2022-02-04T14:48:54+02:00", "to_timestamp", datetime(2022, 2, 4, 12, 48, 54)),
    ("2023-07-11T01:00:00+02:00", "to_date", date(2023, 7, 11)),                             # before: the 10th (UTC)
    ("2022-12-31T23:30:00-02:00", "year", 2022),                                             # before: 2023
    ("2022-01-31T23:30:00-02:00", "month", 1),
    ("2022-01-01T23:30:00-02:00", {"date_format": "%Y-%m-%d %H:%M"}, "2022-01-01 23:30"),     # before: 2022-01-02 01:30
    ("2022-01-01", "to_date", date(2022, 1, 1)), ("2022-01-01", "year", 2022),               # a date without a time: no time zone needed
    (1641031200, {"to_timestamp": {"unit": "s"}}, datetime(2022, 1, 1, 10)),
    (1641031200000, {"to_timestamp": {"unit": "ms"}}, datetime(2022, 1, 1, 10)),
    ("04/02/2022 14:48 +0100", {"to_timestamp": {"format": "%d/%m/%Y %H:%M %z"}}, datetime(2022, 2, 4, 13, 48)),
])
def test_dates(value, chain, expected):
    assert t(value, chain) == expected


@pytest.mark.parametrize("value,chain,fragment", [
    ("2022-01-01T10:00:00", "to_timestamp", "has no time zone"),                         # before: UTC without saying so
    ("2022-01-01", "to_timestamp", "the date (no time, becomes midnight) has no time zone"),
    ("04/02/2022", {"to_timestamp": "%d/%m/%Y"}, "has no time zone"),
    (2022, "year", "declare the unit"), (86400000, "to_timestamp", "declare the unit"),     # before: 1970 / 1972
    (True, "to_date", "boolean"), ("03/02/2022", "to_timestamp", "is not a date/time in the schema's format"),
    (10 ** 20, {"to_timestamp": {"unit": "s"}}, "outside the range"),
])
def test_dates_refuse(value, chain, fragment):
    fails(value, chain, fragment)


def test_naive_timestamps_can_be_declared_utc_and_are_counted():
    out, notes = with_notes("2022-01-01T10:00:00", "to_timestamp", naive_timestamps="utc")
    assert out == datetime(2022, 1, 1, 10) and notes == [("naive_timestamp", "assumed_utc")]
    out, notes = with_notes("2022-01-01T10:00:00.123456789Z", "to_timestamp")
    assert out == datetime(2022, 1, 1, 10, 0, 0, 123456) and notes == [("timestamp_precision", "truncated")]
    assert with_notes("2022-01-01T10:00:00.123456000Z", "to_timestamp")[1] == []             # zeros: nothing is lost


def test_today_is_utc_like_now():
    from datetime import timezone

    assert t(None, "today") == datetime.now(timezone.utc).date()


# --- in the engine: each note reaches the report with table, column and file -------------------
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
