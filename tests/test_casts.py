"""Step 5: casts and untyped columns — nothing is truncated, rounded or guessed without being declared."""
from datetime import date, datetime
from decimal import Decimal

import pytest
import yaml

from cx2iceberg import ConfigError, Document, Engine, TransformError, from_dict
from cx2iceberg import athena_types as T
from cx2iceberg import notes as N
from cx2iceberg.config import load_yaml
from cx2iceberg.numbers import ExactFloat, JsonFloat
from cx2iceberg.reader import loads


def cast(value, t, rounding=None, **settings):
    with N.scope(settings) as notes:
        out = T.cast(value, t, rounding)
    return out, [n[1:3] for n in notes]


def refuses(value, t, fragment, rounding=None):
    with pytest.raises(ValueError) as e:
        T.cast(value, t, rounding)
    assert fragment in str(e.value), str(e.value)


def engine(tables, **cfg):
    return Engine(from_dict({"submodels": [{"name": "s", "tables": tables}], **cfg}))


# --- booleans are not numbers -------------------------------------------------------
@pytest.mark.parametrize("t", ["int", "bigint", "tinyint", "double", "float", "decimal(5,2)"])
def test_booleans_are_not_numbers(t):
    refuses(True, t, "the boolean true is not a number")                                   # before: 1 / 1.0


@pytest.mark.parametrize("value,expected", [(True, True), ("TRUE", True), (" no ", False), (1, True), ("0", False)])
def test_boolean_cast(value, expected):
    assert T.cast(value, "boolean") is expected


# --- integers: the type's range -----------------------------------------------------------
@pytest.mark.parametrize("t,bits", [("tinyint", 8), ("smallint", 16), ("int", 32), ("bigint", 64)])
def test_integer_ranges(t, bits):
    lo, hi = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
    assert T.cast(lo, t) == lo and T.cast(hi, t) == hi and T.cast(str(hi), t) == hi
    refuses(hi + 1, t, f"does not fit in {t}")                                                # before: it only failed at write time
    refuses(lo - 1, t, f"does not fit in {t}")


def test_integers_from_numbers_written_with_a_fraction():
    assert T.cast(loads("1234567890123456789.0"), "bigint") == 1234567890123456789
    assert T.cast("1e3", "int") == 1000 and T.cast(2.0, "int") == 2
    refuses(2.5, "int", "is not an integer")


# --- decimal: nothing rounded without being declared ------------------------------------------
@pytest.mark.parametrize("value,t,expected", [
    ("1.25", "decimal(10,2)", "1.25"), (1.2, "decimal(10,2)", "1.20"), ("-0", "decimal(5,2)", "0.00"),
    (loads("0.12345678901234567890123"), "decimal(38,23)", "0.12345678901234567890123"),
    (int("9" * 35), "decimal(38,0)", "9" * 35),                              # before: an error (28-digit context)
    ("99999999.99", "decimal(10,2)", "99999999.99"),
])
def test_decimal_exact(value, t, expected):
    out, notes = cast(value, t)
    assert out == Decimal(expected) and notes == []


@pytest.mark.parametrize("value,t,fragment", [
    (1.255, "decimal(10,2)", "more than 2 decimal place(s)"),             # before: silently 1.26 (half-even)
    (0.004, "decimal(10,2)", "declare rounding"),                          # before: 0.00
    (123456.78, "decimal(5,2)", "does not fit in decimal(5,2)"),               # before: it only failed at write time
    ("100000000.00", "decimal(10,2)", "at most 8 digit(s)"),
    ("1e400", "decimal(38,2)", "does not fit"),
])
def test_decimal_refuses(value, t, fragment):
    refuses(value, t, fragment)


@pytest.mark.parametrize("mode,expected", [("half_even", "0.12"), ("half_up", "0.13"), ("down", "0.12"), ("ceiling", "0.13")])
def test_decimal_rounding_when_declared_is_counted(mode, expected):
    out, notes = cast("0.125", "decimal(10,2)", mode)
    assert out == Decimal(expected) and notes == [("rounded", "rounded")]
    assert cast("0.12", "decimal(10,2)", mode)[1] == []                                  # no rounding: no note


def test_decimal_type_is_checked_when_loading():
    for t in ("decimal(40,2)", "decimal(2,5)", "decimal(0,0)"):
        with pytest.raises((ConfigError, ValueError), match="precision"):
            from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": f"v ::{t}"}}]}]})


# --- double / float: the number must come out as it was written ----------------------------------
@pytest.mark.parametrize("value", [0.1, "0.1", loads("0.30000000000000004"), loads("69.14590545404381"), 2 ** 53,
                                   Decimal("1.5"), ExactFloat("0.33333333333333333333333333")])
def test_double_accepts_what_it_reproduces(value):
    out, notes = cast(value, "double")
    assert isinstance(out, float) and notes == []


@pytest.mark.parametrize("value,fragment", [
    (2 ** 53 + 1, "9007199254740993 does not fit exactly in a double"),               # before: ...992.0
    (loads("0.12345678901234567890"), "0.12345678901234567890 does not fit exactly"),
    ("0.12345678901234567890", "does not fit exactly"), (Decimal("0.12345678901234567890"), "does not fit exactly"),
    (10 ** 400, "out of range for a double"),
])
def test_double_refuses_lost_digits(value, fragment):
    refuses(value, "double", fragment)


def test_double_with_rounding_nearest_is_counted():
    out, notes = cast(2 ** 53 + 1, "double", "nearest")
    assert out == 9007199254740992.0 and notes == [("rounded", "rounded")]


def test_float_is_32_bits():
    assert T.cast(1.1, "float") == pytest.approx(1.1) and T.cast(0.5, "float") == 0.5
    refuses(1.23456789, "float", "does not fit in a 32-bit float (it would become 1.2345679)")  # before: it lost digits at write time
    refuses(3.5e38, "float", "out of range for a float")
    out, notes = cast(1.23456789, "float", "nearest")
    assert str(out).startswith("1.2345678") and notes == [("rounded", "rounded")]


# --- text, json, map, struct ------------------------------------------------------------
def test_string_keeps_the_number_as_written():
    assert T.cast(loads("0.1234567890123456789"), "string") == "0.1234567890123456789"     # before: 0.12345678901234568
    assert T.cast(True, "string") == "true" and T.cast({"a": Decimal("1.5")}, "string") == '{"a": 1.5}'


def test_json():
    assert T.cast({"a": [1, None]}, "json") == '{"a": [1, null]}' and T.cast('{"a": 1}', "json") == '{"a": 1}'
    refuses("abc", "json", "is not JSON")                                                  # before: it stored 'abc'
    refuses('{"a": NaN}', "json", "is not JSON")


def test_map_and_struct_key_collisions():
    refuses({"1": "a", "1.0": "b"}, "map<double,string>", "give the same key")           # before: one of the two was lost
    refuses({"fossilGhg": 1, "fossil_ghg": 2}, "struct<fossil_ghg:int>", "give the same name")
    refuses([1, 2], "map<string,int>", "expected an object")
    assert T.cast({"amount": 1.255}, "struct<amount:decimal(10,2)>", "half_up") == {"amount": Decimal("1.26")}
    refuses({"amount": 1.255}, "struct<amount:decimal(10,2)>", "declare rounding")


# --- dates -------------------------------------------------------------------------------
def test_timestamp_and_date_casts():
    assert T.cast("2022-02-04T14:48:54+02:00", "timestamp") == datetime(2022, 2, 4, 12, 48, 54)
    assert T.cast("2023-07-11T01:00:00+02:00", "date") == date(2023, 7, 11)              # before: 10 (UTC)
    refuses("2022-01-01T10:00:00", "timestamp", "has no time zone")
    refuses("2022-01-01", "timestamp", "the date (no time, becomes midnight) has no time zone")
    refuses(1641031200, "timestamp", "declare the unit")                                 # before: 2022-01-01 10:00
    out, notes = cast("2022-01-01T10:00:00", "timestamp", naive_timestamps="utc")
    assert out == datetime(2022, 1, 1, 10) and notes == [("naive_timestamp", "assumed_utc")]


# --- untyped columns ----------------------------------------------------------------------
def test_untyped_values_that_would_change_reject_their_document():
    eng = engine([{"name": "t", "columns": {"id": "id", "v": "v"}}])
    res = eng.run([Document({"id": "a", "v": 1.5}, "a"), Document({"id": "b", "v": 2 ** 53 + 1}, "b"),
                   Document({"id": "c", "v": loads("0.12345678901234567890")}, "c"),
                   Document({"id": "d", "v": [1, "x"]}, "d")], partial=True)["t"]
    assert [r["id"] for r in res.rows] == ["a"] and res.types["v"] == "double"
    msgs = {e["file"]: e["message"] for e in eng.report.events("error")}
    assert "does not fit exactly in a double" in msgs["b"] and "more digits than a double" in msgs["c"]
    assert "list with values of different types" in msgs["d"]


def test_untyped_type_conflict_fails_the_batch_with_examples():
    eng = engine([{"name": "t", "columns": {"v": "v"}}])
    with pytest.raises(TransformError) as e:
        eng.run([Document({"v": 1.5}, "a.json"), Document({"v": "n/a"}, "b.json")])    # before: the whole column as text ('1.5')
    assert "untyped column 'v' receives values of different types: double (a.json value 1.5); string (b.json value 'n/a')" \
           in str(e.value) and "Declare the column's type" in str(e.value)
    with pytest.raises(TransformError, match="different types"):
        engine([{"name": "t", "auto": True}]).run([Document({"v": 1}, "a"), Document({"v": True}, "b")])


def test_untyped_all_null_column_is_not_created_but_partition_columns_are_kept():
    # (null keys are an error since step 6; a partition column with only nulls is still created)
    eng = engine([{"name": "t", "partition_by": ["day(p)"], "columns": {"id": "id", "p": "p", "v": "v", "xs": "xs"}}])
    res = eng.run([Document({"id": "a", "v": None, "xs": []}, "a")])["t"]
    assert res.types == {"id": "string", "p": "string"} and res.rows == [{"id": "a", "p": None}]
    assert {e["column"] for e in eng.report.events("warning") if e["reason"] == "untyped_all_null"} == {"v", "xs"}


def test_empty_lists_do_not_conflict_with_typed_lists():
    res = engine([{"name": "t", "columns": {"xs": "xs"}}]).run([Document({"xs": []}, "a"), Document({"xs": [1.5]}, "b"),
                                                                Document({"xs": [None]}, "c")])["t"]
    assert res.types["xs"] == "array<double>"


def test_inferred_struct_values_that_do_not_fit_reject_only_their_document():
    eng = engine([{"name": "t", "columns": {"id": "id", "s": {"path": "s", "type": "struct"}}}])
    res = eng.run([Document({"id": "a", "s": {"x": 1.5}}, "a"), Document({"id": "b", "s": {"x": loads("0.12345678901234567890")}}, "b")],
                  partial=True)["t"]
    assert [r["id"] for r in res.rows] == ["a"] and res.types["s"] == "struct<x:double>"
    assert eng.report.events("error")[0]["file"] == "b"                             # before: it stopped the whole load


# --- configuration: rounding, YAML 1.2, booleans, input ------------------------------------
@pytest.mark.parametrize("col,fragment", [
    ({"path": "v", "type": "string", "rounding": "half_up"}, "rounding only applies"),
    ({"path": "v", "rounding": "half_up"}, "rounding only applies"),
    ({"path": "v", "type": "decimal(5,2)", "rounding": "nearest"}, "is invalid"),
    ({"path": "v", "type": "double", "rounding": "half_up"}, "is invalid"),
])
def test_rounding_is_checked_against_the_type(col, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"v": col}}]}]})


def test_yaml_is_read_as_yaml_1_2_without_duplicate_keys():
    assert load_yaml("m: {yes: Y, no: N, on: L, off: D}")["m"] == {"yes": "Y", "no": "N", "on": "L", "off": "D"}
    assert yaml.safe_load("m: {yes: Y, no: N, on: L, off: D}")["m"] == {True: "L", False: "D"}   # what used to happen
    assert load_yaml("a: 010\nb: 0o17\nc: 0x1A\nd: 1:30\ne: 1e3\nf: ~\ng: true\nh: .5") == \
        {"a": 10, "b": 15, "c": 26, "d": "1:30", "e": 1000.0, "f": None, "g": True, "h": 0.5}
    for text in ("columns: {a: x, a: y}", "a: 1\na: 2"):
        with pytest.raises(yaml.YAMLError, match="repeated key"):
            load_yaml(text)


def test_duplicate_column_in_a_config_file_is_refused(tmp_path):
    from cx2iceberg.config import load

    (tmp_path / "c.yaml").write_text("submodels:\n  - tables:\n      - name: t\n        columns:\n          co2: a\n          co2: b\n")
    with pytest.raises(ConfigError, match="repeated key 'co2'"):
        load(tmp_path / "c.yaml")


@pytest.mark.parametrize("raw,fragment", [
    ({"tables": [{"name": "t", "dedupe": "false", "columns": {"a": "a"}}]}, "dedupe: use true or false"),   # before: True
    ({"tables": [{"name": "t", "keep_empty": "no", "columns": {"a": "a"}}]}, "keep_empty"),
    ({"tables": [{"name": "t", "columns": {"a": {"path": "a", "required": "yes"}}}]}, "required"),
    ({"tables": [{"name": "t", "columns": {"a": {"path": "a", "check": {"in": ["x"], "ignore_case": 1}}}}]}, "ignore_case"),
    ({"unwrap": {"all": "true"}, "tables": [{"name": "t", "auto": True}]}, "all"),
])
def test_boolean_options_must_be_booleans(raw, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [raw]})


@pytest.mark.parametrize("inp,fragment", [
    ({"naive_timestamp": "utc"}, "unknown"), ({"naive_timestamps": "local"}, "unknown time zone: 'local'"),
    ({"list_is_document": "true"}, "use true or false"), ({"envelope": {"payload": "p"}}, "unknown"),
])
def test_input_is_validated(inp, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"input": inp, "submodels": [{"tables": [{"name": "t", "auto": True}]}]})
