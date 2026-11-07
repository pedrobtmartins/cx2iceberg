"""Step 7: writing — declared types (no inference), pre-check before any write or DELETE,
Glue catalog, replace_by of lists that became empty, SQL literals, local files."""
import json
import logging
import sys
import types
from datetime import date, datetime
from decimal import Decimal

import pytest

from cx2iceberg import ConfigError, Document, Engine, from_dict, writer
from cx2iceberg.numbers import JsonFloat
from cx2iceberg.report import Report
from cx2iceberg.writer import (AthenaIcebergWriter, WriteCheckError, _sql_literal, check_offline, check_table,
                               compare_type, to_arrow, to_dataframe, write_local)

pa = pytest.importorskip("pyarrow")


def results(tables, docs, **cfg):
    report = cfg.pop("report", None)
    sub = {"name": "s", "tables": tables, **cfg.pop("sub", {})}
    eng = Engine(from_dict({"submodels": [sub], **cfg}), report=report)
    return eng.run([d if isinstance(d, Document) else Document(d, f"d{i}.json") for i, d in enumerate(docs)], partial=True)


ALL = {"big": "big ::bigint", "i": "i ::int", "n": "n ::tinyint", "x": "x ::double", "f": "f ::float", "b": "b ::boolean",
       "ts": "ts ::timestamp", "day": "day ::date", "d": "d ::decimal(5,2)", "s": "s ::string", "j": "j ::json",
       "xs": "xs ::array<int>", "st": {"path": "st", "type": "struct<a:decimal(5,2)>"}, "mp": {"path": "mp", "type": "map<string,bigint>"}}
FULL = {"big": 9007199254740993, "i": 7, "n": 1, "x": 1.5, "f": 0.5, "b": True, "ts": "9999-12-31T23:59:59Z", "day": "2022-01-01",
        "d": 1.5, "s": "é", "j": {"k": 1}, "xs": [1, None], "st": {"a": 1.25}, "mp": {"k": 1}}


# --- declared types, no inference -----------------------------------------------------
def test_dataframe_uses_the_declared_types():
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    df = to_dataframe(res)
    dtypes = {c: str(t) for c, t in df.dtypes.items()}
    assert dtypes["big"] == "Int64" and dtypes["i"] == "Int32" and dtypes["n"] == "Int8" and dtypes["f"] == "float32"
    assert dtypes["b"] == "boolean" and dtypes["ts"] == "datetime64[us]" and dtypes["d"] == "object"
    assert df["big"][0] == 9007199254740993                     # before: float64 with the 2nd row's null -> ...992
    assert df["ts"][0] == datetime(9999, 12, 31, 23, 59, 59)     # before: pandas 2 broke (outside ns)


def test_arrow_table_has_the_declared_schema():
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    schema = {f.name: str(f.type) for f in to_arrow(res).schema}
    assert schema["n"] == "int8" and schema["d"] == "decimal128(5, 2)" and schema["f"] == "float"    # before: double, (3,2)
    assert schema["mp"] == "map<string, int64>" and schema["st"] == "struct<a: decimal128(5, 2)>"
    res.rows[0]["n"] = 300                                          # a value that does not fit (only possible if something failed earlier)
    with pytest.raises(WriteCheckError, match="table 't', column 'n' \\(tinyint\\)"):
        to_arrow(res)


def test_iceberg_has_no_tinyint_or_smallint():
    # Iceberg only has int and long: in the table they are int; the declared type still validates the range in the cast
    from cx2iceberg.athena_types import athena_type
    from cx2iceberg.writer import ddl
    assert athena_type("tinyint") == "int" and athena_type("array<smallint>") == "array<int>"
    assert athena_type("struct<a:tinyint,j:json>") == "struct<a:int,j:string>"
    res = results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 1}])["t"]
    assert "`n` int" in ddl(res, {})
    rep = Report()
    assert results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 300}], report=rep)["t"].rows == []
    assert rep.count(reason="invalid_value", column="n", kind="error") == 1       # 300 does not fit in a tinyint


def test_offline_check_runs_awswranglers_own_conversion():
    pytest.importorskip("awswrangler")
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    check_offline(res)
    res.rows[0]["i"] = "x"                                          # awswrangler would not accept it either
    with pytest.raises(WriteCheckError):
        check_offline(res)


def test_offline_check_without_awswrangler_still_checks_the_arrow_schema(monkeypatch):
    # dry run on a machine without awswrangler (or with a changed API): the Arrow check is still done
    monkeypatch.setattr(writer, "_wr_conversion", lambda: None)
    res = results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 1}])["t"]
    check_offline(res)
    res.rows[0]["n"] = 300
    with pytest.raises(WriteCheckError, match="column 'n' \\(tinyint\\)"):
        check_offline(res)


def test_awswrangler_api_change_is_not_reported_as_a_data_error(monkeypatch, caplog):
    wr = pytest.importorskip("awswrangler")
    monkeypatch.setattr(wr._arrow, "_df_to_table", lambda table, other: None)          # another signature
    res = results([{"name": "t", "columns": {"i": "i ::int"}}], [{"i": 1}])["t"]
    check_offline(res)                                               # does not block the write
    assert "awswrangler pre-check skipped" in caplog.text


# --- checks that do not depend on the catalog ----------------------------------------------
def test_table_checks():
    res = results([{"name": "q", "partition_by": ["day(at)", "contry"], "columns": {"at": "at ::timestamp", "country": "c"}}],
                  [{"at": "2022-01-01T00:00:00Z", "c": "PT"}])["q"]
    with pytest.raises(WriteCheckError, match="partition_by uses \\['contry'\\]"):
        check_table(res)
    res = results([{"name": "r", "explode": "items", "replace_by": ["ids"], "columns": {"ids": "$root.ids ::array<string>", "x": "x"}}],
                  [{"ids": ["a"], "items": [{"x": 1}]}])["r"]
    with pytest.raises(WriteCheckError, match="replace_by 'ids' must be a simple column"):
        check_table(res)


# --- Glue catalog -----------------------------------------------------------------------------------
@pytest.mark.parametrize("ours,table,expected", [
    ("int", "int", "same"), ("json", "string", "same"), ("decimal(10,2)", "decimal(10, 2)", "same"),
    ("array<int>", "array<int>", "same"), ("struct<a:int>", "struct<a:int>", "same"),
    ("int", "bigint", "table_wider"), ("decimal(10,2)", "decimal(12,2)", "table_wider"),
    ("bigint", "int", "ours_wider"), ("decimal(12,2)", "decimal(10,2)", "ours_wider"),
    ("double", "string", "incompatible"), ("decimal(10,2)", "decimal(10,3)", "incompatible"), ("int", "double", "incompatible"),
    # all our values fit in the table's type (no ALTER): decimal with more integer and decimal digits, integers
    ("decimal(10,2)", "decimal(12,3)", "table_wider"), ("decimal(10,2)", "decimal(11,3)", "table_wider"),
    ("tinyint", "int", "same"), ("smallint", "bigint", "table_wider"),          # in the Iceberg table, tinyint is int
    # they do not fit and Iceberg does not widen like this: the scale changes (it would round) or fewer integer digits
    ("decimal(10,3)", "decimal(12,2)", "incompatible"), ("decimal(12,3)", "decimal(10,2)", "incompatible"),
    ("bigint", "tinyint", "incompatible"),
    # float (32 bits) <-> double: the values would read differently (1.1 -> 1.100000023841858), in the new rows or the old ones
    ("float", "double", "incompatible"), ("double", "float", "incompatible"), ("bigint", "decimal(20,0)", "incompatible"),
])
def test_compare_type(ours, table, expected):
    assert compare_type(ours, table) == expected


class FakeAWS:
    """Fake awswrangler only at the AWS boundary; the conversions are the real ones (if awswrangler is installed)."""

    def __init__(self, monkeypatch, tables):
        self.calls, self.tables = [], tables
        real = None
        try:
            import awswrangler as real
        except ImportError:
            pass
        real = getattr(real, "_real", real)     # 2nd FakeAWS in the same test: the imported module is the previous fake
        fake = types.SimpleNamespace(
            catalog=types.SimpleNamespace(create_database=lambda *a, **k: None,
                                          does_table_exist=lambda database, table, **k: table in self.tables,
                                          get_table_types=lambda database, table, **k: dict(self.tables[table])),
            athena=types.SimpleNamespace(to_iceberg=lambda **k: self.calls.append(("to_iceberg", k["table"], k)),
                                         start_query_execution=lambda sql, **k: self.calls.append(("SQL", sql, None))))
        fake._real = real
        if real is not None:
            fake._data_types, fake._arrow = real._data_types, real._arrow
        monkeypatch.setitem(sys.modules, "awswrangler", fake)
        monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=lambda **k: object()))

    def write(self, res, report=None, **target):
        return AthenaIcebergWriter({"database": "db", "temp_path": "s3://x/t", **target}, report=report).write(res)


def test_incompatible_catalog_type_stops_before_any_delete_or_write(monkeypatch):
    aws = FakeAWS(monkeypatch, {"kids": {"pid": "string", "q": "string"}})
    rep = Report()
    res = results([{"name": "kids", "explode": "items", "replace_by": ["pid"], "columns": {"pid": "$root.id", "q": "q ::double"}}],
                  [{"id": "P", "items": [{"q": 1.5}]}], report=rep)
    with pytest.raises(WriteCheckError, match="the table has string and the configuration double"):
        aws.write(res, report=rep)
    assert aws.calls == []                                           # neither DELETE nor write
    assert rep.status == "failed" and rep.write["tables"]["kids"]["status"] == "not_written"


def test_catalog_wider_type_is_used_and_narrower_is_widened(monkeypatch):
    aws = FakeAWS(monkeypatch, {"t": {"id": "string", "n": "bigint", "m": "int"}})
    rep = Report()
    res = results([{"name": "t", "columns": {"id": "id", "n": "n ::int", "m": "m ::bigint"}}], [{"id": "a", "n": 1, "m": 2}], report=rep)
    aws.write(res, report=rep)
    dtype = aws.calls[0][2]["dtype"]
    assert dtype["n"] == "bigint" and dtype["m"] == "bigint"       # n: the table's type (before: ALTER bigint -> int)
    assert rep.count(reason="column_widened", column="m") == 1
    aws = FakeAWS(monkeypatch, {"t": {"id": "string", "n": "bigint", "m": "int"}})
    with pytest.raises(WriteCheckError, match="schema_evolution: false"):
        aws.write(res, schema_evolution=False)


def test_new_columns_are_reported(monkeypatch):
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    rep = Report()
    res = results([{"name": "t", "columns": {"id": "id", "co2": "co2 ::double"}}], [{"id": "a", "co2": 1.5}], report=rep)
    aws.write(res, report=rep)
    assert rep.count(reason="column_added", column="co2") == 1
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    with pytest.raises(WriteCheckError, match="new column 'co2' and target.schema_evolution: false"):
        aws.write(res, schema_evolution=False)
    assert aws.calls == []


def test_schema_notes_are_not_reported_when_the_precheck_blocks_the_write(monkeypatch):
    # table 'a' would gain a column, but 'b' blocks the load: the report must not say that 'extra' was added
    aws = FakeAWS(monkeypatch, {"a": {"id": "string"}, "b": {"id": "string", "v": "int"}})
    rep = Report()
    res = results([{"name": "a", "columns": {"id": "id", "extra": "n ::int"}}, {"name": "b", "columns": {"id": "id", "v": "v"}}],
                  [{"id": "1", "n": 1, "v": "x"}], report=rep)
    with pytest.raises(WriteCheckError):
        aws.write(res, report=rep)
    assert aws.calls == [] and rep.count(reason="column_added") == 0


def test_merge_would_null_columns_the_config_does_not_produce(monkeypatch):
    table = {"name": "m", "mode": "merge", "keys": ["id"], "columns": {"id": "id", "co2": "co2 ::double"}}
    aws = FakeAWS(monkeypatch, {"m": {"id": "string", "co2": "double", "verified_by": "string"}})
    res = results([table], [{"id": "a", "co2": 1.5}])
    with pytest.raises(WriteCheckError, match="\\['verified_by'\\] exist in the table but the configuration does not produce them"):
        aws.write(res)                                               # before: NULL over 'TUV-X' (see DuckDB)
    assert aws.calls == []
    rep = Report()
    res = results([{**table, "missing_columns": "null"}], [{"id": "a", "co2": 1.5}], report=rep)
    aws.write(res, report=rep)
    assert aws.calls and rep.count(reason="missing_columns", kind="declared") == 1
    aws = FakeAWS(monkeypatch, {"a": {"id": "string", "old": "string"}})
    aws.write(results([{"name": "a", "columns": {"id": "id"}}], [{"id": "x"}]))   # append: new rows with NULL
    assert aws.calls[0][0] == "to_iceberg"
    # auto table: the missing column is a field the new version of the document no longer has -> NULL, counted
    aws = FakeAWS(monkeypatch, {"s": {"catena_x_id": "string", "comment": "string", "co2": "double"}})
    rep = Report()
    aws.write(results([], [{"catenaXId": "u1", "co2": 1.5}], report=rep, sub={"auto_normalize": {"children": "batch"}}),
              report=rep)
    assert aws.calls and rep.count(reason="missing_columns", kind="declared") == 1


# --- replace_by ------------------------------------------------------------------------------------------
CHILD = {"name": "kids", "explode": "items", "mode": "append", "replace_by": ["pid"], "columns": {"pid": "$root.id", "c": "c"}}


def test_replace_by_deletes_old_rows_of_a_list_that_became_empty(monkeypatch):
    aws = FakeAWS(monkeypatch, {"kids": {"pid": "string", "c": "string"}})
    rep = Report()
    res = results([CHILD], [{"id": "P", "items": []}, {"id": "Q", "items": [{"c": "x"}]}], report=rep)
    aws.write(res, report=rep)
    sql = [c[1] for c in aws.calls if c[0] == "SQL"]
    assert sql == ['DELETE FROM "db"."kids" WHERE ("pid" = \'P\') OR ("pid" = \'Q\')']   # before: P kept old children
    assert rep.write["tables"]["kids"]["replaced_documents"] == 2


def test_replace_by_with_all_rows_filtered_still_deletes(monkeypatch):
    aws = FakeAWS(monkeypatch, {"kids": {"pid": "string", "c": "string"}})
    res = results([{**CHILD, "where": {"path": "c", "op": "eq", "value": "keep"}}], [{"id": "P", "items": [{"c": "drop"}]}])
    aws.write(res)
    assert [c[1] for c in aws.calls] == ['DELETE FROM "db"."kids" WHERE ("pid" = \'P\')']


def test_replace_by_literal_uses_the_type_of_the_table_column(monkeypatch):
    # decimal(10,2) declared, decimal(12,3) in the table: written and compared with the table's type
    aws = FakeAWS(monkeypatch, {"t": {"k": "decimal(12,3)", "c": "string"}})
    table = {"name": "t", "explode": "items", "replace_by": ["k"], "columns": {"k": "$root.k ::decimal(10,2)", "c": "c"}}
    aws.write(results([table], [{"k": 1.5, "items": [{"c": "a"}]}]))
    assert aws.calls[0][1] == 'DELETE FROM "db"."t" WHERE ("k" = CAST(\'1.50\' AS decimal(12,3)))'
    assert aws.calls[1][2]["dtype"]["k"] == "decimal(12,3)"


def test_float_and_double_do_not_mix_with_the_table(monkeypatch):
    # 1.1 accepted as a float (32 bits) is 1.100000023841858; in a double column it would read like that -> error, not a widening
    for ours, table in (("float", "double"), ("double", "float")):
        aws = FakeAWS(monkeypatch, {"t": {"x": table}})
        with pytest.raises(WriteCheckError, match="float is 32-bit"):
            aws.write(results([{"name": "t", "columns": {"x": f"x ::{ours}"}}], [{"x": 1.1}]))
        assert aws.calls == []


def test_rejected_documents_never_delete(monkeypatch):
    aws = FakeAWS(monkeypatch, {"kids": {"pid": "string", "c": "string", "n": "int"}})
    res = results([{**CHILD, "columns": {**CHILD["columns"], "n": "n ::int"}}],
                  [{"id": "P", "items": [{"c": "x", "n": "bad"}]}, {"id": "Q", "items": []}])
    aws.write(res)
    assert [c[1] for c in aws.calls if c[0] == "SQL"] == ['DELETE FROM "db"."kids" WHERE ("pid" = \'Q\')']


def test_replace_by_key_of_a_document_without_it_is_an_error():
    rep = Report()
    res = results([CHILD], [{"items": []}], report=rep)
    assert rep.count(reason="null_key", kind="error") == 1 and not res["kids"].replace_keys


@pytest.mark.parametrize("table,fragment", [
    ({**CHILD, "columns": {"pid": "pid", "c": "c"}}, "depends on the explode element"),
    ({**CHILD, "replace_by": ["nope"]}, "replace_by 'nope' is not a declared column"),
])
def test_replace_by_columns_are_validated(table, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [table]}]})


def test_replace_by_on_the_document_id_column_is_allowed():
    res = results([{**CHILD, "replace_by": ["doc"], "columns": {"c": "c"}}], [{"id": "P", "items": []}],
                  sub={"document_id": {"column": "doc", "from": "id"}})
    assert len(res["kids"].replace_keys) == 1


# --- SQL literals of the DELETE ------------------------------------------------------------------------------
@pytest.mark.parametrize("value,t,expected", [
    (1.1, "float", "CAST('1.1' AS REAL)"),                    # before: 1.1 (double) never equal to the stored real
    (1e-05, "double", "CAST('1e-05' AS DOUBLE)"),
    (JsonFloat("0.12345678901234567890"), "double", "CAST('0.12345678901234567890' AS DOUBLE)"),
    (Decimal("1.50"), "decimal(5,2)", "CAST('1.50' AS decimal(5,2))"),
    (7, "bigint", "7"), (True, "boolean", "TRUE"), ("O'Brien", "string", "'O''Brien'"),
    (datetime(2022, 1, 1, 10, 0, 0, 123456), "timestamp", "TIMESTAMP '2022-01-01 10:00:00.123456'"),
    (date(2022, 1, 1), "date", "DATE '2022-01-01'"),
])
def test_sql_literals(value, t, expected):
    assert _sql_literal(value, t) == expected


# --- local files ------------------------------------------------------------------------------------
def test_local_files_keep_types_and_distinguish_null_from_empty(tmp_path):
    res = results([{"name": "l", "columns": {"s": "s ::string", "e": "e ::string", "xs": "xs ::array<string>", "b": "b ::boolean",
                                            "d": "d ::decimal(5,2)", "n": "n ::tinyint", "big": "big ::bigint"}}],
                  [{"e": "", "xs": ["a"], "b": True, "d": 1.5, "n": 1, "big": 9007199254740993}, {"s": "x"}])
    for fmt in ("csv", "jsonl", "parquet"):
        write_local(res, str(tmp_path), fmt)
    assert (tmp_path / "l.csv").read_text().splitlines() == [
        '"s","e","xs","b","d","n","big"', ',"","[""a""]","true","1.50","1","9007199254740993"', '"x",,,,,,']
    first = (tmp_path / "l.jsonl").read_text().splitlines()[0]
    assert first == '{"s": null, "e": "", "xs": ["a"], "b": true, "d": 1.50, "n": 1, "big": 9007199254740993}'
    assert json.loads(first, parse_float=Decimal)["d"] == Decimal("1.50")                 # before: "1.50" (text)
    import pyarrow.parquet as pq

    t = pq.read_table(tmp_path / "l.parquet")
    assert {f.name: str(f.type) for f in t.schema}["n"] == "int8" and t.column("big").to_pylist() == [9007199254740993, None]


def test_auto_normalize_child_from_the_model_deletes_when_the_list_became_empty(monkeypatch, tmp_path):
    # before: with no document of the batch having the list, the child table was not even generated and the old children stayed
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"}, "items": {"type": "array", "items": {
        "type": "object", "properties": {"q": {"type": "number"}}}}}}
    (tmp_path / "m.json").write_text(json.dumps(schema))
    cfg = from_dict({"submodels": [{"name": "s", "model": {"schema": "m.json"}, "auto_normalize": {}}]}, tmp_path)
    res = Engine(cfg).run([Document({"catenaXId": "urn:1", "items": []}, "d.json")])
    aws = FakeAWS(monkeypatch, {"s": {"catena_x_id": "string"}, "s_items": {"catena_x_id": "string", "items_idx": "int", "q": "double"}})
    aws.write(res)
    assert ("SQL", 'DELETE FROM "db"."s_items" WHERE ("catena_x_id" = \'urn:1\')', None) in aws.calls


def test_overwrite_without_rows_empties_the_table(monkeypatch):
    # before: 0 rows = "nothing to write", and the old content stayed while the other tables were replaced
    table = {"name": "t", "mode": "overwrite", "where": {"path": "ok", "op": "eq", "value": True}, "columns": {"id": "id"}}
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    rep = Report()
    aws.write(results([table], [{"id": "a", "ok": False}], report=rep), report=rep)
    assert aws.calls == [("SQL", 'DELETE FROM "db"."t"', None)]
    assert rep.write["tables"]["t"]["emptied"] is True and "table emptied" in rep.render()
    # with no documents of this submodel, the batch says nothing about the table: it is left alone
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    aws.write(results([table], []))
    assert aws.calls == []
    # a rejected document does not count
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    bad = {**table, "columns": {"id": "id", "n": "n ::int"}}
    aws.write(results([bad], [{"id": "a", "ok": True, "n": "x"}]))        # 'x' in an int: document rejected
    assert aws.calls == []


# --- configuration ------------------------------------------------------------------------------------------
@pytest.mark.parametrize("table,fragment", [
    ({"name": "t", "columns": {"fossilGhg": "a", "fossil_ghg": "b"}}, "'fossilGhg' and 'fossil_ghg' give the same column"),
    ({"name": "t", "athena_options": {"mode": "overwrite"}, "columns": {"a": "a"}}, "decided by the configuration"),
    ({"name": "t", "athena_options": {"dtype": {"a": "int"}}, "columns": {"a": "a"}}, "decided by the configuration"),
    ({"name": "t", "mode": "overwrite_partitions", "partition_by": ["day(at)"], "columns": {"at": "at ::timestamp"}}, "partitions by column"),
    ({"name": "t", "mode": "overwrite_partitions", "columns": {"a": "a"}}, "needs partition_by"),
    ({"name": "t", "missing_columns": "keep", "columns": {"a": "a"}}, "missing_columns only accepts null"),
])
def test_write_options_are_validated_when_loading(table, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [table]}]})
    from_dict({"submodels": [{"tables": [{"name": "t", "athena_options": {"s3_additional_kwargs": {}}, "columns": {"a": "a"}}]}]})


@pytest.mark.parametrize("defaults,fragment", [
    ({"athena_options": {"mode": "overwrite"}}, "decided by the configuration"),     # before: it replaced the root table
    ({"mode": "overwrite_partitions", "partition_by": ["day(x)"]}, "partitions by column"),
    ({"missing_columns": "keep"}, "missing_columns only accepts null"),
    ({"mode": "upsert"}, "invalid mode 'upsert'"),
])
def test_auto_normalize_write_options_are_validated_when_loading(defaults, fragment):
    # auto_normalize's tables only exist during the load: the options come from the defaults and are checked at load time
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"name": "s", "auto_normalize": {}, "defaults": defaults}]})


def test_auto_normalize_passes_missing_columns_to_its_tables():
    cfg = from_dict({"submodels": [{"name": "s", "auto_normalize": {"children": "batch"}, "defaults": {"missing_columns": "null"}}]})
    res = Engine(cfg).run([Document({"catenaXId": "u", "items": [{"q": 1}]}, "d.json")])
    assert res["s"].spec.missing_columns == "null" and res["s_items"].spec.missing_columns == "null"


# --- command line and Lambda ------------------------------------------------------------------------------
def test_dry_run_runs_the_offline_precheck(tmp_path, capsys):
    from cx2iceberg.cli import main

    (tmp_path / "c.yaml").write_text("submodels:\n  - tables:\n      - {name: q, partition_by: [contry], columns: {country: c}}\n")
    (tmp_path / "d.json").write_text('{"c": "PT"}')
    with pytest.raises(SystemExit) as e:
        main(["run", "-c", str(tmp_path / "c.yaml"), str(tmp_path / "d.json"), "--dry-run"])
    assert "partition_by uses ['contry']" in str(e.value.code)


def test_missing_pyarrow_is_a_warning_not_silence(monkeypatch, caplog):
    res = results([{"name": "t", "columns": {"i": "i ::int"}}], [{"i": 1}])["t"]
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    check_offline(res)
    assert any(r.levelno == logging.WARNING and "NOT done" in r.getMessage() for r in caplog.records)


def test_lambda_logs_the_report_at_warning_when_something_happened(tmp_path, monkeypatch, caplog):
    import cx2iceberg.lambda_handler as L

    FakeAWS(monkeypatch, {})
    (tmp_path / "c.yaml").write_text('target: {database: db, temp_path: "s3://x/t"}\n'
                                     'submodels: [{name: s, tables: [{name: t, columns: {v: {path: v, type: double, on_error: "null"}}}]}]\n')
    monkeypatch.setenv("CX2ICEBERG_CONFIG", str(tmp_path / "c.yaml"))
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: [Document({"v": "x"}, "a")])
    caplog.set_level(logging.WARNING, logger="cx2iceberg")
    L.handler({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "a.json"}}}]}, None)
    assert any(r.levelno == logging.WARNING and "Losses declared" in r.getMessage() for r in caplog.records)
