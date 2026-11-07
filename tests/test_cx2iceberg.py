import json
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

from cx2iceberg import ConfigError, Document, Engine, TransformError, from_dict, transform
from cx2iceberg.config import load, sanitize
from cx2iceberg.paths import MISSING, Context, resolve, resolve_obj
from cx2iceberg.transforms import apply_chain

EX = Path(__file__).parent.parent / "examples"

SERIAL = {
    "catenaXId": "urn:uuid:abc",
    "localIdentifiers": [
        {"key": "partInstanceId", "value": "SN1"},
        {"key": "manufacturerId", "value": "BPNL1"},
    ],
    "partTypeInformation": {"nameAtManufacturer": "Mirror", "partClassification": []},
    "orders": [
        {"no": "A", "lines": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 2}]},
        {"no": "B", "lines": [{"sku": "z", "qty": 3.5}]},
    ],
}


def run(tables, docs=None, **sub):
    cfg = from_dict({"submodels": [{"name": "s", "tables": tables, **sub}]})
    return Engine(cfg).run([Document(d, "f.json", "urn:x") for d in (docs or [SERIAL])])


# --- paths --------------------------------------------------------------
def test_paths():
    assert resolve_obj(SERIAL, "partTypeInformation.nameAtManufacturer") == "Mirror"
    assert resolve_obj(SERIAL, "localIdentifiers[?key=manufacturerId].value") == ["BPNL1"]
    assert resolve_obj(SERIAL, "localIdentifiers[-1].key") == "manufacturerId"
    assert resolve_obj(SERIAL, "orders[*].lines[*].sku") == ["x", "y", "z"]
    assert resolve_obj(SERIAL, "orders[?no~^B].no") == ["B"]
    assert resolve_obj(SERIAL, "nope.nothing") is MISSING
    assert resolve_obj({"a.b": 1}, "['a.b']") == 1


def test_context_prefixes():
    root = Context([(SERIAL, None)], {"source_file": "f"})
    ctx = root.child(SERIAL["orders"][1], 1).child(SERIAL["orders"][1]["lines"][0], 0)
    assert resolve(ctx, "sku") == "z"
    assert resolve(ctx, "$parent.no") == "B"
    assert resolve(ctx, "$parent.$index") == 1
    assert resolve(ctx, "$root.catenaXId") == "urn:uuid:abc"
    assert resolve(ctx, "$meta.source_file") == "f"


# --- transformations --------------------------------------------------------
@pytest.mark.parametrize("value,chain,expected", [
    ("urn:uuid:123", "urn_suffix", "123"),
    ("  Ab ", ["trim", "lower"], "ab"),
    ("partInstanceId", "snake_case", "part_instance_id"),
    ("catenaXId", "snake_case", "catena_x_id"),
    ("a", {"map": {"a": "A", "_default": "?"}}, "A"),
    ("b", {"map": {"a": "A", "_default": "?"}}, "?"),
    (None, {"default": 0}, 0),
    ({"value": 2500, "unit": "unit:gram"}, {"unit_convert": {"to": "kilogram"}}, 2.5),
    (3, {"unit_convert": {"from": "hourUnitOfTime", "to": "minuteUnitOfTime"}}, 180.0),
    ("2022-02-04T14:48:54+02:00", "to_timestamp", datetime(2022, 2, 4, 12, 48, 54)),
    ("x-123-y", {"regex_extract": r"(\d+)"}, "123"),
    ([1, None, 2], "compact", [1, 2]),
    ([{"a": 1}, {"a": 2}], {"pluck": "a"}, [1, 2]),
    ("urn:samm:io.catenax.pcf:7.0.0#Pcf", {"semantic_id_parts": "version"}, "7.0.0"),
])
def test_transforms(value, chain, expected):
    assert apply_chain(value, chain) == expected


def test_unknown_transform():
    with pytest.raises(ValueError, match="Unknown transformation"):
        apply_chain(1, "does_not_exist")


# --- motor ---------------------------------------------------------------
def test_short_form_and_types():
    r = run([{"name": "t", "columns": {
        "id": "catenaXId | urn_suffix",
        "pid": "localIdentifiers[?key=partInstanceId].value | first | lower",
        "n": "localIdentifiers | count ::int",
        "label": {"template": "{catenaXId}/{partTypeInformation.nameAtManufacturer}"},
        "name": {"paths": ["missing", "partTypeInformation.nameAtManufacturer"]},
        "src": "$meta.source_file",
        "k": {"const": 7, "type": "bigint"},
    }}])["t"]
    assert r.rows == [{"id": "abc", "pid": "sn1", "n": 2, "label": "urn:uuid:abc/Mirror",
                       "name": "Mirror", "src": "f.json", "k": 7}]
    assert r.types["n"] == "int" and r.types["k"] == "bigint" and r.types["id"] == "string"


def test_nested_explode_with_parent_and_where():
    r = run([{"name": "lines", "explode": "orders[*].lines[*]",
              "where": {"path": "qty", "op": "gte", "value": 2},
              "columns": {"order": "$parent.no", "order_idx": "$parent.$index :: int",  # with spaces it is accepted too
                          "line_idx": "$index :: int", "sku": "sku", "qty": "qty"}}])["lines"]
    assert [(x["order"], x["sku"], x["qty"]) for x in r.rows] == [("A", "y", 2.0), ("B", "z", 3.5)]
    assert r.types["qty"] == "double"


def test_keep_empty_explode():
    cfg = from_dict({"submodels": [{"name": "s", "tables": [{"name": "c", "explode": "partTypeInformation.partClassification",
                                                            "keep_empty": True, "columns": {"id": "$root.catenaXId", "std": "classificationStandard"}}]}]})
    eng = Engine(cfg)
    r = eng.run([Document(SERIAL, "f.json", "urn:x")])["c"]
    assert r.rows == [{"id": "urn:uuid:abc"}]       # the row exists (keep_empty)
    # 'std' has no type and only nulls in this batch: it is not created (the type would be guessed) and it is in the report
    assert "std" not in r.types and eng.report.count(reason="untyped_all_null", column="std") == 1


def test_on_error_modes():
    bad = [{"catenaXId": "1", "q": "x"}, {"catenaXId": "2", "q": "3"}]
    cols = lambda mode: {"name": "t", "on_error": mode, "columns": {"id": "catenaXId", "q": {"path": "q", "type": "int"}}}
    assert [r["q"] for r in run([cols("null")], bad)["t"].rows] == [None, 3]
    res = run([cols("skip_row")], bad)["t"]
    assert [r["id"] for r in res.rows] == ["2"] and res.skipped == 1
    with pytest.raises(TransformError, match="column 'q'"):
        run([cols("fail")], bad)


def test_int_cast_does_not_truncate():
    with pytest.raises(TransformError, match="is not an integer"):
        run([{"name": "t", "columns": {"q": {"const": 2.5, "type": "int"}}}])


def test_required_and_dedupe():
    docs = [{"catenaXId": "1", "v": "a"}, {"catenaXId": "1", "v": "b"}, {"v": "c"}]
    res = run([{"name": "t", "mode": "merge", "keys": ["id"], "on_error": "skip_row",
                "columns": {"id": {"path": "catenaXId", "required": True}, "v": "v"}}], docs)["t"]
    assert res.rows == [{"id": "1", "v": "b"}] and res.skipped == 1


def test_auto_flatten():
    r = run([{"name": "t", "auto": True, "auto_exclude": ["orders"], "columns": {"id": "catenaXId"}}])["t"]
    row = r.rows[0]
    assert row["part_type_information_name_at_manufacturer"] == "Mirror"
    assert "catena_x_id" in row and "orders" not in row and "local_identifiers" not in row


def test_auto_normalize_tables_and_collisions():
    doc = {"catenaXId": "P", "createdOn": "2022-02-03T14:48:54.709Z",
           "childItems": [{"catenaXId": "C1", "sub": [{"k": 1}]}, {"catenaXId": "C2"}]}
    # children: batch — without model.schema it must be declared (the children only come from the batch's lists)
    res = run([], [doc], auto_normalize={"table_prefix": "bom", "children": "batch"}, defaults={"mode": "merge"})
    assert set(res) == {"bom", "bom_child_items", "bom_child_items_sub"}
    assert res["bom"].types["created_on"] == "timestamp"
    child = res["bom_child_items"]
    assert [(r["catena_x_id"], r["item_catena_x_id"]) for r in child.rows] == [("P", "C1"), ("P", "C2")]
    assert child.spec.replace_by == ["catena_x_id"] and child.spec.mode == "append"
    assert res["bom_child_items_sub"].rows[0]["child_items_idx"] == 0


def test_metadata_columns():
    r = run([{"name": "t", "metadata_columns": ["source_file", "record_hash"], "columns": {"id": "catenaXId"}}])["t"]
    assert r.rows[0]["cx_source_file"] == "f.json" and len(r.rows[0]["cx_record_hash"]) == 64


def test_matching():
    cfg = from_dict({"submodels": [
        {"name": "a", "match": {"semantic_id": "urn:samm:io.catenax.serial_part:*"}, "tables": [{"name": "a", "columns": {"x": "x"}}]},
        {"name": "b", "match": {"has_paths": ["childItems"]}, "tables": [{"name": "b", "columns": {"x": "x"}}]},
    ]})
    routed = Engine(cfg).route([
        Document({"x": 1}, "1.json", "urn:samm:io.catenax.serial_part:3.0.0#SerialPart"),
        Document({"x": 2, "childItems": []}, "2.json", None),
        Document({"x": 3}, "3.json", None),
    ])
    assert [d.source_file for d in routed["a"]] == ["1.json"]
    assert [d.source_file for d in routed["b"]] == ["2.json"]  # an empty list counts as existing


# --- configuration ----------------------------------------------------------
@pytest.mark.parametrize("raw,msg", [
    ({"submodels": [{"tables": [{"name": "t", "colums": {}}]}]}, "unknown"),
    ({"submodels": [{"tables": [{"name": "t", "mode": "merge", "columns": {"a": "a"}}]}]}, "needs 'keys'"),
    ({"submodels": [{"tables": [{"name": "t", "columns": {"a": {"type": "uuid"}}}]}]}, "Unsupported"),
    ({"submodels": []}, "there are no"),
])
def test_config_errors(raw, msg):
    with pytest.raises((ConfigError, ValueError), match=msg):
        from_dict(raw)


def test_sanitize():
    assert sanitize("catenaXId") == "catena_x_id"
    assert sanitize("classificationID") == "classification_id"
    assert sanitize("2nd-value") == "c_2nd_value"


def test_short_form_in_yaml(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("submodels:\n  - tables:\n      - name: t\n        columns:\n"
                 "          n: localIdentifiers | count ::int\n          d: x ::decimal(10,2)\n")
    cols = {c.name: c for c in load(p).submodels[0].tables[0].columns}
    assert cols["n"].type == "int" and cols["n"].transform == ["count"] and cols["d"].type == "decimal(10,2)"
    p.write_text("submodels:\n  - tables:\n      - name: t\n        columns:\n          n: x :: int\n")
    with pytest.raises(ConfigError, match="::int"):
        load(p)


def test_env_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("DB", "prod_db")
    p = tmp_path / "c.yaml"
    p.write_text("target: {database: ${DB}, workgroup: ${WG:-primary}}\nsubmodels: [{tables: [{name: t, auto: true}]}]")
    cfg = load(p)
    assert cfg.target == {"database": "prod_db", "workgroup": "primary"}


# --- full example + write -------------------------------------------
def test_example_config_end_to_end(tmp_path):
    from cx2iceberg.writer import ddl, write_local

    cfg = load(EX / "config.yaml")
    res = transform(cfg, [str(EX / "data")])
    assert len(res["serial_part"].rows) == 2
    assert res["serial_part"].rows[0]["part_instance_id"] == "SN12345678"
    assert [r["quantity_kg"] for r in res["bom_as_built_edges"].rows] == [2.5, None]
    assert len(res["bom_as_built_alternatives"].rows) == 1
    assert len(res["pcf_pcf_cross_sectoral_standards_used"].rows) == 2
    assert "PARTITIONED BY (day(created_on))" in ddl(res["bom_as_built_edges"], cfg.target)
    files = write_local(res, str(tmp_path), "parquet")
    assert len(files) == len(res)


@pytest.fixture
def fake_wr(monkeypatch):
    calls = {"to_iceberg": [], "sql": []}
    wr = types.SimpleNamespace(
        catalog=types.SimpleNamespace(create_database=lambda *a, **k: None, does_table_exist=lambda **k: True,
                                  get_table_types=lambda **k: {}),   # the writer reads the catalog
        athena=types.SimpleNamespace(
            to_iceberg=lambda **k: calls["to_iceberg"].append(k),
            start_query_execution=lambda sql, **k: calls["sql"].append(sql),
        ),
    )
    boto3 = types.SimpleNamespace(Session=lambda **k: object())
    monkeypatch.setitem(sys.modules, "awswrangler", wr)
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    return calls


def test_athena_writer_calls(fake_wr):
    from cx2iceberg.writer import AthenaIcebergWriter

    cfg = load(EX / "config.yaml")
    res = transform(cfg, [str(EX / "data")])
    AthenaIcebergWriter({**cfg.target, "database": "db"}).write(res)
    by_table = {c["table"]: c for c in fake_wr["to_iceberg"]}
    sp = by_table["serial_part"]
    assert sp["merge_cols"] == ["catenax_id"] and sp["partition_cols"] == ["manufacturing_country"]
    assert sp["dtype"]["local_identifiers_json"] == "string" and sp["dtype"]["site_ids"] == "array<string>"
    assert sp["table_location"] == "s3://my-bucket/warehouse/serial_part/"
    assert "merge_cols" not in by_table["bom_as_built_alternatives"]
    assert any(s.startswith('DELETE FROM "db"."serial_part_local_identifiers" WHERE ("catenax_id" = ')
               for s in fake_wr["sql"])


# --- unwrap / short names / struct / SAMM schema -------------------------
from cx2iceberg import athena_types as AT  # noqa: E402
from cx2iceberg.engine import column_names, unwrap  # noqa: E402
from cx2iceberg.config import parse_unwrap  # noqa: E402

NESTED = {
    "id": "P1",
    "general": [{"comment": "c"}],
    "stages": [{"production": [{"fossil": 1.0, "landMgmt": 2}], "packaging": [{"fossil": 3.0, "landMgmt": 4}]}],
    "attest": [{"name": "a"}, {"name": "b"}],
    "ids": ["x", "y"],
    "empty": [],
}


def test_unwrap_rules():
    r = unwrap(NESTED, parse_unwrap({"all": True, "except": ["attest"]}, "u"))
    assert r["general"] == {"comment": "c"} and r["stages"]["production"]["fossil"] == 1.0
    assert len(r["attest"]) == 2 and r["ids"] == ["x", "y"] and r["empty"] == []
    assert unwrap(NESTED, parse_unwrap(["general", "empty"], "u"))["empty"] is None
    with pytest.raises(TransformError, match="'attest' has 2 elements"):
        unwrap(NESTED, parse_unwrap("*", "u"), "f.json")
    first = unwrap(NESTED, parse_unwrap({"all": True, "on_multiple": "first"}, "u"))
    assert first["attest"] == {"name": "a"}
    with pytest.raises(ConfigError, match="take no indices"):
        parse_unwrap(["general[0]"], "u")


def test_leaf_naming_resolves_collisions():
    names = column_names([("stages", "production", "fossil"), ("stages", "packaging", "fossil"), ("general", "comment")], "leaf")
    assert names == {("stages", "production", "fossil"): "production_fossil",
                     ("stages", "packaging", "fossil"): "packaging_fossil", ("general", "comment"): "comment"}


_EM = {"type": "object", "properties": {"fossil": {"type": "number"}, "landMgmt": {"type": "number"}}}
NESTED_SCHEMA = {"type": "object", "properties": {
    "id": {"type": "string"},
    "general": {"type": "array", "items": {"type": "object", "properties": {"comment": {"type": "string"}}}},
    "stages": {"type": "array", "items": {"type": "object", "properties": {
        "production": {"type": "array", "items": {"$ref": "#/components/schemas/Em"}},
        "packaging": {"type": "array", "items": {"$ref": "#/components/schemas/Em"}}}}},
    "attest": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}}}},
    "ids": {"type": "array", "items": {"type": "string"}},
    "empty": {"type": "array", "items": {"type": "object", "properties": {"z": {"type": "number"}}}}},
    "components": {"schemas": {"Em": _EM}}}


def test_unwrap_with_auto_normalize_and_leaf_naming(tmp_path):
    # naming: leaf needs model.schema (the names come from the model, not from each document's fields)
    (tmp_path / "m.json").write_text(json.dumps(NESTED_SCHEMA))
    cfg = from_dict({"submodels": [{"name": "s", "model": {"schema": "m.json"}, "unwrap": {"all": True, "except": ["attest"]},
                                    "auto_normalize": {"table_prefix": "t", "id_path": "id", "id_column": "doc_id",
                                                       "naming": "leaf"}}]}, tmp_path)
    res = Engine(cfg).run([Document(NESTED, "f.json", "urn:x")])
    assert set(res) == {"t", "t_attest"}
    row = res["t"].rows[0]
    assert row["comment"] == "c" and row["production_fossil"] == 1.0 and row["packaging_land_mgmt"] == 4.0
    assert row["ids"] == ["x", "y"]


@pytest.mark.parametrize("t,expected", [
    ("struct<a:int, bCamel:array<struct<c:string>>>", "struct<a:int,b_camel:array<struct<c:string>>>"),
    ("map<string, decimal(10,2)>", "map<string,decimal(10,2)>"),
    ("array<struct<x:json>>", "array<struct<x:json>>"),
])
def test_type_parsing(t, expected):
    assert AT.normalize(t) == expected
    assert "json" not in AT.athena_type("struct<x:json>")


def test_struct_cast():
    t = "struct<fossil_ghg:double, created:timestamp, tags:array<string>>"
    v = AT.cast([{"fossilGhg": "1.5", "created": "2022-01-01T00:00:00Z", "tags": "a", "extra": 1}], t)
    assert v == {"fossil_ghg": 1.5, "created": datetime(2022, 1, 1), "tags": ["a"]}
    with pytest.raises(ValueError, match="expected 1 object"):
        AT.cast([{}, {}], t)


def test_struct_inference_columns():
    doc = {"id": "1", "stages": NESTED["stages"], "attest": NESTED["attest"], "pre": [{}]}
    res = run([{"name": "t", "columns": {
        "id": "id",
        "stages": {"path": "stages", "type": "struct"},
        "attest": {"path": "attest", "type": "array<struct>"},
        "pre": {"path": "pre", "type": "array<struct>"},
    }}], [doc])["t"]
    assert res.types["stages"].startswith("struct<production:array<struct<fossil:double,land_mgmt:double>>")
    assert res.types["attest"] == "array<struct<name:string>>"
    assert res.types["pre"] == "array<json>"  # empty struct -> json (Athena does not accept struct<>)
    assert res.rows[0]["attest"] == [{"name": "a"}, {"name": "b"}]


SCHEMA = {
    "x-samm-aspect-model-urn": "urn:samm:io.catenax.demo:1.0.0#Demo",
    "type": "object",
    "components": {"schemas": {
        "Timestamp": {"type": "string", "pattern": "..."},
        "Block": {"type": "object", "properties": {"created": {"$ref": "#/components/schemas/Timestamp"},
                                                    "version": {"type": "integer"}}},
        "BlockList": {"type": "array", "items": {"$ref": "#/components/schemas/Block"}},
    }},
    "properties": {"id": {"type": "string"}, "block": {"$ref": "#/components/schemas/BlockList"},
                   "codes": {"type": "array", "items": {"type": "string"}}},
}


def test_init_from_samm_schema_flat_and_struct():
    import yaml

    from cx2iceberg.samm import skeleton
    from cx2iceberg.scaffold import scaffold

    flat = yaml.safe_load(scaffold([skeleton(SCHEMA)], "demo", "urn:x", unwrap_rule="*", naming="leaf", scalar_lists="rows"))
    tables = {t["name"]: t for t in flat["submodels"][0]["tables"]}
    assert tables["demo"]["columns"]["created"] == {"path": "block.created", "type": "timestamp"}
    assert tables["demo"]["columns"]["version"] == {"path": "block.version", "type": "bigint"}
    assert tables["demo_codes"]["columns"]["value"] == "$item"
    struct = yaml.safe_load(scaffold([skeleton(SCHEMA)], "demo", unwrap_rule="*", structs=True))
    cols = struct["submodels"][0]["tables"][0]["columns"]
    assert cols["block"]["type"] == "struct<created:timestamp,version:bigint>"
    assert cols["codes"]["type"] == "array<string>"
    # the generated configuration runs on real data
    cfg = from_dict({"submodels": struct["submodels"]})
    out = Engine(cfg).run([Document({"id": "1", "block": [{"created": "2024-01-01T00:00:00Z", "version": 3}], "codes": ["a"]})])
    assert out["demo"].rows[0]["block"] == {"created": datetime(2024, 1, 1), "version": 3}


@pytest.mark.parametrize("cfg_file,n_tables", [("pcf9_flat.yaml", 9), ("pcf9_struct.yaml", 1)])
def test_pcf9_examples(cfg_file, n_tables):
    wr = pytest.importorskip("awswrangler")       # extra [dev]: without it the test is skipped, it does not fail
    pa = pytest.importorskip("pyarrow")

    from cx2iceberg.athena_types import athena_type
    from cx2iceberg.writer import to_dataframe

    cfg = load(EX / cfg_file)
    res = transform(cfg, [str(EX / "data" / "pcf_9.json")])
    assert len(res) == n_tables
    main = res["pcf9"].rows[0]
    assert main["pcf9_id"] == "3893bb5d-da16-4dc1-9185-11d97476c254"
    for r in res.values():  # the schema must be accepted by pyarrow the way awswrangler builds it
        df = to_dataframe(r)
        schema = wr._data_types.pyarrow_schema_from_pandas(df, index=False, dtype={c: athena_type(t) for c, t in r.types.items()})
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def test_match_any_and_clamp():
    cfg = from_dict({"submodels": [{"name": "a", "match": {"any": [{"semantic_id": "urn:x:*"},
                                                                  {"where": {"path": "v", "op": "regex", "value": "^9"}}]},
                                    "tables": [{"name": "t", "columns": {"v": "v"}}]}]})
    routed = Engine(cfg).route([Document({"v": "9.0"}, "1"), Document({"v": "7"}, "2", "urn:x:1"), Document({"v": "7"}, "3")])
    assert [d.source_file for d in routed["a"]] == ["1", "2"]
    assert apply_chain(-2, {"clamp": {"min": 0}}) == 0 and apply_chain(5, {"clamp": {"max": 0}}) == 0


def _load(p):
    import json as _json

    return _json.loads(Path(p).read_text())


# --- cases / check / case-insensitive map -------------------------------------
def test_cases_conditional_values():
    cols = {"lu": "lu", "emissions": {"cases": [
        {"when": {"path": "lu", "op": "gte", "value": 0}, "path": "lu"},
        {"when": {"path": "kind", "op": "eq", "value": "x"}, "const": -1},
        {"else": 0}], "type": "double"},
        "label": {"cases": [{"when": {"path": "lu", "op": "lt", "value": 0}, "template": "neg:{lu}", "transform": "upper"},
                            {"path": "kind"}]}}
    rows = run([{"name": "t", "columns": cols}], [{"lu": 2}, {"lu": -3}, {"lu": -1, "kind": "x"}])["t"].rows
    assert [r["emissions"] for r in rows] == [2.0, 0.0, -1.0]
    assert [r["label"] for r in rows] == [None, "NEG:-3", "NEG:-1"]
    with pytest.raises(ConfigError, match="source is missing"):
        from_dict({"submodels": [{"tables": [{"name": "t", "columns": {"a": {"cases": [{"when": {"path": "x"}}]}}}]}]})


def test_checks():
    cols = {
        "bpn": {"path": "ids", "type": "array<string>", "check": {"regex": "^urn:BPNL", "on_fail": "warn"}},
        "share": {"path": "share", "type": "double", "check": {"min": 0, "max": 100, "on_fail": "null"}},
        "alloc": {"path": "alloc", "check": {"in": ["cut-off"], "ignore_case": True, "on_fail": "skip_row"}},
        "bio": {"path": "bio", "check": {"required_if": {"path": "mb", "op": "eq", "value": True}}},
    }
    docs = [{"ids": ["urn:BPNL1"], "share": 50, "alloc": "Cut-Off", "mb": True, "bio": 1},
            {"ids": ["x"], "share": 150, "alloc": "cut-off", "mb": True},
            {"ids": [], "share": 1, "alloc": "other"}]
    res = run([{"name": "t", "columns": cols}], docs)["t"]
    assert len(res.rows) == 2 and res.skipped == 1
    assert res.rows[1]["share"] is None and res.rows[1]["bpn"] == ["x"]       # null vs warn
    assert res.check_failures == {"bpn": 1, "share": 1, "alloc": 1, "bio": 1}
    with pytest.raises(TransformError, match="is not in the list"):
        run([{"name": "t", "columns": {"a": {"path": "a", "check": {"in": ["x"], "on_fail": "fail"}}}}], [{"a": "y"}])


def test_map_ignore_case_and_sign_flip():
    assert apply_chain("REVERSE Cut-Off", {"map": {"reverse cut-off": "cut-off", "_ignore_case": True}}) == "cut-off"
    assert apply_chain("Other", {"map": {"x": 1, "_ignore_case": True}}) == "Other"
    assert str(apply_chain(0, {"multiply": -1})) == "0.0"


def test_pcf_v7_to_v9_example():
    cfg = load(EX / "pcf_v7_to_v9.yaml")
    r7 = Engine(cfg).run([Document(_load(EX / "data" / "pcf_7.json"), "7.json")])["pcf"]
    r9 = Engine(cfg).run([Document(_load(EX / "data" / "pcf_9.json"), "9.json")])["pcf"]
    a, b = r7.rows[0], r9.rows[0]
    assert a["source_model"] == "7.0.0" and b["source_model"] == "9.0.0"
    assert set(r7.types) == set(r9.types)                       # both versions produce the same columns
    assert a["declared_unit_of_measurement"] == "liter" and a["cross_sectoral_standards"] == ["ISO Standard 14067"]
    assert a["land_management_biogenic_co2_emissions"] == 0.3 and a["land_management_biogenic_co2_removals"] == 0.0
    assert a["product_classifications"] == ["urn:cpc:011-99000"]
    assert r7.check_failures == {"company_ids": 1} and not r9.check_failures


# --- document_id (the UUID that links a document's tables) -------------------
def _uuids(res):
    return {t: [r["doc_id"] for r in r_.rows] for t, r_ in res.items()}


def test_document_id_key_is_deterministic_and_shared():
    tables = [{"name": "head", "columns": {"id": "catenaXId"}},
              {"name": "lines", "explode": "orders[*].lines", "columns": {"sku": "sku"}}]
    did = {"column": "doc_id", "from": "catenaXId"}
    a = run(tables, [SERIAL, {**SERIAL, "catenaXId": "urn:uuid:other"}], document_id=did)
    b = run(tables, [{**SERIAL, "partTypeInformation": {}}], document_id=did)   # different content, same key
    ua = _uuids(a)
    assert len(set(ua["head"])) == 2
    assert set(ua["lines"]) == set(ua["head"])                        # the same UUID in both tables
    assert ua["lines"][:3] == [ua["head"][0]] * 3
    assert _uuids(b)["head"][0] == ua["head"][0]                     # deterministic
    import uuid as _uuid
    assert _uuid.UUID(ua["head"][0]).version == 5
    assert a["head"].types["doc_id"] == "string" and list(a["head"].types)[0] == "doc_id"


def test_document_id_strategies_and_fallback(caplog):
    t = [{"name": "t", "columns": {"x": "x"}}]
    c1 = run(t, [{"x": 1}], document_id=True)["t"].rows[0]["document_id"]
    c2 = run(t, [{"x": 1}], document_id=True)["t"].rows[0]["document_id"]
    c3 = run(t, [{"x": 2}], document_id=True)["t"].rows[0]["document_id"]
    assert c1 == c2 != c3                                             # content: changes with the content
    r = run(t, [{"x": 1}], document_id={"strategy": "random"})["t"].rows[0]["document_id"]
    assert r != run(t, [{"x": 1}], document_id={"strategy": "random"})["t"].rows[0]["document_id"]
    # missing key: by default the document is rejected (the UUID would change once the field appeared)
    with pytest.raises(TransformError, match="document_id: field\\(s\\) \\['id'\\] missing"):
        run(t, [{"x": 1}], document_id={"from": "id"})
    fb = run(t, [{"x": 1}], document_id={"from": "id", "on_missing": "content"})["t"].rows[0]["document_id"]
    assert fb == c1 and "missing" in caplog.text                   # declared: content, with a warning
    ns = run(t, [{"x": 1}], document_id={"namespace": "other"})["t"].rows[0]["document_id"]
    assert ns != c1


def test_document_id_with_unwrap_and_auto_normalize():
    doc = {"block": [{"id": "A", "items": [{"k": 1}, {"k": 2}]}]}
    res = run([], [doc], unwrap={"paths": ["block"]}, document_id={"column": "doc_id", "from": "block.id"},
              auto_normalize={"table_prefix": "d", "children": "batch"})
    assert res["d"].spec.keys == ["doc_id"]                          # no id_path: the UUID is the key
    child = res["d_block_items"]
    assert child.spec.keys[0] == "doc_id" and child.spec.replace_by == ["doc_id"]
    assert {r["doc_id"] for r in child.rows} == {res["d"].rows[0]["doc_id"]}


@pytest.mark.parametrize("did,msg", [({"strategy": "key"}, "needs 'from'"), ({"strategy": "x"}, "strategy"),
                                     ({"colum": "x"}, "unknown")])
def test_document_id_config_errors(did, msg):
    with pytest.raises(ConfigError, match=msg):
        from_dict({"submodels": [{"document_id": did, "tables": [{"name": "t", "auto": True}]}]})
