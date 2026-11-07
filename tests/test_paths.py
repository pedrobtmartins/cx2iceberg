"""Step 3: paths — wrong shape, missing elements, filters with types, validation at load time."""
import json
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from cx2iceberg import ConfigError, Document, Engine, from_dict
from cx2iceberg.config import _expand_env
from cx2iceberg.paths import MISSING, Context, FilterTypeError, PathShapeError, Projection, parse, resolve, resolve_obj

EX = Path(__file__).parent.parent / "examples"

DOC = {"xs": [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}],
       "stages": [{"co2": 1.5}, {"other": 9}, {"co2": None}, {"co2": 2.0}],
       "orders": [{"lines": [{"sku": "x"}, {"sku": "y"}]}, {"lines": []}, {"no": "C"}],
       "ids": [{"primary": True, "value": "X"}, {"qty": 1.0, "value": "Y"}, {"key": "a=b]c", "value": "Z"}],
       "q": None, "name": "abc", "block": {"c": 1}}


# --- absent vs wrong shape ------------------------------------------------------------
@pytest.mark.parametrize("path", ["nope", "nope.deeper", "q.value", "xs[9]", "q[0]", "block.x"])
def test_absent_is_still_missing(path):
    assert resolve_obj(DOC, path) is MISSING


@pytest.mark.parametrize("path,fragment", [
    ("xs.v", "field 'v' was asked of a list — for every element use '[*].v'"),
    ("name.first", "field 'first' was asked of text 'abc'"),
    ("xs[?k=a][0].v", "index [0] asked of an object — for the 1st result of a [*] or filter use '| first'"),
    ("block[0].c", "the list may have been unwrapped by the unwrap: remove the index"),
    ("name[*]", "[*] asked of text"),
    ("orders[*].lines.sku", "field 'sku' was asked of a list"),
])
def test_wrong_shape_is_an_error(path, fragment):
    with pytest.raises(PathShapeError, match=fragment.replace("[", r"\[").replace("]", r"\]").replace("*", r"\*")
                       .replace("|", r"\|").replace(".", r"\.")):
        resolve_obj(DOC, path)


# --- projections: the same values as before, and how many elements lacked the field ---------
def test_projection_keeps_values_and_counts_missing_elements():
    p = resolve_obj(DOC, "stages[*].co2")
    assert isinstance(p, Projection) and p == [1.5, None, 2.0]                 # the same as the old result
    assert p.missing == 1 and p.slots == [1.5, None, None, 2.0]                # the explicit null is not "missing"
    p = resolve_obj(DOC, "orders[*].lines[0].sku")
    assert p == ["x"] and p.missing == 2                                       # empty list and an element without 'lines'
    p = resolve_obj(DOC, "orders[*].lines[*].sku")
    assert p == ["x", "y"] and p.missing == 1                                  # empty list = 0 elements, nothing missing
    p = resolve_obj(DOC, "nope[*].x")
    assert p == [] and p.missing == 0                                          # the whole list absent: these are not elements


# --- filters with types --------------------------------------------------------------
@pytest.mark.parametrize("path,expected", [
    ("ids[?primary=true].value", ["X"]),                  # before: [] ('True' != 'true')
    ("ids[?qty=1].value", ["Y"]),                         # before: [] ('1.0' != '1')
    ("ids[?qty=1.00].value", ["Y"]),
    ("ids[?key='a=b]c'].value", ["Z"]),                   # quotes: '=' and ']' in the value
    ("ids[?key~a=b].value", ["Z"]),                       # before: split at the '=' (field 'key~a')
    ("xs[?k!=a].v", [2]),
    ("xs[?k=\"a\"].v", [1, 3]),
    ("stages[?co2=null]", [{"other": 9}, {"co2": None}]),  # absent counts as null (as in the columns)
    ("ids[?primary!=true].value", ["Y", "Z"]),            # absent counts as different
    ("ids[?value].value", ["X", "Y", "Z"]),
])
def test_typed_filters(path, expected):
    assert resolve_obj(DOC, path) == expected


@pytest.mark.parametrize("path,fragment", [
    ("ids[?primary='true'].value", "compares the text 'true' with boolean True"),
    ("xs[?v='1'].k", "compares the text '1' with number 1 (for a number write it without quotes)"),
    ("xs[?k=1].v", "compares the number 1 with text 'a' (for text use quotes: [?k='1'])"),
    ("xs[?v~^1].k", "regular expression on number 1"),
])
def test_filter_type_mismatch_is_an_error(path, fragment):
    with pytest.raises(FilterTypeError) as e:
        resolve_obj(DOC, path)
    assert fragment in str(e.value)


def test_filter_parsing():
    assert parse("a[?k=x].v")[1].value == ("k", "=", ("str", "x"))
    assert parse("a[?k!=x]")[1].value == ("k", "!=", ("str", "x"))
    assert parse("a[?k~a=b]")[1].value == ("k", "~", ("regex", "a=b"))
    assert parse("a[?k=a!=b]")[1].value == ("k", "=", ("str", "a!=b"))
    assert parse("a[?n=-1.5e2]")[1].value == ("n", "=", ("num", Decimal("-1.5e2")))
    with pytest.raises(ValueError, match="the field before"):
        parse("a[?=x]")


def test_parent_above_the_root_is_an_error_at_runtime():
    ctx = Context([(DOC, None)]).child(DOC["xs"][0], 0)
    assert resolve(ctx, "$parent.name") == "abc"
    with pytest.raises(PathShapeError, match="above the root"):
        resolve(ctx, "$parent.$parent.name")


# --- motor: missing_elements ----------------------------------------------------------
def run(cols, doc=DOC, sub=None, **table):
    eng = Engine(from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": cols, **table}], **(sub or {})}]}))
    return eng.run([Document(doc, "f.json")], partial=True)["t"].rows, eng.report


@pytest.mark.parametrize("col", [
    "stages[*].co2 | sum", "stages[*].co2 | first", "stages[*].co2 | count ::int", "stages[*].co2 | join(;)",
    "stages[*].co2 ::array<double>", {"template": "{stages[*].co2}"},
])
def test_missing_elements_without_declaration_is_an_error(col):
    rows, rep = run({"v": col})
    assert rows == [] and rep.count(reason="missing_elements", kind="error") == 1
    message = rep.events("error")[0]["message"]
    assert "1 of 4 element(s) without the field" in message
    assert ("in a template" in message) if isinstance(col, dict) else ("declare missing_elements" in message)


def test_missing_elements_skip_and_null():
    rows, rep = run({"s": {"path": "stages[*].co2", "missing_elements": "skip", "type": "array<double>"},
                     "n": {"path": "stages[*].co2", "missing_elements": "null", "type": "array<double>"},
                     "c": {"path": "stages[*].co2", "missing_elements": "null", "transform": "count", "type": "int"}})
    assert rows == [{"s": [1.5, None, 2.0], "n": [1.5, None, None, 2.0], "c": 4}]
    assert rep.count(action="element_skipped") == 1 and rep.count(action="element_nulled") == 2
    rows, _ = run({"v": {"path": "xs[*].v", "transform": "sum"}})                # nothing missing: no need to declare
    assert rows == [{"v": 6.0}]
    with pytest.raises(ConfigError, match="missing_elements must be skip or null"):
        run({"v": {"path": "xs[*].v", "missing_elements": "zero"}})


def test_missing_elements_in_document_id_is_an_error():
    rows, rep = run({"x": "name"}, sub={"document_id": {"from": ["stages[*].co2"]}})
    assert rows == [] and rep.count(reason="missing_elements", kind="error") == 1


# --- engine: wrong shape in each place --------------------------------------------------
def test_wrong_shape_in_a_column_follows_on_error():
    rows, rep = run({"v": "xs.v ::double"})
    assert rows == [] and rep.count(reason="path_shape", kind="error") == 1        # before: silently null
    rows, rep = run({"k": "name", "v": {"path": "xs.v", "on_error": "null", "type": "string"}})
    assert rows == [{"k": "abc", "v": None}] and rep.count(reason="path_shape", action="nulled") == 1
    rows, rep = run({"v": {"path": "xs[?v='1'].k", "on_error": "skip_row"}})
    assert rows == [] and rep.count(reason="filter_type", action="row_skipped") == 1


def test_wrong_shape_in_where_required_if_and_explode():
    rows, rep = run({"k": "k"}, explode="xs", where={"path": "k.sub", "op": "eq", "value": 1})
    assert rows == [] and rep.count(reason="path_shape", column="(where)", kind="error") == 3
    rows, rep = run({"k": "k"}, explode="xs", where={"path": "k.sub", "op": "eq", "value": 1}, on_error="skip_row")
    assert rows == [] and rep.count(action="row_skipped") == 3
    rows, rep = run({"v": {"path": "nope", "check": {"required_if": {"path": "xs.v", "op": "exists"}}}})
    assert rows == [] and rep.count(reason="path_shape", column="v") == 1         # before: it crashed the load
    rows, rep = run({"v": "$item"}, doc={"childItems": "oops"}, explode="childItems")
    assert rows == [] and "explode 'childItems': expected a list" in rep.events("error")[0]["message"]


def test_unwrap_keep_makes_shape_differences_visible():
    docs = [Document({"block": [{"c": 1}]}, "a.json"), Document({"block": [{"c": 1}, {"c": 2}]}, "b.json")]
    eng = Engine(from_dict({"submodels": [{"name": "s", "unwrap": {"all": True, "on_multiple": "keep"},
                                           "tables": [{"name": "t", "columns": {"c": "block.c"}}]}]}))
    res = eng.run(docs, partial=True)["t"]
    assert res.rows == [{"c": 1.0}] and "field 'c' was asked of a list" in eng.report.events("error")[0]["message"]


def test_match_does_not_fail_on_documents_of_another_shape():
    cfg = from_dict({"submodels": [
        {"name": "v9", "match": {"where": {"path": "scope.spec", "op": "regex", "value": "9"}}, "tables": [{"name": "a", "auto": True}]},
        {"name": "v7", "tables": [{"name": "b", "auto": True}]}]})
    res = Engine(cfg).run([Document({"scope": [{"spec": "9"}]}, "x.json")])   # 'scope.spec' on a list: no match
    assert res["a"].rows == [] and len(res["b"].rows) == 1


def test_envelope_path_of_the_wrong_shape_rejects_the_document(tmp_path):
    from cx2iceberg.reader import read_documents

    (tmp_path / "a.json").write_text('{"envelope": [{"payload": {"id": 1}}]}')
    doc = next(read_documents([str(tmp_path)], {"envelope": {"payload_path": "envelope.payload"}}))
    assert doc.error.startswith("envelope: 'envelope.payload': field 'payload' was asked of a list")


# --- validation at load time: context ------------------------------------------------------
@pytest.mark.parametrize("cols,table,fragment", [
    ({"v": "$parent.x"}, {}, "goes up 1 level(s) with $parent, but the table only has 0"),
    ({"v": "$parent.$parent.x"}, {"explode": "xs"}, "goes up 2 level(s)"),
    ({"v": "$root.$parent.x"}, {"explode": "xs"}, "$parent after $root"),
    ({"v": "$index ::int"}, {}, "$index only exists in tables with an explode"),
    ({"v": "$parent.$index"}, {"explode": "xs"}, "$index only exists"),
    ({"v": "$meta.sourcefile"}, {}, "unknown key in $meta"),
    ({"v": "$meta.document_id"}, {}, "unknown key in $meta"),
    ({"v": "a[?k=x"}, {}, "Invalid path"),
    ({"v": {"template": "{a..b[}"}}, {}, "Invalid path"),
    ({"v": {"cases": [{"when": {"path": "a[", "op": "exists"}, "const": 1}]}}, {}, "Invalid path"),
    ({"v": "v"}, {"where": {"any": [{"path": "$parent.x", "op": "exists"}]}}, "$parent"),
    ({"v": "v"}, {"explode": "a[?k"}, "Invalid path"),
])
def test_context_problems_are_found_when_loading(cols, table, fragment):
    with pytest.raises(ConfigError) as e:
        from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": cols, **table}]}]})
    assert fragment in str(e.value)


def test_context_checks_accept_valid_references_and_list_every_problem():
    ok = {"submodels": [{"name": "s", "document_id": True, "tables": [{"name": "t", "explode": "orders[*].lines", "columns": {
        "a": "$parent.$index ::int", "b": "$index ::int", "c": "$root.x", "d": "$parent.$parent.x", "e": "$meta.document_id",
        "f": "$meta.source_file", "g": "$item", "h": "$meta"}}]}]}
    from_dict(ok)
    with pytest.raises(ConfigError) as e:
        from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {
            "a": "$parent.x", "b": "$meta.x", "c": "$index"}}]}]})
    assert str(e.value).startswith("3 problem(s) in the paths")


# --- validation at load time: against the model -----------------------------------------------
SCHEMA = {
    "type": "object",
    "components": {"schemas": {
        "Quantity": {"type": "object", "properties": {"value": {"type": "number"}, "unit": {"type": "string"}}},
        "Child": {"type": "object", "properties": {"catenaXId": {"type": "string"},
                                                   "quantity": {"$ref": "#/components/schemas/Quantity"}}},
        "Block": {"allOf": [{"type": "object", "properties": {"c": {"type": "integer"}}}]},
    }},
    "properties": {
        "catenaXId": {"type": "string"},
        "childItems": {"type": "array", "items": {"$ref": "#/components/schemas/Child"}},
        "block": {"type": "array", "items": {"$ref": "#/components/schemas/Block"}},
        "codes": {"type": "array", "items": {"type": "string"}},
        "extra": {"type": "object"},
    },
}


def with_model(tmp_path, tables, unwrap=None, **sub):
    (tmp_path / "s.json").write_text(json.dumps(SCHEMA))
    return from_dict({"submodels": [{"name": "s", "model": {"schema": "s.json"}, "tables": tables,
                                     **({"unwrap": unwrap} if unwrap else {}), **sub}]}, tmp_path)


def test_valid_paths_pass_the_model_check(tmp_path):
    with_model(tmp_path, [
        {"name": "a", "columns": {"id": "catenaXId", "c": "block.c", "q": "childItems[*].quantity.value",
                                  "f": "childItems[?catenaXId~^urn].quantity.unit", "codes": "codes", "x": "extra.anything.goes"}},
        {"name": "b", "explode": "childItems", "columns": {"p": "$root.catenaXId", "v": "quantity.value", "i": "$index"},
         "where": {"path": "quantity.unit", "op": "eq", "value": "kg"}},
        {"name": "c", "explode": "codes", "columns": {"code": "$item"}},
    ], unwrap={"paths": ["block"]}, document_id={"from": "catenaXId"})


@pytest.mark.parametrize("tables,unwrap,fragment", [
    ([{"name": "t", "columns": {"v": "catenaXid"}}], None, "field 'catenaXid' does not exist in the model. Did you mean 'catenaXId'?"),
    ([{"name": "t", "columns": {"v": "childItems.quantity"}}], None, "'childItems' is a list in the model: use 'childItems[*].quantity'"),
    ([{"name": "t", "columns": {"v": "childItems[*].quantity.valeu"}}], None, "Did you mean 'value'?"),
    ([{"name": "t", "columns": {"v": "block[0].c"}}], {"paths": ["block"]}, "is unwrapped by the unwrap"),
    ([{"name": "t", "columns": {"v": "catenaXId.x"}}], None, "is a simple value in the model"),
    ([{"name": "t", "columns": {"v": "childItems[?catenaXID=a].quantity"}}], None, "'catenaXID' does not exist"),
    ([{"name": "t", "explode": "catenaXId", "columns": {"v": "$item"}}], None, "is not a list in the model"),
    ([{"name": "t", "explode": "childItems", "columns": {"v": "quantity.valeu"}}], None, "Did you mean 'value'?"),
    ([{"name": "t", "explode": "childItems", "columns": {"v": "v"}, "where": {"path": "qty", "op": "exists"}}], None, "'qty' does not exist"),
    ([{"name": "t", "columns": {"v": {"path": "v", "check": {"required_if": {"path": "massBalancingUsed", "op": "eq", "value": True}}}}}],
     None, "'massBalancingUsed' does not exist"),
    ([{"name": "t", "columns": {"v": "catenaXId"}}], {"paths": ["blok"]}, "unwrap: 'blok' does not exist in the model (did you mean 'block'?)"),
    ([{"name": "t", "columns": {"v": "catenaXId"}}], {"paths": ["catenaXId"]}, "unwrap: 'catenaXId' is not a list in the model"),
])
def test_model_problems_are_found_when_loading(tmp_path, tables, unwrap, fragment):
    with pytest.raises(ConfigError) as e:
        with_model(tmp_path, tables, unwrap)
    assert fragment in str(e.value)


def test_outside_model_skips_only_that_column(tmp_path):
    with_model(tmp_path, [{"name": "t", "columns": {"v": {"paths": ["catenaXId", "extWBCSD_catenaXId"], "outside_model": True}}}])
    with pytest.raises(ConfigError, match="'bad' does not exist"):
        with_model(tmp_path, [{"name": "t", "columns": {"v": {"path": "catenaXId", "outside_model": True}, "w": "bad"}}])


def test_official_pcf_schemas_against_the_shipped_examples():
    schemas = {"pcf_v7": "schemas/pcf_7.0.0-schema.json", "pcf_v9": "schemas/pcf_9.0.0-schema.json",
               "pcf9": "schemas/pcf_9.0.0-schema.json"}

    def load_with_model(name):
        raw = yaml.safe_load(_expand_env((EX / name).read_text()))
        for sub in raw["submodels"]:
            sub["model"] = {"schema": schemas[sub["name"]]}
        return from_dict(raw, EX)

    load_with_model("pcf9_flat.yaml")                          # 106 columns, all of them exist in PCF 9.0.0
    load_with_model("pcf9_struct.yaml")
    with pytest.raises(ConfigError) as e:
        load_with_model("pcf_v7_to_v9.yaml")
    # only the alternative names of example [3] (outside the 7.0.0 schema, on purpose) — [13] already uses a 7.0.0 field
    problems = str(e.value).splitlines()[1:]
    assert problems == [
        "  submodels[0] ('pcf_v7').tables['pcf'].columns.fossil_carbon_content: 'pcf.fossilCarbonContent' — field "
        "'fossilCarbonContent' does not exist in the model in 'pcf'. Did you mean 'extWBCSD_fossilCarbonContent'?",
        "  submodels[0] ('pcf_v7').tables['pcf'].columns.status: 'status' — field 'status' does not exist in the model.",
        "  submodels[0] ('pcf_v7').tables['pcf'].columns.allocation_rules_description: 'pcf.allocationRulesDescription' — "
        "field 'allocationRulesDescription' does not exist in the model in 'pcf'. Did you mean 'extWBCSD_allocationRulesDescription'?",
    ]


def test_example_13_requires_biogenic_carbon_content_in_v7_pcfs_created_since_2025():
    import copy

    cfg = load_v7_v9()
    base = json.loads((EX / "data" / "pcf_7.json").read_text())
    del base["pcf"]["carbonContentBiogenic"]
    results = {}
    for created in ("2024-12-31T23:59:59Z", "2025-01-01T00:00:00Z", "2025-06-01T10:00:00Z"):
        doc = copy.deepcopy(base)
        doc["created"] = created
        eng = Engine(cfg)
        res = eng.run([Document(doc, "7.json")])["pcf"]
        results[created] = (res.rows[0]["biogenic_carbon_content"], res.check_failures.get("biogenic_carbon_content", 0))
    assert results == {"2024-12-31T23:59:59Z": (None, 0), "2025-01-01T00:00:00Z": (None, 1), "2025-06-01T10:00:00Z": (None, 1)}
    assert Engine(cfg).run([Document(json.loads((EX / "data" / "pcf_7.json").read_text()), "7.json")])["pcf"] \
        .check_failures == {"company_ids": 1}                  # the official example (2022, with the field) does not fail


def load_v7_v9():
    from cx2iceberg.config import load

    return load(EX / "pcf_v7_to_v9.yaml")
