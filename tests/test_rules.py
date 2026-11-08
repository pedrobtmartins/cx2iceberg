"""Step 6: conditions and checks with types, duplicates, null keys, templates, shared tables, auto collisions,
documents without a submodel, canonical document_id."""
import json
import uuid
from decimal import Decimal

import pytest

from cx2iceberg import ConfigError, Document, DocumentsRejected, Engine, TransformError, from_dict
from cx2iceberg.conditions import ConditionTypeError, evaluate
from cx2iceberg.paths import Context
from cx2iceberg.transforms import apply_chain


def cond(value, c):
    return evaluate(c, Context([({"v": value}, None)]))


def engine(tables, **cfg):
    sub = {"name": "s", "tables": tables, **cfg.pop("sub", {})}
    return Engine(from_dict({"submodels": [sub], **cfg}))


def run(tables, docs, **cfg):
    eng = engine(tables, **cfg)
    res = eng.run([d if isinstance(d, Document) else Document(d, f"d{i}.json") for i, d in enumerate(docs)], partial=True)
    return res, eng.report


def msgs(rep, kind="error"):
    return " | ".join(e["message"] for e in rep.events(kind))


# --- conditions: compare with their type ----------------------------------------------------------
@pytest.mark.parametrize("value,c,expected", [
    (7, {"path": "v", "op": "eq", "value": 7}, True), (7, {"path": "v", "op": "eq", "value": 7.0}, True),
    ("a", {"path": "v", "value": "a"}, True), (True, {"path": "v", "op": "eq", "value": True}, True),
    (None, {"path": "v", "op": "eq", "value": None}, True), (None, {"path": "v", "op": "ne", "value": 3}, True),
    (None, {"path": "v", "op": "eq", "value": 3}, False), (5, {"path": "v", "op": "ne", "value": None}, True),
    (10, {"path": "v", "op": "gt", "value": 9}, True),                        # before (text '10' > '9'): False
    (Decimal("1.5"), {"path": "v", "op": "lte", "value": 1.5}, True),
    ("b", {"path": "v", "op": "gt", "value": "a"}, True),
    ("2022-01-01T10:00:00+02:00", {"path": "v", "op": "lt", "value": "2022-01-01T09:30:00Z"}, True),   # instantes
    ("2022-01-01T00:00:00Z", {"path": "v", "op": "eq", "value": "2022-01-01T01:00:00+01:00"}, True),
    ("2025-03-01T10:00:00Z", {"path": "v", "op": "gte", "value": "2025-01-01"}, True),   # date: as it was written
    ("2024-12-31T23:30:00-02:00", {"path": "v", "op": "gte", "value": "2025-01-01"}, False),
    (2.0, {"path": "v", "op": "in", "value": [1, 2]}, True), ("a", {"path": "v", "op": "in", "value": ["a", 1]}, True),
    (3, {"path": "v", "op": "not_in", "value": [1, 2]}, True), (None, {"path": "v", "op": "not_in", "value": [1]}, True),
    (["a", "b"], {"path": "v", "op": "contains", "value": "a"}, True), ("abc", {"path": "v", "op": "contains", "value": "b"}, True),
    ("ab7", {"path": "v", "op": "regex", "value": r"\d"}, True),
    ([], {"path": "v", "op": "empty"}, True), ("", {"path": "v", "op": "not_empty"}, False), (0, {"path": "v", "op": "exists"}, True),
])
def test_conditions_compare_with_types(value, c, expected):
    assert cond(value, c) is expected


@pytest.mark.parametrize("value,c,fragment", [
    ("7", {"path": "v", "op": "eq", "value": 7}, "compares text '7' with number 7"),           # before: True
    (True, {"path": "v", "op": "eq", "value": 1}, "compares boolean True with number 1"),
    (10, {"path": "v", "op": "gt", "value": "9"}, "cannot order number 10 with text '9'"),   # before: False
    (True, {"path": "v", "op": "gte", "value": 0}, "boolean"), ("n/a", {"path": "v", "op": "gte", "value": 0}, "text"),
    ("10", {"path": "v", "op": "gt", "value": "9"}, "number as text"),
    ("2022-01-01T10:00:00Z", {"path": "v", "op": "gt", "value": "2022-01-01T09:00:00"}, "with a time zone and another without"),
    ("2022-12-31T24:00:00Z", {"path": "v", "op": "gte", "value": "2023-01-01"}, "24:00:00"),
    ("1", {"path": "v", "op": "in", "value": [1, 2]}, "compared with a list of number"),
    (["a"], {"path": "v", "op": "contains", "value": 1}, "has no values of type number"),
    ("abc", {"path": "v", "op": "contains", "value": 1}, "looks for number inside text"),
    (5, {"path": "v", "op": "contains", "value": 5}, "contains on number"),
    (7.5, {"path": "v", "op": "regex", "value": "^7"}, "regex on number"),                   # before: text '7.5'
    ([1], {"path": "v", "op": "eq", "value": 1}, "compares list"),
])
def test_condition_type_mismatch_is_an_error(value, c, fragment):
    with pytest.raises(ConditionTypeError) as e:
        cond(value, c)
    assert fragment in str(e.value)


@pytest.mark.parametrize("c,fragment", [
    ({"path": "v", "op": "nope"}, "unknown operator 'nope'"),
    ({"path": "v", "op": "in", "value": 1}, "'in' needs a list"),          # before: a TypeError stopped the load
    ({"path": "v", "op": "in", "value": []}, "'in' needs a list"),
    ({"path": "v", "op": "gt", "value": True}, "'gt' needs a number or a text"),
    ({"path": "v", "op": "regex", "value": "("}, "invalid regex"),
    ({"path": "v", "op": "exists", "value": 1}, "'exists' takes no value"),
    ({"path": "v", "op": "eq"}, "'eq' needs a simple value"),
    ({"op": "eq", "value": 1}, "'path' is missing"), ({"path": "v", "valeu": 1}, "unknown key(s)"),
    ({"all": {"path": "v"}}, "use {all: [condition, ...]}"), ({"not": {"path": "v"}, "path": "x"}, "use {not: condition} on its own"),
])
def test_conditions_are_validated_when_loading(c, fragment):
    for table in ({"name": "t", "where": c, "columns": {"a": "a"}},
                  {"name": "t", "columns": {"a": {"cases": [{"when": c, "const": 1}]}}},
                  {"name": "t", "columns": {"a": {"path": "a", "check": {"required_if": c}}}}):
        with pytest.raises(ConfigError) as e:
            engine([table])
        assert fragment in str(e.value)
    with pytest.raises(ConfigError, match="unknown|needs|invalid|takes no|missing|use"):
        engine([{"name": "t", "auto": True}], sub={"match": {"where": c}})


def test_condition_errors_follow_on_error_where_they_are_evaluated():
    where = {"path": "v", "op": "gt", "value": 9}
    res, rep = run([{"name": "t", "where": where, "columns": {"id": "id"}}], [{"id": "a", "v": "10"}, {"id": "b", "v": 11}])
    assert [r["id"] for r in res["t"].rows] == ["b"] and rep.count(reason="condition_type", kind="error") == 1
    res, rep = run([{"name": "t", "where": where, "on_error": "skip_row", "columns": {"id": "id"}}], [{"id": "a", "v": "10"}])
    assert res["t"].rows == [] and res["t"].skipped == 1 and rep.status == "ok"
    cases = {"id": "id", "x": {"cases": [{"when": where, "const": 1}, {"else": 0}], "type": "int", "on_error": "null"}}
    res, rep = run([{"name": "t", "columns": cases}], [{"id": "a", "v": "10"}])
    assert res["t"].rows == [{"id": "a", "x": None}] and rep.count(reason="condition_type", action="nulled") == 1


def test_match_with_a_type_mismatch_does_not_match():
    res, rep = run([{"name": "t", "columns": {"id": "id"}}], [{"id": "a", "v": "9"}, {"id": "b", "v": 9}],
                   sub={"match": {"where": {"path": "v", "op": "eq", "value": 9}}}, input={"on_unmatched": "skip"})
    assert [r["id"] for r in res["t"].rows] == ["b"] and rep.count(reason="no_submodel", kind="declared") == 1


def test_match_keys_are_validated():
    with pytest.raises(ConfigError, match="unknown.*sematic_id"):             # before: it accepted everything
        engine([{"name": "t", "auto": True}], sub={"match": {"sematic_id": "urn:x"}})
    with pytest.raises(ConfigError, match="any"):
        engine([{"name": "t", "auto": True}], sub={"match": {"any": []}})


# --- check with types --------------------------------------------------------------------------
def test_check_in_min_max_compare_with_types():
    cols = {"id": "id", "d": {"path": "d", "type": "double", "check": {"in": [1, 2, 3], "on_fail": "fail"}}}
    res, rep = run([{"name": "t", "columns": cols}], [{"id": "a", "d": 2}])
    assert res["t"].rows == [{"id": "a", "d": 2.0}]                                  # before: '2.0' was not in ['2']
    cols = {"id": "id", "s": {"path": "s", "check": {"in": [2, "B"], "ignore_case": True, "on_fail": "fail"}}}
    res, rep = run([{"name": "t", "columns": cols}], [{"id": "a", "s": "b"}, {"id": "c", "s": "2"}])
    assert [r["id"] for r in res["t"].rows] == ["a"] and "only matches 2 in the list, which is of another type" in msgs(rep)
    cols = {"id": "id", "n": {"path": "n", "check": {"min": 0, "max": 10, "on_fail": "fail"}}}
    res, rep = run([{"name": "t", "columns": cols}], [{"id": "a", "n": True}, {"id": "b", "n": 5}, {"id": "c", "n": 11}])
    assert [r["id"] for r in res["t"].rows] == ["b"]
    assert "True is not numeric" in msgs(rep) and "11 > maximum 10" in msgs(rep)      # before: true passed as 1


@pytest.mark.parametrize("check,fragment", [({"min": "0"}, "min: must be a number"), ({"min": 5, "max": 1}, "min greater"),
                                            ({"in": []}, "list of simple values"), ({"in": [[1]]}, "simple values")])
def test_check_is_validated_when_loading(check, fragment):
    with pytest.raises(ConfigError, match=fragment):
        engine([{"name": "t", "columns": {"a": {"path": "a", "check": check}}}])


# --- duplicates -----------------------------------------------------------------------------------
BOM = {"name": "bom", "explode": "items", "mode": "merge", "keys": ["p", "c"], "columns": {"p": "$root.id", "c": "c", "q": "q ::double"}}


def test_duplicate_key_inside_a_document_rejects_it():
    res, rep = run([BOM], [{"id": "P", "items": [{"c": "C1", "q": 2}, {"c": "C1", "q": 3}]}, {"id": "Q", "items": [{"c": "C1", "q": 1}]}])
    assert [r["p"] for r in res["bom"].rows] == ["Q"]                                 # before: q=3 stayed, without an error
    assert "the same key appears twice in the same document (items[0] and items[1])" in msgs(rep)


@pytest.mark.parametrize("mode,kept", [("last", 3.0), ("first", 2.0)])
def test_on_duplicate_is_declared_and_counted(mode, kept):
    res, rep = run([{**BOM, "on_duplicate": mode}], [{"id": "P", "items": [{"c": "C1", "q": 2}, {"c": "C1", "q": 3}]}])
    assert [r["q"] for r in res["bom"].rows] == [kept] and rep.count(reason="duplicate_key", kind="declared") == 1


def test_duplicate_keys_compare_with_types():
    res, rep = run([{"name": "t", "mode": "merge", "keys": ["k"], "columns": {"k": "k"}}],
                   [Document({"k": 1}, "a"), Document({"k": 1.0}, "b")])
    assert len(res["t"].rows) == 1 and rep.count(action="row_replaced") == 1          # 1 and 1.0: the same key


def test_duplicates_across_documents_keep_the_last_with_a_warning():
    res, rep = run([BOM], [{"id": "P", "items": [{"c": "C1", "q": 2}]}, {"id": "P", "items": [{"c": "C1", "q": 5}]}])
    assert [r["q"] for r in res["bom"].rows] == [5.0] and rep.count(reason="duplicate_key", kind="warning") == 1


@pytest.mark.parametrize("table,fragment", [
    ({"name": "t", "on_duplicate": "last", "columns": {"a": "a"}}, "on_duplicate needs keys"),
    ({"name": "t", "keys": ["a"], "on_duplicate": "both", "columns": {"a": "a"}}, "on_duplicate must be"),
    ({"name": "t", "mode": "merge", "keys": ["a"], "dedupe": False, "columns": {"a": "a"}}, "merge with dedupe: false"),
])
def test_duplicate_options_are_validated(table, fragment):
    with pytest.raises(ConfigError, match=fragment):
        engine([table])


# --- null keys --------------------------------------------------------------------------------------
def test_null_keys_are_errors_per_row():
    res, rep = run([BOM], [{"id": "P", "items": [{"q": 1}, {"q": 2}, {"c": "C", "q": 4}]}])
    assert res["bom"].rows == [] and rep.count(reason="null_key", kind="error") == 2   # before: co2 1 and 2 lost
    skip = {**BOM, "columns": {**BOM["columns"], "c": {"path": "c", "on_error": "skip_row"}}}
    res, rep = run([skip], [{"id": "P", "items": [{"q": 1}, {"c": "C", "q": 4}]}])
    assert [r["c"] for r in res["bom"].rows] == ["C"] and rep.count(reason="null_key", action="row_skipped") == 1
    res, rep = run([{"name": "t", "explode": "items", "replace_by": ["p"], "columns": {"p": "$root.id", "x": "x"}}],
                   [{"items": [{"x": 1}]}])
    assert res["t"].rows == [] and rep.count(reason="null_key", kind="error") == 1     # replace_by too


# --- templates -------------------------------------------------------------------------------------------
TPL = {"name": "e", "explode": "items", "columns": {"k": {"template": "{$root.id}|{c}", "type": "string"}}}


def test_incomplete_template_is_an_error_unless_declared():
    # only some parts missing: the present 'P' would be lost (null) or the value would be wrong ('P|') -> error
    res, rep = run([TPL], [{"id": "P", "items": [{"x": 1}]}, {"id": "Q", "items": [{"c": "C"}]}])
    assert [r["k"] for r in res["e"].rows] == ["Q|C"]
    assert rep.count(reason="template_missing", kind="error") == 1 and "no value for c" in msgs(rep)
    for mode, expected in (("empty", "P|"), ("null", None)):
        decl = {**TPL, "columns": {"k": {**TPL["columns"]["k"], "template_missing": mode}}}
        res, rep = run([decl], [{"id": "P", "items": [{"x": 1}]}])
        assert [r["k"] for r in res["e"].rows] == [expected] and rep.count(reason="template_missing", kind="declared") == 1
    # all parts missing: null, like an absent field (nothing is lost), no event
    res, rep = run([TPL], [{"items": [{"x": 1}]}])
    assert [r.get("k") for r in res["e"].rows] == [None] and not rep.events("error") and not rep.events("declared")
    # used in a key: with template_missing: empty, the two elements without 'c' gave the same key 'P|'
    res, rep = run([{"name": "e", "explode": "items", "mode": "merge", "keys": ["k"],
                     "columns": {"k": {"template": "{$root.id}|{c}", "transform": "uuid5", "template_missing": "empty"}}}],
                   [{"id": "P", "items": [{"x": 1}, {"x": 2}]}])
    assert res["e"].rows == [] and rep.count(reason="duplicate_key", kind="error") == 1
    with pytest.raises(ConfigError, match="template_missing accepts empty or null, in a column with a template"):
        engine([{"name": "t", "columns": {"a": {"path": "a", "template_missing": "empty"}}}])


# --- the same table in several rules ---------------------------------------------------------------------
def shared(a, b):
    return {"submodels": [{"name": "v7", "match": {"semantic_id": "v7"}, "tables": [{"name": "pcf", **a}]},
                          {"name": "v9", "match": {"semantic_id": "v9"}, "tables": [{"name": "pcf", **b}]}]}


@pytest.mark.parametrize("a,b,fragment", [
    ({"mode": "merge", "keys": ["id"], "columns": {"id": "id"}}, {"columns": {"id": "id"}}, "mode 'merge'"),
    ({"columns": {"id": "id", "x": "x ::int"}}, {"columns": {"id": "id", "x": "x ::double"}}, "column 'x': type 'int'"),
    ({"columns": {"x": {"path": "x", "type": "decimal(5,2)", "rounding": "half_up"}}}, {"columns": {"x": "x ::decimal(5,2)"}}, "rounding"),
    ({"metadata_columns": True, "columns": {"id": "id"}}, {"columns": {"id": "id"}}, "metadata_columns"),
])
def test_tables_fed_by_several_rules_must_agree(a, b, fragment):
    with pytest.raises(ConfigError, match="different rules for the same table") as e:
        from_dict(shared(a, b))
    assert fragment in str(e.value)


def test_tables_fed_by_several_rules_use_every_rule_s_types():
    cfg = from_dict(shared({"columns": {"id": "id", "a": "a ::int"}}, {"columns": {"id": "id", "a": "a ::int", "b": "b ::decimal(10,3)"}}))
    res = Engine(cfg).run([Document({"id": "x", "a": 1}, "1", "v7"), Document({"id": "y", "a": 2, "b": 1.25}, "2", "v9")])["pcf"]
    assert res.types == {"id": "string", "a": "int", "b": "decimal(10,3)"}           # before: b became double
    assert res.rows[1]["b"] == Decimal("1.250")


# --- collisions in auto mode -----------------------------------------------------------------------------------
def test_auto_collisions():
    res, rep = run([{"name": "t", "auto": True, "columns": {"id": "id"}}], [{"id": "a", "p": {"fossilGhg": 0.5, "fossil_ghg": 9.9}}])
    assert res["t"].rows == [] and "fields 'p.fossilGhg' and 'p.fossil_ghg' give the same column 'p_fossil_ghg'" in msgs(rep)
    res, rep = run([{"name": "t", "auto": True, "columns": {"id": "id", "total": {"path": "total", "type": "decimal(5,1)"}}}],
                   [{"id": "a", "total": 42}])
    assert res["t"].rows == [{"id": "a", "total": Decimal("42.0")}] and rep.status == "ok"   # same source: an override
    other = {"name": "t", "auto": True, "columns": {"id": "id", "total": {"const": 0, "type": "double"}}}
    res, rep = run([other], [{"id": "a", "total": 42.0}])
    assert res["t"].rows == [] and "already exists with another source" in msgs(rep)                # before: 42.0 was lost
    res, rep = run([{**other, "auto_collision": "prefix"}], [{"id": "a", "total": 42.0}])
    assert res["t"].rows == [{"id": "a", "total": 0.0, "item_total": 42.0}]
    res, rep = run([{**other, "auto_collision": "skip"}], [{"id": "a", "total": 42.0}])
    assert res["t"].rows == [{"id": "a", "total": 0.0}] and rep.count(action="auto_skipped", kind="declared") == 1
    with pytest.raises(ConfigError, match="auto_collision must be"):
        engine([{**other, "auto_collision": "ignore"}])


# --- documents without a submodel -----------------------------------------------------------------------
def test_unmatched_documents_are_errors_unless_skipped():
    tables = [{"name": "t", "columns": {"id": "id"}}]
    with pytest.raises(DocumentsRejected, match="no submodel"):                   # before: a warning and load 'ok'
        engine(tables, sub={"match": {"semantic_id": "urn:x"}}).run([Document({"id": "a"}, "a", "urn:y")])
    res, rep = run(tables, [Document({"id": "a"}, "a", "urn:y")], sub={"match": {"semantic_id": "urn:x"}},
                   input={"on_unmatched": "skip"})
    assert rep.status == "ok" and rep.count(reason="no_submodel", kind="declared") == 1
    with pytest.raises(ConfigError, match="on_unmatched"):
        engine(tables, input={"on_unmatched": "ignore"})


# --- canonical document_id -------------------------------------------------------------------------------
NS = uuid.uuid5(uuid.NAMESPACE_URL, "cx2iceberg")


def legacy_uuid(*parts):
    return str(uuid.uuid5(NS, "key:" + "|".join(json.dumps(p, sort_keys=True, default=str) for p in parts)))


def doc_ids(from_, docs, **extra):
    res, rep = run([{"name": "t", "columns": {"x": "x"}}], docs, sub={"document_id": {"from": from_, "column": "u", **extra}})
    return [r["u"] for r in res["t"].rows], rep


def test_document_id_numbers_need_a_type_and_then_agree():
    ids, rep = doc_ids(["id", "version"], [{"id": "P", "version": 0, "x": "a"}])
    assert ids == [] and "field 'version' is int (0): declare its type" in msgs(rep)
    typed = ["id", {"path": "version", "type": "int"}]
    ids, _ = doc_ids(typed, [{"id": "P", "version": v, "x": "a"} for v in (0, "0", 0.0)])
    assert ids == [legacy_uuid("P", 0)] * 3          # before: 3 UUIDs; and the one for version=0 (integer) does not change
    ids, _ = doc_ids(["id"], [{"id": "Peça-1", "x": "a"}])
    assert ids == [legacy_uuid("Peça-1")]              # text: the same UUID as before (no migration)
    ids, rep = doc_ids(typed, [{"id": "P", "version": "v1", "x": "a"}])
    assert ids == [] and "is not a number" in msgs(rep)


def test_document_id_missing_key_is_an_error_unless_declared():
    ids, rep = doc_ids(["id"], [{"x": "a"}])
    assert ids == [] and "the UUID would change once the field appeared" in msgs(rep)
    ids, rep = doc_ids(["id"], [{"x": "a"}], on_missing="content")
    assert len(ids) == 1 and rep.count(reason="document_id_key_missing", kind="declared") == 1
    for bad, fragment in (({"from": [{"path": 1}]}, "from\\[0\\]"), ({"from": ["id"], "on_missing": "skip"}, "on_missing")):
        with pytest.raises(ConfigError, match=fragment):
            engine([{"name": "t", "auto": True}], sub={"document_id": bad})


# --- hash / uuid5 --------------------------------------------------------------------------------------------
def test_hash_and_uuid5_need_text_for_scalars():
    assert apply_chain("P|C1", "uuid5") == str(uuid.uuid5(uuid.NAMESPACE_URL, "P|C1"))        # text: the same as before
    assert apply_chain({"b": 1, "a": 2}, "uuid5") == apply_chain({"a": 2, "b": 1}, "uuid5")
    for v in (1, 1.0, True):
        with pytest.raises(ValueError, match="is not text; use to_string"):                  # before: 1 and 1.0 different
            apply_chain(v, "uuid5")
    assert apply_chain(1.0, ["to_string", "uuid5"]) == str(uuid.uuid5(uuid.NAMESPACE_URL, "1.0"))


# --- notes outside a value do not pile up -------------------------------------------------------------
def test_notes_from_conditions_do_not_accumulate():
    import cx2iceberg.engine as E

    sizes = []
    orig = E.Engine._finalize
    eng = engine([{"name": "t", "explode": "items", "where": {"path": "at", "op": "gte", "value": "2022-01-01T00:00:00Z"},
                   "columns": {"k": "k"}}])
    E.Engine._finalize = lambda self, res: (sizes.append(len(self._notes)), orig(self, res))[1]
    try:
        eng.run([Document({"items": [{"k": i, "at": "2022-06-01T10:00:00.123456789Z"} for i in range(500)]}, "a")])
    finally:
        E.Engine._finalize = orig
    assert sizes == [1]                                # before: 500 (one per row, for the whole load)
