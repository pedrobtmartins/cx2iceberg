"""Modo auto com model.schema: nomes leaf e tabelas filhas fixados pelo modelo, não pelos dados de cada lote."""
import json

import pytest

from cx2iceberg import ConfigError, Document, Engine, from_dict
from cx2iceberg.report import Report

GHG = {"type": "object", "properties": {"fossil": {"type": "number"}}}
SCHEMA = {"type": "object", "properties": {
    "catenaXId": {"type": "string"},
    "ghg": GHG,
    "other": GHG,
    "production": {"type": "object", "properties": {"items": {"type": "array", "items": {
        "type": "object", "properties": {"q": {"type": "number"}}}}}},
    "packaging": {"type": "object", "properties": {"items": {"type": "array", "items": {
        "type": "object", "properties": {"q": {"type": "number"}}}}}},
}}
A = {"catenaXId": "urn:1", "production": {"items": [{"q": 1}]}, "ghg": {"fossil": 1}}
B = {"catenaXId": "urn:2", "production": {"items": [{"q": 2}]}, "packaging": {"items": [{"q": 3}]},
     "ghg": {"fossil": 2}, "other": {"fossil": 9}}


def load(tmp_path, sub, schema=SCHEMA):
    (tmp_path / "m.json").write_text(json.dumps(schema))
    return from_dict({"submodels": [{"name": "s", "model": {"schema": "m.json"}, **sub}]}, tmp_path)


def run(cfg, docs, report=None):
    eng = Engine(cfg, report=report)
    return eng.run([Document(d, f"d{i}.json") for i, d in enumerate(docs)], partial=True)


AUTO_LEAF = {"tables": [{"name": "t", "auto": True, "auto_naming": "leaf", "columns": {"id": "catenaXId"}}]}


# --- nomes leaf ----------------------------------------------------------------------------------------
def test_leaf_names_come_from_the_model_not_from_each_document(tmp_path):
    # antes: ghg.fossil ia para 'fossil' no documento A (sem 'other') e para 'ghg_fossil' no B, na mesma carga;
    # SUM(ghg_fossil) dava 2 em vez de 3, sem nenhum evento
    cfg = load(tmp_path, AUTO_LEAF)
    rows = run(cfg, [A, B])["t"].rows
    assert [r["ghg_fossil"] for r in rows] == [1, 2] and "fossil" not in rows[0] and "fossil" not in rows[1]
    alone = run(cfg, [A])["t"]                                       # noutra carga, o mesmo nome
    assert alone.rows[0]["ghg_fossil"] == 1 and "fossil" not in alone.types


@pytest.mark.parametrize("sub,fragment", [
    (AUTO_LEAF, "auto_naming: leaf \\(tabelas \\['t'\\]\\) precisa de model.schema"),
    ({"auto_normalize": {"naming": "leaf", "children": "batch"}}, "naming: leaf precisa de model.schema"),
])
def test_leaf_without_a_model_is_a_config_error(sub, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"name": "s", **sub}]})
    from_dict({"submodels": [{"name": "s", "tables": [{"name": "t", "auto": True, "columns": {"id": "catenaXId"}}]}]})


def test_field_outside_the_model_gets_its_full_name_and_is_counted(tmp_path):
    rep = Report()
    res = run(load(tmp_path, AUTO_LEAF), [{**A, "extra": {"fossil": 5}}], rep)["t"]
    assert res.rows[0]["extra_fossil"] == 5 and res.rows[0]["ghg_fossil"] == 1
    assert rep.count(reason="outside_model", column="extra_fossil") == 1


def test_field_outside_the_model_cannot_take_a_model_column(tmp_path):
    # o modelo tem a.x (nome leaf 'x'); um campo 'x' na raiz, fora do modelo, daria a mesma coluna
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"},
                                               "a": {"type": "object", "properties": {"x": {"type": "number"}}}}}
    rep = Report()
    res = run(load(tmp_path, AUTO_LEAF, schema), [{"catenaXId": "1", "a": {"x": 1}, "x": 2}], rep)
    assert res["t"].rows == [] and rep.count(reason="auto_collision", kind="error") == 1
    # também quando o documento só traz o campo de fora: a coluna 'x' é de a.x, em todas as cargas
    rep = Report()
    res = run(load(tmp_path, AUTO_LEAF, schema), [{"catenaXId": "1", "x": 2}], rep)
    assert res["t"].rows == [] and rep.count(reason="auto_collision", kind="error") == 1


def test_every_oneof_alternative_is_part_of_the_model(tmp_path):
    # com só a 1.ª alternativa, o 'fossil' de p não colidia e chamava-se 'fossil'; se um documento trouxer q,
    # o mesmo campo mudava de nome
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"}, "v": {"oneOf": [
        {"type": "object", "properties": {"p": GHG}}, {"type": "object", "properties": {"q": GHG}}]}}}
    rows = run(load(tmp_path, AUTO_LEAF, schema), [{"catenaXId": "1", "v": {"p": {"fossil": 1}}}])["t"].rows
    assert rows[0]["p_fossil"] == 1


def test_leaf_names_of_an_exploded_auto_table_use_the_model_position(tmp_path):
    sub = {"tables": [{"name": "it", "explode": "production.items", "auto": True, "auto_naming": "leaf",
                       "columns": {"id": "$root.catenaXId"}}]}
    assert run(load(tmp_path, sub), [A])["it"].rows == [{"id": "urn:1", "q": 1}]


# --- auto_normalize ---------------------------------------------------------------------------------------
def test_auto_normalize_table_names_with_leaf_come_from_the_model(tmp_path):
    # antes: só com A no lote a tabela chamava-se s_items; com A e B, s_production_items
    cfg = load(tmp_path, {"auto_normalize": {"naming": "leaf"}})
    assert set(run(cfg, [A])) == {"s", "s_production_items", "s_packaging_items"}
    assert run(cfg, [A, B])["s_production_items"].rows[0]["q"] == 1


def test_auto_normalize_children_come_from_the_model(tmp_path):
    cfg = load(tmp_path, {"auto_normalize": {}})
    res = run(cfg, [{"catenaXId": "urn:1", "production": {"items": []}}])    # a lista ficou vazia
    child = res["s_production_items"]
    assert child.rows == [] and list(child.replace_keys.values()) == [("urn:1",)]    # o DELETE corre
    assert "s_packaging_items" in res                                # nem aparece no documento


def test_auto_normalize_list_outside_the_model_still_gets_a_table(tmp_path):
    res = run(load(tmp_path, {"auto_normalize": {}}), [{"catenaXId": "urn:1", "extra": [{"k": 1}]}])
    assert res["s_extra"].rows[0]["k"] == 1


@pytest.mark.parametrize("sub,fragment", [
    ({"auto_normalize": {}}, "Declare model.schema \\(recomendado\\) ou auto_normalize.children: batch"),
    ({"auto_normalize": {"children": "schema"}}, "children: schema precisa de model.schema"),
    ({"auto_normalize": {"children": "sometimes"}}, "children deve ser schema ou batch"),
    ({"auto_normalize": {"naming": "short"}}, "naming deve ser full ou leaf"),
])
def test_auto_normalize_children_source_must_be_known(sub, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"name": "s", **sub}]})
    # append nunca apaga: sem modelo, as filhas do lote chegam
    from_dict({"submodels": [{"name": "s", "auto_normalize": {}, "defaults": {"mode": "append"}}]})


def test_children_from_the_batch_are_declared_and_counted():
    cfg = from_dict({"submodels": [{"name": "s", "auto_normalize": {"children": "batch"}}]})
    rep = Report()
    run(cfg, [{"catenaXId": "urn:1", "items": []}], rep)
    assert rep.count(reason="children_from_batch", kind="declared") == 1


def test_lists_of_objects_are_not_columns_in_the_leaf_names(tmp_path):
    # a.fossil é uma lista de objectos (tabela filha, não coluna): não pode obrigar b.fossil a chamar-se b_fossil
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"},
              "a": {"type": "object", "properties": {"fossil": {"type": "array", "items": GHG}}},
              "b": GHG}}
    rows = run(load(tmp_path, AUTO_LEAF, schema), [{"catenaXId": "1", "b": {"fossil": 3}}])["t"].rows
    assert rows[0]["fossil"] == 3


def test_leaf_names_follow_auto_max_depth(tmp_path):
    # com auto_max_depth: 2, a.b é uma folha (o objecto inteiro); o nome vem do modelo e não há campo "fora"
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"}, "a": {"type": "object", "properties": {
        "b": {"type": "object", "properties": {"c": {"type": "number"}}}}}}}
    sub = {"tables": [{"name": "t", "auto": True, "auto_naming": "leaf", "auto_max_depth": 2, "columns": {"id": "catenaXId"}}]}
    rep = Report()
    rows = run(load(tmp_path, sub, schema), [{"catenaXId": "1", "a": {"b": {"c": 1}}}], rep)["t"].rows
    assert "b" in rows[0] and rep.count(reason="outside_model") == 0


def test_model_children_inside_an_unwrapped_list(tmp_path):
    # block é desembrulhada (1:1); block.items é uma lista de objectos do modelo, mesmo vazia neste documento
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"}, "block": {"type": "array", "items": {
        "type": "object", "properties": {"items": {"type": "array", "items": {"type": "object", "properties": {"k": {"type": "number"}}}}}}}}}
    cfg = load(tmp_path, {"unwrap": {"paths": ["block"]}, "auto_normalize": {}}, schema)
    res = run(cfg, [{"catenaXId": "urn:1", "block": [{"items": []}]}])
    assert list(res["s_block_items"].replace_keys.values()) == [("urn:1",)]


def test_children_from_the_batch_still_use_model_names_with_leaf(tmp_path):
    cfg = load(tmp_path, {"auto_normalize": {"naming": "leaf", "children": "batch"}})
    assert set(run(cfg, [A])) == {"s", "s_production_items"}          # antes: s_items (só A no lote)
