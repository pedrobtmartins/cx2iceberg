"""Passo 7: escrita — tipos declarados (sem inferência), pré-verificação antes de qualquer escrita ou DELETE,
catálogo Glue, replace_by de listas que ficaram vazias, literais SQL, ficheiros locais."""
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


# --- tipos declarados, sem inferência -----------------------------------------------------
def test_dataframe_uses_the_declared_types():
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    df = to_dataframe(res)
    dtypes = {c: str(t) for c, t in df.dtypes.items()}
    assert dtypes["big"] == "Int64" and dtypes["i"] == "Int32" and dtypes["n"] == "Int8" and dtypes["f"] == "float32"
    assert dtypes["b"] == "boolean" and dtypes["ts"] == "datetime64[us]" and dtypes["d"] == "object"
    assert df["big"][0] == 9007199254740993                     # antes: float64 com o nulo da 2.ª linha -> ...992
    assert df["ts"][0] == datetime(9999, 12, 31, 23, 59, 59)     # antes: pandas 2 partia (fora de ns)


def test_arrow_table_has_the_declared_schema():
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    schema = {f.name: str(f.type) for f in to_arrow(res).schema}
    assert schema["n"] == "int8" and schema["d"] == "decimal128(5, 2)" and schema["f"] == "float"    # antes: double, (3,2)
    assert schema["mp"] == "map<string, int64>" and schema["st"] == "struct<a: decimal128(5, 2)>"
    res.rows[0]["n"] = 300                                          # valor que não cabe (só possível se algo falhar antes)
    with pytest.raises(WriteCheckError, match="tabela 't', coluna 'n' \\(tinyint\\)"):
        to_arrow(res)


def test_iceberg_has_no_tinyint_or_smallint():
    # o Iceberg só tem int e long: na tabela ficam int; o tipo declarado continua a validar o intervalo no cast
    from cx2iceberg.athena_types import athena_type
    from cx2iceberg.writer import ddl
    assert athena_type("tinyint") == "int" and athena_type("array<smallint>") == "array<int>"
    assert athena_type("struct<a:tinyint,j:json>") == "struct<a:int,j:string>"
    res = results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 1}])["t"]
    assert "`n` int" in ddl(res, {})
    rep = Report()
    assert results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 300}], report=rep)["t"].rows == []
    assert rep.count(reason="invalid_value", column="n", kind="error") == 1       # 300 não cabe num tinyint


def test_offline_check_runs_awswranglers_own_conversion():
    pytest.importorskip("awswrangler")
    res = results([{"name": "t", "columns": ALL}], [FULL, {}])["t"]
    check_offline(res)
    res.rows[0]["i"] = "x"                                          # o awswrangler também não o aceitaria
    with pytest.raises(WriteCheckError):
        check_offline(res)


def test_offline_check_without_awswrangler_still_checks_the_arrow_schema(monkeypatch):
    # dry-run numa máquina sem awswrangler (ou com API mudada): a verificação Arrow continua a ser feita
    monkeypatch.setattr(writer, "_wr_conversion", lambda: None)
    res = results([{"name": "t", "columns": {"n": "n ::tinyint"}}], [{"n": 1}])["t"]
    check_offline(res)
    res.rows[0]["n"] = 300
    with pytest.raises(WriteCheckError, match="coluna 'n' \\(tinyint\\)"):
        check_offline(res)


def test_awswrangler_api_change_is_not_reported_as_a_data_error(monkeypatch, caplog):
    wr = pytest.importorskip("awswrangler")
    monkeypatch.setattr(wr._arrow, "_df_to_table", lambda table, other: None)          # outra assinatura
    res = results([{"name": "t", "columns": {"i": "i ::int"}}], [{"i": 1}])["t"]
    check_offline(res)                                               # não bloqueia a escrita
    assert "pré-verificação pelo awswrangler saltada" in caplog.text


# --- verificações que não dependem do catálogo ----------------------------------------------
def test_table_checks():
    res = results([{"name": "q", "partition_by": ["day(at)", "contry"], "columns": {"at": "at ::timestamp", "country": "c"}}],
                  [{"at": "2022-01-01T00:00:00Z", "c": "PT"}])["q"]
    with pytest.raises(WriteCheckError, match="partition_by usa \\['contry'\\]"):
        check_table(res)
    res = results([{"name": "r", "explode": "items", "replace_by": ["ids"], "columns": {"ids": "$root.ids ::array<string>", "x": "x"}}],
                  [{"ids": ["a"], "items": [{"x": 1}]}])["r"]
    with pytest.raises(WriteCheckError, match="replace_by 'ids' tem de ser uma coluna simples"):
        check_table(res)


# --- catálogo Glue -----------------------------------------------------------------------------------
@pytest.mark.parametrize("ours,table,expected", [
    ("int", "int", "same"), ("json", "string", "same"), ("decimal(10,2)", "decimal(10, 2)", "same"),
    ("array<int>", "array<int>", "same"), ("struct<a:int>", "struct<a:int>", "same"),
    ("int", "bigint", "table_wider"), ("decimal(10,2)", "decimal(12,2)", "table_wider"),
    ("bigint", "int", "ours_wider"), ("decimal(12,2)", "decimal(10,2)", "ours_wider"),
    ("double", "string", "incompatible"), ("decimal(10,2)", "decimal(10,3)", "incompatible"), ("int", "double", "incompatible"),
    # todos os nossos valores cabem no tipo da tabela (sem ALTER): decimal com mais casas inteiras e decimais, inteiros
    ("decimal(10,2)", "decimal(12,3)", "table_wider"), ("decimal(10,2)", "decimal(11,3)", "table_wider"),
    ("tinyint", "int", "same"), ("smallint", "bigint", "table_wider"),          # na tabela Iceberg, tinyint é int
    # não cabem e o Iceberg não alarga assim: escala muda (arredondaria) ou menos casas inteiras
    ("decimal(10,3)", "decimal(12,2)", "incompatible"), ("decimal(12,3)", "decimal(10,2)", "incompatible"),
    ("bigint", "tinyint", "incompatible"),
    # float (32 bits) <-> double: os valores mudariam de leitura (1.1 -> 1.100000023841858), nas novas ou nas antigas
    ("float", "double", "incompatible"), ("double", "float", "incompatible"), ("bigint", "decimal(20,0)", "incompatible"),
])
def test_compare_type(ours, table, expected):
    assert compare_type(ours, table) == expected


class FakeAWS:
    """awswrangler falso só na fronteira AWS; as conversões são as reais (se houver awswrangler)."""

    def __init__(self, monkeypatch, tables):
        self.calls, self.tables = [], tables
        real = None
        try:
            import awswrangler as real
        except ImportError:
            pass
        real = getattr(real, "_real", real)     # 2.º FakeAWS no mesmo teste: o módulo importado é o falso anterior
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
    with pytest.raises(WriteCheckError, match="a tabela tem string e a configuração double"):
        aws.write(res, report=rep)
    assert aws.calls == []                                           # nem DELETE nem escrita
    assert rep.status == "failed" and rep.write["tables"]["kids"]["status"] == "not_written"


def test_catalog_wider_type_is_used_and_narrower_is_widened(monkeypatch):
    aws = FakeAWS(monkeypatch, {"t": {"id": "string", "n": "bigint", "m": "int"}})
    rep = Report()
    res = results([{"name": "t", "columns": {"id": "id", "n": "n ::int", "m": "m ::bigint"}}], [{"id": "a", "n": 1, "m": 2}], report=rep)
    aws.write(res, report=rep)
    dtype = aws.calls[0][2]["dtype"]
    assert dtype["n"] == "bigint" and dtype["m"] == "bigint"       # n: o tipo da tabela (antes: ALTER bigint -> int)
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
    with pytest.raises(WriteCheckError, match="coluna nova 'co2' e target.schema_evolution: false"):
        aws.write(res, schema_evolution=False)
    assert aws.calls == []


def test_schema_notes_are_not_reported_when_the_precheck_blocks_the_write(monkeypatch):
    # a tabela 'a' ganharia uma coluna, mas a 'b' bloqueia a carga: o relatório não pode dizer que 'novo' foi acrescentada
    aws = FakeAWS(monkeypatch, {"a": {"id": "string"}, "b": {"id": "string", "v": "int"}})
    rep = Report()
    res = results([{"name": "a", "columns": {"id": "id", "novo": "n ::int"}}, {"name": "b", "columns": {"id": "id", "v": "v"}}],
                  [{"id": "1", "n": 1, "v": "x"}], report=rep)
    with pytest.raises(WriteCheckError):
        aws.write(res, report=rep)
    assert aws.calls == [] and rep.count(reason="column_added") == 0


def test_merge_would_null_columns_the_config_does_not_produce(monkeypatch):
    table = {"name": "m", "mode": "merge", "keys": ["id"], "columns": {"id": "id", "co2": "co2 ::double"}}
    aws = FakeAWS(monkeypatch, {"m": {"id": "string", "co2": "double", "verified_by": "string"}})
    res = results([table], [{"id": "a", "co2": 1.5}])
    with pytest.raises(WriteCheckError, match="\\['verified_by'\\] existem na tabela mas a configuração não as produz"):
        aws.write(res)                                               # antes: NULL por cima de 'TUV-X' (ver DuckDB)
    assert aws.calls == []
    rep = Report()
    res = results([{**table, "missing_columns": "null"}], [{"id": "a", "co2": 1.5}], report=rep)
    aws.write(res, report=rep)
    assert aws.calls and rep.count(reason="missing_columns", kind="declared") == 1
    aws = FakeAWS(monkeypatch, {"a": {"id": "string", "old": "string"}})
    aws.write(results([{"name": "a", "columns": {"id": "id"}}], [{"id": "x"}]))   # append: linhas novas com NULL
    assert aws.calls[0][0] == "to_iceberg"
    # tabela auto: a coluna em falta é um campo que a nova versão do documento já não tem -> NULL, contado
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
    assert sql == ['DELETE FROM "db"."kids" WHERE ("pid" = \'P\') OR ("pid" = \'Q\')']   # antes: P ficava com filhos antigos
    assert rep.write["tables"]["kids"]["replaced_documents"] == 2


def test_replace_by_with_all_rows_filtered_still_deletes(monkeypatch):
    aws = FakeAWS(monkeypatch, {"kids": {"pid": "string", "c": "string"}})
    res = results([{**CHILD, "where": {"path": "c", "op": "eq", "value": "keep"}}], [{"id": "P", "items": [{"c": "drop"}]}])
    aws.write(res)
    assert [c[1] for c in aws.calls] == ['DELETE FROM "db"."kids" WHERE ("pid" = \'P\')']


def test_replace_by_literal_uses_the_type_of_the_table_column(monkeypatch):
    # decimal(10,2) declarado, decimal(12,3) na tabela: escreve-se e compara-se com o tipo da tabela
    aws = FakeAWS(monkeypatch, {"t": {"k": "decimal(12,3)", "c": "string"}})
    table = {"name": "t", "explode": "items", "replace_by": ["k"], "columns": {"k": "$root.k ::decimal(10,2)", "c": "c"}}
    aws.write(results([table], [{"k": 1.5, "items": [{"c": "a"}]}]))
    assert aws.calls[0][1] == 'DELETE FROM "db"."t" WHERE ("k" = CAST(\'1.50\' AS decimal(12,3)))'
    assert aws.calls[1][2]["dtype"]["k"] == "decimal(12,3)"


def test_float_and_double_do_not_mix_with_the_table(monkeypatch):
    # 1.1 aceite como float (32 bits) é 1.100000023841858; numa coluna double leria-se assim -> erro, não alargamento
    for ours, table in (("float", "double"), ("double", "float")):
        aws = FakeAWS(monkeypatch, {"t": {"x": table}})
        with pytest.raises(WriteCheckError, match="float é de 32 bits"):
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
    ({**CHILD, "columns": {"pid": "pid", "c": "c"}}, "depende do elemento do explode"),
    ({**CHILD, "replace_by": ["nope"]}, "replace_by 'nope' não é uma coluna declarada"),
])
def test_replace_by_columns_are_validated(table, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [table]}]})


def test_replace_by_on_the_document_id_column_is_allowed():
    res = results([{**CHILD, "replace_by": ["doc"], "columns": {"c": "c"}}], [{"id": "P", "items": []}],
                  sub={"document_id": {"column": "doc", "from": "id"}})
    assert len(res["kids"].replace_keys) == 1


# --- literais SQL do DELETE ------------------------------------------------------------------------------
@pytest.mark.parametrize("value,t,expected", [
    (1.1, "float", "CAST('1.1' AS REAL)"),                    # antes: 1.1 (double) nunca igual ao real guardado
    (1e-05, "double", "CAST('1e-05' AS DOUBLE)"),
    (JsonFloat("0.12345678901234567890"), "double", "CAST('0.12345678901234567890' AS DOUBLE)"),
    (Decimal("1.50"), "decimal(5,2)", "CAST('1.50' AS decimal(5,2))"),
    (7, "bigint", "7"), (True, "boolean", "TRUE"), ("O'Brien", "string", "'O''Brien'"),
    (datetime(2022, 1, 1, 10, 0, 0, 123456), "timestamp", "TIMESTAMP '2022-01-01 10:00:00.123456'"),
    (date(2022, 1, 1), "date", "DATE '2022-01-01'"),
])
def test_sql_literals(value, t, expected):
    assert _sql_literal(value, t) == expected


# --- ficheiros locais ------------------------------------------------------------------------------------
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
    assert json.loads(first, parse_float=Decimal)["d"] == Decimal("1.50")                 # antes: "1.50" (texto)
    import pyarrow.parquet as pq

    t = pq.read_table(tmp_path / "l.parquet")
    assert {f.name: str(f.type) for f in t.schema}["n"] == "int8" and t.column("big").to_pylist() == [9007199254740993, None]


def test_auto_normalize_child_from_the_model_deletes_when_the_list_became_empty(monkeypatch, tmp_path):
    # antes: sem nenhum documento do lote com a lista, a tabela filha nem era gerada e os filhos antigos ficavam
    schema = {"type": "object", "properties": {"catenaXId": {"type": "string"}, "items": {"type": "array", "items": {
        "type": "object", "properties": {"q": {"type": "number"}}}}}}
    (tmp_path / "m.json").write_text(json.dumps(schema))
    cfg = from_dict({"submodels": [{"name": "s", "model": {"schema": "m.json"}, "auto_normalize": {}}]}, tmp_path)
    res = Engine(cfg).run([Document({"catenaXId": "urn:1", "items": []}, "d.json")])
    aws = FakeAWS(monkeypatch, {"s": {"catena_x_id": "string"}, "s_items": {"catena_x_id": "string", "items_idx": "int", "q": "double"}})
    aws.write(res)
    assert ("SQL", 'DELETE FROM "db"."s_items" WHERE ("catena_x_id" = \'urn:1\')', None) in aws.calls


def test_overwrite_without_rows_empties_the_table(monkeypatch):
    # antes: 0 linhas = "nada a escrever", e o conteúdo antigo ficava enquanto as outras tabelas eram substituídas
    table = {"name": "t", "mode": "overwrite", "where": {"path": "ok", "op": "eq", "value": True}, "columns": {"id": "id"}}
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    rep = Report()
    aws.write(results([table], [{"id": "a", "ok": False}], report=rep), report=rep)
    assert aws.calls == [("SQL", 'DELETE FROM "db"."t"', None)]
    assert rep.write["tables"]["t"]["emptied"] is True and "tabela esvaziada" in rep.render()
    # sem documentos deste submodelo, o lote não diz nada sobre a tabela: não se lhe toca
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    aws.write(results([table], []))
    assert aws.calls == []
    # um documento rejeitado não conta
    aws = FakeAWS(monkeypatch, {"t": {"id": "string"}})
    bad = {**table, "columns": {"id": "id", "n": "n ::int"}}
    aws.write(results([bad], [{"id": "a", "ok": True, "n": "x"}]))        # 'x' num int: documento rejeitado
    assert aws.calls == []


# --- configuração ------------------------------------------------------------------------------------------
@pytest.mark.parametrize("table,fragment", [
    ({"name": "t", "columns": {"fossilGhg": "a", "fossil_ghg": "b"}}, "'fossilGhg' e 'fossil_ghg' dão a mesma coluna"),
    ({"name": "t", "athena_options": {"mode": "overwrite"}, "columns": {"a": "a"}}, "decididas pela configuração"),
    ({"name": "t", "athena_options": {"dtype": {"a": "int"}}, "columns": {"a": "a"}}, "decididas pela configuração"),
    ({"name": "t", "mode": "overwrite_partitions", "partition_by": ["day(at)"], "columns": {"at": "at ::timestamp"}}, "partições por coluna"),
    ({"name": "t", "mode": "overwrite_partitions", "columns": {"a": "a"}}, "precisa de partition_by"),
    ({"name": "t", "missing_columns": "keep", "columns": {"a": "a"}}, "missing_columns só aceita null"),
])
def test_write_options_are_validated_when_loading(table, fragment):
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"tables": [table]}]})
    from_dict({"submodels": [{"tables": [{"name": "t", "athena_options": {"s3_additional_kwargs": {}}, "columns": {"a": "a"}}]}]})


@pytest.mark.parametrize("defaults,fragment", [
    ({"athena_options": {"mode": "overwrite"}}, "decididas pela configuração"),     # antes: substituía a tabela raiz
    ({"mode": "overwrite_partitions", "partition_by": ["day(x)"]}, "partições por coluna"),
    ({"missing_columns": "keep"}, "missing_columns só aceita null"),
    ({"mode": "upsert"}, "mode 'upsert' inválido"),
])
def test_auto_normalize_write_options_are_validated_when_loading(defaults, fragment):
    # as tabelas do auto_normalize só existem durante a carga: as opções vêm dos defaults e verificam-se ao carregar
    with pytest.raises(ConfigError, match=fragment):
        from_dict({"submodels": [{"name": "s", "auto_normalize": {}, "defaults": defaults}]})


def test_auto_normalize_passes_missing_columns_to_its_tables():
    cfg = from_dict({"submodels": [{"name": "s", "auto_normalize": {"children": "batch"}, "defaults": {"missing_columns": "null"}}]})
    res = Engine(cfg).run([Document({"catenaXId": "u", "items": [{"q": 1}]}, "d.json")])
    assert res["s"].spec.missing_columns == "null" and res["s_items"].spec.missing_columns == "null"


# --- linha de comandos e Lambda ------------------------------------------------------------------------------
def test_dry_run_runs_the_offline_precheck(tmp_path, capsys):
    from cx2iceberg.cli import main

    (tmp_path / "c.yaml").write_text("submodels:\n  - tables:\n      - {name: q, partition_by: [contry], columns: {country: c}}\n")
    (tmp_path / "d.json").write_text('{"c": "PT"}')
    with pytest.raises(SystemExit) as e:
        main(["run", "-c", str(tmp_path / "c.yaml"), str(tmp_path / "d.json"), "--dry-run"])
    assert "partition_by usa ['contry']" in str(e.value.code)


def test_missing_pyarrow_is_a_warning_not_silence(monkeypatch, caplog):
    res = results([{"name": "t", "columns": {"i": "i ::int"}}], [{"i": 1}])["t"]
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    check_offline(res)
    assert any(r.levelno == logging.WARNING and "NÃO feita" in r.getMessage() for r in caplog.records)


def test_lambda_logs_the_report_at_warning_when_something_happened(tmp_path, monkeypatch, caplog):
    import cx2iceberg.lambda_handler as L

    FakeAWS(monkeypatch, {})
    (tmp_path / "c.yaml").write_text('target: {database: db, temp_path: "s3://x/t"}\n'
                                     'submodels: [{name: s, tables: [{name: t, columns: {v: {path: v, type: double, on_error: "null"}}}]}]\n')
    monkeypatch.setenv("CX2ICEBERG_CONFIG", str(tmp_path / "c.yaml"))
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: [Document({"v": "x"}, "a")])
    caplog.set_level(logging.WARNING, logger="cx2iceberg")
    L.handler({"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "a.json"}}}]}, None)
    assert any(r.levelno == logging.WARNING and "Perdas declaradas" in r.getMessage() for r in caplog.records)
