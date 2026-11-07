"""Escrita dos resultados: Athena/Iceberg (awswrangler), ficheiros locais e DDL.

Os tipos escritos são os declarados (res.types), nunca adivinhados dos dados:

* `to_dataframe` constrói cada coluna com o tipo pandas certo (Int64, datetime64[us]...). Antes o pandas
  adivinhava: um bigint com um nulo no lote passava por float64 (9007199254740993 ficava ...992) e um
  timestamp depois de 2262 partia no pandas 2.
* `to_arrow` dá a tabela Arrow com o schema declarado (Parquet local igual ao que vai para o Athena; antes um
  tinyint ficava double e um decimal(5,2) ficava decimal(3,2) num ficheiro e decimal(4,2) noutro).
* Antes de qualquer escrita (e antes do DELETE do replace_by) todas as tabelas são pré-verificadas: conversão
  Arrow, o caminho do próprio awswrangler, e a comparação com o catálogo Glue (tipos, colunas em falta no
  merge, colunas novas). Um problema pára a carga sem escrever nada.
"""
from __future__ import annotations

import json
import logging
import math
import re
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from .athena_types import INFER_STRUCT, athena_type, parse, render
from .engine import TableResult
from .numbers import JsonFloat
from .report import Report

log = logging.getLogger("cx2iceberg")
MAX_NAME = 255          # limite do Glue para nomes de tabelas e colunas
_PANDAS = {"tinyint": "Int8", "smallint": "Int16", "int": "Int32", "bigint": "Int64", "double": "float64",
           "float": "float32", "boolean": "boolean", "timestamp": "datetime64[us]"}
_PARTITION = re.compile(r"^\s*(?:(?:year|month|day|hour)\(\s*(\w+)\s*\)|(?:bucket|truncate)\(\s*\d+\s*,\s*(\w+)\s*\)|(\w+))\s*$")


class WriteCheckError(ValueError):
    """A pré-verificação encontrou um problema: nada foi escrito."""


def _location(target: dict, res: TableResult) -> str | None:
    if res.spec.location:
        return res.spec.location
    base = target.get("location")
    return f"{base.rstrip('/')}/{res.spec.name}/" if base else None


# --- pandas e Arrow com os tipos declarados -------------------------------------------------
def to_dataframe(res: TableResult):
    """DataFrame com um dtype por coluna a partir do tipo declarado (nada inferido)."""
    import pandas as pd

    cols = {}
    for name, t in res.types.items():
        values = [r.get(name) for r in res.rows]
        dtype = _PANDAS.get(t)
        cols[name] = pd.array(values, dtype=dtype) if dtype else pd.Series(values, dtype=object)
    return pd.DataFrame(cols, columns=list(res.types), index=range(len(res.rows)))


def arrow_type(t: str):
    import pyarrow as pa

    def conv(tree):
        if isinstance(tree, str):
            return {"string": pa.string(), "json": pa.string(), "tinyint": pa.int8(), "smallint": pa.int16(),
                    "int": pa.int32(), "bigint": pa.int64(), "double": pa.float64(), "float": pa.float32(),
                    "boolean": pa.bool_(), "timestamp": pa.timestamp("us"), "date": pa.date32()}[tree]
        kind = tree[0]
        if kind == "decimal":
            return pa.decimal128(tree[1], tree[2])
        if kind == "array":
            return pa.list_(conv(tree[1]))
        if kind == "map":
            return pa.map_(conv(tree[1]), conv(tree[2]))
        if tree == INFER_STRUCT:
            raise ValueError("struct sem campos (inferido) não tem tipo Arrow")
        return pa.struct([(n, conv(ft)) for n, ft in tree[1]])

    return conv(parse(t))


def _arrow_value(v: Any, tree) -> Any:
    """Valor Python -> forma que o pyarrow aceita para o tipo (map: lista de pares)."""
    if v is None or isinstance(tree, str):
        return v
    if tree[0] == "array":
        return [_arrow_value(x, tree[1]) for x in v]
    if tree[0] == "map":
        return [(k, _arrow_value(x, tree[2])) for k, x in v.items()]
    if tree[0] == "struct":
        return {n: _arrow_value(v.get(n), ft) for n, ft in tree[1]}
    return v


def to_arrow(res: TableResult):
    """Tabela Arrow com o schema declarado; um valor que não cabe no tipo é WriteCheckError (tabela, coluna)."""
    import pyarrow as pa

    arrays, fields = [], []
    for name, t in res.types.items():
        tree, pt = parse(t), arrow_type(t)
        values = [_arrow_value(r.get(name), tree) for r in res.rows]
        try:
            arrays.append(pa.array(values, type=pt, from_pandas=False))
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as e:
            raise WriteCheckError(f"tabela '{res.spec.name}', coluna '{name}' ({t}): {e}") from None
        fields.append(pa.field(name, pt))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def check_offline(res: TableResult) -> None:
    """Pré-verificação sem AWS: o schema Arrow declarado e, se houver awswrangler, o mesmo caminho que ele usa
    para preparar o Parquet da escrita (cast do pandas + schema + conversão)."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:  # pragma: no cover - pyarrow é opcional para o dry-run
        log.warning("pré-verificação dos tipos de escrita NÃO feita: pyarrow não instalado (pip install pyarrow)")
        return
    to_arrow(res)
    funcs = _wr_conversion()
    if funcs is None:
        return
    cast_pandas, schema_from_pandas, df_to_table = funcs
    dtype = {c: athena_type(t) for c, t in res.types.items()}
    try:
        df = cast_pandas(to_dataframe(res), dict(dtype))
        df_to_table(df, schema_from_pandas(df, index=False, dtype=dtype), False, dtype)
    except Exception as e:  # noqa: BLE001 - com a API confirmada, qualquer falha aqui é dos dados
        raise WriteCheckError(f"tabela '{res.spec.name}': o awswrangler não consegue converter os dados: {e}") from None


_WR_SIGNATURES = {"cast_pandas_with_athena_types": ["df", "dtype"], "pyarrow_schema_from_pandas": ["df", "index"],
                  "_df_to_table": ["df", "schema", "index", "dtype"]}


def _wr_conversion():
    """As funções (internas) com que o awswrangler prepara o Parquet, se existirem com os argumentos esperados.
    Uma versão do awswrangler com outra API não pode parecer um erro nos dados: aí esta parte é saltada (aviso)."""
    import inspect

    try:
        import awswrangler as wr
        funcs = (wr._data_types.cast_pandas_with_athena_types, wr._data_types.pyarrow_schema_from_pandas,
                 wr._arrow._df_to_table)
    except (ImportError, AttributeError):
        return None
    for f, (name, expected) in zip(funcs, _WR_SIGNATURES.items()):   # pelo nome esperado, não f.__name__ (pode vir embrulhada)
        try:
            params = list(inspect.signature(f).parameters)
        except (TypeError, ValueError):
            params = None
        if params is None or params[:len(expected)] != expected:
            log.warning("pré-verificação pelo awswrangler saltada: %s mudou de argumentos (%s)", name, params)
            return None
    return funcs


def _partition_columns(spec) -> list[str]:
    out = []
    for p in spec.partition_by:
        m = _PARTITION.match(p)
        if not m:
            raise WriteCheckError(f"tabela '{spec.name}': partition_by '{p}' não reconhecido "
                                  f"(coluna, year/month/day/hour(coluna), bucket(n, coluna), truncate(n, coluna))")
        out.append(next(g for g in m.groups() if g))
    return out


def check_table(res: TableResult) -> None:
    """Verificações que não dependem do catálogo: nomes, partições, colunas do replace_by."""
    spec = res.spec
    if len(spec.name) > MAX_NAME:
        raise WriteCheckError(f"tabela '{spec.name[:40]}…': nome com mais de {MAX_NAME} caracteres")
    long = [c for c in res.types if len(c) > MAX_NAME]
    if long:
        raise WriteCheckError(f"tabela '{spec.name}': colunas com nome acima de {MAX_NAME} caracteres: {[c[:40] for c in long]}")
    missing = [c for c in _partition_columns(spec) if c not in res.types]
    if missing:
        raise WriteCheckError(f"tabela '{spec.name}': partition_by usa {missing}, que não são colunas da tabela "
                              f"({list(res.types)})")
    for c in spec.replace_by:
        t = res.types.get(c)
        if t is None or t == "json" or t.startswith(("array", "map", "struct")):
            raise WriteCheckError(f"tabela '{spec.name}': replace_by '{c}' tem de ser uma coluna simples (é {t})")


# --- comparação com o catálogo ---------------------------------------------------------------
def _tree(t: str):
    try:
        return parse(t)
    except ValueError:
        return t.lower().replace(" ", "")


_INTS = ("tinyint", "smallint", "int", "bigint")


def _decimal(x) -> bool:
    return isinstance(x, tuple) and x[0] == "decimal"


def _fits(x, y) -> bool:
    """Todo o valor do tipo x cabe, exacto, no tipo y? (inteiros mais largos, decimal com pelo menos as mesmas
    casas inteiras e decimais: decimal(10,2) cabe em decimal(12,3)). float->double não: o 1.1 aceite como float
    (32 bits) passaria a ler-se 1.100000023841858 numa coluna double."""
    if x in _INTS and y in _INTS:
        return _INTS.index(y) >= _INTS.index(x)
    return _decimal(x) and _decimal(y) and y[2] >= x[2] and y[1] - y[2] >= x[1] - x[2]


def _iceberg_promotes(x, y) -> bool:
    """Alargamentos que o Iceberg faz com ALTER (o schema_evolution do awswrangler) e que não mudam os valores
    já guardados: int->bigint, decimal(p,s)->decimal(p2,s) com p2 > p. O Iceberg também faz float->double, mas
    aí as linhas antigas mudam de leitura (1.1 passa a 1.100000023841858): é erro, não alargamento."""
    if (x, y) == ("int", "bigint"):
        return True
    return _decimal(x) and _decimal(y) and y[2] == x[2] and y[1] > x[1]


def compare_type(ours: str, table: str) -> str:
    """'same' | 'table_wider' (escreve-se com o tipo da tabela: todos os nossos valores cabem, sem ALTER)
    | 'ours_wider' (a tabela é alargada, alargamento que o Iceberg aceita) | 'incompatible' (erro)."""
    a, b = _tree(athena_type(ours)), _tree(table)
    if a == b:
        return "same"
    if _fits(a, b):
        return "table_wider"
    if _iceberg_promotes(b, a):
        return "ours_wider"
    return "incompatible"


# --- local --------------------------------------------------------------------------------------
def _json_text(v: Any) -> str:
    """JSON exacto: Decimal como número (todos os algarismos), datas em ISO 8601, números do documento como
    foram escritos. Antes um Decimal saía como texto ("1.50")."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, JsonFloat):
        return v.text
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError(f"{v!r} não é um número JSON")
        return repr(v)
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueError(f"{v} não é um número JSON")
        return str(v)
    if isinstance(v, (datetime, date, time)):
        return json.dumps(v.isoformat())
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, dict):
        return "{" + ", ".join(f"{json.dumps(str(k), ensure_ascii=False)}: {_json_text(x)}" for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_json_text(x) for x in v) + "]"
    raise ValueError(f"{type(v).__name__} não tem representação JSON")


def _csv_field(v: Any) -> str:
    """Nulo: campo vazio sem aspas; tudo o resto entre aspas (o texto vazio fica ""). Antes nulo e "" eram iguais,
    listas saíam no formato do Python (['a']) e booleanos como True."""
    if v is None:
        return ""
    if isinstance(v, str):
        text = v
    elif isinstance(v, (dict, list, tuple)):
        text = _json_text(v)
    elif isinstance(v, (datetime, date, time)):
        text = v.isoformat()
    else:
        text = _json_text(v)
    return '"' + text.replace('"', '""') + '"'


def write_local(results: dict[str, TableResult], out_dir: str, fmt: str = "csv", report: Report | None = None) -> list[str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    if report is not None:
        report.start_write("local", {name: len(res.rows) for name, res in results.items()})
    for name, res in results.items():
        path = out / f"{name}.{fmt}"
        try:
            if fmt == "jsonl":
                with path.open("w", encoding="utf-8") as f:
                    for r in res.rows:
                        f.write("{" + ", ".join(f"{json.dumps(k)}: {_json_text(r.get(k))}" for k in res.types) + "}\n")
            elif fmt == "parquet":
                import pyarrow.parquet as pq

                pq.write_table(to_arrow(res), path)
            else:
                with path.open("w", encoding="utf-8", newline="") as f:
                    f.write(",".join(_csv_field(c) for c in res.types) + "\n")
                    for r in res.rows:
                        f.write(",".join(_csv_field(r.get(c)) for c in res.types) + "\n")
        except Exception as e:
            if report is not None:
                report.write_failed(name, e, path=str(path))
            raise
        if report is not None:
            report.table_written(name, path=str(path))
        written.append(str(path))
    return written


# --- DDL ------------------------------------------------------------------------------------------
def ddl(res: TableResult, target: dict) -> str:
    db = target.get("database", "<database>")
    cols = ",\n".join(f"  `{c}` {athena_type(t)}" for c, t in res.types.items())
    parts = [f"CREATE TABLE IF NOT EXISTS `{db}`.`{res.spec.name}` (\n{cols}\n)"]
    if res.spec.partition_by:
        parts.append(f"PARTITIONED BY ({', '.join(res.spec.partition_by)})")
    parts.append(f"LOCATION '{_location(target, res) or 's3://<bucket>/<path>/' + res.spec.name + '/'}'")
    parts.append("TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='snappy')")
    return "\n".join(parts) + ";"


# --- Athena / Iceberg -------------------------------------------------------------------------------
class AthenaIcebergWriter:
    def __init__(self, target: dict, report: Report | None = None):
        import awswrangler as wr
        import boto3

        self.wr = wr
        self.target = target
        self.report = report
        for k in ("database", "temp_path"):
            if not target.get(k):
                raise ValueError(f"target.{k} é obrigatório para escrever no Athena")
        self.session = boto3.Session(region_name=target.get("region"), profile_name=target.get("profile"))
        self.workgroup = target.get("workgroup")
        if target.get("create_database", True):
            wr.catalog.create_database(target["database"], exist_ok=True, boto3_session=self.session)

    def precheck(self, results: dict[str, TableResult]) -> dict[str, dict[str, str]]:
        """Tudo o que pode falhar na escrita, verificado antes de escrever (e antes de qualquer DELETE).
        Devolve o dtype a usar por tabela (com o tipo da tabela quando este é um alargamento do declarado)."""
        problems: list[str] = []
        dtypes: dict[str, dict[str, str]] = {}
        self._pending: list[tuple] = []      # avisos (coluna nova, alargada...) só vão para o relatório se a escrita avançar
        db = self.target["database"]
        for name, res in results.items():
            try:
                check_table(res)
                if res.rows:
                    check_offline(res)
            except WriteCheckError as e:
                problems.append(str(e))
                continue
            dtype = {c: athena_type(t) for c, t in res.types.items()}
            if self.wr.catalog.does_table_exist(database=db, table=name, boto3_session=self.session):
                table_types = self.wr.catalog.get_table_types(database=db, table=name, boto3_session=self.session)
                problems += self._compare_catalog(res, dtype, table_types or {})
            dtypes[name] = dtype
        if problems:
            raise WriteCheckError("pré-verificação da escrita (nada foi escrito):\n  " + "\n  ".join(problems))
        for note in self._pending:
            self._note(*note)
        return dtypes

    def _compare_catalog(self, res: TableResult, dtype: dict[str, str], table_types: dict[str, str]) -> list[str]:
        spec, problems = res.spec, []
        evolution = self.target.get("schema_evolution", True)
        for col, t in res.types.items():
            if col not in table_types:
                if not evolution:
                    problems.append(f"tabela '{spec.name}': coluna nova '{col}' e target.schema_evolution: false")
                else:
                    self._pending.append((spec, col, "column_added", f"coluna nova '{col}' ({dtype[col]}) acrescentada à tabela"))
                continue
            how = compare_type(t, table_types[col])
            if how == "table_wider":
                dtype[col] = table_types[col]          # escreve-se com o tipo da tabela (sem perdas, sem ALTER)
            elif how == "ours_wider":
                if not evolution:
                    problems.append(f"tabela '{spec.name}', coluna '{col}': {table_types[col]} na tabela, {dtype[col]} "
                                    f"na configuração, e target.schema_evolution: false")
                else:
                    self._pending.append((spec, col, "column_widened", f"coluna '{col}': {table_types[col]} -> {dtype[col]}"))
            elif how == "incompatible":
                why = ("float é de 32 bits: um 1.1 passaria a ler-se 1.100000023841858; declare na coluna o tipo da tabela"
                       if {dtype[col], table_types[col]} == {"float", "double"} else
                       "o Iceberg não muda este tipo: o ALTER falharia depois do DELETE")
                problems.append(f"tabela '{spec.name}', coluna '{col}': a tabela tem {table_types[col]} e a configuração "
                                f"{dtype[col]} ({why})")
        absent = [c for c in table_types if c not in res.types]
        if absent and spec.mode == "merge" and res.rows:
            if spec.auto and spec.missing_columns != "null":
                # tabela auto: as colunas vêm dos campos dos documentos; a que falta é um campo que os documentos
                # deste lote já não têm, e NULL é o valor verdadeiro da nova versão. Fica contado.
                self._pending.append((spec, None, "missing_columns",
                                      f"colunas {absent} sem campo nos documentos deste lote: NULL nas linhas "
                                      f"actualizadas (auto: true)", "declared"))
            elif spec.missing_columns != "null":
                problems.append(f"tabela '{spec.name}': as colunas {absent} existem na tabela mas a configuração não as "
                                f"produz; o MERGE poria NULL nas linhas actualizadas. Declare missing_columns: null se "
                                f"for esse o objectivo")
            else:
                self._pending.append((spec, None, "missing_columns",
                                      f"colunas {absent} fora da configuração: NULL nas linhas actualizadas "
                                      f"(missing_columns: null)", "declared"))
        return problems

    def _note(self, spec, column, reason, message, kind="warning"):
        log.warning("tabela '%s': %s", spec.name, message)      # visível também sem relatório (write_athena)
        if self.report is not None:
            self.report.add(kind, reason, "schema_change" if kind == "warning" else "nulled", message,
                            table=spec.name, column=column)

    def write(self, results: dict[str, TableResult]) -> dict[str, int]:
        # `self.written` e o relatório vão sendo preenchidos: se uma tabela falhar, fica registado o que já foi escrito
        self.written: dict[str, int] = {}
        if self.report is not None:
            self.report.start_write("athena", {name: len(res.rows) for name, res in results.items()})
        todo = {n: r for n, r in results.items()
                if r.rows or (r.spec.replace_by and r.replace_keys) or _empties(r)}
        try:
            dtypes = self.precheck(todo)
        except WriteCheckError as e:
            if self.report is not None:
                self.report.write_blocked(str(e))
            raise
        for name, res in results.items():
            if name not in todo:
                log.info("Tabela '%s' sem linhas — nada a escrever", name)
                continue
            self._deleted = False
            emptied = False
            try:
                if _empties(res):
                    emptied = self._empty(res)
                if res.spec.replace_by:
                    self._replace_by(res, dtypes[name])
                if res.rows:
                    self._write_one(res, dtypes[name])
            except Exception as e:
                if self.report is not None:
                    extra = {"deleted_before_failure": True} if self._deleted else {}
                    note = " (o DELETE do replace_by já tinha sido executado: as linhas antigas destes ids foram " \
                           "apagadas e as novas não foram escritas)" if self._deleted else ""
                    self.report.write_failed(name, f"{e}{note}", **extra)
                raise
            self.written[name] = len(res.rows)
            if self.report is not None:
                extra = {"replaced_documents": len(set(res.replace_keys.values()))} if res.spec.replace_by else {}
                self.report.table_written(name, **extra, **({"emptied": True} if emptied else {}))
        return dict(self.written)

    def _query(self, sql: str):
        log.debug("SQL: %s", sql)
        self.wr.athena.start_query_execution(
            sql, database=self.target["database"], workgroup=self.workgroup, boto3_session=self.session, wait=True
        )

    def _replace_by(self, res: TableResult, dtype: dict[str, str]):
        """Apaga as linhas dos documentos deste lote (todos os aceites, mesmo os que agora não têm linhas).
        Os literais usam o tipo da coluna na tabela (dtype da pré-verificação), não o declarado: com `float`
        declarado e `double` na tabela, CAST('1.1' AS REAL) não é igual ao 1.1 guardado."""
        db, table = self.target["database"], res.spec.name
        if not self.wr.catalog.does_table_exist(database=db, table=table, boto3_session=self.session):
            return
        cols = res.spec.replace_by
        values = sorted(set(res.replace_keys.values()), key=str)
        for i in range(0, len(values), 300):
            chunk = values[i:i + 300]
            conds = " OR ".join(
                "(" + " AND ".join(f'"{c}" = {_sql_literal(v, dtype[c])}' for c, v in zip(cols, vals)) + ")"
                for vals in chunk
            )
            self._query(f'DELETE FROM "{db}"."{table}" WHERE {conds}')
            self._deleted = True

    def _empty(self, res: TableResult) -> bool:
        """overwrite sem linhas, com documentos aceites: a tabela fica vazia, como o lote diz (antes ficava o
        conteúdo antigo, e as outras tabelas do mesmo submodelo eram substituídas)."""
        db, table = self.target["database"], res.spec.name
        if not self.wr.catalog.does_table_exist(database=db, table=table, boto3_session=self.session):
            return False
        self._query(f'DELETE FROM "{db}"."{table}"')
        self._deleted = True
        return True

    def _write_one(self, res: TableResult, dtype: dict[str, str]):
        spec = res.spec
        kwargs: dict[str, Any] = dict(
            df=to_dataframe(res),
            database=self.target["database"],
            table=spec.name,
            temp_path=f"{self.target['temp_path'].rstrip('/')}/{spec.name}/{uuid.uuid4().hex}/",
            table_location=_location(self.target, res),
            partition_cols=spec.partition_by or None,
            dtype=dtype,
            workgroup=self.workgroup,
            boto3_session=self.session,
            keep_files=False,
            schema_evolution=self.target.get("schema_evolution", True),
            fill_missing_columns_in_df=True,
        )
        if spec.mode == "merge":
            kwargs.update(merge_cols=spec.keys, merge_condition="update")
        elif spec.mode in ("overwrite", "overwrite_partitions"):
            kwargs["mode"] = spec.mode
        kwargs.update(spec.athena_options)        # só opções que a configuração não decide (validado na carga)
        log.info("A escrever %d linha(s) em %s.%s (mode=%s)", len(res.rows), self.target["database"], spec.name, spec.mode)
        self.wr.athena.to_iceberg(**kwargs)


def _empties(res: TableResult) -> bool:
    """overwrite com zero linhas mas com documentos aceites do submodelo neste lote."""
    return res.spec.mode == "overwrite" and not res.rows and bool(res.documents)


def _sql_literal(v: Any, t: str) -> str:
    """Literal SQL com o tipo da coluna. Números não inteiros com CAST: um `float` (real) comparado com o literal
    double 1.1 nunca é igual (1.100000023841858 != 1.1) e o DELETE não apagava nada."""
    if v is None:
        return "NULL"
    if t in ("int", "bigint", "smallint", "tinyint"):
        return str(int(v))
    if t in ("double", "float") or t.startswith("decimal"):
        text = v.text if isinstance(v, JsonFloat) else repr(v) if isinstance(v, float) else str(v)
        return f"CAST('{text}' AS {'REAL' if t == 'float' else 'DOUBLE' if t == 'double' else render(parse(t))})"
    if t == "boolean":
        return "TRUE" if v else "FALSE"
    if t == "timestamp":
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if t == "date":
        return f"DATE '{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"
