"""Escrita dos resultados: Athena/Iceberg (awswrangler), ficheiros locais e DDL."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .athena_types import athena_type
from .engine import TableResult

log = logging.getLogger("cx2iceberg")


def _location(target: dict, res: TableResult) -> str | None:
    if res.spec.location:
        return res.spec.location
    base = target.get("location")
    return f"{base.rstrip('/')}/{res.spec.name}/" if base else None


def to_dataframe(res: TableResult):
    import pandas as pd

    df = pd.DataFrame(res.rows, columns=list(res.types))
    for col, t in res.types.items():
        if t == "timestamp":
            df[col] = pd.to_datetime(df[col])
    return df


# --- DDL ----------------------------------------------------------------
def ddl(res: TableResult, target: dict) -> str:
    db = target.get("database", "<database>")
    cols = ",\n".join(f"  `{c}` {athena_type(t)}" for c, t in res.types.items())
    parts = [f"CREATE TABLE IF NOT EXISTS `{db}`.`{res.spec.name}` (\n{cols}\n)"]
    if res.spec.partition_by:
        parts.append(f"PARTITIONED BY ({', '.join(res.spec.partition_by)})")
    parts.append(f"LOCATION '{_location(target, res) or 's3://<bucket>/<path>/' + res.spec.name + '/'}'")
    parts.append("TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='snappy')")
    return "\n".join(parts) + ";"


# --- local --------------------------------------------------------------
def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return v


def write_local(results: dict[str, TableResult], out_dir: str, fmt: str = "csv") -> list[str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, res in results.items():
        path = out / f"{name}.{fmt}"
        if fmt == "jsonl":
            with path.open("w", encoding="utf-8") as f:
                for r in res.rows:
                    f.write(json.dumps({k: _jsonable(v) for k, v in r.items()}, ensure_ascii=False) + "\n")
        else:
            df = to_dataframe(res)
            if fmt == "parquet":
                df.to_parquet(path, index=False)
            else:
                df.to_csv(path, index=False)
        written.append(str(path))
    return written


# --- Athena / Iceberg ----------------------------------------------------
class AthenaIcebergWriter:
    def __init__(self, target: dict):
        import awswrangler as wr
        import boto3

        self.wr = wr
        self.target = target
        for k in ("database", "temp_path"):
            if not target.get(k):
                raise ValueError(f"target.{k} é obrigatório para escrever no Athena")
        self.session = boto3.Session(region_name=target.get("region"), profile_name=target.get("profile"))
        self.workgroup = target.get("workgroup")
        if target.get("create_database", True):
            wr.catalog.create_database(target["database"], exist_ok=True, boto3_session=self.session)

    def write(self, results: dict[str, TableResult]) -> dict[str, int]:
        written = {}
        for name, res in results.items():
            if not res.rows:
                log.info("Tabela '%s' sem linhas — nada a escrever", name)
                continue
            self._write_one(res)
            written[name] = len(res.rows)
        return written

    def _query(self, sql: str):
        log.debug("SQL: %s", sql)
        self.wr.athena.start_query_execution(
            sql, database=self.target["database"], workgroup=self.workgroup, boto3_session=self.session, wait=True
        )

    def _replace_by(self, res: TableResult):
        db, table = self.target["database"], res.spec.name
        if not self.wr.catalog.does_table_exist(database=db, table=table, boto3_session=self.session):
            return
        cols = res.spec.replace_by
        values = sorted({tuple(r[c] for c in cols) for r in res.rows}, key=str)
        for i in range(0, len(values), 300):
            chunk = values[i:i + 300]
            conds = " OR ".join(
                "(" + " AND ".join(f'"{c}" = {_sql_literal(v, res.types[c])}' for c, v in zip(cols, vals)) + ")"
                for vals in chunk
            )
            self._query(f'DELETE FROM "{db}"."{table}" WHERE {conds}')

    def _write_one(self, res: TableResult):
        spec = res.spec
        if spec.replace_by:
            self._replace_by(res)
        kwargs: dict[str, Any] = dict(
            df=to_dataframe(res),
            database=self.target["database"],
            table=spec.name,
            temp_path=f"{self.target['temp_path'].rstrip('/')}/{spec.name}/{uuid.uuid4().hex}/",
            table_location=_location(self.target, res),
            partition_cols=spec.partition_by or None,
            dtype={c: athena_type(t) for c, t in res.types.items()},
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
        kwargs.update(spec.athena_options)
        log.info("A escrever %d linha(s) em %s.%s (mode=%s)", len(res.rows), self.target["database"], spec.name, spec.mode)
        self.wr.athena.to_iceberg(**kwargs)


def _sql_literal(v: Any, t: str) -> str:
    if v is None:
        return "NULL"
    if t in ("int", "bigint", "smallint", "tinyint", "double", "float") or t.startswith("decimal"):
        return str(v)
    if t == "boolean":
        return "TRUE" if v else "FALSE"
    if t == "timestamp":
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if t == "date":
        return f"DATE '{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"
