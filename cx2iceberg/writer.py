"""Writing the results: Athena/Iceberg (awswrangler), local files and DDL.

The types written are the declared ones (res.types), never guessed from the data:

* `to_dataframe` builds each column with the right pandas type (Int64, datetime64[us]...). Before, pandas
  guessed: a bigint with a null in the batch went through float64 (9007199254740993 became ...992) and a
  timestamp after 2262 broke in pandas 2.
* `to_arrow` gives the Arrow table with the declared schema (local Parquet equal to what goes to Athena; before, a
  tinyint became double and a decimal(5,2) became decimal(3,2) in one file and decimal(4,2) in another).
* Before any write (and before the replace_by DELETE) every table is pre-checked: Arrow conversion,
  awswrangler's own path, and the comparison with the Glue catalog (types, columns missing in a
  merge, new columns). A problem stops the load without writing anything.
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
MAX_NAME = 255          # Glue's limit for table and column names
_PANDAS = {"tinyint": "Int8", "smallint": "Int16", "int": "Int32", "bigint": "Int64", "double": "float64",
           "float": "float32", "boolean": "boolean", "timestamp": "datetime64[us]"}
_PARTITION = re.compile(r"^\s*(?:(?:year|month|day|hour)\(\s*(\w+)\s*\)|(?:bucket|truncate)\(\s*\d+\s*,\s*(\w+)\s*\)|(\w+))\s*$")


class WriteCheckError(ValueError):
    """The pre-check found a problem: nothing was written."""


def _location(target: dict, res: TableResult) -> str | None:
    if res.spec.location:
        return res.spec.location
    base = target.get("location")
    return f"{base.rstrip('/')}/{res.spec.name}/" if base else None


# --- pandas and Arrow with the declared types -------------------------------------------------
def to_dataframe(res: TableResult):
    """DataFrame with one dtype per column from the declared type (nothing inferred)."""
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
            raise ValueError("a struct without fields (inferred) has no Arrow type")
        return pa.struct([(n, conv(ft)) for n, ft in tree[1]])

    return conv(parse(t))


def _arrow_value(v: Any, tree) -> Any:
    """Python value -> the form pyarrow accepts for the type (map: list of pairs)."""
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
    """Arrow table with the declared schema; a value that does not fit the type is a WriteCheckError (table, column)."""
    import pyarrow as pa

    arrays, fields = [], []
    for name, t in res.types.items():
        tree, pt = parse(t), arrow_type(t)
        values = [_arrow_value(r.get(name), tree) for r in res.rows]
        try:
            arrays.append(pa.array(values, type=pt, from_pandas=False))
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as e:
            raise WriteCheckError(f"table '{res.spec.name}', column '{name}' ({t}): {e}") from None
        fields.append(pa.field(name, pt))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def check_offline(res: TableResult) -> None:
    """Pre-check without AWS: the declared Arrow schema and, if awswrangler is installed, the same path it uses
    to prepare the write's Parquet (pandas cast + schema + conversion)."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:  # pragma: no cover - pyarrow is optional for the dry run
        log.warning("write type pre-check NOT done: pyarrow is not installed (pip install pyarrow)")
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
    except Exception as e:  # noqa: BLE001 - with the API confirmed, any failure here comes from the data
        raise WriteCheckError(f"table '{res.spec.name}': awswrangler cannot convert the data: {e}") from None


_WR_SIGNATURES = {"cast_pandas_with_athena_types": ["df", "dtype"], "pyarrow_schema_from_pandas": ["df", "index"],
                  "_df_to_table": ["df", "schema", "index", "dtype"]}


def _wr_conversion():
    """The (internal) functions awswrangler uses to prepare the Parquet, if they exist with the expected arguments.
    An awswrangler version with another API must not look like an error in the data: then this part is skipped (warning)."""
    import inspect

    try:
        import awswrangler as wr
        funcs = (wr._data_types.cast_pandas_with_athena_types, wr._data_types.pyarrow_schema_from_pandas,
                 wr._arrow._df_to_table)
    except (ImportError, AttributeError):
        return None
    for f, (name, expected) in zip(funcs, _WR_SIGNATURES.items()):   # by the expected name, not f.__name__ (it may be wrapped)
        try:
            params = list(inspect.signature(f).parameters)
        except (TypeError, ValueError):
            params = None
        if params is None or params[:len(expected)] != expected:
            log.warning("awswrangler pre-check skipped: %s changed its arguments (%s)", name, params)
            return None
    return funcs


def _partition_columns(spec) -> list[str]:
    out = []
    for p in spec.partition_by:
        m = _PARTITION.match(p)
        if not m:
            raise WriteCheckError(f"table '{spec.name}': partition_by '{p}' not recognised "
                                  f"(column, year/month/day/hour(column), bucket(n, column), truncate(n, column))")
        out.append(next(g for g in m.groups() if g))
    return out


def check_table(res: TableResult) -> None:
    """Checks that do not depend on the catalog: names, partitions, replace_by columns."""
    spec = res.spec
    if len(spec.name) > MAX_NAME:
        raise WriteCheckError(f"table '{spec.name[:40]}…': name longer than {MAX_NAME} characters")
    long = [c for c in res.types if len(c) > MAX_NAME]
    if long:
        raise WriteCheckError(f"table '{spec.name}': column names longer than {MAX_NAME} characters: {[c[:40] for c in long]}")
    missing = [c for c in _partition_columns(spec) if c not in res.types]
    if missing:
        raise WriteCheckError(f"table '{spec.name}': partition_by uses {missing}, which are not columns of the table "
                              f"({list(res.types)})")
    for c in spec.replace_by:
        t = res.types.get(c)
        if t is None or t == "json" or t.startswith(("array", "map", "struct")):
            raise WriteCheckError(f"table '{spec.name}': replace_by '{c}' must be a simple column (it is {t})")


# --- comparison with the catalog ---------------------------------------------------------------
def _tree(t: str):
    try:
        return parse(t)
    except ValueError:
        return t.lower().replace(" ", "")


_INTS = ("tinyint", "smallint", "int", "bigint")


def _decimal(x) -> bool:
    return isinstance(x, tuple) and x[0] == "decimal"


def _fits(x, y) -> bool:
    """Does every value of type x fit, exactly, in type y? (wider integers, decimal with at least the same
    integer and decimal digits: decimal(10,2) fits in decimal(12,3)). float->double does not: 1.1 accepted as a float
    (32 bits) would read as 1.100000023841858 in a double column."""
    if x in _INTS and y in _INTS:
        return _INTS.index(y) >= _INTS.index(x)
    return _decimal(x) and _decimal(y) and y[2] >= x[2] and y[1] - y[2] >= x[1] - x[2]


def _iceberg_promotes(x, y) -> bool:
    """Widenings Iceberg does with ALTER (awswrangler's schema_evolution) that do not change the values
    already stored: int->bigint, decimal(p,s)->decimal(p2,s) with p2 > p. Iceberg also does float->double, but
    then the old rows read differently (1.1 becomes 1.100000023841858): an error, not a widening."""
    if (x, y) == ("int", "bigint"):
        return True
    return _decimal(x) and _decimal(y) and y[2] == x[2] and y[1] > x[1]


def compare_type(ours: str, table: str) -> str:
    """'same' | 'table_wider' (written with the table's type: all our values fit, no ALTER)
    | 'ours_wider' (the table is widened, a widening Iceberg accepts) | 'incompatible' (error)."""
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
    """Exact JSON: Decimal as a number (every digit), dates in ISO 8601, the document's numbers as
    they were written. Before, a Decimal came out as text ("1.50")."""
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
            raise ValueError(f"{v!r} is not a JSON number")
        return repr(v)
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueError(f"{v} is not a JSON number")
        return str(v)
    if isinstance(v, (datetime, date, time)):
        return json.dumps(v.isoformat())
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, dict):
        return "{" + ", ".join(f"{json.dumps(str(k), ensure_ascii=False)}: {_json_text(x)}" for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_json_text(x) for x in v) + "]"
    raise ValueError(f"{type(v).__name__} has no JSON representation")


def _csv_field(v: Any) -> str:
    """Null: an empty field without quotes; everything else in quotes (empty text becomes ""). Before, null and "" were the same,
    lists came out in Python's format (['a']) and booleans as True."""
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
                raise ValueError(f"target.{k} is required to write to Athena")
        self.session = boto3.Session(region_name=target.get("region"), profile_name=target.get("profile"))
        self.workgroup = target.get("workgroup")
        if target.get("create_database", True):
            wr.catalog.create_database(target["database"], exist_ok=True, boto3_session=self.session)

    def precheck(self, results: dict[str, TableResult]) -> dict[str, dict[str, str]]:
        """Everything that can fail in the write, checked before writing (and before any DELETE).
        Returns the dtype to use per table (with the table's type when it is a widening of the declared one)."""
        problems: list[str] = []
        dtypes: dict[str, dict[str, str]] = {}
        self._pending: list[tuple] = []      # warnings (new column, widened...) only go to the report if the write goes ahead
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
            raise WriteCheckError("write pre-check (nothing was written):\n  " + "\n  ".join(problems))
        for note in self._pending:
            self._note(*note)
        return dtypes

    def _compare_catalog(self, res: TableResult, dtype: dict[str, str], table_types: dict[str, str]) -> list[str]:
        spec, problems = res.spec, []
        evolution = self.target.get("schema_evolution", True)
        for col, t in res.types.items():
            if col not in table_types:
                if not evolution:
                    problems.append(f"table '{spec.name}': new column '{col}' and target.schema_evolution: false")
                else:
                    self._pending.append((spec, col, "column_added", f"new column '{col}' ({dtype[col]}) added to the table"))
                continue
            how = compare_type(t, table_types[col])
            if how == "table_wider":
                dtype[col] = table_types[col]          # written with the table's type (lossless, no ALTER)
            elif how == "ours_wider":
                if not evolution:
                    problems.append(f"table '{spec.name}', column '{col}': {table_types[col]} in the table, {dtype[col]} "
                                    f"in the configuration, and target.schema_evolution: false")
                else:
                    self._pending.append((spec, col, "column_widened", f"column '{col}': {table_types[col]} -> {dtype[col]}"))
            elif how == "incompatible":
                why = ("float is 32-bit: a 1.1 would read as 1.100000023841858; declare the table's type on the column"
                       if {dtype[col], table_types[col]} == {"float", "double"} else
                       "Iceberg does not change this type: the ALTER would fail after the DELETE")
                problems.append(f"table '{spec.name}', column '{col}': the table has {table_types[col]} and the configuration "
                                f"{dtype[col]} ({why})")
        absent = [c for c in table_types if c not in res.types]
        if absent and spec.mode == "merge" and res.rows:
            if spec.auto and spec.missing_columns != "null":
                # auto table: the columns come from the documents' fields; a missing one is a field the documents
                # of this batch no longer have, and NULL is the true value of the new version. It is counted.
                self._pending.append((spec, None, "missing_columns",
                                      f"columns {absent} have no field in this batch's documents: NULL in the updated "
                                      f"rows (auto: true)", "declared"))
            elif spec.missing_columns != "null":
                problems.append(f"table '{spec.name}': columns {absent} exist in the table but the configuration does not "
                                f"produce them; the MERGE would put NULL in the updated rows. Declare missing_columns: null if "
                                f"that is the intent")
            else:
                self._pending.append((spec, None, "missing_columns",
                                      f"columns {absent} not in the configuration: NULL in the updated rows "
                                      f"(missing_columns: null)", "declared"))
        return problems

    def _note(self, spec, column, reason, message, kind="warning"):
        log.warning("table '%s': %s", spec.name, message)      # visible even without a report (write_athena)
        if self.report is not None:
            self.report.add(kind, reason, "schema_change" if kind == "warning" else "nulled", message,
                            table=spec.name, column=column)

    def write(self, results: dict[str, TableResult]) -> dict[str, int]:
        # `self.written` and the report are filled in as it goes: if a table fails, what was already written is recorded
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
                log.info("Table '%s' has no rows — nothing to write", name)
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
                    note = " (the replace_by DELETE had already run: the old rows of these ids were " \
                           "deleted and the new ones were not written)" if self._deleted else ""
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
        """Deletes the rows of this batch's documents (every accepted one, even those that now have no rows).
        The literals use the column's type in the table (the pre-check's dtype), not the declared one: with `float`
        declared and `double` in the table, CAST('1.1' AS REAL) is not equal to the stored 1.1."""
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
        """overwrite with no rows but with accepted documents: the table becomes empty, as the batch says (before, the
        old content stayed, and the other tables of the same submodel were replaced)."""
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
        kwargs.update(spec.athena_options)        # only options the configuration does not decide (validated at load time)
        log.info("Writing %d row(s) to %s.%s (mode=%s)", len(res.rows), self.target["database"], spec.name, spec.mode)
        self.wr.athena.to_iceberg(**kwargs)


def _empties(res: TableResult) -> bool:
    """overwrite with zero rows but with accepted documents of the submodel in this batch."""
    return res.spec.mode == "overwrite" and not res.rows and bool(res.documents)


def _sql_literal(v: Any, t: str) -> str:
    """SQL literal with the column's type. Non-integer numbers with CAST: a `float` (real) compared with the double
    literal 1.1 is never equal (1.100000023841858 != 1.1) and the DELETE deleted nothing."""
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
