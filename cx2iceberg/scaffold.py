"""Generates an initial configuration from example JSONs or from the SAMM JSON Schema (`cx2iceberg init`)."""
from __future__ import annotations

from typing import Any

import yaml

from . import athena_types as T
from .config import parse_unwrap, sanitize
from .engine import column_names, unwrap


def _is_obj_list(v: Any) -> bool:
    return isinstance(v, list) and any(isinstance(x, dict) for x in v)


def _leaves(obj: dict, parts: tuple, out: dict[tuple, Any], scalar_lists: str):
    for k, v in obj.items():
        p = parts + (k,)
        if isinstance(v, dict):
            _leaves(v, p, out, scalar_lists)
        elif _is_obj_list(v) or (isinstance(v, list) and scalar_lists == "rows"):
            continue
        else:
            out[p] = T.merge(out.get(p), T.infer_deep(v))


def _lists(obj: Any, levels: list[str], cur: list[str], found: dict[tuple, dict], scalar_lists: str):
    if not isinstance(obj, dict):
        return
    for k, v in obj.items():
        if isinstance(v, dict):
            _lists(v, levels, cur + [k], found, scalar_lists)
        elif _is_obj_list(v):
            lv = tuple(levels + [".".join(cur + [k])])
            leaves = found.setdefault(lv, {})
            for x in v:
                if isinstance(x, dict):
                    _leaves(x, (), leaves, scalar_lists)
                    _lists(x, list(lv), [], found, scalar_lists)
        elif isinstance(v, list) and scalar_lists == "rows":
            lv = tuple(levels + [".".join(cur + [k])])
            leaves = found.setdefault(lv, {})
            for x in v:
                leaves[("$item",)] = T.merge(leaves.get(("$item",)), T.infer_deep(x))


def _col(path: str, tree) -> Any:
    t = T.render(T.complete(tree))
    return path if t == "string" else {"path": path, "type": t}


def _find_id(leaves: dict[tuple, Any], id_path: str | None) -> str | None:
    paths = [".".join(p) for p in leaves]
    if id_path:
        return id_path if id_path in paths else None
    for cand in ("catenaXId", "id"):
        if cand in paths:
            return cand
    ends = sorted((p for p in paths if p.endswith((".catenaXId", ".id"))), key=len)
    return ends[0] if ends else None


def scaffold(
    docs: list[Any],
    name: str = "submodel",
    semantic_id: str | None = None,
    id_path: str | None = None,
    unwrap_rule: Any = None,
    naming: str = "full",
    structs: bool = False,
    scalar_lists: str = "array",
) -> str:
    """scalar_lists: 'array' (array<...> column) | 'rows' (child table, 1 row per value)."""
    rule = parse_unwrap(unwrap_rule, "unwrap") if unwrap_rule else None
    if rule:
        docs = [unwrap(d, rule) for d in docs]
    docs = [d for d in docs if isinstance(d, dict)]
    prefix = sanitize(name)
    root: dict[tuple, Any] = {}
    for d in docs:
        _leaves(d, (), root, scalar_lists)
    idp = _find_id(root, id_path)
    id_col = sanitize(idp.split(".")[-1]) if idp else None
    if id_col == "id":
        id_col = f"{prefix}_id"

    if structs:
        tables = [_struct_table(docs, prefix, idp, id_col)]
    else:
        tables = [_root_table(root, prefix, idp, id_col, naming)]
        found: dict[tuple, dict] = {}
        for d in docs:
            _lists(d, [], [], found, scalar_lists)
        tnames = column_names([tuple(".".join(lv).split(".")) for lv in found], naming)
        for levels, leaves in found.items():
            tname = sanitize(f"{prefix}_{tnames[tuple('.'.join(levels).split('.'))]}")
            tables.append(_child_table(levels, leaves, idp, id_col, naming, tname))

    first = next(iter(map(".".join, root)), "")
    sub: dict[str, Any] = {"name": prefix, "match": {"semantic_id": semantic_id} if semantic_id else {"has_paths": [idp or first]}}
    if unwrap_rule:
        sub["unwrap"] = unwrap_rule
    sub["tables"] = tables
    cfg = {
        "target": {
            "database": "${ATHENA_DATABASE:-catenax}",
            "location": "${ICEBERG_LOCATION:-s3://my-bucket/warehouse}",
            "temp_path": "${ATHENA_TEMP:-s3://my-bucket/tmp}",
            "workgroup": "${ATHENA_WORKGROUP:-primary}",
            "region": "${AWS_REGION:-eu-west-1}",
        },
        **({"input": {"semantic_id": semantic_id}} if semantic_id else {}),
        "defaults": {"metadata_columns": True, "on_error": "fail"},
        "submodels": [sub],
    }
    header = (
        "# Configuration generated automatically by `cx2iceberg init`.\n"
        "# Review names, types and keys; remove the columns you do not need.\n"
        "# Numbers without a type in the schema are suggested as double (JSON does not tell 1 from 1.0).\n"
        + ("" if idp else "# WARNING: no document id was found — define keys or use --id-path.\n")
        + "# Full reference: README.md\n"
    )
    return header + yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True, width=200)


def _root_table(root, prefix, idp, id_col, naming) -> dict:
    names = column_names(list(root), naming)
    cols: dict[str, Any] = {}
    if idp:
        cols[id_col] = idp
    for p, tree in root.items():
        path = ".".join(p)
        if path != idp:
            cols[names[p]] = _col(path, tree)
    return {"name": prefix, **({"mode": "merge", "keys": [id_col]} if idp else {"mode": "append"}), "columns": cols}


def _child_table(levels, leaves, idp, id_col, naming, tname) -> dict:
    cols: dict[str, Any] = {}
    if idp:
        cols[id_col] = f"$root.{idp}"
    n = len(levels)
    idx = []
    for j, lv in enumerate(levels):
        c = sanitize(f"{lv.split('.')[-1]}_idx")
        if c in idx:
            c = f"{c}_{j}"
        cols[c] = {"path": "$parent." * (n - 1 - j) + "$index", "type": "int"}
        idx.append(c)
    real = {p: t for p, t in leaves.items() if p != ("$item",)}
    names = column_names(list(real), naming)
    for p, tree in real.items():
        nm = names[p]
        cols[f"item_{nm}" if nm in cols else nm] = _col(".".join(p), tree)
    if ("$item",) in leaves:
        cols["value"] = _col("$item", leaves[("$item",)])
    t = {
        "name": tname,
        "explode": "[*].".join(levels) + "[*]" if n > 1 else levels[0],
        "keys": ([id_col] if idp else []) + idx,
        "columns": cols,
    }
    if not leaves:  # entity without properties in the model: take whatever comes in the data
        t["auto"] = True
    if idp:
        t.update(mode="append", replace_by=[id_col])
    return t


def _struct_table(docs, prefix, idp, id_col) -> dict:
    """1 table: top-level scalars as columns, blocks as struct/array<struct> with an explicit type."""
    top: dict[str, Any] = {}
    for d in docs:
        for k, v in d.items():
            top[k] = T.merge(top.get(k), T.infer_deep(v))
    cols: dict[str, Any] = {}
    if idp:
        cols[id_col] = idp
    for k, tree in top.items():
        cols[sanitize(k)] = _col(k, tree)
    return {"name": prefix, **({"mode": "merge", "keys": [id_col]} if idp else {"mode": "append"}), "columns": cols}
