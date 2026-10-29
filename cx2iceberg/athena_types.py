"""Tipos Athena/Iceberg: parsing, inferência e cast de valores Python.

Tipos suportados (aninháveis):
    string int bigint smallint tinyint double float boolean timestamp date json decimal(p,s)
    array<T>  map<K,V>  struct<campo:T, ...>

`struct` e `array<struct>` sem campos significam "inferir a estrutura a partir dos dados".
`json` é gravado como string (objecto serializado).
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any, Union

from .transforms import _parse_dt

ALIASES = {
    "str": "string", "text": "string", "varchar": "string",
    "integer": "int", "long": "bigint", "float64": "double", "number": "double",
    "bool": "boolean", "datetime": "timestamp",
}
SIMPLE = {"string", "int", "bigint", "smallint", "tinyint", "double", "float", "boolean", "timestamp", "date", "json"}
INFER_STRUCT = ("struct", ())  # struct sem campos = inferir

ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")

# Árvore de tipo: str (simples) | ("decimal", p, s) | ("array", T) | ("map", K, V) | ("struct", ((nome, T), ...))
Tree = Union[str, tuple]


def _split_top(s: str) -> list[str]:
    parts, depth, cur = [], 0, ""
    for ch in s:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return [p.strip() for p in parts]


@lru_cache(maxsize=2048)
def parse(t: str) -> Tree:
    from .config import sanitize  # evitar import circular

    s = str(t).strip()
    low = s.lower().replace(" ", "")
    low = ALIASES.get(low, low)
    if low in SIMPLE:
        return low
    if low == "struct":
        return INFER_STRUCT
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", low)
    if m:
        return ("decimal", int(m.group(1)), int(m.group(2)))
    m = re.fullmatch(r"(?i)array\s*<(.+)>", s)
    if m:
        return ("array", parse(m.group(1)))
    m = re.fullmatch(r"(?i)map\s*<(.+)>", s)
    if m:
        kv = _split_top(m.group(1))
        if len(kv) != 2:
            raise ValueError(f"map inválido: '{t}'")
        return ("map", parse(kv[0]), parse(kv[1]))
    m = re.fullmatch(r"(?i)struct\s*<(.*)>", s)
    if m:
        fields = []
        for f in _split_top(m.group(1)):
            if ":" not in f:
                raise ValueError(f"campo de struct sem tipo: '{f}' em '{t}'")
            name, ft = f.split(":", 1)
            fields.append((sanitize(name.strip()), parse(ft)))
        return ("struct", tuple(fields))
    raise ValueError(f"Tipo não suportado: '{t}'")


def render(tree: Tree, ddl: bool = False) -> str:
    """Árvore -> texto. Com ddl=True, json passa a string (tipo físico)."""
    if isinstance(tree, str):
        return "string" if ddl and tree == "json" else tree
    kind = tree[0]
    if kind == "decimal":
        return f"decimal({tree[1]},{tree[2]})"
    if kind == "array":
        return f"array<{render(tree[1], ddl)}>"
    if kind == "map":
        return f"map<{render(tree[1], ddl)},{render(tree[2], ddl)}>"
    if not tree[1]:
        return "struct"
    return "struct<" + ",".join(f"{n}:{render(ft, ddl)}" for n, ft in tree[1]) + ">"


def normalize(t: str) -> str:
    return render(parse(t))


def athena_type(t: str) -> str:
    return render(parse(t), ddl=True)


def needs_inference(t: str) -> bool:
    tree = parse(t)
    while isinstance(tree, tuple) and tree[0] == "array":
        tree = tree[1]
    return tree == INFER_STRUCT


# --- inferência ----------------------------------------------------------
def infer(value: Any) -> str | None:
    """Tipo plano de uma coluna (objectos -> json)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float, Decimal)):
        # JSON não distingue 1 de 1.0 e o Iceberg não promove bigint -> double:
        # sem tipo explícito, números são sempre double.
        return "double"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date):
        return "date"
    if isinstance(value, dict):
        return "json"
    if isinstance(value, list):
        inner = {infer(v) for v in value if v is not None}
        if len(inner) == 1 and next(iter(inner)) in ("string", "double", "boolean"):
            return f"array<{inner.pop()}>"
        return "json" if any(isinstance(v, (dict, list)) for v in value) else "array<string>"
    return "string"


def widen(a: str | None, b: str | None) -> str | None:
    """Combina dois tipos planos inferidos para a mesma coluna."""
    if a is None or a == b:
        return b if a is None else a
    if b is None:
        return a
    if "json" in (a, b):
        return "json"
    return "string"


def infer_deep(value: Any) -> Tree | None:
    """Tipo completo (struct/array aninhados). None = desconhecido (só nulos/listas vazias)."""
    from .config import sanitize

    if value is None:
        return None
    declared = getattr(value, "athena_type", None)  # folha de um esqueleto SAMM (samm.Typed)
    if declared:
        return parse(declared)
    if isinstance(value, dict):
        fields: dict[str, Tree | None] = {}
        for k, v in value.items():
            n = sanitize(k)
            fields[n] = merge(fields.get(n), infer_deep(v))
        return ("struct", tuple(fields.items()))
    if isinstance(value, list):
        inner = None
        for v in value:
            inner = merge(inner, infer_deep(v))
        return ("array", inner)
    if isinstance(value, str) and ISO_DATETIME.match(value):
        return "timestamp"
    return infer(value)


def merge(a: Tree | None, b: Tree | None) -> Tree | None:
    if a is None:
        return b
    if b is None or a == b:
        return a
    if isinstance(a, tuple) and isinstance(b, tuple) and a[0] == b[0] == "struct":
        fields = dict(a[1])
        for n, t in b[1]:
            fields[n] = merge(fields.get(n), t)
        return ("struct", tuple(fields.items()))
    if isinstance(a, tuple) and isinstance(b, tuple) and a[0] == b[0] == "array":
        return ("array", merge(a[1], b[1]))
    if isinstance(a, tuple) or isinstance(b, tuple):
        return "json"
    if {a, b} == {"timestamp", "string"}:
        return "string"
    return widen(a, b)


def complete(tree: Tree | None) -> Tree:
    """Substitui desconhecidos por string e structs vazios por json (o Athena não aceita struct<>)."""
    if tree is None:
        return "string"
    if isinstance(tree, tuple):
        if tree[0] == "array":
            return ("array", complete(tree[1]))
        if tree[0] == "map":
            return ("map", complete(tree[1]), complete(tree[2]))
        if tree[0] == "struct":
            if not tree[1]:
                return "json"
            return ("struct", tuple((n, complete(t)) for n, t in tree[1]))
    return tree


# --- cast ----------------------------------------------------------------
def cast(value: Any, t: str) -> Any:
    return _cast(value, parse(t))


def _cast(value: Any, t: Tree) -> Any:
    if value is None:
        return None
    if isinstance(t, tuple):
        kind = t[0]
        if kind == "decimal":
            if value == "":
                return None
            try:
                return Decimal(str(value)).quantize(Decimal(1).scaleb(-t[2]))
            except InvalidOperation as e:
                raise ValueError(f"'{value}' não é decimal") from e
        if kind == "array":
            items = value if isinstance(value, list) else [value]
            return [_cast(v, t[1]) for v in items]
        if kind == "map":
            return {_cast(k, t[1]): _cast(v, t[2]) for k, v in dict(value).items()}
        if kind == "struct":
            return _cast_struct(value, t)
    if t == "json":
        return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    if t == "string":
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, default=str)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)
    if value == "":
        return None
    if t in ("int", "bigint", "smallint", "tinyint"):
        if isinstance(value, bool):
            return int(value)
        d = Decimal(str(value).strip())
        if d != d.to_integral_value():
            raise ValueError(f"'{value}' não é inteiro (use type: double ou decimal)")
        return int(d)
    if t in ("double", "float"):
        return float(value)
    if t == "boolean":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
        raise ValueError(f"'{value}' não é booleano")
    if t == "timestamp":
        return _parse_dt(value)
    if t == "date":
        return value if isinstance(value, date) and not isinstance(value, datetime) else _parse_dt(value).date()
    raise ValueError(f"Tipo não suportado: {t}")


def _cast_struct(value: Any, t: Tree) -> dict | None:
    from .config import sanitize

    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, list):  # lista 1:1 tratada como objecto
        if not value:
            return None
        if len(value) > 1:
            raise ValueError(f"esperado 1 objecto para struct, recebidos {len(value)}")
        value = value[0]
    if not isinstance(value, dict):
        raise ValueError(f"esperado objecto para struct, recebido {type(value).__name__}")
    by_name = {sanitize(k): v for k, v in value.items()}
    return {n: _cast(value.get(n, by_name.get(n)), ft) for n, ft in t[1]}
