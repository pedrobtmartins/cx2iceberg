"""Tipos Athena/Iceberg: parsing, inferência e cast de valores Python.

Tipos suportados (aninháveis):
    string int bigint smallint tinyint double float boolean timestamp date json decimal(p,s)
    array<T>  map<K,V>  struct<campo:T, ...>

`struct` e `array<struct>` sem campos significam "inferir a estrutura a partir dos dados".
`json` é gravado como string (objecto serializado).
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from functools import lru_cache
from typing import Any, Union

from . import dates
from .notes import note
from .numbers import ASCII_SPACE, MODES, NumberError, to_decimal, to_float
from .values import json_text, text_of

INT_BITS = {"tinyint": 8, "smallint": 16, "int": 32, "bigint": 64}
MAX_DECIMAL_PRECISION = 38   # limite do Athena/Iceberg

ALIASES = {
    "str": "string", "text": "string", "varchar": "string",
    "integer": "int", "long": "bigint", "float64": "double", "number": "double",
    "bool": "boolean", "datetime": "timestamp",
}
SIMPLE = {"string", "int", "bigint", "smallint", "tinyint", "double", "float", "boolean", "timestamp", "date", "json"}
INFER_STRUCT = ("struct", ())  # struct sem campos = inferir

# data/hora no formato do schema (xsd:dateTime): só estes textos são detectados como datas no modo auto
ISO_DATETIME = re.compile("^" + dates.DATETIME_RE.pattern + "$", re.ASCII)

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
        p, sc = int(m.group(1)), int(m.group(2))
        if not 1 <= p <= MAX_DECIMAL_PRECISION or sc > p:
            raise ValueError(f"'{t}': a precisão tem de estar entre 1 e {MAX_DECIMAL_PRECISION} e a escala não pode ser maior")
        return ("decimal", p, sc)
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


# O Iceberg não tem inteiros de 8/16 bits (só int e long): na tabela, tinyint/smallint são int. O tipo declarado
# continua a validar o intervalo de cada valor no cast (300 num tinyint é erro), por isso nada muda nos valores.
_PHYSICAL = {"json": "string", "tinyint": "int", "smallint": "int"}


def render(tree: Tree, ddl: bool = False) -> str:
    """Árvore -> texto. Com ddl=True, o tipo físico da tabela Iceberg: json -> string, tinyint/smallint -> int."""
    if isinstance(tree, str):
        return _PHYSICAL.get(tree, tree) if ddl else tree
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
def cast(value: Any, t: str, rounding: str | None = None) -> Any:
    """Converte para o tipo. `rounding` (da coluna): modo de arredondamento para decimal (half_up, ...) ou
    'nearest' para double/float; sem ele, um valor que não caiba exactamente dá erro."""
    return _cast(value, parse(t), rounding)


def _cast(value: Any, t: Tree, rounding: str | None = None) -> Any:
    if value is None:
        return None
    if isinstance(t, tuple):
        kind = t[0]
        if kind == "decimal":
            if _blank(value):
                return None
            return _decimal(value, t[1], t[2], rounding)
        if kind == "array":
            items = value if isinstance(value, list) else [value]
            return [_cast(v, t[1], rounding) for v in items]
        if kind == "map":
            if not isinstance(value, dict):
                raise ValueError(f"esperado objecto para {render(t)}, recebido {type(value).__name__}")
            out: dict = {}
            for k, v in value.items():
                ck = _cast(k, t[1], rounding)
                if ck in out:
                    raise ValueError(f"as chaves {k!r} e outra dão a mesma chave {ck!r} em {render(t)}")
                out[ck] = _cast(v, t[2], rounding)
            return out
        if kind == "struct":
            return _cast_struct(value, t, rounding)
    if t == "json":
        if isinstance(value, str):
            from .reader import loads  # noqa: PLC0415 - evitar import circular

            try:
                loads(value)              # texto já em JSON (ex.: to_json): tem de ser JSON válido
            except ValueError as e:
                raise ValueError(f"{value[:80]!r} não é JSON ({e}); para guardar texto use type: string") from None
            return value
        return json_text(value)
    if t == "string":
        return text_of(value)
    if value == "" or (t in NUMERIC and _blank(value)):
        return None
    if t in INT_BITS:
        d = to_decimal(value)                # número do documento: texto original (12345678901234567890.0 fica exacto)
        if not d.is_finite() or d != d.to_integral_value():
            raise ValueError(f"'{value}' não é inteiro (use type: double ou decimal)")
        bits = INT_BITS[t]
        if not -(2 ** (bits - 1)) <= d < 2 ** (bits - 1):
            raise ValueError(f"{int(d)} não cabe em {t} ({bits} bits: de {-(2 ** (bits - 1))} a {2 ** (bits - 1) - 1})")
        return int(d)
    if t in ("double", "float"):
        return to_float(value, rounding, single=t == "float")
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
        return dates.to_timestamp(value)
    if t == "date":
        return dates.to_date(value)
    raise ValueError(f"Tipo não suportado: {t}")


def _decimal(value: Any, p: int, sc: int, rounding: str | None) -> Decimal:
    d = to_decimal(value)                    # número do documento: texto original (exacto)
    if not d.is_finite():
        raise NumberError(f"{value!r} não é um número finito")
    with localcontext() as ctx:
        ctx.prec = 2 * MAX_DECIMAL_PRECISION + 4   # a precisão por omissão (28) é menor do que decimal(38, …)
        try:
            q = d.quantize(Decimal(1).scaleb(-sc), rounding=MODES.get(rounding or "half_even"))
        except InvalidOperation:
            raise ValueError(f"{d} não cabe em decimal({p},{sc})") from None
    if q != d:
        if rounding is None:
            raise ValueError(f"{d} tem mais de {sc} casa(s) decimal(is) para decimal({p},{sc}); declare "
                             f"rounding (ex.: half_up) na coluna para arredondar")
        note("declared", "rounded", "rounded", f"{d} arredondado para {q} ({rounding})")
    if abs(q) >= Decimal(10) ** (p - sc):
        raise ValueError(f"{d} não cabe em decimal({p},{sc}) (máximo {p - sc} algarismo(s) antes da vírgula)")
    return q


NUMERIC = {"int", "bigint", "smallint", "tinyint", "double", "float"}


def _blank(value: Any) -> bool:
    """Texto vazio (ou só espaços ASCII) numa coluna numérica conta como nulo (e o motor regista-o)."""
    return isinstance(value, str) and not value.strip(ASCII_SPACE)


def _cast_struct(value: Any, t: Tree, rounding: str | None = None) -> dict | None:
    from .config import sanitize
    from .reader import loads  # noqa: PLC0415

    if isinstance(value, str):
        value = loads(value)
    if isinstance(value, list):  # lista 1:1 tratada como objecto
        if not value:
            return None
        if len(value) > 1:
            raise ValueError(f"esperado 1 objecto para struct, recebidos {len(value)}")
        value = value[0]
    if not isinstance(value, dict):
        raise ValueError(f"esperado objecto para struct, recebido {type(value).__name__}")
    by_name: dict[str, Any] = {}
    for k, v in value.items():
        n = sanitize(k)
        if n in by_name:
            raise ValueError(f"os campos {k!r} e {by_name[n][0]!r} dão o mesmo nome {n!r} no struct")
        by_name[n] = (k, v)
    return {n: _cast(value[n] if n in value else by_name.get(n, (None, None))[1], ft, rounding) for n, ft in t[1]}
