"""Condições (`where`, `cases.when`, `check.required_if`, `match.where`): validadas na carga e comparadas com tipo.

Antes comparava-se texto com texto: `{op: gt, value: "9"}` deixava 10 de fora ('10' < '9'), `gte 0` aceitava
'n/a' e true, `eq 7` aceitava o texto '7', e datas com fusos diferentes eram ordenadas como texto.

    números    comparam como números (1 e 1.0 iguais); booleanos não são números
    datas      texto no formato do schema compara como instante (10:00+02:00 é antes de 09:30Z); se um dos lados
               for uma data sem hora, compara a data tal como foi escrita (ex.: created >= "2025-01-01")
    texto      igualdade exacta; ordem (gt/lt...) só entre textos que não sejam números
    tipos diferentes (número e texto, booleano e texto...) -> ConditionTypeError (com o on_error de quem avalia)
"""
from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Any

from . import dates
from .numbers import AMBIGUOUS, JSON_NUMBER, to_decimal
from .paths import MISSING, Context, PathError, Projection, resolve

OPS = {"eq", "ne", "in", "not_in", "exists", "missing", "regex", "gt", "gte", "lt", "lte", "contains", "empty", "not_empty"}
NO_VALUE_OPS = {"exists", "missing", "empty", "not_empty"}


class ConditionTypeError(PathError):
    """A condição compara tipos diferentes."""

    reason = "condition_type"


def _kind(x: Any) -> str:
    if isinstance(x, bool):
        return "booleano"
    if isinstance(x, (int, float, Decimal)):
        return "número"
    if isinstance(x, str):
        return "texto"
    if isinstance(x, list):
        return "lista"
    if isinstance(x, dict):
        return "objecto"
    if isinstance(x, datetime):
        return "data/hora"
    return type(x).__name__


# --- validação na carga ------------------------------------------------------------
def validate(cond: Any, where: str) -> None:
    """Estrutura, operador e tipo do valor; um regex inválido. Lança ValueError (a configuração converte)."""
    if cond is None:
        return
    if isinstance(cond, list):
        for i, c in enumerate(cond):
            validate(c, f"{where}[{i}]")
        return
    if not isinstance(cond, dict):
        raise ValueError(f"{where}: condição inválida {cond!r}")
    for key in ("all", "any"):
        if key in cond:
            if set(cond) != {key} or not isinstance(cond[key], list):
                raise ValueError(f"{where}: use {{{key}: [condição, ...]}} sozinho")
            for i, c in enumerate(cond[key]):
                validate(c, f"{where}.{key}[{i}]")
            return
    if "not" in cond:
        if set(cond) != {"not"}:
            raise ValueError(f"{where}: use {{not: condição}} sozinho")
        validate(cond["not"], f"{where}.not")
        return
    unknown = set(cond) - {"path", "op", "value"}
    if unknown:
        raise ValueError(f"{where}: chave(s) desconhecida(s) {sorted(unknown)} (use path, op, value)")
    if not isinstance(cond.get("path"), str):
        raise ValueError(f"{where}: falta 'path'")
    op = cond.get("op", "eq" if "value" in cond else "exists")
    if op not in OPS:
        raise ValueError(f"{where}: operador '{op}' desconhecido; use um de {sorted(OPS)}")
    value = cond.get("value")
    if op in NO_VALUE_OPS:
        if "value" in cond:
            raise ValueError(f"{where}: '{op}' não leva value")
    elif op in ("in", "not_in"):
        if not isinstance(value, list) or not value or any(isinstance(x, (list, dict)) for x in value):
            raise ValueError(f"{where}: '{op}' precisa de uma lista de valores simples")
    elif op == "regex":
        if not isinstance(value, str):
            raise ValueError(f"{where}: regex precisa de um texto")
        try:
            re.compile(value)
        except re.error as e:
            raise ValueError(f"{where}: regex inválido: {e}") from None
    elif op in ("gt", "gte", "lt", "lte"):
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ValueError(f"{where}: '{op}' precisa de um número ou de um texto (ex.: data)")
    elif "value" not in cond or isinstance(value, (list, dict)):
        raise ValueError(f"{where}: '{op}' precisa de um value simples (use null para nulo)")


# --- avaliação ----------------------------------------------------------------------
def _date_like(s: str):
    """Texto no formato do schema -> (datetime, é_data_sem_hora, fim_do_dia); None se não for data."""
    try:
        dt, origin, end = dates.parse_text(s)
    except ValueError:
        return None
    return dt, origin == "date", end


def _looks_number(s: str) -> bool:
    t = s.strip()
    return bool(JSON_NUMBER.fullmatch(t) or AMBIGUOUS.fullmatch(t))


def _same(a: Any, b: Any, text: str) -> bool:
    ka, kb = _kind(a), _kind(b)
    if ka != kb:
        raise ConditionTypeError(f"{text}: compara {ka} {a!r} com {kb} {b!r}")
    if ka == "número":
        return to_decimal(a) == to_decimal(b)
    if ka == "texto":
        da, db = _date_like(a), _date_like(b)
        if da and db:
            return _order_dates(da, db, a, b, text) == 0
    return a == b


def _order_dates(da, db, a, b, text) -> int:
    (x, x_date, x_end), (y, y_date, y_end) = da, db
    if x_date or y_date:                      # uma data sem hora: compara-se a data tal como foi escrita
        if x_end or y_end:
            raise ConditionTypeError(f"{text}: 24:00:00 comparado com uma data é ambíguo ({a!r}, {b!r})")
        x, y = x.date(), y.date()
    elif (x.tzinfo is None) != (y.tzinfo is None):
        raise ConditionTypeError(f"{text}: compara uma data/hora com fuso e outra sem fuso ({a!r}, {b!r})")
    return (x > y) - (x < y)


def _order(a: Any, b: Any, text: str) -> int:
    ka, kb = _kind(a), _kind(b)
    if ka != kb or ka not in ("número", "texto"):
        raise ConditionTypeError(f"{text}: não se ordena {ka} {a!r} com {kb} {b!r}")
    if ka == "número":
        x, y = to_decimal(a), to_decimal(b)
        return (x > y) - (x < y)
    da, db = _date_like(a), _date_like(b)
    if da and db:
        return _order_dates(da, db, a, b, text)
    if _looks_number(a) or _looks_number(b):
        raise ConditionTypeError(f"{text}: número em texto ({a!r} / {b!r}) ordenado como texto daria '9' > '10'; "
                                 f"use to_number ou um valor numérico")
    return (a > b) - (a < b)


def evaluate(cond: Any, ctx: Context) -> bool:
    if cond is None:
        return True
    if isinstance(cond, list):
        return all(evaluate(c, ctx) for c in cond)
    if "all" in cond:
        return all(evaluate(c, ctx) for c in cond["all"])
    if "any" in cond:
        return any(evaluate(c, ctx) for c in cond["any"])
    if "not" in cond:
        return not evaluate(cond["not"], ctx)
    op = cond.get("op", "eq" if "value" in cond else "exists")
    v = resolve(ctx, cond["path"])
    if isinstance(v, Projection):
        v = list(v)
    exp = cond.get("value")
    text = f"condição {cond['path']} {op} {exp!r}" if op not in NO_VALUE_OPS else f"condição {cond['path']} {op}"
    if op == "exists":
        return v is not MISSING and v is not None
    if op == "missing":
        return v is MISSING or v is None
    if op == "empty":
        return v is MISSING or v is None or v in ("", [], {})
    if op == "not_empty":
        return not (v is MISSING or v is None or v in ("", [], {}))
    if v is MISSING or v is None:
        return op in ("ne", "not_in") if exp is not None else op == "eq"
    if exp is None:                           # eq/ne null com um valor presente
        return op == "ne"
    if op in ("eq", "ne"):
        return _same(v, exp, text) == (op == "eq")
    if op in ("in", "not_in"):
        candidates = [e for e in exp if _kind(e) == _kind(v)]
        if not candidates:
            raise ConditionTypeError(f"{text}: {_kind(v)} {v!r} comparado com uma lista de {_kind(exp[0])}")
        return any(_same(v, e, text) for e in candidates) == (op == "in")
    if op == "regex":
        if not isinstance(v, str):
            raise ConditionTypeError(f"{text}: regex sobre {_kind(v)} {v!r} (só se aplica a texto)")
        return re.search(exp, v) is not None
    if op == "contains":
        if isinstance(v, list):
            same = [x for x in v if x is not None and _kind(x) == _kind(exp)]
            if v and not same:
                raise ConditionTypeError(f"{text}: a lista não tem valores do tipo {_kind(exp)}")
            return any(_same(x, exp, text) for x in same)
        if isinstance(v, str):
            if not isinstance(exp, str):
                raise ConditionTypeError(f"{text}: procura {_kind(exp)} dentro de texto")
            return exp in v
        raise ConditionTypeError(f"{text}: contains sobre {_kind(v)} (use uma lista ou texto)")
    c = _order(v, exp, text)
    return {"gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}[op]
