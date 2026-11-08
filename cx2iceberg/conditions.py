"""Conditions (`where`, `cases.when`, `check.required_if`, `match.where`): validated at load time and compared with their type.

Before, text was compared with text: `{op: gt, value: "9"}` left 10 out ('10' < '9'), `gte 0` accepted
'n/a' and true, `eq 7` accepted the text '7', and dates with different time zones were sorted as text.

    numbers    compare as numbers (1 and 1.0 are equal); booleans are not numbers
    dates      text in the schema's format compares as an instant (10:00+02:00 is before 09:30Z); if one side
               is a date without a time, the date is compared as it was written (e.g. created >= "2025-01-01")
    text       exact equality; order (gt/lt...) only between texts that are not numbers
    different types (number and text, boolean and text...) -> ConditionTypeError (with the evaluator's on_error)
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
    """The condition compares different types."""

    reason = "condition_type"


def _kind(x: Any) -> str:
    if isinstance(x, bool):
        return "boolean"
    if isinstance(x, (int, float, Decimal)):
        return "number"
    if isinstance(x, str):
        return "text"
    if isinstance(x, list):
        return "list"
    if isinstance(x, dict):
        return "object"
    if isinstance(x, datetime):
        return "date/time"
    return type(x).__name__


# --- validation at load time ------------------------------------------------------------
def validate(cond: Any, where: str) -> None:
    """Structure, operator and type of the value; an invalid regex. Raises ValueError (the configuration converts it)."""
    if cond is None:
        return
    if isinstance(cond, list):
        for i, c in enumerate(cond):
            validate(c, f"{where}[{i}]")
        return
    if not isinstance(cond, dict):
        raise ValueError(f"{where}: invalid condition {cond!r}")
    for key in ("all", "any"):
        if key in cond:
            if set(cond) != {key} or not isinstance(cond[key], list):
                raise ValueError(f"{where}: use {{{key}: [condition, ...]}} on its own")
            for i, c in enumerate(cond[key]):
                validate(c, f"{where}.{key}[{i}]")
            return
    if "not" in cond:
        if set(cond) != {"not"}:
            raise ValueError(f"{where}: use {{not: condition}} on its own")
        validate(cond["not"], f"{where}.not")
        return
    unknown = set(cond) - {"path", "op", "value"}
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {sorted(unknown)} (use path, op, value)")
    if not isinstance(cond.get("path"), str):
        raise ValueError(f"{where}: 'path' is missing")
    op = cond.get("op", "eq" if "value" in cond else "exists")
    if op not in OPS:
        raise ValueError(f"{where}: unknown operator '{op}'; use one of {sorted(OPS)}")
    value = cond.get("value")
    if op in NO_VALUE_OPS:
        if "value" in cond:
            raise ValueError(f"{where}: '{op}' takes no value")
    elif op in ("in", "not_in"):
        if not isinstance(value, list) or not value or any(isinstance(x, (list, dict)) for x in value):
            raise ValueError(f"{where}: '{op}' needs a list of simple values")
    elif op == "regex":
        if not isinstance(value, str):
            raise ValueError(f"{where}: regex needs a text")
        try:
            re.compile(value)
        except re.error as e:
            raise ValueError(f"{where}: invalid regex: {e}") from None
    elif op in ("gt", "gte", "lt", "lte"):
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ValueError(f"{where}: '{op}' needs a number or a text (e.g. a date)")
    elif "value" not in cond or isinstance(value, (list, dict)):
        raise ValueError(f"{where}: '{op}' needs a simple value (use null for null)")


# --- evaluation ----------------------------------------------------------------------
def _date_like(s: str):
    """Text in the schema's format -> (datetime, is_date_without_time, end_of_day); None if it is not a date."""
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
        raise ConditionTypeError(f"{text}: compares {ka} {a!r} with {kb} {b!r}")
    if ka == "number":
        return to_decimal(a) == to_decimal(b)
    if ka == "text":
        da, db = _date_like(a), _date_like(b)
        if da and db:
            return _order_dates(da, db, a, b, text) == 0
    return a == b


def _order_dates(da, db, a, b, text) -> int:
    (x, x_date, x_end), (y, y_date, y_end) = da, db
    if x_date or y_date:                      # a date without a time: the date is compared as it was written
        if x_end or y_end:
            raise ConditionTypeError(f"{text}: 24:00:00 compared with a date is ambiguous ({a!r}, {b!r})")
        x, y = x.date(), y.date()
    elif (x.tzinfo is None) != (y.tzinfo is None):
        raise ConditionTypeError(f"{text}: compares a date/time with a time zone and another without ({a!r}, {b!r})")
    return (x > y) - (x < y)


def _order(a: Any, b: Any, text: str) -> int:
    ka, kb = _kind(a), _kind(b)
    if ka != kb or ka not in ("number", "text"):
        raise ConditionTypeError(f"{text}: cannot order {ka} {a!r} with {kb} {b!r}")
    if ka == "number":
        x, y = to_decimal(a), to_decimal(b)
        return (x > y) - (x < y)
    da, db = _date_like(a), _date_like(b)
    if da and db:
        return _order_dates(da, db, a, b, text)
    if _looks_number(a) or _looks_number(b):
        raise ConditionTypeError(f"{text}: number as text ({a!r} / {b!r}) sorted as text would give '9' > '10'; "
                                 f"use to_number or a numeric value")
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
    text = f"condition {cond['path']} {op} {exp!r}" if op not in NO_VALUE_OPS else f"condition {cond['path']} {op}"
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
    if exp is None:                           # eq/ne null with a value present
        return op == "ne"
    if op in ("eq", "ne"):
        return _same(v, exp, text) == (op == "eq")
    if op in ("in", "not_in"):
        candidates = [e for e in exp if _kind(e) == _kind(v)]
        if not candidates:
            raise ConditionTypeError(f"{text}: {_kind(v)} {v!r} compared with a list of {_kind(exp[0])}")
        return any(_same(v, e, text) for e in candidates) == (op == "in")
    if op == "regex":
        if not isinstance(v, str):
            raise ConditionTypeError(f"{text}: regex on {_kind(v)} {v!r} (only applies to text)")
        return re.search(exp, v) is not None
    if op == "contains":
        if isinstance(v, list):
            same = [x for x in v if x is not None and _kind(x) == _kind(exp)]
            if v and not same:
                raise ConditionTypeError(f"{text}: the list has no values of type {_kind(exp)}")
            return any(_same(x, exp, text) for x in same)
        if isinstance(v, str):
            if not isinstance(exp, str):
                raise ConditionTypeError(f"{text}: looks for {_kind(exp)} inside text")
            return exp in v
        raise ConditionTypeError(f"{text}: contains on {_kind(v)} (use a list or text)")
    c = _order(v, exp, text)
    return {"gt": c > 0, "gte": c >= 0, "lt": c < 0, "lte": c <= 0}[op]
