"""Text and JSON from values, always the same way (string/json columns, to_string, join, to_json, templates).

Before, each place did it its own way: `to_string(True)` gave 'True' and the cast to string gave 'true';
`join` of an object gave Python's repr (`{'k': 1}`); `to_json` of a Decimal gave the text `"1.5"` (in quotes)
and of a date `"2022-01-01 10:00:00"` (without the `T`).

    text:   boolean -> true/false;  a document number -> as it was written;  dates -> ISO 8601;  object/list -> JSON
    JSON:   Decimal and numbers with more than 15 digits only if the double reproduces them (otherwise an error: there
            is no JSON number without losing digits; use type: string or decimal); NaN/inf -> error
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from .numbers import JsonFloat, TextFloat


class ValueTextError(ValueError):
    pass


def text_of(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, JsonFloat):
        return v.text
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, (int, Decimal)):
        return str(v)
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, (dict, list, tuple)):
        return json_text(v)
    raise ValueTextError(f"{type(v).__name__} has no text representation")


def jsonable(v: Any) -> Any:
    """Structure with JSON types only, without losing digits (or an error)."""
    if v is None or isinstance(v, (bool, str, int)) and not isinstance(v, TextFloat):
        return v
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueTextError(f"{v!r} is not a JSON number")
        if isinstance(v, TextFloat) and Decimal(repr(float(v))) != Decimal(v.text):
            raise ValueTextError(f"{v.text} has too many digits for a JSON number (it would become {float(v)!r}); "
                                 f"use type: string or decimal")
        return float(v)
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueTextError(f"{v} is not a JSON number")
        f = float(v)
        if Decimal(repr(f)) != v:
            raise ValueTextError(f"{v} has too many digits for a JSON number (it would become {f!r}); use type: string or decimal")
        return int(v) if v == v.to_integral_value() and v.as_tuple().exponent >= 0 else f
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            key = k if isinstance(k, str) else text_of(k)
            if key in out:
                raise ValueTextError(f"two keys give the same text {key!r} in JSON")
            out[key] = jsonable(x)
        return out
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    raise ValueTextError(f"{type(v).__name__} has no JSON representation")


def json_text(v: Any, sort_keys: bool = False) -> str:
    return json.dumps(jsonable(v), ensure_ascii=False, sort_keys=sort_keys, allow_nan=False)
