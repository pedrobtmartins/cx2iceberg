"""Texto e JSON a partir de valores, sempre da mesma maneira (colunas string/json, to_string, join, to_json, templates).

Antes cada sítio fazia à sua maneira: `to_string(True)` dava 'True' e o cast para string dava 'true';
`join` de um objecto dava o repr do Python (`{'k': 1}`); `to_json` de um Decimal dava o texto `"1.5"` (com aspas)
e de uma data `"2022-01-01 10:00:00"` (sem o `T`).

    texto:  booleano -> true/false;  número do documento -> como foi escrito;  datas -> ISO 8601;  objecto/lista -> JSON
    JSON:   Decimal e números com mais de 15 algarismos só se o double os reproduzir (senão erro: não há número JSON
            sem perder algarismos; use type: string ou decimal); NaN/inf -> erro
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
    raise ValueTextError(f"{type(v).__name__} não tem representação em texto")


def jsonable(v: Any) -> Any:
    """Estrutura só com tipos JSON, sem perder algarismos (ou erro)."""
    if v is None or isinstance(v, (bool, str, int)) and not isinstance(v, TextFloat):
        return v
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueTextError(f"{v!r} não é um número JSON")
        if isinstance(v, TextFloat) and Decimal(repr(float(v))) != Decimal(v.text):
            raise ValueTextError(f"{v.text} tem algarismos a mais para um número JSON (ficaria {float(v)!r}); "
                                 f"use type: string ou decimal")
        return float(v)
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueTextError(f"{v} não é um número JSON")
        f = float(v)
        if Decimal(repr(f)) != v:
            raise ValueTextError(f"{v} tem algarismos a mais para um número JSON (ficaria {f!r}); use type: string ou decimal")
        return int(v) if v == v.to_integral_value() and v.as_tuple().exponent >= 0 else f
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            key = k if isinstance(k, str) else text_of(k)
            if key in out:
                raise ValueTextError(f"duas chaves dão o mesmo texto {key!r} em JSON")
            out[key] = jsonable(x)
        return out
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    raise ValueTextError(f"{type(v).__name__} não tem representação em JSON")


def json_text(v: Any, sort_keys: bool = False) -> str:
    return json.dumps(jsonable(v), ensure_ascii=False, sort_keys=sort_keys, allow_nan=False)
