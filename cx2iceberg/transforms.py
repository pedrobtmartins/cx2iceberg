"""Transformações aplicáveis a valores de colunas.

Cada transformação recebe (valor, argumento) e devolve o novo valor.
Na configuração usam-se assim:

    transform: upper                      # sem argumento
    transform: {regex_extract: '^urn:uuid:(.*)$'}
    transform: [trim, lower, {map: {a: 1, b: 2}}]   # cadeia

Para registar transformações próprias:

    from cx2iceberg.transforms import register
    @register("minha")
    def minha(value, arg): ...

ou na configuração: `plugins: [meu_modulo]` (o módulo é importado e pode chamar register).
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid as _uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable

REGISTRY: dict[str, Callable[[Any, Any], Any]] = {}


def register(name: str):
    def deco(fn):
        REGISTRY[name] = fn
        return fn

    return deco


def apply_chain(value: Any, chain: Any) -> Any:
    if chain is None:
        return value
    if not isinstance(chain, list):
        chain = [chain]
    for t in chain:
        if isinstance(t, str):
            name, arg = t, None
        elif isinstance(t, dict) and len(t) == 1:
            name, arg = next(iter(t.items()))
        else:
            raise ValueError(f"Transformação inválida: {t!r}")
        fn = REGISTRY.get(name)
        if fn is None:
            raise ValueError(f"Transformação desconhecida: '{name}'. Disponíveis: {sorted(REGISTRY)}")
        value = fn(value, arg)
    return value


def _scalar(fn):
    """Aplica a função elemento a elemento se o valor for lista; ignora None."""

    def wrapper(value, arg):
        if value is None:
            return None
        if isinstance(value, list):
            return [None if v is None else fn(v, arg) for v in value]
        return fn(value, arg)

    wrapper.__doc__ = fn.__doc__
    return wrapper


# --- texto ---------------------------------------------------------------
register("trim")(_scalar(lambda v, a: str(v).strip()))
register("upper")(_scalar(lambda v, a: str(v).upper()))
register("lower")(_scalar(lambda v, a: str(v).lower()))
register("prefix")(_scalar(lambda v, a: f"{a}{v}"))
register("suffix")(_scalar(lambda v, a: f"{v}{a}"))
register("to_string")(_scalar(lambda v, a: v if isinstance(v, str) else json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)))
register("length")(lambda v, a: None if v is None else len(v))


@register("snake_case")
@_scalar
def _snake(v, a):
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(v))
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


@register("replace")
@_scalar
def _replace(v, a):
    old, new = a
    return str(v).replace(old, new)


@register("regex_replace")
@_scalar
def _regex_replace(v, a):
    pattern, repl = a
    return re.sub(pattern, repl, str(v))


@register("regex_extract")
@_scalar
def _regex_extract(v, a):
    """Devolve o grupo 1 (ou o match inteiro se não houver grupos)."""
    m = re.search(a, str(v))
    if not m:
        return None
    return m.group(1) if m.groups() else m.group(0)


@register("substring")
@_scalar
def _substring(v, a):
    start, end = (a + [None])[:2] if isinstance(a, list) else (a, None)
    return str(v)[start:end]


@register("split")
def _split(v, a):
    if v is None:
        return None
    return [p.strip() for p in str(v).split(a or ",")]


@register("join")
def _join(v, a):
    if v is None:
        return None
    if not isinstance(v, list):
        return str(v)
    return (a if a is not None else ",").join("" if x is None else str(x) for x in v)


@register("urn_suffix")
@_scalar
def _urn_suffix(v, a):
    """'urn:uuid:1234' -> '1234';  'urn:samm:io.catenax.x:1.0.0#X' -> 'X'."""
    s = str(v)
    if "#" in s:
        return s.rsplit("#", 1)[1]
    return s.rsplit(":", 1)[-1]


@register("semantic_id_parts")
def _semantic_id_parts(v, a):
    """'urn:samm:io.catenax.serial_part:3.0.0#SerialPart' -> namespace/version/name.

    Com arg 'namespace' | 'version' | 'name' devolve só essa parte.
    """
    if v is None:
        return None
    m = re.match(r"urn:(?:samm|bamm):([^:]+):([^#]+)#(.+)", str(v))
    if not m:
        return None
    parts = {"namespace": m.group(1), "version": m.group(2), "name": m.group(3)}
    return parts[a] if a else parts


# --- valores / nulos -----------------------------------------------------
@register("default")
def _default(v, a):
    return a if v is None or v == "" or v == [] else v


@register("null_if")
def _null_if(v, a):
    vals = a if isinstance(a, list) else [a]
    return None if v in vals else v


@register("map")
def _map(v, a):
    """Tradução de valores. '_default': valor por omissão (senão mantém o original);
    '_ignore_case: true' compara sem distinguir maiúsculas."""
    ci = bool(a.get("_ignore_case"))
    table = {str(k).lower(): v for k, v in a.items()} if ci else a

    def one(x):
        key = str(x).lower() if ci else (x if x in a else str(x))
        if key in table and not str(key).startswith("_"):
            return table[key]
        return a.get("_default", x)

    if v is None:
        return a.get("_null")
    return [one(x) for x in v] if isinstance(v, list) else one(v)


@register("const")
def _const(v, a):
    return a


# --- números -------------------------------------------------------------
def _num(v):
    if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
        return v
    return Decimal(str(v).strip().replace(",", ".")) if "." in str(v) or "," in str(v) else int(str(v).strip())


register("to_number")(_scalar(lambda v, a: _num(v)))
register("multiply")(_scalar(lambda v, a: float(_num(v)) * float(a) + 0.0))  # + 0.0 evita -0.0
register("divide")(_scalar(lambda v, a: float(_num(v)) / float(a)))
register("add")(_scalar(lambda v, a: float(_num(v)) + float(a)))
register("round")(_scalar(lambda v, a: round(float(_num(v)), a or 0)))
register("abs")(_scalar(lambda v, a: abs(_num(v))))


@register("clamp")
@_scalar
def _clamp(v, a):
    """Limita o valor: {min: 0} (negativos -> 0), {max: 0} (positivos -> 0), ou ambos."""
    x = float(_num(v))
    if a.get("min") is not None:
        x = max(x, float(a["min"]))
    if a.get("max") is not None:
        x = min(x, float(a["max"]))
    return x


# Factores para a unidade base (SI) — nomes como no vocabulário de unidades Catena-X / SAMM
_UNITS: dict[str, tuple[str, float]] = {
    # massa (base kg)
    "kilogram": ("mass", 1.0), "gram": ("mass", 1e-3), "milligram": ("mass", 1e-6),
    "tonneMetricTon": ("mass", 1e3), "tonne": ("mass", 1e3), "pound": ("mass", 0.45359237),
    # comprimento (base m)
    "metre": ("length", 1.0), "meter": ("length", 1.0), "centimetre": ("length", 1e-2),
    "millimetre": ("length", 1e-3), "kilometre": ("length", 1e3), "inch": ("length", 0.0254),
    # volume (base m3)
    "cubicMetre": ("volume", 1.0), "litre": ("volume", 1e-3), "millilitre": ("volume", 1e-6),
    "cubicCentimetre": ("volume", 1e-6),
    # área (base m2)
    "squareMetre": ("area", 1.0), "squareCentimetre": ("area", 1e-4), "squareMillimetre": ("area", 1e-6),
    # tempo (base s)
    "secondUnitOfTime": ("time", 1.0), "second": ("time", 1.0), "minuteUnitOfTime": ("time", 60.0),
    "hourUnitOfTime": ("time", 3600.0), "day": ("time", 86400.0),
    # energia (base J)
    "joule": ("energy", 1.0), "kilojoule": ("energy", 1e3), "megajoule": ("energy", 1e6),
    "kilowattHour": ("energy", 3.6e6), "wattHour": ("energy", 3600.0),
    # unidades
    "piece": ("count", 1.0), "unit:piece": ("count", 1.0),
}


def _unit_name(u: str) -> str:
    return str(u).split(":", 1)[1] if str(u).startswith("unit:") else str(u)


@register("unit_convert")
def _unit_convert(v, a):
    """Converte quantidades Catena-X.

    O valor pode ser um número (usar {from: gram, to: kilogram}) ou um objecto
    {value: 12, unit: 'unit:gram'} / {value: 12, unit: 'unit:gram'} (usar {to: kilogram}).
    """
    if v is None:
        return None
    src = a.get("from")
    if isinstance(v, dict):
        src = v.get("unit") or v.get("measurementUnit") or src
        v = v.get("value", v.get("quantityNumber"))
    if v is None or src is None:
        return None
    s, t = _UNITS.get(_unit_name(src)), _UNITS.get(_unit_name(a["to"]))
    if not s or not t or s[0] != t[0]:
        raise ValueError(f"Conversão impossível de {src} para {a['to']}")
    return float(_num(v)) * s[1] / t[1]


# --- datas ---------------------------------------------------------------
def _parse_dt(v, fmt=None) -> datetime:
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, (int, float)):
        dt = datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc)
    elif fmt:
        dt = datetime.strptime(str(v), fmt)
    else:
        s = str(v).strip().replace("Z", "+00:00")
        if len(s) == 10:
            s += "T00:00:00"
        dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


register("to_timestamp")(_scalar(lambda v, a: _parse_dt(v, a)))
register("to_date")(_scalar(lambda v, a: _parse_dt(v, a).date()))
register("date_format")(_scalar(lambda v, a: _parse_dt(v).strftime(a)))
register("year")(_scalar(lambda v, a: _parse_dt(v).year))
register("month")(_scalar(lambda v, a: _parse_dt(v).month))


# --- listas --------------------------------------------------------------
def _list(v):
    return [] if v is None else v if isinstance(v, list) else [v]


register("first")(lambda v, a: _list(v)[0] if _list(v) else None)
register("last")(lambda v, a: _list(v)[-1] if _list(v) else None)
register("count")(lambda v, a: len(_list(v)))
register("distinct")(lambda v, a: list(dict.fromkeys(json.dumps(x, sort_keys=True, default=str) if isinstance(x, (dict, list)) else x for x in _list(v))))
register("sort")(lambda v, a: sorted(_list(v), reverse=bool(a)))
register("compact")(lambda v, a: [x for x in _list(v) if x not in (None, "")])
register("sum")(lambda v, a: sum(float(_num(x)) for x in _list(v) if x is not None) if _list(v) else None)
register("min")(lambda v, a: min((x for x in _list(v) if x is not None), default=None))
register("max")(lambda v, a: max((x for x in _list(v) if x is not None), default=None))
register("pluck")(lambda v, a: [x.get(a) if isinstance(x, dict) else None for x in _list(v)])


# --- JSON / estruturas ---------------------------------------------------
register("to_json")(lambda v, a: None if v is None else json.dumps(v, ensure_ascii=False, sort_keys=bool(a), default=str))
register("from_json")(lambda v, a: None if v is None else json.loads(v) if isinstance(v, str) else v)
register("keys")(lambda v, a: list(v.keys()) if isinstance(v, dict) else None)
register("get")(lambda v, a: v.get(a) if isinstance(v, dict) else None)


# --- booleanos / hash / ids ---------------------------------------------
@register("to_bool")
@_scalar
def _bool(v, a):
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "sim", "s"):
        return True
    if s in ("false", "0", "no", "n", "nao", "não"):
        return False
    return None


@register("hash")
def _hash(v, a):
    if v is None:
        return None
    data = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return hashlib.new(a or "sha256", data.encode()).hexdigest()


@register("uuid5")
def _uuid5(v, a):
    """UUID determinístico a partir do valor (bom para chaves sintéticas)."""
    if v is None:
        return None
    data = v if isinstance(v, str) else json.dumps(v, sort_keys=True, default=str)
    return str(_uuid.uuid5(_uuid.NAMESPACE_URL, data))


register("uuid4")(lambda v, a: str(_uuid.uuid4()))
register("now")(lambda v, a: datetime.now(timezone.utc).replace(tzinfo=None))
register("today")(lambda v, a: date.today())
