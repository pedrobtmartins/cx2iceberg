"""Transformações aplicáveis a valores de colunas.

Cada transformação recebe (valor, argumento) e devolve o novo valor.
Na configuração usam-se assim:

    transform: upper                      # sem argumento
    transform: {regex_extract: '^urn:uuid:(.*)$'}
    transform: [trim, lower, {map: {a: 1, b: 2}}]   # cadeia

Os nomes e os argumentos são validados ao carregar a configuração (ARG_CHECKS). Nenhuma transformação muda ou
perde um valor sem o dizer: o que não é possível dá erro (com o on_error da coluna); o que foi declarado
(ex.: `sum: {nulls: skip}`) fica contado no relatório.

    números   contas em Decimal sobre o valor escrito (0.1 * 3 = 0.3, não 0.30000000000000004); texto só com
              parse_number; booleanos não são números
    listas    sum/min/max falham com nulos (resultado parcial), a não ser com {nulls: skip}; min/max/sort comparam
              números como números, datas ISO como instantes, e recusam números em texto e tipos misturados
    datas     ver dates.py (instantes em UTC; date/year/month/date_format no fuso do documento)

Para registar transformações próprias:

    from cx2iceberg.transforms import register
    @register("minha")
    def minha(value, arg): ...

ou na configuração: `plugins: [meu_modulo]` (o módulo é importado e pode chamar register).
"""
from __future__ import annotations

import hashlib
import re
import uuid as _uuid
from datetime import datetime, timezone
from decimal import Context, Decimal, DivisionByZero, InvalidOperation
from typing import Any, Callable

from . import dates
from .notes import note
from .numbers import AMBIGUOUS, JSON_NUMBER, MODES, NumberError, check_format, computed, parse_number, short, to_decimal
from .values import json_text, text_of

REGISTRY: dict[str, Callable[[Any, Any], Any]] = {}
ARG_CHECKS: dict[str, Callable[[Any], Any]] = {}   # validação dos argumentos ao carregar a configuração
_CTX = Context(prec=60)                             # contas exactas (a precisão por omissão do Decimal é 28)


class TransformValueError(ValueError):
    pass


def register(name: str):
    def deco(fn):
        REGISTRY[name] = fn
        return fn

    return deco


def _steps(chain: Any) -> list[tuple[str, Any]]:
    if chain is None:
        return []
    out = []
    for t in chain if isinstance(chain, list) else [chain]:
        if isinstance(t, str):
            out.append((t, None))
        elif isinstance(t, dict) and len(t) == 1:
            out.append(next(iter(t.items())))
        else:
            raise ValueError(f"Transformação inválida: {t!r}")
    return out


def check_chain(chain: Any) -> None:
    """Valida nomes e argumentos ao carregar a configuração. Sem isto, um nome mal escrito só falhava nos dados
    e, com on_error: null, anulava todos os valores da coluna."""
    for name, arg in _steps(chain):
        if name not in REGISTRY:
            raise ValueError(f"Transformação desconhecida: '{name}'. Disponíveis: {sorted(REGISTRY)}")
        if name in ARG_CHECKS:
            try:
                ARG_CHECKS[name](arg)
            except (ValueError, TypeError, re.error) as e:
                raise ValueError(f"{name}: {e}") from None


def apply_chain(value: Any, chain: Any) -> Any:
    for name, arg in _steps(chain):
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


# --- validação de argumentos ------------------------------------------------
def _no_arg(name):
    def check(a):
        if a is not None:
            raise ValueError(f"não leva argumento (recebeu {a!r})")
    ARG_CHECKS[name] = check


def _need_str(a):
    if not isinstance(a, str):
        raise ValueError(f"precisa de um texto como argumento (recebeu {a!r})")


def _opt_str(a):
    if a is not None:
        _need_str(a)


def _number_arg(a) -> Decimal:
    if isinstance(a, bool) or not isinstance(a, (int, float)):
        raise ValueError(f"precisa de um número como argumento (recebeu {a!r})")
    return to_decimal(a)


def _regex(pattern):
    _need_str(pattern)
    return re.compile(pattern)


# --- texto ---------------------------------------------------------------
def _txt(v) -> str:
    return text_of(v)


register("trim")(_scalar(lambda v, a: _txt(v).strip()))
register("upper")(_scalar(lambda v, a: _txt(v).upper()))
register("lower")(_scalar(lambda v, a: _txt(v).lower()))
def _affix(a) -> str:
    _need_str(a)            # sem argumento dava 'None' + valor
    return a


register("prefix")(_scalar(lambda v, a: f"{_affix(a)}{_txt(v)}"))
register("suffix")(_scalar(lambda v, a: f"{_txt(v)}{_affix(a)}"))
register("to_string")(_scalar(lambda v, a: _txt(v)))
ARG_CHECKS["prefix"] = ARG_CHECKS["suffix"] = _need_str
for _n in ("trim", "upper", "lower", "to_string", "snake_case", "urn_suffix"):
    _no_arg(_n)


@register("length")
def _length(v, a):
    if v is None:
        return None
    if not isinstance(v, (str, list, dict)):
        raise TransformValueError(f"length: {short(v)} não é texto nem lista")
    return len(v)


_no_arg("length")


@register("snake_case")
@_scalar
def _snake(v, a):
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", _txt(v))
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


@register("replace")
@_scalar
def _replace(v, a):
    old, new = a
    return _txt(v).replace(old, new)


def _check_pair(a, compile_first=False):
    if not (isinstance(a, list) and len(a) == 2 and all(isinstance(x, str) for x in a)):
        raise ValueError(f"precisa de [antigo, novo] (dois textos), recebeu {a!r}")
    if compile_first:
        re.compile(a[0])


ARG_CHECKS["replace"] = _check_pair
ARG_CHECKS["regex_replace"] = lambda a: _check_pair(a, True)


@register("regex_replace")
@_scalar
def _regex_replace(v, a):
    pattern, repl = a
    return re.sub(pattern, repl, _txt(v))


def _regex_extract_arg(a):
    if isinstance(a, dict):
        if set(a) - {"pattern", "no_match"} or "pattern" not in a:
            raise ValueError("use regex_extract: '<regex>' ou {pattern: '<regex>', no_match: null}")
        if a.get("no_match", "fail") not in ("fail", None, "null"):
            raise ValueError("no_match só aceita null (anula o valor, contado no relatório)")
        return _regex(a["pattern"]), a.get("no_match", "fail") != "fail"
    return _regex(a), False


ARG_CHECKS["regex_extract"] = _regex_extract_arg


@register("regex_extract")
@_scalar
def _regex_extract(v, a):
    """Devolve o grupo 1 (ou o match inteiro se não houver grupos). Sem match: erro, a não ser com no_match: null."""
    rx, null_ok = _regex_extract_arg(a)
    text = _txt(v)
    m = rx.search(text)
    if not m:
        if not null_ok:
            raise TransformValueError(f"regex_extract: {text!r} não corresponde a /{rx.pattern}/ "
                                      f"(use {{pattern: ..., no_match: null}} para anular)")
        note("declared", "no_match", "nulled", f"{text!r} não corresponde a /{rx.pattern}/")
        return None
    return m.group(1) if m.groups() else m.group(0)


@register("substring")
@_scalar
def _substring(v, a):
    start, end = (list(a) + [None])[:2] if isinstance(a, list) else (a, None)
    return _txt(v)[start:end]


def _substring_arg(a):
    parts = a if isinstance(a, list) else [a]
    if not 1 <= len(parts) <= 2 or not all(x is None or (isinstance(x, int) and not isinstance(x, bool)) for x in parts):
        raise ValueError(f"use substring: início ou [início, fim] (inteiros), recebeu {a!r}")


ARG_CHECKS["substring"] = _substring_arg


@register("split")
def _split(v, a):
    if v is None:
        return None
    return [p.strip() for p in _txt(v).split(a or ",")]


@register("join")
def _join(v, a):
    """Junta com o separador (omissão ','). Um elemento nulo é erro: '' e nulo ficariam iguais."""
    if v is None:
        return None
    if not isinstance(v, list):
        return _txt(v)
    if any(x is None for x in v):
        raise TransformValueError(f"join: a lista tem nulos ({short(v)}); use compact antes (ou missing_elements: skip)")
    return (a if a is not None else ",").join(_txt(x) for x in v)


ARG_CHECKS["split"] = ARG_CHECKS["join"] = _opt_str


@register("urn_suffix")
@_scalar
def _urn_suffix(v, a):
    """'urn:uuid:1234' -> '1234';  'urn:samm:io.catenax.x:1.0.0#X' -> 'X'."""
    s = _txt(v)
    if "#" in s:
        return s.rsplit("#", 1)[1]
    return s.rsplit(":", 1)[-1]


@register("semantic_id_parts")
def _semantic_id_parts(v, a):
    """'urn:samm:io.catenax.serial_part:3.0.0#SerialPart' -> namespace/version/name.

    Com arg 'namespace' | 'version' | 'name' devolve só essa parte. Um texto que não é semantic id dá erro.
    """
    if v is None:
        return None
    m = re.match(r"urn:(?:samm|bamm):([^:]+):([^#]+)#(.+)", _txt(v))
    if not m:
        raise TransformValueError(f"semantic_id_parts: {short(v)} não é um semantic id (urn:samm:<namespace>:<versão>#<nome>)")
    parts = {"namespace": m.group(1), "version": m.group(2), "name": m.group(3)}
    return parts[a] if a else parts


def _semantic_arg(a):
    if a not in (None, "namespace", "version", "name"):
        raise ValueError("use namespace, version ou name")


ARG_CHECKS["semantic_id_parts"] = _semantic_arg


# --- valores / nulos: igualdade com tipos (True não é 1, '1' não é 1) -------------
def typed_key(x: Any, ignore_case: bool = False) -> tuple:
    if x is None:
        return ("null",)
    if isinstance(x, bool):
        return ("bool", x)
    if isinstance(x, (int, float, Decimal)):
        return ("num", to_decimal(x))
    if isinstance(x, str):
        return ("str", x.lower() if ignore_case else x)
    return ("json", json_text(x, sort_keys=True))


@register("default")
def _default(v, a):
    return a if v is None or v == "" or v == [] else v


@register("null_if")
def _null_if(v, a):
    vals = {typed_key(x) for x in (a if isinstance(a, list) else [a])}
    return None if typed_key(v) in vals else v


@register("const")
def _const(v, a):
    return a


_MAP_CACHE: dict[int, tuple[dict, tuple[dict, bool]]] = {}


def _cached_map_table(a) -> tuple[dict, bool]:
    hit = _MAP_CACHE.get(id(a))
    if hit is None or hit[0] is not a:          # guarda o próprio objecto: um id reutilizado não engana a cache
        hit = _MAP_CACHE[id(a)] = (a, _map_table(a))
    return hit[1]


def _map_table(a) -> tuple[dict, bool]:
    if not isinstance(a, dict):
        raise ValueError("map precisa de um objecto {valor: tradução}")
    ci = a.get("_ignore_case", False)
    if not isinstance(ci, bool):
        raise ValueError("_ignore_case tem de ser true ou false")
    table: dict = {}
    for k, v in a.items():
        if isinstance(k, str) and k.startswith("_"):
            if k not in ("_default", "_ignore_case", "_null"):
                raise ValueError(f"chave especial desconhecida {k!r} (use _default, _null, _ignore_case)")
            continue
        if k is None:
            raise ValueError("chave nula: para traduzir nulos use _null")
        key = typed_key(k, ci)
        if key in table:
            raise ValueError(f"a chave {k!r} repete outra chave ({key[1]!r})")
        table[key] = v
    return table, ci


ARG_CHECKS["map"] = _map_table


@register("map")
def _map(v, a):
    """Tradução de valores, comparando com tipo (a chave 1 não traduz o texto '1').
    '_default': valor quando não há tradução (senão mantém o original); '_null': valor para nulos;
    '_ignore_case: true': texto sem distinguir maiúsculas."""
    table, ci = _cached_map_table(a)

    def one(x):
        if isinstance(x, (dict, list)):
            raise TransformValueError(f"map: {short(x)} não é um valor simples")
        key = typed_key(x, ci)
        if key in table:
            return table[key]
        # mesma escrita, tipo diferente (ex.: número 1 e chave '1'; chave true e texto 'true'): não se adivinha
        other = [k for k in table if k[0] != key[0] and len(k) > 1 and text_of(k[1]).lower() == text_of(x).lower()]
        if other:
            raise TransformValueError(f"map: o valor {x!r} ({key[0]}) só corresponde à chave {other[0][1]!r} "
                                      f"({other[0][0]}); escreva a chave com o tipo do valor")
        return a.get("_default", x)

    if v is None:
        return a.get("_null")
    return [one(x) for x in v] if isinstance(v, list) else one(v)


# --- números: contas em Decimal sobre o valor escrito ---------------------------
def _dec(v) -> Decimal:
    try:
        return to_decimal(v)
    except NumberError:
        raise
    except Exception as e:  # noqa: BLE001
        raise NumberError(f"{short(v)} não é um número") from e


def _num(v, decimal: str | None = None, thousands: str | None = None):
    """Número tal como está; texto por parse_number (sem adivinhar: '1,234' e '1.234' dão erro sem formato).

    Texto inteiro -> int; com parte decimal ou expoente -> Decimal (exacto). Booleano -> erro.
    """
    if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
        return v
    if not isinstance(v, str):
        raise NumberError(f"{short(v)} não é um número")
    d = parse_number(v, decimal, thousands)
    return int(d) if d.as_tuple().exponent == 0 else d   # '12' -> 12; '1.0', '2e3' -> Decimal


def _number_format(a: Any) -> tuple[str | None, str | None]:
    if a is None:
        return None, None
    if not isinstance(a, dict) or set(a) - {"decimal", "thousands"}:
        raise ValueError("to_number aceita {decimal: ',' | '.', thousands: '.' | ',' | ' ' | \"'\"}")
    check_format(a.get("decimal"), a.get("thousands"))
    return a.get("decimal"), a.get("thousands")


def _calc(op: str, x: Decimal, y: Decimal) -> Decimal:
    try:
        return getattr(_CTX, op)(x, y)
    except (DivisionByZero, InvalidOperation):
        raise NumberError(f"{op}: {x} por {y} não tem resultado") from None


register("to_number")(_scalar(lambda v, a: _num(v, *_number_format(a))))
ARG_CHECKS["to_number"] = _number_format
register("multiply")(_scalar(lambda v, a: computed(_calc("multiply", _dec(v), _number_arg(a)))))
register("divide")(_scalar(lambda v, a: computed(_calc("divide", _dec(v), _number_arg(a)))))
register("add")(_scalar(lambda v, a: computed(_calc("add", _dec(v), _number_arg(a)))))
ARG_CHECKS["multiply"] = ARG_CHECKS["add"] = _number_arg


def _divide_arg(a):
    if _number_arg(a) == 0:
        raise ValueError("divisão por zero")


ARG_CHECKS["divide"] = _divide_arg


@register("abs")
@_scalar
def _abs(v, a):
    n = _num(v)
    return abs(n) if isinstance(n, int) else computed(abs(_dec(n)))


_no_arg("abs")


def _round_arg(a) -> tuple[int, str]:
    if not isinstance(a, dict) or set(a) != {"digits", "mode"}:
        raise ValueError("indique as casas e o modo: round: {digits: 2, mode: half_up} "
                         f"(modos: {', '.join(MODES)}); o modo muda o resultado (0.125 -> 0.12 ou 0.13)")
    if isinstance(a["digits"], bool) or not isinstance(a["digits"], int):
        raise ValueError("digits tem de ser um inteiro")
    if a["mode"] not in MODES:
        raise ValueError(f"modo {a['mode']!r} desconhecido; use um de {list(MODES)}")
    return a["digits"], a["mode"]


ARG_CHECKS["round"] = _round_arg


@register("round")
@_scalar
def _round(v, a):
    digits, mode = _round_arg(a)
    return computed(_dec(v).quantize(Decimal(1).scaleb(-digits), rounding=MODES[mode], context=_CTX))


def _clamp_arg(a) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(a, dict) or not a or set(a) - {"min", "max"}:
        raise ValueError("use clamp: {min: 0} / {max: 0} / {min: 0, max: 100}")
    lo = None if a.get("min") is None else _number_arg(a["min"])
    hi = None if a.get("max") is None else _number_arg(a["max"])
    if lo is not None and hi is not None and lo > hi:
        raise ValueError("min maior do que max")
    return lo, hi


ARG_CHECKS["clamp"] = _clamp_arg


@register("clamp")
@_scalar
def _clamp(v, a):
    """Limita o valor: {min: 0} (negativos -> 0), {max: 0} (positivos -> 0), ou ambos. Cada valor alterado é contado."""
    lo, hi = _clamp_arg(a)
    d = x = _dec(v)
    if lo is not None and x < lo:
        x = lo
    if hi is not None and x > hi:
        x = hi
    if x != d:
        note("declared", "clamped", "clamped", f"{short(v)} limitado a {x}")
    return computed(x)


# Factores para a unidade base (SI), exactos — nomes do vocabulário de unidades SAMM/Catena-X
_UNITS: dict[str, tuple[str, Decimal]] = {k: (dim, Decimal(f)) for k, (dim, f) in {
    # massa (base kg)
    "kilogram": ("mass", "1"), "gram": ("mass", "0.001"), "milligram": ("mass", "0.000001"),
    "tonneMetricTon": ("mass", "1000"), "tonne": ("mass", "1000"), "pound": ("mass", "0.45359237"),
    # comprimento (base m)
    "metre": ("length", "1"), "meter": ("length", "1"), "centimetre": ("length", "0.01"),
    "millimetre": ("length", "0.001"), "kilometre": ("length", "1000"), "inch": ("length", "0.0254"),
    # volume (base m3); 'liter'/'cubic meter': nomes das unidades declaradas do PCF
    "cubicMetre": ("volume", "1"), "litre": ("volume", "0.001"), "liter": ("volume", "0.001"),
    "cubic meter": ("volume", "1"), "millilitre": ("volume", "0.000001"), "cubicCentimetre": ("volume", "0.000001"),
    # área (base m2)
    "squareMetre": ("area", "1"), "square meter": ("area", "1"), "squareCentimetre": ("area", "0.0001"),
    "squareMillimetre": ("area", "0.000001"),
    # tempo (base s)
    "secondUnitOfTime": ("time", "1"), "second": ("time", "1"), "minuteUnitOfTime": ("time", "60"),
    "hourUnitOfTime": ("time", "3600"), "day": ("time", "86400"),
    # energia (base J)
    "joule": ("energy", "1"), "kilojoule": ("energy", "1000"), "megajoule": ("energy", "1000000"),
    "kilowattHour": ("energy", "3600000"), "kilowatt hour": ("energy", "3600000"), "wattHour": ("energy", "3600"),
    # unidades
    "piece": ("count", "1"),
}.items()}


def _unit_name(u: Any) -> str:
    """'unit:gram', 'gram', '...#gram' (datatypeURI) ou {lexicalValue: 'gram'} (BOM antigo) -> 'gram'."""
    if isinstance(u, dict) and isinstance(u.get("lexicalValue"), str):
        u = u["lexicalValue"]
    if not isinstance(u, str):
        raise TransformValueError(f"unidade inválida: {short(u)}")
    u = u.strip()
    if "#" in u:
        u = u.rsplit("#", 1)[1]
    return u.split(":", 1)[1] if u.startswith("unit:") else u


def _unit(u: Any) -> tuple[str, Decimal]:
    name = _unit_name(u)
    if name not in _UNITS:
        raise TransformValueError(f"unidade desconhecida: {short(u)} (conhecidas: {', '.join(sorted(_UNITS))})")
    return _UNITS[name]


def _unit_convert_arg(a):
    if not isinstance(a, dict) or "to" not in a or set(a) - {"to", "from"}:
        raise ValueError("use unit_convert: {to: kilogram} (e from: gram se o documento não trouxer a unidade)")
    to = _unit(a["to"])
    if a.get("from") is not None and _unit(a["from"])[0] != to[0]:
        raise ValueError(f"from {a['from']!r} e to {a['to']!r} não são da mesma grandeza")


ARG_CHECKS["unit_convert"] = _unit_convert_arg


def _only_one(v: dict, keys: tuple[str, ...], what: str, norm: Callable[[Any], Any]) -> Any:
    """O valor de uma de várias chaves equivalentes (value/quantityNumber, unit/measurementUnit); se vierem duas
    com valores diferentes, é um conflito (não se escolhe uma)."""
    found = [(k, v[k]) for k in keys if v.get(k) is not None]
    if len(found) > 1 and len({norm(x) for _, x in found}) > 1:
        raise TransformValueError(f"unit_convert: {what} em conflito: " + ", ".join(f"{k}={short(x)}" for k, x in found))
    return found[0][1] if found else None


@register("unit_convert")
def _unit_convert(v, a):
    """Converte quantidades Catena-X.

    O valor pode ser um número (com {from: gram, to: kilogram}) ou um objecto {value, unit} / {quantityNumber,
    measurementUnit} (com {to: kilogram}). Sem unidade no documento e sem `from`: erro (antes: nulo).
    Com `from` e sem unidade no objecto: usa-se `from` e fica contado.
    """
    if v is None:
        return None
    if isinstance(v, list):
        return [None if x is None else _unit_convert(x, a) for x in v]
    if isinstance(v, dict):
        number = _only_one(v, ("value", "quantityNumber"), "valores", typed_key)
        src = _only_one(v, ("unit", "measurementUnit"), "unidades", _unit_name)
        if number is None:
            return None                                      # quantidade sem número: nulo (como um campo ausente)
        if src is None:
            if a.get("from") is None:
                raise TransformValueError(f"unit_convert: a quantidade {short(v)} não tem unidade; indique "
                                          f"from: <unidade> para a assumir")
            src = a["from"]
            note("declared", "unit_fallback", "assumed_unit", f"{short(v)} sem unidade: assumida {a['from']}")
    else:
        number, src = v, a.get("from")
        if src is None:
            raise TransformValueError(f"unit_convert: {short(v)} não tem unidade; indique from: <unidade>")
    s, t = _unit(src), _unit(a["to"])
    if s[0] != t[0]:
        raise TransformValueError(f"unit_convert: {short(src)} ({s[0]}) não se converte em {a['to']} ({t[0]})")
    return computed(_calc("divide", _calc("multiply", _dec(number), s[1]), t[1]))


# --- datas ---------------------------------------------------------------
def _timestamp_arg(a) -> tuple[str | None, str | None]:
    if a is None:
        return None, None
    if isinstance(a, str):
        return a, None
    if isinstance(a, dict) and not set(a) - {"format", "unit"}:
        if a.get("unit") is not None and a["unit"] not in dates.UNITS:
            raise ValueError(f"unit deve ser um de {list(dates.UNITS)}")
        if a.get("format") is not None:
            _need_str(a["format"])
        return a.get("format"), a.get("unit")
    raise ValueError("use to_timestamp, to_timestamp: '<formato strptime>' ou {format: ..., unit: s | ms}")


ARG_CHECKS["to_timestamp"] = _timestamp_arg
ARG_CHECKS["to_date"] = _opt_str
ARG_CHECKS["date_format"] = _need_str
_no_arg("year")
_no_arg("month")
register("to_timestamp")(_scalar(lambda v, a: dates.to_timestamp(v, *_timestamp_arg(a))))
register("to_date")(_scalar(lambda v, a: dates.to_date(v, a)))
register("date_format")(_scalar(lambda v, a: dates.wall(v).strftime(a)))
register("year")(_scalar(lambda v, a: dates.wall(v).year))
register("month")(_scalar(lambda v, a: dates.wall(v).month))


# --- listas --------------------------------------------------------------
def _list(v):
    return [] if v is None else v if isinstance(v, list) else [v]


def _nulls_arg(a) -> bool:
    if a is None:
        return False
    if not isinstance(a, dict) or set(a) != {"nulls"} or a["nulls"] != "skip":
        raise ValueError("o único argumento é {nulls: skip}")
    return True


def _without_nulls(name: str, v: Any, a: Any) -> list:
    items = _list(v)
    nulls = sum(x is None for x in items)
    if nulls:
        if not _nulls_arg(a):
            raise TransformValueError(f"{name}: {nulls} de {len(items)} valor(es) nulo(s): o resultado seria parcial; "
                                      f"declare {name}: {{nulls: skip}} para os ignorar")
        note("declared", "nulls_skipped", "skipped", f"{name}: {nulls} de {len(items)} valor(es) nulo(s) ignorado(s)")
    return [x for x in items if x is not None]


def _order_key(name: str, xs: list) -> Callable[[Any], Any]:
    """Números como números; datas ISO como instantes; o resto como texto. Números em texto e tipos misturados: erro."""
    kinds = {typed_key(x)[0] for x in xs}
    if kinds == {"num"}:
        return to_decimal
    if kinds == {"str"}:
        if any(JSON_NUMBER.fullmatch(x.strip()) or AMBIGUOUS.fullmatch(x.strip()) for x in xs):
            raise TransformValueError(f"{name}: há números em texto ({short(xs)}): ordenados como texto dariam '9' > '10'; "
                                      f"use to_number antes")
        if all(re.match(r"\d{4}-\d{2}-\d{2}", x.strip()) for x in xs):
            instants = [dates._parse(x)[0] for x in xs]
            if len({d.tzinfo is None for d in instants}) > 1:
                raise TransformValueError(f"{name}: datas com e sem fuso horário não se comparam ({short(xs)})")
            by = dict(zip(map(id, xs), instants))
            return lambda x: by[id(x)]
        return str
    raise TransformValueError(f"{name}: valores de tipos diferentes ou não comparáveis ({sorted(kinds)}): {short(xs)}")


@register("sum")
def _sum(v, a):
    xs = _without_nulls("sum", v, a)
    if not xs:
        return None
    total = Decimal(0)
    for x in xs:
        total = _calc("add", total, _dec(x))
    return computed(total)


@register("min")
def _min(v, a):
    xs = _without_nulls("min", v, a)
    return min(xs, key=_order_key("min", xs)) if xs else None


@register("max")
def _max(v, a):
    xs = _without_nulls("max", v, a)
    return max(xs, key=_order_key("max", xs)) if xs else None


for _n in ("sum", "min", "max"):
    ARG_CHECKS[_n] = _nulls_arg


@register("sort")
def _sort(v, a):
    xs = _list(v)
    if any(x is None for x in xs):
        raise TransformValueError("sort: a lista tem nulos; use compact antes (ou missing_elements: skip)")
    return sorted(xs, key=_order_key("sort", xs), reverse=bool(a)) if xs else []


def _bool_arg(a):
    if a is not None and not isinstance(a, bool):
        raise ValueError("só aceita true ou false")


ARG_CHECKS["sort"] = ARG_CHECKS["to_json"] = _bool_arg


@register("distinct")
def _distinct(v, a):
    """Valores distintos, pela ordem, comparando com tipo (True, 1 e '1' são diferentes; 1 e 1.0 iguais)."""
    seen: dict = {}
    for x in _list(v):
        seen.setdefault(typed_key(x), x)
    return list(seen.values())


@register("single")
def _single(v, a):
    """O único valor: vazio -> nulo; vários valores diferentes -> erro (first escolheria um em silêncio)."""
    xs = _distinct(v, None)
    if len(xs) > 1:
        raise TransformValueError(f"single: {len(xs)} valores diferentes ({short(xs)}); use first se qualquer um servir")
    return xs[0] if xs else None


register("first")(lambda v, a: _list(v)[0] if _list(v) else None)
register("last")(lambda v, a: _list(v)[-1] if _list(v) else None)
register("count")(lambda v, a: len(_list(v)))
register("compact")(lambda v, a: [x for x in _list(v) if x not in (None, "")])


@register("pluck")
def _pluck(v, a):
    out = []
    for x in _list(v):
        if not isinstance(x, dict) or a not in x:
            raise TransformValueError(f"pluck: {short(x)} não tem o campo {a!r}; use o caminho lista[*].{a} "
                                      f"com missing_elements")
        out.append(x[a])
    return out


ARG_CHECKS["pluck"] = ARG_CHECKS["get"] = _need_str
for _n in ("distinct", "single", "first", "last", "count", "compact", "keys", "from_json", "to_bool", "uuid5", "uuid4",
           "now", "today"):
    _no_arg(_n)


# --- JSON / estruturas ---------------------------------------------------
register("to_json")(lambda v, a: None if v is None else json_text(v, sort_keys=bool(a)))


@register("from_json")
def _from_json(v, a):
    from .reader import loads  # noqa: PLC0415 - leitor estrito (sem NaN nem chaves repetidas)

    return None if v is None else loads(v) if isinstance(v, str) else v


@register("keys")
def _keys(v, a):
    if v is None:
        return None
    if not isinstance(v, dict):
        raise TransformValueError(f"keys: {short(v)} não é um objecto")
    return list(v.keys())


@register("get")
def _get(v, a):
    if v is None:
        return None
    if not isinstance(v, dict):
        raise TransformValueError(f"get: {short(v)} não é um objecto")
    return v.get(a)


# --- booleanos / hash / ids ---------------------------------------------
@register("to_bool")
@_scalar
def _bool(v, a):
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    s = str(v).strip().lower() if isinstance(v, str) else None
    if s in ("true", "1", "yes", "y", "sim", "s"):
        return True
    if s in ("false", "0", "no", "n", "nao", "não"):
        return False
    raise TransformValueError(f"to_bool: {short(v)} não é um booleano reconhecido")


def _identity_text(name: str, v: Any) -> str:
    """Texto usado em hash/uuid5: texto tal como está, objetos/listas em JSON ordenado. Um número ou booleano
    sozinho é erro: 1 e 1.0 (ou '1') dariam identidades diferentes; escolha o texto com to_string antes."""
    import json  # noqa: PLC0415

    if isinstance(v, str):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, default=str)
    raise TransformValueError(f"{name}: {short(v)} não é texto; use to_string (ou um template) antes, para que 1 e "
                              f"1.0 não dêem valores diferentes")


@register("hash")
def _hash(v, a):
    if v is None:
        return None
    data = _identity_text("hash", v)
    return hashlib.new(a or "sha256", data.encode()).hexdigest()


def _hash_arg(a):
    if a is not None and a not in hashlib.algorithms_available:
        raise ValueError(f"algoritmo desconhecido {a!r}")


ARG_CHECKS["hash"] = _hash_arg


@register("uuid5")
def _uuid5(v, a):
    """UUID determinístico a partir do valor (bom para chaves sintéticas)."""
    if v is None:
        return None
    data = _identity_text("uuid5", v)
    return str(_uuid.uuid5(_uuid.NAMESPACE_URL, data))


register("uuid4")(lambda v, a: str(_uuid.uuid4()))
register("now")(lambda v, a: datetime.now(timezone.utc).replace(tzinfo=None))
register("today")(lambda v, a: datetime.now(timezone.utc).date())   # UTC, como now e cx_ingested_at

