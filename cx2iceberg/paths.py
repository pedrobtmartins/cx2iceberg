"""Linguagem de caminhos para navegar no JSON de um submodelo.

Sintaxe suportada (combinável):

    partTypeInformation.classification       campo aninhado
    childItems[0].catenaXId                  índice de lista (negativos também: [-1])
    childItems[*].catenaXId                  todos os elementos (devolve lista)
    localIdentifiers[?key=manufacturerId].value   filtro por igualdade
    localIdentifiers[?key!=van].value        filtro por diferença
    localIdentifiers[?key~^BPN].value        filtro por expressão regular
    localIdentifiers[?value].key             filtro por existência/truthy
    "a.b com pontos"                         chave com caracteres especiais: ['a.b']

Prefixos de contexto (para linhas geradas com `explode`):

    $root.catenaXId     objecto raiz do submodelo
    $parent.x           elemento do nível de explode anterior
    $parent.$parent.x   dois níveis acima
    $item               elemento actual (útil para listas de escalares)
    $index              posição do elemento actual na lista explodida
    $meta.source_file   metadados (source_file, semantic_id, ingested_at, document_id)

Campo ausente e forma errada são coisas diferentes:

    * um campo que não existe (ou um objecto intermédio a null) dá MISSING -> nulo, como sempre;
    * pedir um campo a uma lista (`childItems.quantity` em vez de `childItems[*].quantity`), pedir um
      campo a um valor simples, ou um índice a um objecto (`xs[?k=a][0]`) é um erro (PathShapeError):
      o caminho não serve para este documento e antes dava nulo em silêncio.

Com [*] ou filtros o resultado é uma `Projection` (uma lista com os valores, como antes) que sabe quantos
elementos não tinham o campo (`missing`) e onde (`slots`, com None nessas posições). O motor decide o que
fazer com isso (ver `missing_elements` nas colunas).

Valores dos filtros, com tipo:  [?k=abc] ou [?k='abc'] texto;  [?n=1] número (1 e 1.0 são iguais);
[?b=true] booleano;  [?x=null] nulo. Comparar tipos diferentes (texto com número, ...) é um erro
(FilterTypeError), em vez de nunca corresponder.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache
from typing import Any

MISSING = object()

_TOKEN_RE = re.compile(
    r"""
    \[\s*'(?P<qkey>[^']*)'\s*\]                          # ['chave.com.pontos']
  | \[\s*(?P<idx>-?\d+)\s*\]                             # [0]
  | \[\s*\*\s*\]                                         # [*]
    (?P<star>)
  | \[\s*\?(?P<filter>(?:'[^']*'|"[^"]*"|[^\]'"])+)\]    # [?campo=valor]  (valor entre aspas pode ter ']')
  | (?P<key>[^.\[\]]+)                                   # nome simples
  | \.                                                   # separador
    """,
    re.VERBOSE,
)
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", re.ASCII)
TEMPLATE_RE = re.compile(r"\{([^{}]+)\}")   # "{$root.catenaXId}|{catenaXId}": um caminho por {…}


class PathError(ValueError):
    """Erro num caminho ao aplicá-lo a um documento. `reason` é o código no relatório."""

    reason = "path_shape"


class PathShapeError(PathError):
    """O caminho não tem a forma do documento (campo pedido a uma lista, índice pedido a um objecto...)."""


class FilterTypeError(PathError):
    """Um filtro compara tipos diferentes (ex.: texto '1' com o número 1)."""

    reason = "filter_type"


@dataclass(frozen=True)
class Step:
    kind: str  # key | idx | star | filter
    value: Any = None
    text: str = ""


@dataclass
class Context:
    """Pilha de níveis: [(raiz, None), (elem_nivel1, idx1), ...]."""

    stack: list[tuple[Any, int | None]]
    meta: dict[str, Any] = field(default_factory=dict)

    def child(self, obj: Any, index: int) -> "Context":
        return Context(self.stack + [(obj, index)], self.meta)


class Projection(list):
    """Resultado de um caminho com [*] ou filtro: os valores encontrados (como antes) e os elementos sem o campo.

    missing  nº de elementos que não tinham o campo (ou o índice)
    slots    um lugar por elemento, com None onde faltava (para `missing_elements: null`)
    """

    __slots__ = ("missing", "slots", "path")

    def __init__(self, values=(), missing: int = 0, slots: list | None = None, path: str = ""):
        super().__init__(values)
        self.missing = missing
        self.slots = list(values) if slots is None else slots
        self.path = path


@lru_cache(maxsize=4096)
def parse(path: str) -> tuple[Step, ...]:
    steps: list[Step] = []
    pos = 0
    path = path.strip()
    while pos < len(path):
        m = _TOKEN_RE.match(path, pos)
        if not m or m.end() == pos:
            raise ValueError(f"Caminho inválido perto de '{path[pos:]}' em '{path}'")
        pos = m.end()
        text = m.group(0)
        if m.group("qkey") is not None:
            steps.append(Step("key", m.group("qkey"), text))
        elif m.group("idx") is not None:
            steps.append(Step("idx", int(m.group("idx")), text))
        elif m.group("star") is not None:
            steps.append(Step("star", None, text))
        elif m.group("filter") is not None:
            steps.append(Step("filter", _parse_filter(m.group("filter")), text))
        elif m.group("key") is not None:
            steps.append(Step("key", m.group("key").strip(), text))
    return tuple(steps)


def _parse_filter(expr: str) -> tuple[str, str, tuple[str, Any] | None]:
    """'campo=valor' -> (campo, op, (tipo, valor)). O operador é o PRIMEIRO que aparece: '[?key~a=b]' é um
    regex 'a=b' sobre key (antes era partido no '=')."""
    for i, ch in enumerate(expr):
        if expr.startswith("!=", i):
            op, j = "!=", i + 2
        elif ch in "=~":
            op, j = ch, i + 1
        else:
            continue
        left, right = expr[:i].strip(), expr[j:].strip()
        if not left:
            raise ValueError(f"Filtro inválido '[?{expr}]': falta o campo antes de '{op}'")
        return left, op, ("regex", _unquote(right)) if op == "~" else _literal(right)
    return expr.strip(), "exists", None


def _unquote(text: str) -> str:
    return text[1:-1] if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"" else text


def _literal(text: str) -> tuple[str, Any]:
    """Valor à direita do filtro: 'x'/"x" texto; true/false; null; número JSON; o resto é texto."""
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        return "str", text[1:-1]
    if text in ("true", "false"):
        return "bool", text == "true"
    if text == "null":
        return "null", None
    if _NUMBER.fullmatch(text):
        return "num", Decimal(text)
    return "str", text


def _kind(v: Any) -> str:
    if isinstance(v, bool):
        return "booleano"
    if isinstance(v, (int, float, Decimal)):
        return "número"
    return {str: "texto", list: "lista", dict: "objecto"}.get(type(v), type(v).__name__)


_SIMPLE_KEY = re.compile(r"[^.\[\]'\"$]+")


@lru_cache(maxsize=1024)
def _is_simple_key(field_path: str) -> bool:
    return bool(_SIMPLE_KEY.fullmatch(field_path))


def _filter_ok(item: Any, flt: tuple[str, str, tuple[str, Any] | None], text: str) -> bool:
    field_path, op, lit = flt
    if isinstance(item, dict) and _is_simple_key(field_path):   # caso comum: [?key=...] sobre objectos
        val = item.get(field_path, MISSING)
    else:
        val = resolve_obj(item, field_path)
    if op == "exists":
        return val is not MISSING and val not in (None, "", [], {})
    if val is MISSING or val is None:                    # campo ausente ou nulo
        if lit is not None and lit[0] == "null":
            return op == "="
        return op == "!="
    kind, expected = lit
    if op == "~":
        if not isinstance(val, str):
            raise FilterTypeError(f"filtro {text}: expressão regular sobre {_kind(val)} {val!r} (só se aplica a texto)")
        return re.search(expected, val) is not None
    if kind == "null":
        equal = False
    elif kind == "num":
        if not isinstance(val, (int, float, Decimal)) or isinstance(val, bool):
            raise FilterTypeError(f"filtro {text}: compara o número {expected} com {_kind(val)} {val!r}"
                                  + (f" (para texto use aspas: [?{field_path}{op}'{expected}'])" if isinstance(val, str) else ""))
        equal = Decimal(str(val)) == expected if not isinstance(val, Decimal) else val == expected
    elif kind == "bool":
        if not isinstance(val, bool):
            raise FilterTypeError(f"filtro {text}: compara o booleano {str(expected).lower()} com {_kind(val)} {val!r}")
        equal = val is expected
    else:
        if not isinstance(val, str):
            raise FilterTypeError(f"filtro {text}: compara o texto {expected!r} com {_kind(val)} {val!r}"
                                  + (" (para um número escreva-o sem aspas)" if isinstance(val, (int, float)) else ""))
        equal = val == expected
    return equal if op == "=" else not equal


def resolve_obj(obj: Any, path: str) -> Any:
    """Resolve um caminho sobre um objecto. Devolve MISSING se não existir; PathShapeError se a forma não servir.

    Se o caminho contiver [*] ou filtros, devolve sempre uma Projection (lista).
    """
    steps = parse(path)
    multi_path = any(s.kind in ("star", "filter") for s in steps)
    multi = False              # já passámos por um [*]/filtro: a partir daqui, um lugar por elemento
    current: list[Any] = [obj]
    for step in steps:
        nxt: list[Any] = []
        for cur in current:
            if cur is MISSING:
                nxt.append(MISSING)                      # o elemento já não tinha o campo: continua a faltar
                continue
            if step.kind == "key":
                if isinstance(cur, dict):
                    nxt.append(cur.get(step.value, MISSING))
                elif cur is None:
                    nxt.append(MISSING)                  # objecto intermédio a null: ausente
                elif isinstance(cur, list):
                    raise PathShapeError(f"'{path}': o campo '{step.value}' foi pedido a uma lista"
                                         f" — para todos os elementos use '[*].{step.value}'")
                else:
                    raise PathShapeError(f"'{path}': o campo '{step.value}' foi pedido a {_kind(cur)} {cur!r}")
            elif step.kind == "idx":
                if isinstance(cur, list):
                    nxt.append(cur[step.value] if -len(cur) <= step.value < len(cur) else MISSING)
                elif cur is None:
                    nxt.append(MISSING)
                elif isinstance(cur, dict):
                    hint = (" — para o 1.º resultado de um [*] ou filtro use '| first'" if multi
                            else " — a lista pode ter sido desembrulhada pelo unwrap: retire o índice")
                    raise PathShapeError(f"'{path}': índice {step.text} pedido a um objecto{hint}")
                else:
                    raise PathShapeError(f"'{path}': índice {step.text} pedido a {_kind(cur)} {cur!r}")
            elif step.kind in ("star", "filter"):
                if cur is None:
                    nxt.append(MISSING)
                    continue
                if isinstance(cur, list):
                    items = cur
                elif isinstance(cur, dict):
                    items = list(cur.values()) if step.kind == "star" else [cur]   # objecto: lista 1:1 desembrulhada
                else:
                    raise PathShapeError(f"'{path}': {step.text} pedido a {_kind(cur)} {cur!r}")
                nxt.extend(items if step.kind == "star" else [i for i in items if _filter_ok(i, step.value, step.text)])
        current = nxt
        if step.kind in ("star", "filter"):
            multi = True
        if not multi and current == [MISSING]:
            # ausente antes de qualquer [*]/filtro: o caminho inteiro não existe (não são elementos em falta)
            return Projection(path=path) if multi_path else MISSING
    if not multi_path:
        return current[0]
    values = [v for v in current if v is not MISSING]
    return Projection(values, len(current) - len(values), [None if v is MISSING else v for v in current], path)


@lru_cache(maxsize=4096)
def _prefixes(path: str) -> tuple[tuple[str, ...], str]:
    """'$parent.$parent.x' -> (('parent', 'parent'), 'x'). Em cache: não se volta a analisar em cada linha."""
    rest, ops = path.strip(), []
    while True:
        for name in ("root", "parent", "item"):
            tok = "$" + name
            if rest == tok or rest.startswith(tok + "."):
                ops.append(name)
                rest = rest[len(tok):].lstrip(".")
                break
        else:
            return tuple(ops), rest


def resolve(ctx: Context, path: str) -> Any:
    """Resolve um caminho tendo em conta os prefixos $root/$parent/$item/$index/$meta."""
    ops, rest = _prefixes(path)
    level = len(ctx.stack) - 1
    for op in ops:
        if op == "root":
            level = 0
        elif op == "parent":
            if level == 0:
                raise PathShapeError(f"'{path.strip()}': $parent acima da raiz (a tabela tem {len(ctx.stack) - 1} "
                                     f"nível(is) de explode)")
            level -= 1
    if rest == "$index":
        return ctx.stack[level][1]
    if rest.startswith("$meta"):
        key = rest[len("$meta"):].lstrip(".")
        return ctx.meta.get(key, MISSING) if key else dict(ctx.meta)
    obj = ctx.stack[level][0]
    if not rest:
        return obj
    return resolve_obj(obj, rest)


def context_refs(path: str) -> tuple[bool, int, str]:
    """Para validar na carga: (começa na raiz?, nº de $parent a partir daí, resto do caminho).

    '$parent.$parent.x' -> (False, 2, 'x');  '$root.x' -> (True, 0, 'x');  '$root.$parent.x' -> (True, 1, 'x').
    """
    rest, from_root, up = path.strip(), False, 0
    while True:
        if rest == "$root" or rest.startswith("$root."):
            from_root, up, rest = True, 0, rest[len("$root"):].lstrip(".")
        elif rest == "$parent" or rest.startswith("$parent."):
            up, rest = up + 1, rest[len("$parent"):].lstrip(".")
        elif rest == "$item" or rest.startswith("$item."):
            rest = rest[len("$item"):].lstrip(".")
        else:
            return from_root, up, rest


def split_explode(spec: str | list[str]) -> list[str]:
    """'a[*].b[*]' -> ['a', 'b'];  ['a', 'b.c'] mantém-se."""
    if isinstance(spec, list):
        return spec
    parts = [p.strip(".") for p in re.split(r"\[\s*\*\s*\]", spec)]
    return [p for p in parts if p]
