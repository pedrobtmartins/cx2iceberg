"""Linguagem de caminhos para navegar no JSON de um submodelo.

Sintaxe suportada (combinável):

    partTypeInformation.classification       campo aninhado
    childItems[0].catenaXId                  índice de lista (negativos também: [-1])
    childItems[*].catenaXId                  todos os elementos (devolve lista)
    localIdentifiers[?key=manufacturerId].value   filtro por igualdade
    localIdentifiers[?key!=van].value        filtro por diferença
    localIdentifiers[?value].key             filtro por existência/truthy
    "a.b com pontos"                         chave com caracteres especiais: ['a.b']

Prefixos de contexto (para linhas geradas com `explode`):

    $root.catenaXId     objecto raiz do submodelo
    $parent.x           elemento do nível de explode anterior
    $parent.$parent.x   dois níveis acima
    $item               elemento actual (útil para listas de escalares)
    $index              posição do elemento actual na lista explodida
    $meta.source_file   metadados (source_file, semantic_id, ingested_at, ...)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

MISSING = object()

_TOKEN_RE = re.compile(
    r"""
    \[\s*'(?P<qkey>[^']*)'\s*\]           # ['chave.com.pontos']
  | \[\s*(?P<idx>-?\d+)\s*\]              # [0]
  | \[\s*\*\s*\]                          # [*]
    (?P<star>)
  | \[\s*\?(?P<filter>[^\]]+)\]           # [?campo=valor]
  | (?P<key>[^.\[\]]+)                    # nome simples
  | \.                                    # separador
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class Step:
    kind: str  # key | idx | star | filter
    value: Any = None


@dataclass
class Context:
    """Pilha de níveis: [(raiz, None), (elem_nivel1, idx1), ...]."""

    stack: list[tuple[Any, int | None]]
    meta: dict[str, Any] = field(default_factory=dict)

    def child(self, obj: Any, index: int) -> "Context":
        return Context(self.stack + [(obj, index)], self.meta)


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
        if m.group("qkey") is not None:
            steps.append(Step("key", m.group("qkey")))
        elif m.group("idx") is not None:
            steps.append(Step("idx", int(m.group("idx"))))
        elif m.group("star") is not None:
            steps.append(Step("star"))
        elif m.group("filter") is not None:
            steps.append(Step("filter", _parse_filter(m.group("filter"))))
        elif m.group("key") is not None:
            steps.append(Step("key", m.group("key").strip()))
    return tuple(steps)


def _parse_filter(expr: str) -> tuple[str, str, str | None]:
    for op in ("!=", "=", "~"):
        if op in expr:
            left, right = expr.split(op, 1)
            return left.strip(), op, right.strip().strip("'\"")
    return expr.strip(), "exists", None


def _filter_ok(item: Any, flt: tuple[str, str, str | None]) -> bool:
    field_path, op, expected = flt
    val = resolve_obj(item, field_path)
    if op == "exists":
        return val is not MISSING and val not in (None, "", [], {})
    if val is MISSING:
        return op == "!="
    if op == "=":
        return str(val) == expected
    if op == "!=":
        return str(val) != expected
    if op == "~":
        return re.search(expected or "", str(val)) is not None
    return False


def resolve_obj(obj: Any, path: str) -> Any:
    """Resolve um caminho sobre um objecto. Devolve MISSING se não existir.

    Se o caminho contiver [*] ou filtros, devolve sempre uma lista.
    """
    steps = parse(path)
    multi = any(s.kind in ("star", "filter") for s in steps)
    current = [obj]
    for step in steps:
        nxt: list[Any] = []
        for cur in current:
            if step.kind == "key":
                if isinstance(cur, dict) and step.value in cur:
                    nxt.append(cur[step.value])
            elif step.kind == "idx":
                if isinstance(cur, list) and -len(cur) <= step.value < len(cur):
                    nxt.append(cur[step.value])
            elif step.kind == "star":
                if isinstance(cur, list):
                    nxt.extend(cur)
                elif isinstance(cur, dict):
                    nxt.extend(cur.values())
            elif step.kind == "filter":
                items = cur if isinstance(cur, list) else [cur]
                nxt.extend(i for i in items if _filter_ok(i, step.value))
        current = nxt
    if multi:
        return current
    return current[0] if current else MISSING


def resolve(ctx: Context, path: str) -> Any:
    """Resolve um caminho tendo em conta os prefixos $root/$parent/$item/$index/$meta."""
    path = path.strip()
    level = len(ctx.stack) - 1
    rest = path
    while True:
        if rest == "$root" or rest.startswith("$root."):
            level, rest = 0, rest[len("$root"):].lstrip(".")
        elif rest == "$parent" or rest.startswith("$parent."):
            level, rest = max(level - 1, 0), rest[len("$parent"):].lstrip(".")
        elif rest == "$item" or rest.startswith("$item."):
            rest = rest[len("$item"):].lstrip(".")
        else:
            break
    if rest == "$index":
        return ctx.stack[level][1]
    if rest.startswith("$meta"):
        key = rest[len("$meta"):].lstrip(".")
        return ctx.meta.get(key, MISSING) if key else dict(ctx.meta)
    obj = ctx.stack[level][0]
    if not rest:
        return obj
    return resolve_obj(obj, rest)


def split_explode(spec: str | list[str]) -> list[str]:
    """'a[*].b[*]' -> ['a', 'b'];  ['a', 'b.c'] mantém-se."""
    if isinstance(spec, list):
        return spec
    parts = [p.strip(".") for p in re.split(r"\[\s*\*\s*\]", spec)]
    return [p for p in parts if p]
