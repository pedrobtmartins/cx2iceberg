"""Path language for navigating a submodel's JSON.

Supported syntax (combinable):

    partTypeInformation.classification       nested field
    childItems[0].catenaXId                  list index (negatives too: [-1])
    childItems[*].catenaXId                  every element (returns a list)
    localIdentifiers[?key=manufacturerId].value   filter by equality
    localIdentifiers[?key!=van].value        filter by inequality
    localIdentifiers[?key~^BPN].value        filter by regular expression
    localIdentifiers[?value].key             filter by existence/truthy
    "a.b with dots"                          key with special characters: ['a.b']

Context prefixes (for rows generated with `explode`):

    $root.catenaXId     root object of the submodel
    $parent.x           element of the previous explode level
    $parent.$parent.x   two levels up
    $item               current element (useful for lists of scalars)
    $index              position of the current element in the exploded list
    $meta.source_file   metadata (source_file, semantic_id, ingested_at, document_id)

An absent field and a wrong shape are different things:

    * a field that does not exist (or an intermediate object that is null) gives MISSING -> null, as always;
    * asking a list for a field (`childItems.quantity` instead of `childItems[*].quantity`), asking a simple
      value for a field, or an object for an index (`xs[?k=a][0]`) is an error (PathShapeError):
      the path does not fit this document, and before it silently gave null.

With [*] or filters the result is a `Projection` (a list with the values, as before) that knows how many
elements lacked the field (`missing`) and where (`slots`, with None in those positions). The engine decides what
to do with that (see `missing_elements` on the columns).

Filter values, with types:  [?k=abc] or [?k='abc'] text;  [?n=1] number (1 and 1.0 are equal);
[?b=true] boolean;  [?x=null] null. Comparing different types (text with a number, ...) is an error
(FilterTypeError), instead of never matching.
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
    \[\s*'(?P<qkey>[^']*)'\s*\]                          # ['key.with.dots']
  | \[\s*(?P<idx>-?\d+)\s*\]                             # [0]
  | \[\s*\*\s*\]                                         # [*]
    (?P<star>)
  | \[\s*\?(?P<filter>(?:'[^']*'|"[^"]*"|[^\]'"])+)\]    # [?field=value]  (a quoted value may contain ']')
  | (?P<key>[^.\[\]]+)                                   # simple name
  | \.                                                   # separator
    """,
    re.VERBOSE,
)
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", re.ASCII)
TEMPLATE_RE = re.compile(r"\{([^{}]+)\}")   # "{$root.catenaXId}|{catenaXId}": one path per {…}


class PathError(ValueError):
    """Error in a path when applying it to a document. `reason` is the code in the report."""

    reason = "path_shape"


class PathShapeError(PathError):
    """The path does not fit the document's shape (field asked of a list, index asked of an object...)."""


class FilterTypeError(PathError):
    """A filter compares different types (e.g. text '1' with the number 1)."""

    reason = "filter_type"


@dataclass(frozen=True)
class Step:
    kind: str  # key | idx | star | filter
    value: Any = None
    text: str = ""


@dataclass
class Context:
    """Stack of levels: [(root, None), (level1_elem, idx1), ...]."""

    stack: list[tuple[Any, int | None]]
    meta: dict[str, Any] = field(default_factory=dict)

    def child(self, obj: Any, index: int) -> "Context":
        return Context(self.stack + [(obj, index)], self.meta)


class Projection(list):
    """Result of a path with [*] or a filter: the values found (as before) and the elements without the field.

    missing  number of elements that lacked the field (or the index)
    slots    one slot per element, with None where it was missing (for `missing_elements: null`)
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
            raise ValueError(f"Invalid path near '{path[pos:]}' in '{path}'")
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
    """'field=value' -> (field, op, (type, value)). The operator is the FIRST one that appears: '[?key~a=b]' is a
    regex 'a=b' on key (before, it was split at the '=')."""
    for i, ch in enumerate(expr):
        if expr.startswith("!=", i):
            op, j = "!=", i + 2
        elif ch in "=~":
            op, j = ch, i + 1
        else:
            continue
        left, right = expr[:i].strip(), expr[j:].strip()
        if not left:
            raise ValueError(f"Invalid filter '[?{expr}]': the field before '{op}' is missing")
        return left, op, ("regex", _unquote(right)) if op == "~" else _literal(right)
    return expr.strip(), "exists", None


def _unquote(text: str) -> str:
    return text[1:-1] if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"" else text


def _literal(text: str) -> tuple[str, Any]:
    """Value on the right of the filter: 'x'/"x" text; true/false; null; JSON number; the rest is text."""
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
        return "boolean"
    if isinstance(v, (int, float, Decimal)):
        return "number"
    return {str: "text", list: "list", dict: "object"}.get(type(v), type(v).__name__)


_SIMPLE_KEY = re.compile(r"[^.\[\]'\"$]+")


@lru_cache(maxsize=1024)
def _is_simple_key(field_path: str) -> bool:
    return bool(_SIMPLE_KEY.fullmatch(field_path))


def _filter_ok(item: Any, flt: tuple[str, str, tuple[str, Any] | None], text: str) -> bool:
    field_path, op, lit = flt
    if isinstance(item, dict) and _is_simple_key(field_path):   # common case: [?key=...] on objects
        val = item.get(field_path, MISSING)
    else:
        val = resolve_obj(item, field_path)
    if op == "exists":
        return val is not MISSING and val not in (None, "", [], {})
    if val is MISSING or val is None:                    # field absent or null
        if lit is not None and lit[0] == "null":
            return op == "="
        return op == "!="
    kind, expected = lit
    if op == "~":
        if not isinstance(val, str):
            raise FilterTypeError(f"filter {text}: regular expression on {_kind(val)} {val!r} (only applies to text)")
        return re.search(expected, val) is not None
    if kind == "null":
        equal = False
    elif kind == "num":
        if not isinstance(val, (int, float, Decimal)) or isinstance(val, bool):
            raise FilterTypeError(f"filter {text}: compares the number {expected} with {_kind(val)} {val!r}"
                                  + (f" (for text use quotes: [?{field_path}{op}'{expected}'])" if isinstance(val, str) else ""))
        equal = Decimal(str(val)) == expected if not isinstance(val, Decimal) else val == expected
    elif kind == "bool":
        if not isinstance(val, bool):
            raise FilterTypeError(f"filter {text}: compares the boolean {str(expected).lower()} with {_kind(val)} {val!r}")
        equal = val is expected
    else:
        if not isinstance(val, str):
            raise FilterTypeError(f"filter {text}: compares the text {expected!r} with {_kind(val)} {val!r}"
                                  + (" (for a number write it without quotes)" if isinstance(val, (int, float)) else ""))
        equal = val == expected
    return equal if op == "=" else not equal


def resolve_obj(obj: Any, path: str) -> Any:
    """Resolves a path on an object. Returns MISSING if it does not exist; PathShapeError if the shape does not fit.

    If the path contains [*] or filters, it always returns a Projection (list).
    """
    steps = parse(path)
    multi_path = any(s.kind in ("star", "filter") for s in steps)
    multi = False              # we already went through a [*]/filter: from here on, one slot per element
    current: list[Any] = [obj]
    for step in steps:
        nxt: list[Any] = []
        for cur in current:
            if cur is MISSING:
                nxt.append(MISSING)                      # the element already lacked the field: still missing
                continue
            if step.kind == "key":
                if isinstance(cur, dict):
                    nxt.append(cur.get(step.value, MISSING))
                elif cur is None:
                    nxt.append(MISSING)                  # intermediate object is null: absent
                elif isinstance(cur, list):
                    raise PathShapeError(f"'{path}': field '{step.value}' was asked of a list"
                                         f" — for every element use '[*].{step.value}'")
                else:
                    raise PathShapeError(f"'{path}': field '{step.value}' was asked of {_kind(cur)} {cur!r}")
            elif step.kind == "idx":
                if isinstance(cur, list):
                    nxt.append(cur[step.value] if -len(cur) <= step.value < len(cur) else MISSING)
                elif cur is None:
                    nxt.append(MISSING)
                elif isinstance(cur, dict):
                    hint = (" — for the 1st result of a [*] or filter use '| first'" if multi
                            else " — the list may have been unwrapped by the unwrap: remove the index")
                    raise PathShapeError(f"'{path}': index {step.text} asked of an object{hint}")
                else:
                    raise PathShapeError(f"'{path}': index {step.text} asked of {_kind(cur)} {cur!r}")
            elif step.kind in ("star", "filter"):
                if cur is None:
                    nxt.append(MISSING)
                    continue
                if isinstance(cur, list):
                    items = cur
                elif isinstance(cur, dict):
                    items = list(cur.values()) if step.kind == "star" else [cur]   # object: an unwrapped 1:1 list
                else:
                    raise PathShapeError(f"'{path}': {step.text} asked of {_kind(cur)} {cur!r}")
                nxt.extend(items if step.kind == "star" else [i for i in items if _filter_ok(i, step.value, step.text)])
        current = nxt
        if step.kind in ("star", "filter"):
            multi = True
        if not multi and current == [MISSING]:
            # absent before any [*]/filter: the whole path does not exist (these are not missing elements)
            return Projection(path=path) if multi_path else MISSING
    if not multi_path:
        return current[0]
    values = [v for v in current if v is not MISSING]
    return Projection(values, len(current) - len(values), [None if v is MISSING else v for v in current], path)


@lru_cache(maxsize=4096)
def _prefixes(path: str) -> tuple[tuple[str, ...], str]:
    """'$parent.$parent.x' -> (('parent', 'parent'), 'x'). Cached: not parsed again for every row."""
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
    """Resolves a path taking the $root/$parent/$item/$index/$meta prefixes into account."""
    ops, rest = _prefixes(path)
    level = len(ctx.stack) - 1
    for op in ops:
        if op == "root":
            level = 0
        elif op == "parent":
            if level == 0:
                raise PathShapeError(f"'{path.strip()}': $parent above the root (the table has {len(ctx.stack) - 1} "
                                     f"explode level(s))")
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
    """For validation at load time: (starts at the root?, number of $parent from there, rest of the path).

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
    """'a[*].b[*]' -> ['a', 'b'];  ['a', 'b.c'] stays as it is."""
    if isinstance(spec, list):
        return spec
    parts = [p.strip(".") for p in re.split(r"\[\s*\*\s*\]", spec)]
    return [p for p in parts if p]
