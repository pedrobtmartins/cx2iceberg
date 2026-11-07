"""Validation of the paths when the configuration is loaded (before any document is read).

Always:
    * every path must be valid (columns, paths, templates, cases, where, check.required_if,
      explode, document_id.from) — before, a malformed path only failed on the data;
    * `$parent` cannot go above the root (the table has N explode levels);
    * `$index` only exists in tables with an explode;
    * `$meta.<key>` must be one of the known keys (`document_id` only with document_id declared).

With `model.schema` on the submodel, each path is walked in the schema, with the `unwrap` already applied:
    * a field that does not exist in the model (with a suggestion of the closest name);
    * a field asked of a list without [*], an index asked of a list the unwrap turns into an object;
    * an explode that does not point to a list;
    * `unwrap` paths that do not exist or are not lists.
A column with `outside_model: true` is not checked against the model (e.g. an extension field outside the schema).
Objects without `properties` (or with `additionalProperties` explicitly allowed) are open: they are not checked.
In SAMM schemas the properties are declared, and a field outside them is almost always a typo.
The `match` paths are not checked: they serve to recognise documents of other versions.
"""
from __future__ import annotations

import difflib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .paths import TEMPLATE_RE, context_refs, parse

META_KEYS = {"source_file", "semantic_id", "ingested_at"}
MAX_PROBLEMS = 40


# --- collecting the paths --------------------------------------------------------
def _cond_paths(cond: Any) -> Iterator[str]:
    if cond is None:
        return
    if isinstance(cond, list):
        for c in cond:
            yield from _cond_paths(c)
    elif isinstance(cond, dict):
        for key in ("all", "any"):
            for c in cond.get(key, []) or []:
                yield from _cond_paths(c)
        if "not" in cond:
            yield from _cond_paths(cond["not"])
        if isinstance(cond.get("path"), str):
            yield cond["path"]


def _source_paths(col) -> Iterator[str]:
    if col.path:
        yield col.path
    for p in col.paths or []:
        yield p
    if col.template is not None:
        for m in TEMPLATE_RE.finditer(col.template):
            yield m.group(1)


def column_paths(col) -> Iterator[tuple[str, bool]]:
    """(path, check against the model?) of a column."""
    check_model = not col.outside_model
    for p in _source_paths(col):
        yield p, check_model
    for cond, case in col.cases or []:
        for p in _cond_paths(cond):
            yield p, True
        for p in _source_paths(case):
            yield p, check_model
    if col.check and "required_if" in col.check:
        for p in _cond_paths(col.check["required_if"]):
            yield p, True


# --- the model's schema --------------------------------------------------------------
@dataclass
class Pos:
    node: dict | None          # schema node already resolved; None = unknown (not checked from there down)
    raw: str                   # path with keys only (what the unwrap uses)
    unwrapped: bool = False    # a list the unwrap turns into its single element
    source: dict | None = None  # the same node unresolved (with every oneOf/anyOf alternative)


class Problem(Exception):
    pass


class SchemaWalker:
    def __init__(self, schema: dict, rule: dict | None):
        from .samm import _merge_all_of  # noqa: PLC0415 - avoids a circular import

        self._merge = _merge_all_of
        self.comps = schema.get("components", {}).get("schemas", {})
        self.rule = rule
        self.root = self.pos(schema, "")

    def resolve(self, node: dict) -> dict:
        return self._merge(node or {}, self.comps)[0]

    @staticmethod
    def kind(n: dict | None) -> str:
        if n is None:
            return "unknown"
        t = n.get("type")
        if t == "object" or "properties" in n:
            return "object"
        if t == "array":
            return "array"
        if t in ("string", "number", "integer", "boolean") or "enum" in n:
            return "scalar"
        return "unknown"

    def pos(self, node: dict, raw: str) -> Pos:
        n = self.resolve(node)
        if self.kind(n) == "array":
            items = self.resolve(n.get("items", {}))
            rule = self.rule
            if rule and raw not in rule["except"] and (raw in rule["paths"] or (rule["all"] and self.kind(items) == "object")):
                return Pos(items if self.kind(items) != "unknown" else None, raw, True, n.get("items", {}))
        return Pos(n if self.kind(n) != "unknown" else None, raw, source=node)

    def walk(self, pos: Pos, path: str) -> Pos:
        shown = ""
        for step in parse(path):
            if pos.node is None:
                return pos
            n, k = pos.node, self.kind(pos.node)
            where = f" in '{shown}'" if shown else ""
            if step.kind == "key":
                if k == "array":
                    raise Problem(f"'{shown}' is a list in the model: use '{shown}[*].{step.value}'")
                if k == "scalar":
                    raise Problem(f"'{shown}' is a simple value in the model: it has no field '{step.value}'")
                props = n.get("properties") or {}
                if step.value not in props:
                    extra = n.get("additionalProperties")
                    if not props or extra is True or isinstance(extra, dict):
                        return Pos(None, pos.raw)   # open object (no declared properties): not checked
                    near = difflib.get_close_matches(step.value, list(props), n=1)
                    hint = f" Did you mean '{near[0]}'?" if near else ""
                    raise Problem(f"field '{step.value}' does not exist in the model{where}.{hint}")
                raw = f"{pos.raw}.{step.value}" if pos.raw else step.value
                pos = self.pos(props[step.value], raw)
            elif step.kind in ("star", "filter"):
                if pos.unwrapped:
                    if step.kind == "star":
                        return Pos(None, pos.raw)       # [*] on an object: its values (not checked)
                elif k == "array":
                    pos = self.pos(n.get("items", {}), pos.raw)
                else:
                    raise Problem(f"{step.text} asked of '{shown}', which is not a list in the model")
                if step.kind == "filter":
                    self.walk(pos, step.value[0])       # the filter's field exists in the elements
            elif step.kind == "idx":
                if pos.unwrapped:
                    raise Problem(f"'{shown}' is unwrapped by the unwrap (it becomes an object): remove the index {step.text}")
                if k != "array":
                    raise Problem(f"index {step.text} asked of '{shown}', which is not a list in the model")
                pos = self.pos(n.get("items", {}), pos.raw)
            shown += step.text if step.kind != "key" or not shown else f".{step.text}"
        return pos


# --- validation --------------------------------------------------------------------
def check_config(cfg) -> None:
    from .config import ConfigError  # noqa: PLC0415

    problems: list[str] = []
    for i, sub in enumerate(cfg.submodels):
        where = f"submodels[{i}] ('{sub.name}')"
        walker = _walker(cfg, sub, where, problems)
        sub.walker = walker
        if walker is not None:
            for t in sub.tables:
                if t.auto and t.auto_naming == "leaf":
                    t.leaf_names = _fixed_leaf_names(walker, t, f"{where}.tables['{t.name}']", problems)
        if walker is not None and sub.unwrap:
            for p in sorted(sub.unwrap["paths"] | sub.unwrap["except"]):
                _check_unwrap_path(walker, p, f"{where}.unwrap", problems)
        for p, _ in (sub.document_id or {}).get("from", []):
            _check(p, 0, [walker.root] if walker else None, walker, sub, f"{where}.document_id.from", problems, True)
        for t in sub.tables:
            levels = _explode_positions(walker, t, f"{where}.tables['{t.name}'].explode", problems)
            depth = len(t.explode)
            for col in t.columns:
                for p, model in column_paths(col):
                    _check(p, depth, levels, walker, sub, f"{where}.tables['{t.name}'].columns.{col.name}", problems, model)
            for p in _cond_paths(t.where):
                _check(p, depth, levels, walker, sub, f"{where}.tables['{t.name}'].where", problems, True)
    if problems:
        more = f"\n  … and {len(problems) - MAX_PROBLEMS} more" if len(problems) > MAX_PROBLEMS else ""
        raise ConfigError(f"{len(problems)} problem(s) in the paths:\n  " + "\n  ".join(problems[:MAX_PROBLEMS]) + more)


class LeafNames:
    """Leaf names fixed from the model: path -> column, and column -> path (to detect that a field
    outside the model, with its full name, would take the column of a model field)."""

    def __init__(self, names: dict[tuple[str, ...], str]):
        self.names = names
        self.owner = {n: ".".join(p) for p, n in names.items()}


def leaf_names_for(walker: SchemaWalker, pos: Pos, exclude: list[str], max_depth: int, lists: str) -> LeafNames:
    from .engine import column_names  # noqa: PLC0415 - avoids a circular import

    return LeafNames(column_names(sorted(model_leaf_paths(walker, pos, exclude, max_depth, lists)), "leaf"))


def _fixed_leaf_names(walker: SchemaWalker, t, where: str, problems: list[str]) -> LeafNames | None:
    try:
        pos = model_position(walker, t.explode)
    except Problem as e:
        problems.append(f"{where}: auto_naming: leaf — {e}")
        return None
    if pos is None:
        problems.append(f"{where}: auto_naming: leaf — the model does not describe this table's elements (open object): "
                        f"the names cannot be fixed. Use auto_naming: full")
        return None
    return leaf_names_for(walker, pos, t.auto_exclude, t.auto_max_depth, t.auto_lists)


def _walker(cfg, sub, where: str, problems: list[str]) -> SchemaWalker | None:
    if not sub.model:
        return None
    path = Path(os.path.normpath((cfg.base_dir or Path.cwd()) / sub.model["schema"]))
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        problems.append(f"{where}.model.schema: could not read {path}: {e}")
        return None
    return SchemaWalker(schema, sub.unwrap)


def _check_unwrap_path(walker: SchemaWalker, p: str, where: str, problems: list[str]) -> None:
    node, raw = walker.resolve(walker.root.node or {}), ""
    for key in p.split("."):
        if walker.kind(node) == "array":
            node = walker.resolve(node.get("items", {}))
        props = (node or {}).get("properties") or {}
        if key not in props:
            near = difflib.get_close_matches(key, list(props), n=1)
            problems.append(f"{where}: '{p}' does not exist in the model" + (f" (did you mean '{near[0]}'?)" if near else ""))
            return
        node = walker.resolve(props[key])
    if walker.kind(node) != "array":
        problems.append(f"{where}: '{p}' is not a list in the model")


def _explode_positions(walker, t, where: str, problems: list[str]) -> list[Pos] | None:
    if walker is None:
        for lv in t.explode:
            _parse_ok(lv, where, problems)
        return None
    levels = [walker.root]
    for lv in t.explode:
        if not _parse_ok(lv, where, problems):
            return None
        try:
            pos = walker.walk(levels[-1], lv)
        except Problem as e:
            problems.append(f"{where} '{lv}': {e}")
            return None
        if pos.node is not None and not pos.unwrapped:
            if walker.kind(pos.node) != "array":
                problems.append(f"{where} '{lv}': is not a list in the model")
                return None
            pos = walker.pos(pos.node.get("items", {}), pos.raw)
        levels.append(pos)
    return levels


def _parse_ok(path: str, where: str, problems: list[str]) -> bool:
    try:
        parse(path)
        return True
    except ValueError as e:
        problems.append(f"{where}: {e}")
        return False


def _check(path: str, depth: int, levels: list[Pos] | None, walker, sub, where: str, problems: list[str],
           model: bool) -> None:
    from_root, up, rest = context_refs(path)
    if from_root and up:
        problems.append(f"{where}: '{path}' — $parent after $root goes above the root")
        return
    if not from_root and up > depth:
        problems.append(f"{where}: '{path}' goes up {up} level(s) with $parent, but the table only has {depth} explode level(s)")
        return
    level = 0 if from_root else depth - up
    if rest == "$index":
        if level == 0:
            problems.append(f"{where}: '{path}' — $index only exists in tables with an explode (here it is always null)")
        return
    if rest.startswith("$meta"):
        key = rest[len("$meta"):].lstrip(".")
        allowed = META_KEYS | ({"document_id"} if sub.document_id else set())
        if key and key not in allowed:
            near = difflib.get_close_matches(key, sorted(allowed), n=1)
            problems.append(f"{where}: '{path}' — unknown key in $meta. Available: {sorted(allowed)}"
                            + (f" (did you mean '{near[0]}'?)" if near else ""))
        return
    if not rest:
        return
    if not _parse_ok(rest, f"{where}: '{path}'", problems):
        return
    if model and walker is not None and levels is not None:
        try:
            walker.walk(levels[level], rest)
        except Problem as e:
            problems.append(f"{where}: '{path}' — {e}")


# --- the model's shapes for auto mode ------------------------------------------------------------
# Auto mode (`auto` columns, `auto_normalize` tables) sees the fields present in the documents. Two decisions
# cannot depend on that: the `leaf` names (the same field went to `fossil` in one document and `ghg_fossil` in another)
# and the auto_normalize child tables (a list that was empty in every document of the batch produced no
# table, and the old rows stayed). With `model.schema` both come from the model, the same in every load.
def variants(walker: SchemaWalker, node: dict | None, seen: tuple = ()) -> list[dict]:
    """The node in all its shapes: $ref followed, allOf merged, and each oneOf/anyOf alternative (the
    SchemaWalker keeps only the first; here a field that only exists in the 2nd alternative also counts)."""
    node = node or {}
    if "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[-1]
        if name in seen:
            return [{"type": "object", "properties": {}}]
        extra = {k: v for k, v in node.items() if k != "$ref"}
        return variants(walker, {**walker.comps.get(name, {}), **extra}, seen + (name,))
    for key in ("oneOf", "anyOf"):
        if key in node:
            rest = {k: v for k, v in node.items() if k != key}
            return [v for alt in node[key] for v in variants(walker, {**alt, **rest}, seen)]
    if "allOf" in node:
        return [walker.resolve(node)]
    return [node]


def _unwrapped(walker: SchemaWalker, raw: str, items: list[dict]) -> bool:
    """Is the list in `raw` turned into an object by the unwrap? (same rule as engine.unwrap)"""
    rule = walker.rule
    if not rule or raw in rule["except"]:
        return False
    return raw in rule["paths"] or (rule["all"] and any(walker.kind(i) == "object" for i in items))


def _keeps_lists(walker: SchemaWalker) -> bool:
    return bool(walker.rule) and walker.rule.get("on_multiple") == "keep"


def model_position(walker: SchemaWalker, explode: list[str]) -> Pos | None:
    """Position in the model of the elements of a table with these explode levels (None = the model does not describe it)."""
    pos = walker.root
    for lv in explode:
        pos = walker.walk(pos, lv)
        if pos.node is None:
            return None
        if not pos.unwrapped:
            if walker.kind(pos.node) != "array":
                raise Problem(f"'{lv}' is not a list in the model")
            pos = walker.pos(pos.node.get("items", {}), pos.raw)
    return pos if pos.node is not None else None


def model_leaf_paths(walker: SchemaWalker, pos: Pos, exclude: list[str], max_depth: int, lists: str) -> set[tuple[str, ...]]:
    """Every path that auto mode (engine._leaves) can produce from this position, in any valid
    document. The rules are the same: objects up to max_depth, lists of objects are not columns with lists=array."""
    from .config import sanitize  # noqa: PLC0415 - avoids a circular import

    out: set[tuple[str, ...]] = set()

    def obj(node: dict, raw: str, parts: tuple[str, ...], depth: int):
        for v in variants(walker, node):
            for k, child in (v.get("properties") or {}).items():
                p = parts + (k,)
                if ".".join(p) in exclude or k in exclude or sanitize("_".join(p)) in exclude:
                    continue
                craw = f"{raw}.{k}" if raw else k
                for c in variants(walker, child):
                    kind = walker.kind(c)
                    if kind == "array":
                        items = variants(walker, c.get("items", {}))
                        unwrapped = _unwrapped(walker, craw, items)
                        if unwrapped:
                            for i in items:
                                as_dict(i, craw, p, depth)
                        if not unwrapped or _keeps_lists(walker):
                            objects = any(walker.kind(i) in ("object", "array") for i in items)
                            if not (lists == "skip" or (objects and lists == "array")):
                                out.add(p)
                    elif kind == "object":
                        as_dict(c, craw, p, depth)
                    else:
                        out.add(p)

    def as_dict(node: dict, raw: str, p: tuple[str, ...], depth: int):
        if depth + 1 < max_depth:
            obj(node, raw, p, depth + 1)
        else:
            out.add(p)

    obj(pos.source if pos.source is not None else pos.node, pos.raw, (), 0)
    return out


def model_list_paths(walker: SchemaWalker, max_depth: int) -> set[tuple[str, ...]]:
    """The model's lists of objects, in the format of engine._array_paths (one path per explode level),
    with the unwrap already applied: the child tables auto_normalize can generate, whether or not the batch has data."""
    found: set[tuple[str, ...]] = set()

    def walk(node: dict, raw: str, levels: tuple[str, ...], cur: tuple[str, ...], depth: int):
        if depth > max_depth:
            return
        for v in variants(walker, node):
            for k, child in (v.get("properties") or {}).items():
                craw = f"{raw}.{k}" if raw else k
                for c in variants(walker, child):
                    kind = walker.kind(c)
                    if kind == "object":
                        walk(c, craw, levels, cur + (k,), depth + 1)
                    elif kind == "array":
                        items = variants(walker, c.get("items", {}))
                        unwrapped = _unwrapped(walker, craw, items)
                        if unwrapped:
                            for i in items:
                                walk(i, craw, levels, cur + (k,), depth + 1)
                        if (not unwrapped or _keeps_lists(walker)) and any(walker.kind(i) == "object" for i in items):
                            lv = levels + (".".join(cur + (k,)),)
                            found.add(lv)
                            for i in items:
                                walk(i, craw, lv, (), depth + 1)

    walk(walker.root.source or {}, "", (), (), 0)
    return found
