"""Leitura de JSON Schemas gerados pelo SAMM (ex.: io.catenax.pcf/9.0.0/gen/Pcf-schema.json).

Gera um documento "esqueleto" com todos os campos do modelo, em que cada folha
conhece o seu tipo Athena. Serve para o `cx2iceberg init --schema`.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class Typed:
    """Folha de um documento sintético: só sabe o tipo."""

    def __init__(self, athena_type: str):
        self.athena_type = athena_type

    def __repr__(self):
        return f"Typed({self.athena_type})"


def _leaf_type(node: dict, ref_name: str | None) -> str:
    urn = node.get("x-samm-aspect-model-urn", "")
    names = {ref_name or "", urn.rsplit("#", 1)[-1]}
    if names & {"Timestamp"} or node.get("format") == "date-time":
        return "timestamp"
    if names & {"Date"} or node.get("format") == "date":
        return "date"
    t = node.get("type")
    if t == "integer":
        return "bigint"
    if t == "number":
        return "double"
    if t == "boolean":
        return "boolean"
    return "string"


def skeleton(schema: dict) -> Any:
    comps = schema.get("components", {}).get("schemas", {})

    def walk(node: dict, ref_name: str | None = None, seen: tuple = ()) -> Any:
        while "$ref" in node or ("allOf" in node and len(node["allOf"]) == 1):
            if "$ref" in node:
                ref_name = node["$ref"].rsplit("/", 1)[-1]
                if ref_name in seen:  # recursão no modelo
                    return Typed("json")
                seen = seen + (ref_name,)
                node = comps[ref_name]
            else:
                node = node["allOf"][0]
        for key in ("oneOf", "anyOf"):
            if key in node:  # escolhe a primeira alternativa
                return walk(node[key][0], ref_name, seen)
        t = node.get("type")
        if t == "object" or "properties" in node:
            return {k: walk(v, None, seen) for k, v in node.get("properties", {}).items()}
        if t == "array":
            return [walk(node.get("items", {}), None, seen)]
        return Typed(_leaf_type(node, ref_name))

    return walk(schema)


def load_skeleton(path: str | Path) -> tuple[Any, str | None]:
    """Devolve (documento esqueleto, URN do aspecto)."""
    schema = json.loads(Path(path).read_text(encoding="utf-8"))
    return skeleton(schema), schema.get("x-samm-aspect-model-urn")


# --- árvore de campos para o construtor visual (studio) --------------------
_ACRONYMS = {"id", "ids", "co2", "ghg", "pcf", "dqr", "bpn", "bpnl", "bpns", "url", "uri", "urn", "cpc", "ipcc", "ccs", "van", "uuid"}


def label(key: str) -> str:
    """'pcfExcludingBiogenicUptake' -> 'PCF excluding biogenic uptake'."""
    import re

    words = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", key)
    words = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", words).replace("_", " ").split()
    out = [("IDs" if w.lower() == "ids" else w.upper()) if w.lower() in _ACRONYMS else w.lower() for w in words]
    if out and out[0].islower():
        out[0] = out[0][:1].upper() + out[0][1:]
    return " ".join(out)


def _merge_all_of(node: dict, comps: dict) -> tuple[dict, str | None]:
    """Segue $ref e junta allOf (os traits SAMM vêm como allOf de várias restrições)."""
    ref_name = None
    seen = set()
    while True:
        if "$ref" in node:
            ref_name = node["$ref"].rsplit("/", 1)[-1]
            if ref_name in seen:
                return {"type": "object", "properties": {}}, ref_name
            seen.add(ref_name)
            extra = {k: v for k, v in node.items() if k != "$ref"}
            node = {**comps.get(ref_name, {}), **extra}
            continue
        if "allOf" in node:
            merged = {k: v for k, v in node.items() if k != "allOf"}
            for part in node["allOf"]:
                sub, name = _merge_all_of(part, comps)
                ref_name = ref_name or name
                for k, v in sub.items():
                    merged.setdefault(k, v)
            node = merged
            continue
        for key in ("oneOf", "anyOf"):
            if key in node:
                node = {**node[key][0], **{k: v for k, v in node.items() if k != key}}
                break
        else:
            return node, ref_name


def _applies(path: str, rule: dict | None, object_list: bool) -> bool:
    if not rule or path in rule["except"]:
        return False
    return path in rule["paths"] or (rule["all"] and object_list)


def _first(v: Any) -> Any:
    return v[0] if isinstance(v, list) and v else (None if isinstance(v, list) else v)


def field_tree(schema: dict, rule: dict | None = None, example: Any = None) -> list[dict]:
    """Campos do modelo, já com os caminhos que o motor usa depois do `unwrap`.

    Cada nó: key, label, description, path, kind (object|list|leaf|values), type, required,
    enum, minimum, maximum, pattern, example, in_example, single (lista tratada como 1 bloco), children.
    """
    comps = schema.get("components", {}).get("schemas", {})

    def walk(node: dict, raw_path: str, expr: str, ex: Any, depth: int) -> list[dict]:
        node, _ = _merge_all_of(node, comps)
        required = set(node.get("required", []))
        out = []
        for key, prop in (node.get("properties") or {}).items():
            desc = prop.get("description")
            resolved, ref_name = _merge_all_of(prop, comps)
            desc = desc or resolved.get("description")
            rp = f"{raw_path}.{key}" if raw_path else key
            p = f"{expr}.{key}" if expr else key
            value = ex.get(key) if isinstance(ex, dict) else None
            item = {"key": key, "label": label(key), "description": desc, "path": p, "required": key in required,
                    "in_example": isinstance(ex, dict) and key in ex}
            t = resolved.get("type")
            if (t == "object" or "properties" in resolved) and depth < 12:
                item.update(kind="object", children=walk(resolved, rp, p, value, depth + 1))
            elif t == "array":
                items, iref = _merge_all_of(resolved.get("items", {}), comps)
                if items.get("type") == "object" or "properties" in items:
                    single = _applies(rp, rule, True) or (isinstance(value, dict))
                    child_expr = p if single else f"{p}[*]"
                    item.update(kind="list", single=single, count=len(value) if isinstance(value, list) else None,
                                children=walk(items, rp, child_expr, value if single else _first(value), depth + 1))
                else:
                    base = _leaf_type(items, iref)
                    item.update(kind="values", type=f"array<{base}>", example=value)
            else:
                item.update(kind="leaf", type=_leaf_type(resolved, ref_name), example=value)
                for k in ("enum", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
                    if k in resolved:
                        item[k] = resolved[k]
                if "pattern" in resolved:
                    item["pattern"] = True
            out.append(item)
        return out

    return walk(schema, "", "", example, 0)
