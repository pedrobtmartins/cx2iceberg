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
