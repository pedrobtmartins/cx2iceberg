"""Leitura e validação da configuração YAML."""
from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from . import athena_types as T
from .paths import split_explode

MODES = {"append", "overwrite", "overwrite_partitions", "merge"}
ON_ERROR = {"fail", "null", "skip_row"}
METADATA = ("source_file", "semantic_id", "ingested_at", "record_hash")


class ConfigError(ValueError):
    pass


@dataclass
class ColumnSpec:
    name: str
    path: str | None = None
    paths: list[str] | None = None          # coalesce: primeiro não-nulo
    template: str | None = None             # "{$root.catenaXId}-{key}"
    const: Any = None
    has_const: bool = False
    type: str | None = None                 # None = inferido
    transform: Any = None
    default: Any = None
    required: bool = False
    on_error: str | None = None
    comment: str | None = None
    cases: list[tuple[Any, "ColumnSpec"]] | None = None  # [(condição | None, origem)], a 1ª que passa ganha
    check: dict[str, Any] | None = None                  # validações depois do cast


@dataclass
class TableSpec:
    name: str
    columns: list[ColumnSpec]
    explode: list[str] = field(default_factory=list)
    keep_empty: bool = False                # explode "outer": gera linha mesmo com lista vazia
    where: Any = None
    keys: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    mode: str = "append"
    dedupe: bool = True
    auto: bool = False                      # achata automaticamente os campos escalares
    auto_exclude: list[str] = field(default_factory=list)
    auto_max_depth: int = 5
    auto_naming: str = "full"               # full (a_b_c) | leaf (c, sobe níveis só se colidir)
    auto_collision: str = "skip"            # skip | prefix (item_<nome>) quando o nome já existe
    auto_dates: bool = True                 # strings ISO-8601 -> timestamp no modo auto
    auto_lists: str = "array"               # array | json | skip  (listas de objectos -> json|skip)
    on_error: str = "fail"
    metadata_columns: list[str] = field(default_factory=list)
    replace_by: list[str] = field(default_factory=list)  # DELETE ... WHERE col IN (lote) antes de inserir
    location: str | None = None
    comment: str | None = None
    athena_options: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubmodelSpec:
    name: str
    match: dict[str, Any]
    tables: list[TableSpec]
    auto_normalize: dict[str, Any] | None = None
    defaults: dict[str, Any] = field(default_factory=dict)
    common_columns: list[ColumnSpec] = field(default_factory=list)
    unwrap: dict[str, Any] | None = None
    document_id: dict[str, Any] | None = None  # UUID por documento, em todas as tabelas do submodelo


@dataclass
class Config:
    target: dict[str, Any]
    submodels: list[SubmodelSpec]
    input: dict[str, Any]
    defaults: dict[str, Any]


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env(text: str) -> str:
    def repl(m):
        val = os.environ.get(m.group(1), m.group(2))
        if val is None:
            raise ConfigError(f"Variável de ambiente '{m.group(1)}' não definida (use ${{{m.group(1)}:-valor}})")
        return val

    return _ENV_RE.sub(repl, text)


def sanitize(name: str) -> str:
    """Nome válido para Athena: minúsculas, [a-z0-9_], camelCase -> snake_case."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", s).strip("_").lower()
    s = re.sub(r"_+", "_", s)
    if not s:
        raise ConfigError(f"Nome inválido: '{name}'")
    return f"c_{s}" if s[0].isdigit() else s


def _check_keys(d: dict, allowed: set[str], where: str):
    unknown = set(d) - allowed
    if unknown:
        raise ConfigError(f"{where}: chave(s) desconhecida(s) {sorted(unknown)}. Permitidas: {sorted(allowed)}")


_COL_KEYS = {"path", "paths", "template", "const", "type", "transform", "default", "required", "on_error", "comment",
             "cases", "check"}
_SOURCES = ("path", "paths", "template", "const")
_CHECK_KEYS = {"regex", "min", "max", "in", "ignore_case", "required", "required_if", "on_fail"}
CHECK_ON_FAIL = {"warn", "null", "skip_row", "fail"}


def parse_cases(name: str, spec: Any, where: str) -> list[tuple[Any, ColumnSpec]]:
    """cases: [{when: <condição>, path|paths|template|const: ..., transform: ...}, ..., {else: valor}]"""
    if not isinstance(spec, list) or not spec:
        raise ConfigError(f"{where}: 'cases' tem de ser uma lista não vazia")
    out = []
    for i, case in enumerate(spec):
        w = f"{where}[{i}]"
        if not isinstance(case, dict):
            raise ConfigError(f"{w}: cada caso é um objecto com when + origem, ou {{else: valor}}")
        if "else" in case:
            if len(case) != 1:
                raise ConfigError(f"{w}: 'else' não se combina com outras chaves (use {{path: ...}} sem 'when')")
            out.append((None, ColumnSpec(name, const=case["else"], has_const=True)))
            continue
        _check_keys(case, {"when", "transform", *_SOURCES}, w)
        if not any(k in case for k in _SOURCES):
            raise ConfigError(f"{w}: falta a origem do valor (path, paths, template ou const)")
        body = {k: v for k, v in case.items() if k != "when"}
        out.append((case.get("when"), parse_column(name, body, w)))
    return out


def parse_check(spec: Any, where: str) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise ConfigError(f"{where}: 'check' tem de ser um objecto")
    _check_keys(spec, _CHECK_KEYS, where)
    chk = dict(spec)
    chk.setdefault("on_fail", "warn")
    if chk["on_fail"] not in CHECK_ON_FAIL:
        raise ConfigError(f"{where}.on_fail deve ser um de {sorted(CHECK_ON_FAIL)}")
    if "regex" in chk:
        try:
            chk["_regex"] = re.compile(chk["regex"], re.IGNORECASE if chk.get("ignore_case") else 0)
        except re.error as e:
            raise ConfigError(f"{where}.regex inválido: {e}") from e
    if "in" in chk:
        vals = chk["in"] if isinstance(chk["in"], list) else [chk["in"]]
        chk["_in"] = {str(v).lower() if chk.get("ignore_case") else str(v) for v in vals}
    return chk


_SHORT_TYPE = re.compile(r"\s*::\s*([a-z0-9_<>(),\s]+?)\s*$", re.IGNORECASE)
_SHORT_TF = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\((.*)\))?\s*$")


def parse_short(spec: str, where: str) -> dict[str, Any]:
    """Forma compacta: 'caminho | transf | transf(arg) ::tipo'.

    Em YAML sem aspas, '::' tem de vir colado ao tipo ('::int'): ': ' iniciaria um mapa.
    """
    out: dict[str, Any] = {}
    m = _SHORT_TYPE.search(spec)
    if m:
        spec, out["type"] = spec[:m.start()], m.group(1)
    path, *tfs = [s.strip() for s in spec.split(" | ")]
    out["path"] = path
    chain = []
    for tf in tfs:
        m = _SHORT_TF.match(tf)
        if not m:
            raise ConfigError(f"{where}: transformação inválida '{tf}' (use nome ou nome(arg))")
        chain.append(m.group(1) if m.group(2) is None else {m.group(1): yaml.safe_load(m.group(2))})
    if chain:
        out["transform"] = chain
    return out


def parse_column(name: str, spec: Any, where: str) -> ColumnSpec:
    col_name = sanitize(name)
    if isinstance(spec, str):
        spec = parse_short(spec, f"{where}.{name}")
    if spec is None:
        return ColumnSpec(col_name, path=name)  # coluna: ~  -> usa o próprio nome como caminho
    if not isinstance(spec, dict):
        return ColumnSpec(col_name, const=spec, has_const=True)
    _check_keys(spec, _COL_KEYS, f"{where}.{name}")
    sources = [k for k in ("path", "paths", "template", "const", "cases") if k in spec]
    if len(sources) > 1:
        raise ConfigError(f"{where}.{name}: use apenas um de path/paths/template/const/cases")
    col = ColumnSpec(
        col_name,
        path=spec.get("path"),
        paths=spec.get("paths"),
        template=spec.get("template"),
        const=spec.get("const"),
        has_const="const" in spec,
        type=T.normalize(spec["type"]) if spec.get("type") else None,
        transform=spec.get("transform"),
        default=spec.get("default"),
        required=bool(spec.get("required", False)),
        on_error=spec.get("on_error"),
        comment=spec.get("comment"),
        cases=parse_cases(col_name, spec["cases"], f"{where}.{name}.cases") if "cases" in spec else None,
        check=parse_check(spec["check"], f"{where}.{name}.check") if "check" in spec else None,
    )
    if not sources and col.transform is None:
        col.path = name
    if col.on_error and col.on_error not in ON_ERROR:
        raise ConfigError(f"{where}.{name}: on_error deve ser um de {sorted(ON_ERROR)}")
    return col


def parse_columns(spec: Any, where: str) -> list[ColumnSpec]:
    if spec is None:
        return []
    if isinstance(spec, list):  # lista de nomes = caminhos com o mesmo nome
        return [parse_column(c, None, where) if isinstance(c, str) else parse_column(*next(iter(c.items())), where) for c in spec]
    return [parse_column(k, v, where) for k, v in spec.items()]


_TABLE_KEYS = {
    "name", "columns", "explode", "keep_empty", "where", "keys", "partition_by", "mode", "dedupe",
    "auto", "auto_exclude", "auto_max_depth", "auto_lists", "auto_dates", "auto_collision", "auto_naming", "on_error", "metadata_columns",
    "location", "comment", "athena_options", "replace_by",
}


def _metadata(value: Any) -> list[str]:
    if value is True:
        return list(METADATA)
    if not value:
        return []
    bad = set(value) - set(METADATA)
    if bad:
        raise ConfigError(f"metadata_columns desconhecidas: {sorted(bad)}. Permitidas: {METADATA}")
    return list(value)


def parse_table(spec: dict, defaults: dict, common: list[ColumnSpec], where: str) -> TableSpec:
    _check_keys(spec, _TABLE_KEYS, where)
    if "name" not in spec:
        raise ConfigError(f"{where}: falta 'name'")
    merged = {**defaults, **spec}
    explicit = parse_columns(spec.get("columns"), f"{where}.columns")
    names = {c.name for c in explicit}
    columns = [c for c in common if c.name not in names] + explicit
    t = TableSpec(
        name=sanitize(spec["name"]),
        columns=columns,
        explode=split_explode(spec["explode"]) if spec.get("explode") else [],
        keep_empty=bool(merged.get("keep_empty", False)),
        where=spec.get("where"),
        keys=[sanitize(k) for k in merged.get("keys", []) or []],
        partition_by=list(merged.get("partition_by", []) or []),
        mode=merged.get("mode", "append"),
        dedupe=bool(merged.get("dedupe", True)),
        auto=bool(merged.get("auto", False)),
        auto_exclude=list(merged.get("auto_exclude", []) or []),
        auto_max_depth=int(merged.get("auto_max_depth", 5)),
        auto_lists=merged.get("auto_lists", "array"),
        auto_dates=bool(merged.get("auto_dates", True)),
        auto_collision=merged.get("auto_collision", "skip"),
        auto_naming=merged.get("auto_naming", "full"),
        on_error=merged.get("on_error", "fail"),
        metadata_columns=_metadata(merged.get("metadata_columns", False)),
        replace_by=[sanitize(k) for k in merged.get("replace_by", []) or []],
        location=spec.get("location"),
        comment=spec.get("comment"),
        athena_options={**defaults.get("athena_options", {}), **spec.get("athena_options", {})},
    )
    if t.mode not in MODES:
        raise ConfigError(f"{where}: mode '{t.mode}' inválido. Use {sorted(MODES)}")
    if t.mode == "merge" and not t.keys:
        raise ConfigError(f"{where}: mode 'merge' precisa de 'keys'")
    if t.on_error not in ON_ERROR:
        raise ConfigError(f"{where}: on_error deve ser um de {sorted(ON_ERROR)}")
    if t.auto_naming not in ("full", "leaf"):
        raise ConfigError(f"{where}: auto_naming deve ser full|leaf")
    if t.auto_lists not in ("array", "json", "skip"):
        raise ConfigError(f"{where}: auto_lists deve ser array|json|skip")
    if not t.columns and not t.auto:
        raise ConfigError(f"{where}: defina 'columns' ou 'auto: true'")
    return t


_SUBMODEL_KEYS = {"name", "match", "tables", "auto_normalize", "defaults", "common_columns", "unwrap", "document_id"}
DOC_ID_STRATEGIES = {"key", "content", "random"}


def parse_document_id(spec: Any, where: str) -> dict[str, Any] | None:
    """document_id: true  |  {column: doc_id, from: [catenaXId], strategy: key|content|random, namespace: ...}"""
    if not spec:
        return None
    if spec is True:
        spec = {}
    if not isinstance(spec, dict):
        raise ConfigError(f"{where}: use true ou um objecto {{column, from, strategy, namespace}}")
    _check_keys(spec, {"column", "from", "strategy", "namespace"}, where)
    src = spec.get("from")
    src = [src] if isinstance(src, str) else list(src or [])
    strategy = spec.get("strategy", "key" if src else "content")
    if strategy not in DOC_ID_STRATEGIES:
        raise ConfigError(f"{where}.strategy deve ser um de {sorted(DOC_ID_STRATEGIES)}")
    if strategy == "key" and not src:
        raise ConfigError(f"{where}: strategy 'key' precisa de 'from' (caminhos dos campos que identificam o documento)")
    return {"column": sanitize(spec.get("column", "document_id")), "from": src, "strategy": strategy,
            "namespace": str(spec.get("namespace", "cx2iceberg"))}


def parse_unwrap(spec: Any, where: str) -> dict[str, Any] | None:
    """unwrap: [a.b, c]  |  "*"  |  {all: true, paths: [...], except: [...], on_multiple: fail|first|keep}"""
    if not spec:
        return None
    if spec == "*" or spec is True:
        spec = {"all": True}
    elif isinstance(spec, list):
        spec = {"paths": spec}
    _check_keys(spec, {"all", "paths", "except", "on_multiple"}, where)
    rule = {
        "all": bool(spec.get("all", False)),
        "paths": set(spec.get("paths", []) or []),
        "except": set(spec.get("except", []) or []),
        "on_multiple": spec.get("on_multiple", "fail"),
    }
    if rule["on_multiple"] not in ("fail", "first", "keep"):
        raise ConfigError(f"{where}.on_multiple deve ser fail|first|keep")
    bad = [p for p in rule["paths"] | rule["except"] if "[" in p]
    if bad:
        raise ConfigError(f"{where}: caminhos de unwrap não levam índices: {bad}")
    return rule
_DEFAULT_KEYS = _TABLE_KEYS - {"name", "columns", "explode", "where", "location", "comment"}


def load(path: str | Path) -> Config:
    text = _expand_env(Path(path).read_text(encoding="utf-8"))
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        hint = ""
        if "mapping values are not allowed" in str(e):
            hint = " Dica: na forma compacta escreva '::int' (sem espaço depois de '::') ou ponha o valor entre aspas."
        raise ConfigError(f"YAML inválido em {path}: {e}.{hint}") from e
    return from_dict(raw)


def from_dict(raw: dict) -> Config:
    _check_keys(raw, {"target", "input", "defaults", "submodels", "plugins"}, "config")
    for mod in raw.get("plugins", []) or []:
        importlib.import_module(mod)
    target = raw.get("target", {}) or {}
    _check_keys(target, {"database", "location", "temp_path", "workgroup", "region", "profile", "create_database", "schema_evolution"}, "target")
    defaults = raw.get("defaults", {}) or {}
    _check_keys(defaults, _DEFAULT_KEYS, "defaults")
    subs = []
    for i, s in enumerate(raw.get("submodels", []) or []):
        where = f"submodels[{i}]"
        _check_keys(s, _SUBMODEL_KEYS, where)
        name = s.get("name") or f"submodel_{i}"
        sdefaults = {**defaults, **(s.get("defaults") or {})}
        common = parse_columns(s.get("common_columns"), f"{where}.common_columns")
        tables = [parse_table(t, sdefaults, common, f"{where}.tables[{j}]") for j, t in enumerate(s.get("tables", []) or [])]
        an = s.get("auto_normalize")
        if an is True:
            an = {}
        if an is not None:
            _check_keys(an, {"table_prefix", "id_path", "id_column", "exclude", "max_depth", "naming"}, f"{where}.auto_normalize")
        if not tables and an is None:
            raise ConfigError(f"{where}: defina 'tables' e/ou 'auto_normalize'")
        subs.append(SubmodelSpec(name, s.get("match") or {}, tables, an, sdefaults, common,
                                 parse_unwrap(s.get("unwrap"), f"{where}.unwrap"),
                                 parse_document_id(s.get("document_id"), f"{where}.document_id")))
    if not subs:
        raise ConfigError("config: não há 'submodels'")
    return Config(target, subs, raw.get("input", {}) or {}, defaults)
