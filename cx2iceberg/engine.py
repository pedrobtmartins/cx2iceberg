"""Aplica as regras da configuração a documentos JSON e produz linhas por tabela."""
from __future__ import annotations

import dataclasses
import fnmatch
import hashlib
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from . import athena_types as T
from .config import ColumnSpec, Config, ConfigError, SubmodelSpec, TableSpec, _metadata, sanitize
from .paths import MISSING, Context, resolve
from .transforms import apply_chain

log = logging.getLogger("cx2iceberg")

METADATA_TYPES = {
    "source_file": ("cx_source_file", "string"),
    "semantic_id": ("cx_semantic_id", "string"),
    "ingested_at": ("cx_ingested_at", "timestamp"),
    "record_hash": ("cx_record_hash", "string"),
}


@dataclass
class Document:
    data: Any
    source_file: str | None = None
    semantic_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)  # ex.: document_id


@dataclass
class TableResult:
    spec: TableSpec
    rows: list[dict[str, Any]] = field(default_factory=list)
    types: dict[str, str] = field(default_factory=dict)
    skipped: int = 0
    check_failures: dict[str, int] = field(default_factory=dict)  # coluna -> nº de valores que falharam o check


ISO_DATETIME = T.ISO_DATETIME


class RowError(Exception):
    pass


class TransformError(ValueError):
    pass


# --- condições (where / match.where) ------------------------------------
_OPS = {"eq", "ne", "in", "not_in", "exists", "missing", "regex", "gt", "gte", "lt", "lte", "contains", "empty", "not_empty"}


def evaluate(cond: Any, ctx: Context) -> bool:
    if cond is None:
        return True
    if isinstance(cond, list):
        return all(evaluate(c, ctx) for c in cond)
    if "all" in cond:
        return all(evaluate(c, ctx) for c in cond["all"])
    if "any" in cond:
        return any(evaluate(c, ctx) for c in cond["any"])
    if "not" in cond:
        return not evaluate(cond["not"], ctx)
    op = cond.get("op", "eq" if "value" in cond else "exists")
    if op not in _OPS:
        raise ConfigError(f"Operador '{op}' desconhecido. Use {sorted(_OPS)}")
    v = resolve(ctx, cond["path"])
    exp = cond.get("value")
    if op == "exists":
        return v is not MISSING and v is not None
    if op == "missing":
        return v is MISSING or v is None
    if op == "empty":
        return v in (MISSING, None, "", [], {})
    if op == "not_empty":
        return v not in (MISSING, None, "", [], {})
    if v is MISSING or v is None:
        return op in ("ne", "not_in")
    if op == "eq":
        return v == exp or str(v) == str(exp)
    if op == "ne":
        return not (v == exp or str(v) == str(exp))
    if op == "in":
        return v in exp or str(v) in map(str, exp)
    if op == "not_in":
        return not (v in exp or str(v) in map(str, exp))
    if op == "regex":
        return re.search(exp, str(v)) is not None
    if op == "contains":
        return exp in v if isinstance(v, (list, str)) else False
    try:
        a, b = (float(v), float(exp)) if not isinstance(exp, str) else (str(v), exp)
    except (TypeError, ValueError):
        a, b = str(v), str(exp)
    return {"gt": a > b, "gte": a >= b, "lt": a < b, "lte": a <= b}[op]


# --- matching de submodelos ---------------------------------------------
def matches(sub: SubmodelSpec, doc: Document) -> bool:
    return _match(sub.match, doc)


def _match(m: dict, doc: Document) -> bool:
    """Todos os critérios têm de passar; `any: [{...}, {...}]` passa se um dos blocos passar."""
    if not m:
        return True
    if "any" in m and not any(_match(x, doc) for x in m["any"]):
        return False
    if "semantic_id" in m:
        pats = m["semantic_id"] if isinstance(m["semantic_id"], list) else [m["semantic_id"]]
        if not doc.semantic_id or not any(fnmatch.fnmatchcase(doc.semantic_id, p) for p in pats):
            return False
    if "file" in m:
        pats = m["file"] if isinstance(m["file"], list) else [m["file"]]
        f = doc.source_file or ""
        if not any(fnmatch.fnmatch(f, p) or fnmatch.fnmatch(f.rsplit("/", 1)[-1], p) for p in pats):
            return False
    ctx = Context([(doc.data, None)])
    for p in m.get("has_paths", []) or []:
        if resolve(ctx, p) in (MISSING, None):
            return False
    return evaluate(m.get("where"), ctx)


# --- motor ---------------------------------------------------------------
class Engine:
    def __init__(self, config: Config, match_all: bool | None = None):
        self.config = config
        self.match_all = config.input.get("match_all", False) if match_all is None else match_all
        self.ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.validation: list[dict[str, Any]] = []   # documentos que não cumprem o schema do modelo

    def route(self, docs: Iterable[Document]) -> dict[str, list[Document]]:
        routed: dict[str, list[Document]] = {s.name: [] for s in self.config.submodels}
        for doc in docs:
            hit = False
            for sub in self.config.submodels:
                if matches(sub, doc):
                    routed[sub.name].append(doc)
                    hit = True
                    if not self.match_all:
                        break
            if not hit:
                log.warning("Nenhum submodelo da configuração corresponde a %s (semantic_id=%s)", doc.source_file, doc.semantic_id)
        return routed

    def run(self, docs: Iterable[Document]) -> dict[str, TableResult]:
        results: dict[str, TableResult] = {}
        routed = self.route(docs)
        for sub in self.config.submodels:
            sub_docs = routed[sub.name]
            if sub.validate != "off":
                sub_docs = self._validate(sub, sub_docs)
            if sub.unwrap:
                sub_docs = [Document(unwrap(d.data, sub.unwrap, d.source_file), d.source_file, d.semantic_id,
                                     {**d.meta, "_raw": d.data}) for d in sub_docs]
            if sub.document_id:
                sub_docs = [self._with_document_id(sub, d) for d in sub_docs]
            tables = list(sub.tables)
            if sub.auto_normalize is not None:
                tables += normalize_specs(sub, sub_docs)
            if sub.document_id:
                tables = [_with_column(t, ColumnSpec(sub.document_id["column"], path="$meta.document_id", type="string"))
                          for t in tables]
            for spec in tables:
                res = results.setdefault(spec.name, TableResult(spec))
                if res.spec is not spec and res.spec.columns != spec.columns:
                    log.info("Tabela '%s' alimentada por mais de uma regra", spec.name)
                for doc in sub_docs:
                    self._rows_for(spec, doc, res)
        for res in results.values():
            self._finalize(res)
        return results

    def _validate(self, sub: SubmodelSpec, docs: list[Document]) -> list[Document]:
        validator = schema_validator(self.config, sub)
        kept = []
        for d in docs:
            problems = [describe_error(e) for e in sorted(validator.iter_errors(d.data), key=lambda e: list(e.absolute_path))]
            if not problems:
                kept.append(d)
                continue
            self.validation.append({"source": d.source_file, "submodel": sub.name, "errors": problems})
            summary = "; ".join(f"{p['path'] or '(raiz)'}: {p['message']}" for p in problems[:3])
            more = f" (+{len(problems) - 3})" if len(problems) > 3 else ""
            msg = f"{d.source_file} não cumpre o schema de '{sub.name}': {summary}{more}"
            if sub.validate == "fail":
                raise TransformError(msg)
            if sub.validate == "skip":
                log.warning("%s — documento ignorado", msg)
                continue
            log.warning(msg)
            kept.append(d)
        return kept

    def _with_document_id(self, sub: SubmodelSpec, doc: Document) -> Document:
        rule = sub.document_id
        raw = doc.meta.get("_raw", doc.data)  # conteúdo como chegou (antes do unwrap)
        if rule["strategy"] == "random":
            value = str(uuid.uuid4())
        else:
            ns = uuid.uuid5(uuid.NAMESPACE_URL, rule["namespace"])
            name = None
            if rule["strategy"] == "key":
                ctx = Context([(doc.data, None)])
                parts = [resolve(ctx, p) for p in rule["from"]]
                if any(p in (MISSING, None, "") for p in parts):
                    log.warning("document_id: campo(s) %s em falta em %s — usado o conteúdo do documento",
                                [p for p, v in zip(rule["from"], parts) if v in (MISSING, None, "")], doc.source_file)
                else:
                    name = "key:" + "|".join(json.dumps(p, sort_keys=True, default=str) for p in parts)
            if name is None:
                name = "content:" + json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
            value = str(uuid.uuid5(ns, name))
        meta = {k: v for k, v in doc.meta.items() if k != "_raw"}
        return Document(doc.data, doc.source_file, doc.semantic_id, {**meta, "document_id": value})

    # --- geração de linhas -----------------------------------------------
    def _contexts(self, ctx: Context, levels: list[str], keep_empty: bool) -> Iterator[Context]:
        if not levels:
            yield ctx
            return
        val = resolve(ctx, levels[0])
        items = [] if val is MISSING or val is None else val if isinstance(val, list) else [val]
        if not items and keep_empty:
            yield from self._contexts(ctx.child({}, None), levels[1:], keep_empty)
        for i, item in enumerate(items):
            yield from self._contexts(ctx.child(item, i), levels[1:], keep_empty)

    def _rows_for(self, spec: TableSpec, doc: Document, res: TableResult):
        self._res = res
        meta = {"source_file": doc.source_file, "semantic_id": doc.semantic_id, "ingested_at": self.ingested_at,
                **{k: v for k, v in doc.meta.items() if not k.startswith("_")}}
        root = Context([(doc.data, None)], meta)
        for ctx in self._contexts(root, spec.explode, spec.keep_empty):
            if not evaluate(spec.where, ctx):
                continue
            try:
                row = self._row(spec, ctx)
            except RowError as e:
                res.skipped += 1
                log.debug("Linha ignorada em %s: %s", spec.name, e)
                continue
            res.rows.append(row)

    def _row(self, spec: TableSpec, ctx: Context) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for col in spec.columns:
            row[col.name] = self._value(spec, col, ctx)
        if spec.auto:
            obj = ctx.stack[-1][0]
            if isinstance(obj, dict):
                for name, val in flatten(obj, spec.auto_exclude, spec.auto_max_depth, spec.auto_lists, spec.auto_naming).items():
                    if spec.auto_dates and isinstance(val, str) and ISO_DATETIME.match(val):
                        val = T.cast(val, "timestamp")
                    if name in row and spec.auto_collision == "prefix":
                        name = f"item_{name}"
                    row.setdefault(name, val)
            elif obj is not None and "value" not in row:
                row["value"] = obj  # lista de escalares explodida
        if "record_hash" in spec.metadata_columns:
            row["cx_record_hash"] = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
        for m in spec.metadata_columns:
            if m != "record_hash":
                row[METADATA_TYPES[m][0]] = ctx.meta.get(m)
        return row

    def _value(self, spec: TableSpec, col: ColumnSpec, ctx: Context) -> Any:
        on_error = col.on_error or spec.on_error
        raw: Any = None
        try:
            if col.cases is not None:
                for cond, case in col.cases:
                    if cond is None or evaluate(cond, ctx):
                        raw = apply_chain(_source(case, ctx), case.transform)
                        break
            else:
                raw = _source(col, ctx)
            v = apply_chain(raw, col.transform)
            if v is None and col.default is not None:
                v = col.default
            if col.type and not T.needs_inference(col.type):
                v = T.cast(v, col.type)
        except Exception as e:  # noqa: BLE001 - erros do utilizador nas regras
            msg = f"tabela '{spec.name}', coluna '{col.name}', ficheiro '{ctx.meta.get('source_file')}', valor {raw!r}: {e}"
            if on_error == "null":
                log.warning("Valor anulado — %s", msg)
                return None
            if on_error == "skip_row":
                raise RowError(msg) from e
            raise TransformError(msg) from e
        if col.required and v is None:
            msg = f"coluna obrigatória '{col.name}' vazia na tabela '{spec.name}' ({ctx.meta.get('source_file')})"
            if on_error == "skip_row":
                raise RowError(msg)
            raise TransformError(msg)
        if col.check:
            v = self._check(spec, col, v, ctx)
        return v

    def _check(self, spec: TableSpec, col: ColumnSpec, v: Any, ctx: Context) -> Any:
        problem = check_value(col.check, v, ctx)
        if problem is None:
            return v
        res = getattr(self, "_res", None)
        if res is not None:
            res.check_failures[col.name] = res.check_failures.get(col.name, 0) + 1
        msg = f"check falhou — tabela '{spec.name}', coluna '{col.name}', ficheiro '{ctx.meta.get('source_file')}': {problem}"
        action = col.check["on_fail"]
        if action == "fail":
            raise TransformError(msg)
        if action == "skip_row":
            raise RowError(msg)
        log.warning(msg)
        return None if action == "null" else v

    # --- tipos, dedupe ----------------------------------------------------
    def _finalize(self, res: TableResult):
        spec = res.spec
        explicit = {c.name: c.type for c in spec.columns if c.type}
        for name, t in list(explicit.items()):
            if T.needs_inference(t):  # type: struct / array<struct> -> estrutura inferida dos dados
                tree = None
                for r in res.rows:
                    v = r.get(name)
                    if T.parse(t)[0] == "struct" and isinstance(v, list) and len(v) == 1:
                        v = v[0]  # lista 1:1
                    tree = T.merge(tree, T.infer_deep(v))
                if T.parse(t)[0] == "array" and isinstance(tree, tuple) and tree[0] != "array":
                    tree = ("array", tree)
                explicit[name] = T.render(T.complete(tree))
                for r in res.rows:
                    r[name] = T.cast(r.get(name), explicit[name])
        order: dict[str, None] = {}
        for c in spec.columns:
            order[c.name] = None
        for r in res.rows:
            for k in r:
                order.setdefault(k, None)
        types: dict[str, str] = {}
        for name in order:
            if name in explicit:
                types[name] = explicit[name]
                continue
            meta = next((t for n, t in METADATA_TYPES.values() if n == name), None)
            if meta:
                types[name] = meta
                continue
            t = None
            for r in res.rows:
                t = T.widen(t, T.infer(r.get(name)))
            types[name] = t or "string"
        for r in res.rows:
            for name, t in types.items():
                if name not in explicit:
                    r[name] = T.cast(r.get(name), t)
                else:
                    r.setdefault(name, None)
        missing = [k for k in spec.keys + spec.replace_by if k not in types]
        if missing:
            raise ConfigError(f"Tabela '{spec.name}': colunas-chave {missing} não existem. Colunas: {list(types)}")
        if spec.keys:
            nulls = sum(1 for r in res.rows if any(r.get(k) is None for k in spec.keys))
            if nulls:
                log.warning("Tabela '%s': %d linha(s) com chave nula", spec.name, nulls)
            if spec.dedupe:
                uniq: dict[tuple, dict] = {}
                for r in res.rows:
                    uniq[tuple(json.dumps(r.get(k), default=str) for k in spec.keys)] = r
                if len(uniq) < len(res.rows):
                    log.info("Tabela '%s': %d duplicado(s) removido(s)", spec.name, len(res.rows) - len(uniq))
                res.rows = list(uniq.values())
        res.types = types


def _source(col: ColumnSpec, ctx: Context) -> Any:
    if col.has_const:
        raw = col.const
    elif col.paths:
        raw = next((v for v in (resolve(ctx, p) for p in col.paths) if v not in (MISSING, None, "")), None)
    elif col.template is not None:
        raw = _render(col.template, ctx)
    elif col.path:
        raw = resolve(ctx, col.path)
    else:
        raw = None
    return None if raw is MISSING else raw


def check_value(chk: dict, v: Any, ctx: Context) -> str | None:
    """Devolve a descrição do problema, ou None se o valor é válido."""
    if v is None or v == []:
        if chk.get("required"):
            return "valor obrigatório em falta"
        if "required_if" in chk and evaluate(chk["required_if"], ctx):
            return f"valor obrigatório quando {chk['required_if']}"
        return None
    for item in v if isinstance(v, list) else [v]:
        if item is None:
            continue
        s = str(item)
        if "_regex" in chk and not chk["_regex"].search(s):
            return f"{item!r} não corresponde a /{chk['regex']}/"
        if "_in" in chk and (s.lower() if chk.get("ignore_case") else s) not in chk["_in"]:
            return f"{item!r} não está na lista {chk['in']}"
        if "min" in chk or "max" in chk:
            try:
                x = float(item)
            except (TypeError, ValueError):
                return f"{item!r} não é numérico"
            if "min" in chk and x < float(chk["min"]):
                return f"{item!r} < mínimo {chk['min']}"
            if "max" in chk and x > float(chk["max"]):
                return f"{item!r} > máximo {chk['max']}"
    return None


_VALIDATORS: dict[str, Any] = {}


def schema_validator(config: Config, sub: SubmodelSpec):
    """Validador jsonschema para o schema SAMM do submodelo (em cache por ficheiro)."""
    try:
        import jsonschema
    except ImportError as e:  # pragma: no cover
        raise ConfigError("Para validar documentos instale jsonschema (pip install jsonschema)") from e
    path = Path(os.path.normpath((config.base_dir or Path.cwd()) / sub.model["schema"]))
    key = str(path.resolve())
    if key not in _VALIDATORS:
        if not path.is_file():
            raise ConfigError(f"Submodelo '{sub.name}': schema não encontrado em {path}")
        schema = json.loads(path.read_text(encoding="utf-8"))
        _VALIDATORS[key] = jsonschema.validators.validator_for(schema)(schema)
    return _VALIDATORS[key]


_TYPE_NAMES = {"string": "texto", "number": "número", "integer": "número inteiro", "boolean": "verdadeiro/falso",
               "object": "objecto", "array": "lista", "null": "nulo"}


def describe_error(e) -> dict[str, str]:
    """Erro jsonschema -> {path, message} legível em português."""
    path = ""
    for part in e.absolute_path:
        path += f"[{part}]" if isinstance(part, int) else (f".{part}" if path else str(part))
    v, val = e.validator, e.validator_value
    if v == "required":
        m = re.search(r"'([^']+)' is a required property", e.message)
        msg = f"falta o campo obrigatório '{m.group(1) if m else e.message}'"
    elif v == "type":
        names = " ou ".join(_TYPE_NAMES.get(t, t) for t in (val if isinstance(val, list) else [val]))
        msg = f"{json.dumps(e.instance, ensure_ascii=False, default=str)[:60]} devia ser {names}"
    elif v == "pattern":
        kind = "data/hora ISO 8601" if "[0-9]{3,}" in str(val) and "T" in str(val) else "o formato esperado"
        msg = f"{json.dumps(e.instance, ensure_ascii=False)[:60]} não tem {kind}"
    elif v == "enum":
        msg = f"{json.dumps(e.instance, ensure_ascii=False)[:60]} não é um dos valores permitidos ({', '.join(map(str, val))})"
    elif v in ("minimum", "exclusiveMinimum"):
        msg = f"{e.instance} é menor que o mínimo {val}"
    elif v in ("maximum", "exclusiveMaximum"):
        msg = f"{e.instance} é maior que o máximo {val}"
    elif v == "minItems":
        msg = f"a lista tem de ter pelo menos {val} elemento(s)"
    elif v == "minLength":
        msg = "não pode estar vazio"
    elif v == "additionalProperties":
        msg = f"campo não previsto no modelo: {e.message.split('(')[-1].rstrip(')')}"
    else:
        msg = e.message
    return {"path": path, "message": msg, "rule": v}


def _with_column(spec: TableSpec, col: ColumnSpec) -> TableSpec:
    """Cópia da tabela com a coluna à cabeça (se ainda não existir)."""
    if any(c.name == col.name for c in spec.columns):
        return spec
    return dataclasses.replace(spec, columns=[col] + spec.columns)


_TPL_RE = re.compile(r"\{([^{}]+)\}")


def _render(template: str, ctx: Context) -> str | None:
    def repl(m):
        v = resolve(ctx, m.group(1))
        if v is MISSING or v is None:
            return ""
        return json.dumps(v, default=str) if isinstance(v, (dict, list)) else str(v)

    return _TPL_RE.sub(repl, template)


def flatten(obj: dict, exclude: list[str], max_depth: int, lists: str, naming: str = "full") -> dict[str, Any]:
    """Achata um objecto: {a: {b: 1}} -> {a_b: 1} (naming=full) ou {b: 1} (naming=leaf)."""
    leaves: list[tuple[tuple[str, ...], Any]] = []

    def walk(o: dict, parts: tuple[str, ...], depth: int):
        for k, v in o.items():
            p = parts + (k,)
            dotted = ".".join(p)
            if dotted in exclude or k in exclude or sanitize("_".join(p)) in exclude:
                continue
            if isinstance(v, dict):
                if depth + 1 < max_depth and v:
                    walk(v, p, depth + 1)
                elif v or depth + 1 >= max_depth:
                    leaves.append((p, v))
            elif isinstance(v, list):
                objects = any(isinstance(x, (dict, list)) for x in v)
                if lists == "skip" or (objects and lists == "array"):
                    continue  # listas de objectos -> tabelas filhas (explode / auto_normalize)
                leaves.append((p, json.dumps(v, ensure_ascii=False, default=str) if lists == "json" else v))
            else:
                leaves.append((p, v))

    walk(obj, (), 0)
    names = column_names([p for p, _ in leaves], naming)
    out: dict[str, Any] = {}
    for p, v in leaves:
        out.setdefault(names[p], v)
    return out


def column_names(paths: list[tuple[str, ...]], naming: str = "full") -> dict[tuple[str, ...], str]:
    """Nomes de coluna para caminhos. 'leaf' usa só o último campo e acrescenta
    os níveis de cima apenas onde há colisão (ex.: production_stage_x vs packaging_stage_x)."""
    if naming == "full":
        return {p: sanitize("_".join(p)) for p in paths}
    depth = {p: 1 for p in paths}
    while True:
        groups: dict[str, list[tuple[str, ...]]] = {}
        for p in paths:
            groups.setdefault(sanitize("_".join(p[-depth[p]:])), []).append(p)
        clash = [ps for ps in groups.values() if len(ps) > 1 and any(depth[p] < len(p) for p in ps)]
        if not clash:
            return {p: n for n, ps in groups.items() for p in ps}
        for ps in clash:
            for p in ps:
                depth[p] = min(depth[p] + 1, len(p))


# --- unwrap: listas 1:1 -> objectos ---------------------------------------
def unwrap(obj: Any, rule: dict, source: str | None = None, path: str = "") -> Any:
    """Substitui listas que na prática são 1:1 pelo seu único elemento.

    rule = {paths: set, all: bool, except: set, on_multiple: fail|first|keep}
    Caminhos sem índices: 'pcfAssessmentAndMethodology.pcfAssessmentInformation.time'.
    """
    if isinstance(obj, list):
        return [unwrap(x, rule, source, path) for x in obj]
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        p = f"{path}.{k}" if path else k
        if isinstance(v, list) and p not in rule["except"]:
            explicit = p in rule["paths"]
            objects = bool(v) and all(isinstance(x, dict) for x in v)
            if explicit or (rule["all"] and objects):
                if not v:
                    v = None
                elif len(v) == 1:
                    v = v[0]
                elif rule["on_multiple"] == "fail":
                    raise TransformError(f"unwrap: '{p}' tem {len(v)} elementos em '{source}' (esperado 1). "
                                         f"Use on_multiple: first|keep ou retire-o do unwrap.")
                elif rule["on_multiple"] == "first":
                    log.warning("unwrap: '%s' tem %d elementos em %s — usado o primeiro", p, len(v), source)
                    v = v[0]
        out[k] = unwrap(v, rule, source, p)
    return out


# --- auto_normalize -----------------------------------------------------
def _array_paths(obj: Any, levels: list[str], cur: list[str], found: dict[tuple, None], depth: int, max_depth: int):
    if not isinstance(obj, dict) or depth > max_depth:
        return
    for k, v in obj.items():
        if isinstance(v, dict):
            _array_paths(v, levels, cur + [k], found, depth + 1, max_depth)
        elif isinstance(v, list) and any(isinstance(x, dict) for x in v):
            lv = levels + [".".join(cur + [k])]
            found[tuple(lv)] = None
            for x in v:
                _array_paths(x, lv, [], found, depth + 1, max_depth)


def normalize_specs(sub: SubmodelSpec, docs: list[Document]) -> list[TableSpec]:
    """Gera tabelas automaticamente: 1 tabela raiz + 1 tabela por cada lista de objectos."""
    an = sub.auto_normalize or {}
    d = sub.defaults
    prefix = sanitize(an.get("table_prefix", sub.name))
    if "id_path" not in an and sub.document_id:  # sem id de negócio: o UUID do documento liga as tabelas
        id_path, id_col = "$meta.document_id", sub.document_id["column"]
    else:
        id_path = an.get("id_path", "catenaXId")
        id_col = sanitize(an.get("id_column", id_path.split(".")[-1]))
    exclude = list(an.get("exclude", []) or [])
    max_depth = int(an.get("max_depth", 10))
    meta_cols = _metadata(d.get("metadata_columns", False))
    mode = d.get("mode", "merge")
    naming = an.get("naming", "full")
    base = dict(auto=True, auto_naming=naming, auto_lists="array", auto_max_depth=max_depth, metadata_columns=meta_cols,
                partition_by=list(d.get("partition_by", []) or []), on_error=d.get("on_error", "fail"),
                athena_options=dict(d.get("athena_options", {}) or {}))
    common = [c for c in sub.common_columns if c.name != id_col]
    specs = [TableSpec(name=prefix, columns=[ColumnSpec(id_col, path=id_path)] + common, keys=[id_col],
                       mode=mode, auto_exclude=exclude + [id_path], **base)]
    found: dict[tuple, None] = {}
    for doc in docs:
        _array_paths(doc.data, [], [], found, 0, max_depth)
    kept = [lv for lv in found if not any(x in exclude for x in lv)]
    tnames = column_names([tuple(".".join(lv).split(".")) for lv in kept], naming)
    for levels in kept:
        n = len(levels)
        cols = [ColumnSpec(id_col, path=f"$root.{id_path}")] + common
        idx_cols = []
        for j, lv in enumerate(levels):
            name = sanitize(f"{lv.split('.')[-1]}_idx")
            if name in idx_cols:
                name = f"{name}_{j}"
            cols.append(ColumnSpec(name, path="$parent." * (n - 1 - j) + "$index", type="int"))
            idx_cols.append(name)
        child_mode = "append" if mode == "merge" else mode
        specs.append(TableSpec(
            name=sanitize(f"{prefix}_{tnames[tuple('.'.join(levels).split('.'))]}"), columns=cols, explode=list(levels),
            keys=[id_col] + idx_cols, mode=child_mode, dedupe=True,
            replace_by=[id_col] if mode == "merge" else [], auto_exclude=exclude, auto_collision="prefix", **base,
        ))
    return specs
