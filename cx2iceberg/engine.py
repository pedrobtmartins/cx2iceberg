"""Aplica as regras da configuração a documentos JSON e produz linhas por tabela."""
from __future__ import annotations

import dataclasses
import fnmatch
import functools
import hashlib
import json
import logging
import math
import os
import re
import uuid
from array import array
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator

from . import athena_types as T
from . import notes as N
from .conditions import evaluate
from .config import ColumnSpec, Config, ConfigError, SubmodelSpec, TableSpec, _metadata, sanitize
from .paths import MISSING, TEMPLATE_RE, Context, PathError, PathShapeError, Projection, resolve
from .report import NO_VALUE, Report, ReportLimit, short_value
from .transforms import apply_chain, typed_key
from .numbers import JsonFloat, to_decimal
from .values import json_text, text_of

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
    index: int | None = None   # posição no ficheiro (lista JSON ou JSON Lines); None se o ficheiro é um só documento
    error: str | None = None   # não foi possível ler (ex.: JSON inválido, não é UTF-8): o documento é rejeitado
    notice: str | None = None  # nada a processar, mas a registar (ex.: ficheiro sem documentos)


class RowOrigins:
    """De que documento e de que posição do explode veio cada linha, em memória compacta.

    Um dict por linha custava ~240 bytes (mais do que a própria linha). Aqui guardam-se inteiros em arrays:
    8 bytes para o nº do documento, 8 por nível de explode e 2 para o caminho de explode (uma tabela pode ser
    alimentada por regras com explodes diferentes). Ficheiro, posição no ficheiro e o texto 'childItems[3]' só
    são reconstruídos quando um evento do relatório precisa deles.
    """

    ABSENT, EMPTY = -2, -1   # nível que esta linha não tem | explode vazio com keep_empty

    __slots__ = ("docs", "layout", "positions", "layouts")

    def __init__(self):
        self.docs = array("q")
        self.layout = array("H")
        self.positions: list[array] = []
        self.layouts: list[tuple[str, ...]] = []

    def __len__(self) -> int:
        return len(self.docs)

    def append(self, doc: int, explode: tuple[str, ...], indices: tuple[int | None, ...]) -> None:
        try:
            lid = self.layouts.index(explode)
        except ValueError:
            lid = len(self.layouts)
            self.layouts.append(explode)
        while len(self.positions) < len(indices):
            self.positions.append(array("q", [self.ABSENT]) * len(self.docs))
        for j, col in enumerate(self.positions):
            col.append(self.ABSENT if j >= len(indices) else self.EMPTY if indices[j] is None else indices[j])
        self.docs.append(doc)
        self.layout.append(lid)

    def select(self, keep: list[int]) -> "RowOrigins":
        out = RowOrigins()
        out.layouts = self.layouts
        out.docs = array("q", (self.docs[i] for i in keep))
        out.layout = array("H", (self.layout[i] for i in keep))
        out.positions = [array("q", (col[i] for i in keep)) for col in self.positions]
        return out

    def locator(self, i: int) -> str | None:
        names = self.layouts[self.layout[i]]
        if not names:
            return None
        parts = []
        for j, lv in enumerate(names):
            v = self.positions[j][i]
            parts.append(f"{lv}[{'-' if v == self.EMPTY else v}]")
        return ".".join(parts)

    def nbytes(self) -> int:
        arrays = [self.docs, self.layout, *self.positions]
        return sum(a.itemsize * len(a) for a in arrays)


@dataclass
class TableResult:
    spec: TableSpec
    rows: list[dict[str, Any]] = field(default_factory=list)
    types: dict[str, str] = field(default_factory=dict)
    skipped: int = 0
    check_failures: dict[str, int] = field(default_factory=dict)  # coluna -> nº de valores que falharam o check
    origins: RowOrigins = field(default_factory=RowOrigins, repr=False)  # por linha: documento e posição (compacto)
    inferred: dict[str, str] = field(default_factory=dict, repr=False)   # tipo final das colunas struct inferidas
    specs: list = field(default_factory=list, repr=False)                # todas as regras que alimentam a tabela
    # replace_by: chave de CADA documento aceite que passou pela tabela, mesmo sem linhas (lista que ficou vazia,
    # linhas todas excluídas): o DELETE das linhas antigas sai daqui, não das linhas (antes ficavam filhos antigos)
    replace_keys: dict[int, tuple] = field(default_factory=dict, repr=False)
    # documentos aceites que passaram pela tabela: com mode overwrite e zero linhas, a tabela fica vazia (é o que
    # o lote diz); sem nenhum documento, o lote não diz nada sobre ela e não se lhe toca
    documents: set[int] = field(default_factory=set, repr=False)


ISO_DATETIME = T.ISO_DATETIME
MAX_DEPTH = 64   # níveis de aninhamento; os modelos Catena-X ficam muito abaixo (PCF 9.0: ~12)
_SURROGATE = re.compile("[\ud800-\udfff]")
NUMERIC_TYPES = ("int", "bigint", "smallint", "tinyint", "double", "float")


@functools.lru_cache(maxsize=1024)
def _numeric_base(t: str) -> bool:
    base = t
    while base.startswith("array<"):
        base = base[len("array<"):-1]
    return base.startswith(NUMERIC_TYPES + ("decimal",))


def _numeric_texts(v: Any, t: str | None) -> list[str]:
    """Textos que vão ser lidos como número (coluna numérica ou array de números): para o relatório."""
    if not t or not isinstance(v, (str, list)) or not _numeric_base(t):
        return []
    return [x for x in (v if isinstance(v, list) else [v]) if isinstance(x, str)]


def document_problem(data: Any) -> str | None:
    """Razão para um documento não poder seguir tal como está, ou None.

    Profundidade acima de MAX_DEPTH (protege as funções recursivas e a carga inteira), números não finitos
    (NaN, inf: um documento construído em código ou colado no studio) e texto com surrogates isolados
    ('\\ud800': passa no motor mas não pode ser gravado em UTF-8 — falharia só na escrita).
    Percurso iterativo; o caminho só é calculado (2.ª passagem) quando há problema.
    """
    def scan(with_path: bool) -> tuple[str, str] | None:
        stack: list[tuple[Any, int, str]] = [(data, 0, "")]
        while stack:
            obj, depth, path = stack.pop()
            if depth >= MAX_DEPTH:
                return f"mais de {MAX_DEPTH} níveis de aninhamento", path
            items = obj.items() if isinstance(obj, dict) else enumerate(obj)
            for k, v in items:
                if isinstance(k, str) and not k.isascii() and _SURROGATE.search(k):
                    return "chave com um carácter UTF-16 isolado (ex.: \\ud800), que não pode ser gravado em UTF-8", path
                sub = "" if not with_path else (f"{path}.{k}" if path else str(k)) if isinstance(obj, dict) else f"{path}[{k}]"
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1, sub))
                elif isinstance(v, str):
                    if not v.isascii() and _SURROGATE.search(v):
                        return "texto com um carácter UTF-16 isolado (ex.: \\ud800), que não pode ser gravado em UTF-8", sub
                elif isinstance(v, float) and not math.isfinite(v):
                    return f"número não finito ({v})", sub
        return None

    if not isinstance(data, (dict, list)):
        return None
    if scan(False) is None:
        return None
    reason, path = scan(True)
    path = path or "(raiz)"
    return f"{reason} em '{path if len(path) <= 120 else path[:120] + '…'}'"


class RowError(Exception):
    """A linha é ignorada (skip_row declarado); o evento já está no relatório."""


class _Quiet(Exception):
    """Problema num cálculo silencioso (chave do replace_by ao nível do documento)."""


class _ValueFailed(Exception):
    """Valor inválido sem perda declarada; o erro já está no relatório e o documento fica rejeitado."""


class TransformError(ValueError):
    pass


class TemplateIncomplete(ValueError):
    """Template com só algumas partes em falta, sem template_missing declarado."""

    reason = "template_missing"


class MissingElements(PathError):
    """Um [*]/filtro com elementos sem o campo, numa coluna sem `missing_elements` declarado (ou numa chave)."""

    reason = "missing_elements"


class DocumentsRejected(TransformError):
    """Um ou mais documentos têm erros. `results` tem as linhas dos restantes; `report` tem os detalhes."""

    def __init__(self, report: Report, results: dict[str, "TableResult"]):
        self.report, self.results = report, results
        msgs = report.error_messages(10)
        more = f"\n  … (+{report.errors - len(msgs)} erro(s))" if report.errors > len(msgs) else ""
        super().__init__(f"{len(report.rejected)} documento(s) rejeitado(s); nada deles foi escrito. Erros:\n  "
                         + "\n  ".join(msgs) + more)


# --- condições: ver conditions.py (validadas na carga, comparadas com tipo) --------


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
    try:
        for p in m.get("has_paths", []) or []:
            if resolve(ctx, p) in (MISSING, None):
                return False
        return evaluate(m.get("where"), ctx)
    except PathError:
        return False   # o documento tem outra forma (ex.: outra versão do modelo): não corresponde


# --- motor ---------------------------------------------------------------
class Engine:
    """Aplica a configuração a documentos.

    Os erros não param no primeiro: cada documento com erros é rejeitado por inteiro (nenhuma linha dele,
    em nenhuma tabela) e tudo fica em `self.report`. `run()` lança `DocumentsRejected` se houver documentos
    rejeitados; com `partial=True` devolve as linhas dos restantes (quem chama decide o que escrever).
    Erros de configuração e de lote (ex.: limite de erros) lançam sempre excepção e não devolvem nada.
    """

    def __init__(self, config: Config, match_all: bool | None = None, report: Report | None = None):
        self.config = config
        self.match_all = config.input.get("match_all", False) if match_all is None else match_all
        self.ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.validation: list[dict[str, Any]] = []   # documentos que não cumprem o schema do modelo
        self.report = report if report is not None else Report()

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

    def run(self, docs: Iterable[Document], partial: bool = False) -> dict[str, TableResult]:
        settings = {"naive_timestamps": self.config.input.get("naive_timestamps", "fail")}
        try:
            with N.scope(settings) as notes:
                self._notes = notes       # notas das conversões (perdas declaradas, avisos) de cada valor
                results = self._run(docs)
        except ReportLimit as e:
            self.report.failure = str(e)
            err = TransformError(f"{e}. Primeiros erros:\n  " + "\n  ".join(self.report.error_messages(10)))
            err.report = self.report
            raise err from None
        except (ConfigError, TransformError) as e:
            self.report.failure = str(e)
            e.report = self.report
            raise
        if self.report.rejected and not partial:
            raise DocumentsRejected(self.report, results)
        return results

    def _run(self, docs: Iterable[Document]) -> dict[str, TableResult]:
        rep = self.report
        # número de sequência de cada documento nesta carga: identifica-o mesmo depois do unwrap/document_id
        docs = [dataclasses.replace(d, meta={**d.meta, "_seq": (rep.documents + i)}) for i, d in enumerate(docs)]
        rep.documents += len(docs)
        self._doc_info = {d.meta["_seq"]: (d.source_file, d.index) for d in docs}   # 1 entrada por documento
        readable = []
        for d in docs:
            if d.error is not None:
                self._doc_event(d, "error", "unreadable", "document_rejected", d.error)
            elif d.notice is not None:
                self._doc_event(d, "warning", "empty_file", "ignored", d.notice)
            elif (problem := document_problem(d.data)) is not None:
                self._doc_event(d, "error", "invalid_document", "document_rejected", problem)
            else:
                readable.append(d)
        routed = self.route(readable)
        self._notes.clear()           # notas das condições do match não descrevem valores gravados
        matched = {id(d) for ds in routed.values() for d in ds}
        skip_unmatched = self.config.input.get("on_unmatched", "fail") == "skip"
        for d in readable:
            if id(d) not in matched:
                msg = f"nenhum submodelo da configuração corresponde (semantic_id={d.semantic_id})"
                if skip_unmatched:
                    self._doc_event(d, "declared", "no_submodel", "ignored", msg)
                else:
                    self._doc_event(d, "error", "no_submodel", "document_rejected",
                                    msg + "; declare input.on_unmatched: skip para o ignorar")
        results: dict[str, TableResult] = {}
        for sub in self.config.submodels:
            sub_docs = routed[sub.name]
            if sub.validate != "off":
                sub_docs = self._validate(sub, sub_docs)
            if sub.unwrap:
                sub_docs = self._unwrap(sub, sub_docs)
            if sub.document_id:
                sub_docs = [x for x in (self._with_document_id(sub, d) for d in sub_docs) if x is not None]
                self._notes.clear()
            tables = list(sub.tables)
            if sub.auto_normalize is not None:
                generated = normalize_specs(sub, sub_docs)
                tables += generated
                if sub_docs and sub.auto_normalize.get("children") == "batch" and generated[0].mode != "append":
                    children = [t.name for t in generated[1:]]
                    rep.add("declared", "children_from_batch", "not_deleted",
                            f"auto_normalize.children: batch — tabelas filhas só das listas presentes neste lote "
                            f"({children or 'nenhuma'}); as de uma lista que ficou vazia em todos os documentos do lote "
                            f"não foram apagadas nem substituídas", table=generated[0].name)
            if sub.document_id:
                tables = [_with_column(t, ColumnSpec(sub.document_id["column"], path="$meta.document_id", type="string"))
                          for t in tables]
            for spec in tables:
                res = results.setdefault(spec.name, TableResult(spec))
                if spec not in res.specs:
                    res.specs.append(spec)          # a configuração garante que concordam (modo, chaves, tipos)
                for doc in sub_docs:
                    self._rows_for(spec, doc, res)
        for name, res in results.items():
            self._check_duplicates(res)   # chave repetida no mesmo documento: erro (ou on_duplicate declarado)
            self._cast_inferred(res)      # pode rejeitar documentos (valor que não cabe no tipo inferido)
        rejected = set(rep.rejected)
        for name, res in results.items():
            dropped = 0
            res.replace_keys = {d: k for d, k in res.replace_keys.items() if d not in rejected}   # rejeitado: não apaga
            res.documents -= rejected
            if rejected:
                keep = [i for i, d in enumerate(res.origins.docs) if d not in rejected]
                dropped = len(res.rows) - len(keep)
                if dropped:
                    res.rows = [res.rows[i] for i in keep]
                    res.origins = res.origins.select(keep)
            try:
                self._finalize(res)
            except (ConfigError, TransformError):
                raise
            except Exception as e:  # noqa: BLE001 - erro ao nível da tabela (tipos inferidos), não de um documento
                raise TransformError(f"tabela '{name}': {e}") from e
            rep.tables[name] = {"rows": len(res.rows), "rows_from_rejected_documents": dropped}
        return results

    # --- eventos do relatório --------------------------------------------
    def _doc_event(self, doc: Document, kind: str, reason: str, action: str, message: str) -> None:
        """Evento ao nível do documento (sem tabela/coluna)."""
        self.report.add(kind, reason, action, message, file=doc.source_file, document=doc.index,
                        doc_key=doc.meta.get("_seq"))

    def _event(self, kind: str, reason: str, action: str, message: str, spec: TableSpec, ctx: Context,
               column: str | None = None, value: Any = NO_VALUE) -> bool:
        """Evento ao nível de uma linha: documento actual, posição no explode, tabela e coluna.

        Devolve True se o evento ficou como exemplo no relatório (só esses vão também para o log).
        """
        if getattr(self, "_quiet", False):
            return False
        doc = self._doc
        return self.report.add(kind, reason, action, message, table=spec.name, column=column, file=doc.source_file,
                               document=doc.index, row=_locator(spec, ctx), value=value, doc_key=doc.meta.get("_seq"))

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
            msg = f"não cumpre o schema de '{sub.name}': {summary}{more}"
            if sub.validate == "fail":
                self._doc_event(d, "error", "schema", "document_rejected", msg)
                continue
            if sub.validate == "skip":
                log.warning("%s %s — documento ignorado", d.source_file, msg)
                self._doc_event(d, "declared", "schema", "ignored", msg)
                continue
            log.warning("%s %s", d.source_file, msg)
            self._doc_event(d, "warning", "schema", "kept", msg)
            kept.append(d)
        return kept

    def _unwrap(self, sub: SubmodelSpec, docs: list[Document]) -> list[Document]:
        out = []
        for d in docs:
            firsts: list[tuple[str, int]] = []
            try:
                data = unwrap(d.data, sub.unwrap, d.source_file, firsts=firsts)
            except TransformError as e:
                self._doc_event(d, "error", "unwrap_multiple", "document_rejected", str(e))
                continue
            for p, n in firsts:
                self._doc_event(d, "declared", "unwrap_multiple", "first_element",
                                f"'{p}' tem {n} elementos; usado o primeiro (unwrap on_multiple: first)")
            out.append(dataclasses.replace(d, data=data, meta={**d.meta, "_raw": d.data}))
        return out

    def _with_document_id(self, sub: SubmodelSpec, doc: Document) -> Document | None:
        rule = sub.document_id
        raw = doc.meta.get("_raw", doc.data)  # conteúdo como chegou (antes do unwrap)
        if rule["strategy"] == "random":
            value = str(uuid.uuid4())
        else:
            ns = uuid.uuid5(uuid.NAMESPACE_URL, rule["namespace"])
            name = None
            if rule["strategy"] == "key":
                ctx = Context([(doc.data, None)])
                try:
                    parts = [_key_part(resolve(ctx, p)) for p, _ in rule["from"]]
                    missing = [p for (p, _), v in zip(rule["from"], parts) if v is MISSING or v is None or v == ""]
                    if not missing:
                        parts = [_canonical_part(p, t, v) for (p, t), v in zip(rule["from"], parts)]
                except (PathError, ValueError) as e:
                    self._doc_event(doc, "error", getattr(e, "reason", "invalid_value"), "document_rejected",
                                    f"document_id: {e}")
                    return None
                if missing:
                    msg = f"document_id: campo(s) {missing} em falta"
                    if rule["on_missing"] != "content":
                        self._doc_event(doc, "error", "document_id_key_missing", "document_rejected",
                                        msg + "; o UUID mudaria quando o campo aparecesse (linhas duplicadas). "
                                              "Declare document_id.on_missing: content para usar o conteúdo")
                        return None
                    log.warning("%s em %s — usado o conteúdo do documento", msg, doc.source_file)
                    self._doc_event(doc, "declared", "document_id_key_missing", "content_hash",
                                    msg + "; UUID calculado a partir do conteúdo (on_missing: content)")
                else:
                    name = "key:" + "|".join(json.dumps(p, sort_keys=True, default=str) for p in parts)
            if name is None:
                name = "content:" + json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
            value = str(uuid.uuid5(ns, name))
        meta = {k: v for k, v in doc.meta.items() if k != "_raw"}
        return dataclasses.replace(doc, meta={**meta, "document_id": value})

    # --- geração de linhas -----------------------------------------------
    def _contexts(self, ctx: Context, levels: list[str], keep_empty: bool) -> Iterator[Context]:
        if not levels:
            yield ctx
            return
        val = resolve(ctx, levels[0])
        if val is not MISSING and val is not None and not isinstance(val, (list, dict)):
            raise PathShapeError(f"explode '{levels[0]}': esperada uma lista (ou objecto), encontrado {type(val).__name__} {val!r}")
        items = [] if val is MISSING or val is None else val if isinstance(val, list) else [val]
        if not items and keep_empty:
            yield from self._contexts(ctx.child({}, None), levels[1:], keep_empty)
        for i, item in enumerate(items):
            yield from self._contexts(ctx.child(item, i), levels[1:], keep_empty)

    def _rows_for(self, spec: TableSpec, doc: Document, res: TableResult):
        self._res, self._doc = res, doc
        meta = {"source_file": doc.source_file, "semantic_id": doc.semantic_id, "ingested_at": self.ingested_at,
                **{k: v for k, v in doc.meta.items() if not k.startswith("_")}}
        root = Context([(doc.data, None)], meta)
        res.documents.add(doc.meta.get("_seq", -1))
        if spec.replace_by and not self._replace_key(spec, doc, root, res):
            return
        try:
            for ctx in self._contexts(root, spec.explode, spec.keep_empty):
                # notas fora de um valor (ex.: datas comparadas no where) não são guardadas: sem isto acumulavam
                # uma por linha durante a carga inteira
                self._notes.clear()
                try:
                    keep = evaluate(spec.where, ctx)
                    if not keep:
                        self._event("declared", "where_false", "row_not_generated", "linha excluída pelo where da tabela",
                                    spec, ctx)
                        continue
                    row = self._row(spec, ctx)
                except PathError as e:    # caminho do where com a forma errada: on_error da tabela ('null' não se aplica)
                    try:
                        self._problem(spec, "(where)", ctx, NO_VALUE, str(e),
                                      "skip_row" if spec.on_error == "skip_row" else "fail", e.reason)
                    except RowError:
                        res.skipped += 1
                    except _ValueFailed:
                        pass
                    continue
                except RowError as e:
                    res.skipped += 1
                    log.debug("Linha ignorada em %s: %s", spec.name, e)
                    continue
                except _ValueFailed:
                    continue
                if (null_keys := [k for k in _key_columns(spec) if row.get(k) is None]):
                    try:
                        self._problem(spec, null_keys[0], ctx, NO_VALUE,
                                      f"chave nula ({', '.join(null_keys)}): a linha não pode ser identificada",
                                      _key_on_error(spec, null_keys), "null_key")
                    except RowError:
                        res.skipped += 1
                    except _ValueFailed:
                        pass
                    continue
                res.rows.append(row)
                res.origins.append(doc.meta.get("_seq", -1), tuple(spec.explode), tuple(idx for _, idx in ctx.stack[1:]))
        except PathError as e:
            # o explode não tem a forma do documento: nenhuma linha desta tabela é fiável -> documento rejeitado
            self.report.add("error", e.reason, "document_rejected", str(e), table=spec.name, file=doc.source_file,
                            document=doc.index, doc_key=doc.meta.get("_seq"))

    def _replace_key(self, spec: TableSpec, doc: Document, root: Context, res: TableResult) -> bool:
        """Chave do replace_by calculada ao nível do documento (as colunas são de $root/$meta, validado na carga).
        Sem ela, um documento cuja lista ficou vazia não apagava as linhas antigas. False = documento rejeitado."""
        ctx = root
        for _ in spec.explode:
            ctx = ctx.child({}, None)
        cols = {c.name: c for c in spec.columns}
        start = len(self._notes)
        self._quiet = True
        try:
            key = tuple(self._value_inner(spec, cols[name], ctx) for name in spec.replace_by)
            problem = None if all(v is not None for v in key) else \
                f"replace_by: {', '.join(n for n, v in zip(spec.replace_by, key) if v is None)} nulo(s) neste documento"
        except (_Quiet, RowError, _ValueFailed, ValueError) as e:
            problem = f"replace_by: não foi possível calcular a chave do documento: {e}"
        finally:
            self._quiet = False
            del self._notes[start:]
        if problem:
            self.report.add("error", "null_key", "document_rejected", problem + " (as linhas antigas não seriam apagadas)",
                            table=spec.name, file=doc.source_file, document=doc.index, doc_key=doc.meta.get("_seq"))
            return False
        res.replace_keys[doc.meta.get("_seq", -1)] = key
        return True

    def _row(self, spec: TableSpec, ctx: Context) -> dict[str, Any]:
        row: dict[str, Any] = {}
        failed = False  # continua nas outras colunas para o relatório mostrar todos os erros da linha
        for col in spec.columns:
            try:
                row[col.name] = self._value(spec, col, ctx)
            except _ValueFailed:
                failed = True
        if spec.auto:
            obj = ctx.stack[-1][0]
            if isinstance(obj, dict):
                fields, sources, collisions, outside = flatten_sources(
                    _leaves(obj, spec.auto_exclude, spec.auto_max_depth, spec.auto_lists), spec.auto_naming,
                    spec.leaf_names if spec.auto_naming == "leaf" else None)
                for name, dotted in outside:
                    self._event("warning", "outside_model", "full_name",
                                f"o campo '{dotted}' não está no modelo: vai para a coluna '{name}' (nome completo; os "
                                f"nomes leaf são fixados pelo model.schema)", spec, ctx, name)
                for name, first, second in collisions:
                    try:
                        self._problem(spec, name, ctx, NO_VALUE, f"os campos '{first}' e '{second}' dão a mesma coluna "
                                      f"'{name}'; exclua um com auto_exclude ou declare colunas explícitas",
                                      "skip_row" if spec.on_error == "skip_row" else "fail", "auto_collision")
                    except _ValueFailed:
                        failed = True
                explicit_cols = {c.name: c for c in spec.columns}
                level = len(ctx.stack) - 1
                for name, val in fields.items():
                    if name in row:
                        if _same_source(explicit_cols.get(name), sources[name], level):
                            continue                 # a coluna explícita substitui este campo (mesma origem)
                        if spec.auto_collision == "prefix" and f"item_{name}" not in row:
                            name = f"item_{name}"
                        elif spec.auto_collision == "skip":
                            self._event("declared", "auto_collision", "auto_skipped",
                                        f"o campo '{sources[name]}' não entra: a coluna '{name}' já existe (auto_collision: skip)",
                                        spec, ctx, name)
                            continue
                        else:
                            try:
                                self._problem(spec, name, ctx, NO_VALUE,
                                              f"o campo '{sources[name]}' dá a coluna '{name}', que já existe com outra "
                                              f"origem; declare auto_collision: prefix (item_{name}) ou skip",
                                              "skip_row" if spec.on_error == "skip_row" else "fail", "auto_collision")
                            except _ValueFailed:
                                failed = True
                            continue
                    start = len(self._notes)
                    try:
                        problem = untyped_problem(val)
                        if problem:
                            raise ValueError(problem)
                        if spec.auto_dates and isinstance(val, str) and ISO_DATETIME.match(val):
                            val = T.cast(val, "timestamp")
                        for kind, reason, action, message in self._notes[start:]:
                            self._event(kind, reason, action, message, spec, ctx, name)
                    except ValueError as e:
                        try:
                            val = self._problem(spec, name, ctx, val, str(e), spec.on_error,
                                                "untyped_value" if problem else "invalid_value")
                        except _ValueFailed:
                            failed = True
                            continue
                    finally:
                        del self._notes[start:]
                    row[name] = val
            elif obj is not None and "value" not in row:
                row["value"] = obj  # lista de escalares explodida
        if failed:
            raise _ValueFailed
        if "record_hash" in spec.metadata_columns:
            row["cx_record_hash"] = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
        for m in spec.metadata_columns:
            if m != "record_hash":
                row[METADATA_TYPES[m][0]] = ctx.meta.get(m)
        return row

    def _problem(self, spec: TableSpec, column: str, ctx: Context, raw: Any, message: str, on_error: str,
                 reason: str) -> Any:
        """Valor que não pode ser gravado tal como veio: aplica o on_error e regista o evento.

        null -> anula (declarado); skip_row -> ignora a linha (declarado); fail -> erro, documento rejeitado.
        """
        self._value_problem = True
        if getattr(self, "_quiet", False):
            raise _Quiet(message)
        if on_error == "null":
            if self._event("declared", reason, "nulled", message, spec, ctx, column, raw):
                log.warning("Valor anulado — tabela '%s', coluna '%s', ficheiro '%s', valor %r: %s",
                            spec.name, column, ctx.meta.get("source_file"), raw, message)
            return None
        if on_error == "skip_row":
            self._event("declared", reason, "row_skipped", message, spec, ctx, column, raw)
            raise RowError(message)
        self._event("error", reason, "document_rejected", message, spec, ctx, column, raw)
        raise _ValueFailed

    def _value(self, spec: TableSpec, col: ColumnSpec, ctx: Context) -> Any:
        notes = self._notes
        start = len(notes)
        self._value_problem = False
        try:
            v = self._value_inner(spec, col, ctx)
            if not self._value_problem:   # um valor anulado/ignorado não deixa as notas do que se calculou antes
                for kind, reason, action, message in notes[start:]:
                    self._event(kind, reason, action, message, spec, ctx, col.name)
            return v
        finally:
            del notes[start:]

    def _value_inner(self, spec: TableSpec, col: ColumnSpec, ctx: Context) -> Any:
        on_error = col.on_error or spec.on_error
        raw: Any = None
        defaulted = False
        try:
            if col.cases is not None:
                for cond, case in col.cases:
                    if cond is None or evaluate(cond, ctx):
                        raw = apply_chain(self._elements(spec, col, ctx, self._source(spec, col, case, ctx)), case.transform)
                        break
            else:
                raw = self._elements(spec, col, ctx, self._source(spec, col, col, ctx))
            v = apply_chain(raw, col.transform)
            if v is None and col.default is not None:
                v, defaulted = col.default, True
            texts = _numeric_texts(v, col.type)
            if col.type and not T.needs_inference(col.type):
                v = T.cast(v, col.type, col.rounding)
        except ConfigError:
            raise  # erro da configuração (ex.: operador desconhecido): pára a carga, não é um problema do valor
        except Exception as e:  # noqa: BLE001 - erros dos dados ou das regras do utilizador
            return self._problem(spec, col.name, ctx, raw, str(e), on_error, getattr(e, "reason", "invalid_value"))
        if defaulted:
            self._event("declared", "missing_value", "default_applied", f"valor em falta; usado o default {col.default!r}",
                        spec, ctx, col.name, raw)
        if texts:
            if any(not t.strip(" \t\r\n") for t in texts):
                self._event("warning", "empty_text", "nulled", f"texto vazio numa coluna {col.type}: fica nulo",
                            spec, ctx, col.name, raw)
            if any(t.strip(" \t\r\n") for t in texts):
                self._event("warning", "number_as_text", "kept", f"número recebido como texto e lido como {v!r}",
                            spec, ctx, col.name, raw)
        if col.required and v is None:
            # obrigatório: 'null' não se aplica (anular um obrigatório não faz sentido) -> falha
            return self._problem(spec, col.name, ctx, raw, "coluna obrigatória vazia",
                                 "skip_row" if on_error == "skip_row" else "fail", "required_empty")
        if col.check:
            v = self._check(spec, col, v, ctx)
        if col.type is None and (problem := untyped_problem(v)):
            return self._problem(spec, col.name, ctx, v, problem, on_error, "untyped_value")
        return v

    def _source(self, spec: TableSpec, col: ColumnSpec, src: ColumnSpec, ctx: Context) -> Any:
        """Origem do valor (`src` é a coluna ou um caso do `cases`).

        Template com TODAS as partes em falta: nulo, como um campo ausente (nada se perde). Com só ALGUMAS em
        falta, as partes presentes perder-se-iam (nulo) ou o valor ficaria errado ('P|', igual para todos os
        elementos sem a parte): erro, a não ser com `template_missing: empty` (partes vazias) ou `null` (contado).
        """
        if src.template is None:
            return _source(src, ctx)
        text, missing, total = _render(src.template, ctx)
        if not missing:
            return text
        if len(missing) == total:
            return None
        msg = f"template {src.template!r}: sem valor para {', '.join(missing)}"
        if col.template_missing == "empty":
            self._event("declared", "template_missing", "rendered_empty", msg + " (ficaram vazias)", spec, ctx, col.name)
            return text
        if col.template_missing == "null":
            self._event("declared", "template_missing", "nulled", msg + " (valor nulo)", spec, ctx, col.name)
            return None
        raise TemplateIncomplete(msg + "; as partes presentes perder-se-iam: declare template_missing: empty ou null")

    def _elements(self, spec: TableSpec, col: ColumnSpec, ctx: Context, raw: Any) -> Any:
        """Resultado de um [*]/filtro: se há elementos sem o campo, só segue com `missing_elements` declarado.

        skip -> só os valores encontrados (como antes); null -> um nulo por elemento em falta. Sem declaração é
        erro: first/last/join/count/sum/arrays dariam resultados diferentes consoante a interpretação.
        """
        if not isinstance(raw, Projection):
            return raw
        if raw.missing:
            msg = f"'{raw.path}': {raw.missing} de {len(raw.slots)} elemento(s) sem o campo"
            if col.missing_elements is None:
                raise MissingElements(f"{msg}; declare missing_elements: skip (ignorar esses elementos) "
                                      f"ou null (um nulo por elemento)")
            action = "element_skipped" if col.missing_elements == "skip" else "element_nulled"
            self._event("declared", "missing_elements", action, msg, spec, ctx, col.name, raw.slots)
            return list(raw) if col.missing_elements == "skip" else list(raw.slots)
        return list(raw)

    def _check_duplicates(self, res: TableResult) -> None:
        """A mesma chave duas vezes no MESMO documento é um conflito nos dados (ex.: o mesmo filho duas vezes num
        BOM, com quantidades diferentes): erro, a não ser com on_duplicate: last | first (contado)."""
        spec = res.spec
        if not spec.keys or not spec.dedupe or len(res.origins) != len(res.rows):
            return
        seen: dict[tuple, int] = {}
        drop: set[int] = set()
        for i, r in enumerate(res.rows):
            doc = res.origins.docs[i]
            key = (doc,) + tuple(typed_key(r.get(k)) for k in spec.keys)
            if key not in seen:
                seen[key] = i
                continue
            first = seen[key]
            file, index = self._doc_info.get(doc, (None, None))
            keys = {k: r.get(k) for k in spec.keys}
            where = f"{res.origins.locator(first) or '(documento)'} e {res.origins.locator(i) or '(documento)'}"
            if spec.on_duplicate is None:
                self.report.add("error", "duplicate_key", "document_rejected",
                                f"a mesma chave aparece duas vezes no mesmo documento ({where}); declare on_duplicate: "
                                f"last | first se for de esperar", table=spec.name, file=file, document=index,
                                row=res.origins.locator(i), keys=keys, doc_key=doc)
                continue
            loser = first if spec.on_duplicate == "last" else i
            if spec.on_duplicate == "last":
                seen[key] = i
            drop.add(loser)
            self.report.add("declared", "duplicate_key", "row_removed",
                            f"chave repetida no mesmo documento ({where}); fica a {'última' if spec.on_duplicate == 'last' else 'primeira'}",
                            table=spec.name, file=file, document=index, row=res.origins.locator(loser), keys=keys)
        if drop:
            keep = [i for i in range(len(res.rows)) if i not in drop]
            res.rows = [res.rows[i] for i in keep]
            res.origins = res.origins.select(keep)

    def _cast_inferred(self, res: TableResult) -> None:
        """Colunas `type: struct` / `array<struct>`: estrutura inferida das linhas (dos documentos não rejeitados) e
        cast de cada valor; um valor que não serve rejeita o seu documento (e não a carga inteira)."""
        spec = res.spec
        res.inferred = {}
        rejected = set(self.report.rejected)
        keep = [i for i, d in enumerate(res.origins.docs) if d not in rejected] if len(res.origins) == len(res.rows) \
            else list(range(len(res.rows)))
        for col in spec.columns:
            if not col.type or not T.needs_inference(col.type):
                continue
            t = col.type
            tree = None
            for i in keep:
                v = res.rows[i].get(col.name)
                if T.parse(t)[0] == "struct" and isinstance(v, list) and len(v) == 1:
                    v = v[0]  # lista 1:1
                tree = T.merge(tree, T.infer_deep(v))
            if T.parse(t)[0] == "array" and isinstance(tree, tuple) and tree[0] != "array":
                tree = ("array", tree)
            res.inferred[col.name] = final = T.render(T.complete(tree))
            for i in keep:
                r = res.rows[i]
                start = len(self._notes)
                try:
                    r[col.name] = T.cast(r.get(col.name), final, col.rounding)
                except (ValueError, TypeError) as e:
                    doc = res.origins.docs[i] if len(res.origins) == len(res.rows) else None
                    file, index = self._doc_info.get(doc, (None, None))
                    self.report.add("error", "invalid_value", "document_rejected", f"{e} (tipo inferido {final})",
                                    table=spec.name, column=col.name, file=file, document=index,
                                    row=res.origins.locator(i) if doc is not None else None,
                                    value=r.get(col.name), doc_key=doc)
                finally:
                    del self._notes[start:]

    def _check(self, spec: TableSpec, col: ColumnSpec, v: Any, ctx: Context) -> Any:
        try:
            problem = check_value(col.check, v, ctx)
        except PathError as e:    # caminho do required_if com a forma errada
            return self._problem(spec, col.name, ctx, v, str(e), col.on_error or spec.on_error, e.reason)
        if problem is None:
            return v
        res = getattr(self, "_res", None)
        if res is not None:
            res.check_failures[col.name] = res.check_failures.get(col.name, 0) + 1
        action = col.check["on_fail"]
        if action == "warn":
            if self._event("warning", "check", "kept", problem, spec, ctx, col.name, v):
                log.warning("check falhou — tabela '%s', coluna '%s', ficheiro '%s': %s",
                            spec.name, col.name, ctx.meta.get("source_file"), problem)
            return v
        return self._problem(spec, col.name, ctx, v, problem, action, "check")

    # --- tipos, dedupe ----------------------------------------------------
    def _finalize(self, res: TableResult):
        spec = res.spec
        origins = res.origins if len(res.origins) == len(res.rows) else None

        def where(i: int) -> dict[str, Any]:
            if origins is None:
                return {}
            file, index = getattr(self, "_doc_info", {}).get(origins.docs[i], (None, None))
            return {"file": file, "document": index, "row": origins.locator(i)}

        all_columns = [c for sp in (res.specs or [spec]) for c in sp.columns]
        explicit = {c.name: res.inferred.get(c.name, c.type) for c in all_columns if c.type}
        order: dict[str, None] = {}
        for c in all_columns:
            order[c.name] = None
        for r in res.rows:
            for k in r:
                order.setdefault(k, None)
        types: dict[str, str] = {}
        protected = set(spec.keys) | set(spec.replace_by) | {
            n for n in order if any(re.search(rf"\b{re.escape(n)}\b", p) for p in spec.partition_by)}
        conflicts, omitted = [], []
        for name in order:
            if name in explicit:
                types[name] = explicit[name]
                continue
            meta = next((t for n, t in METADATA_TYPES.values() if n == name), None)
            if meta:
                types[name] = meta
                continue
            seen: dict[str, int] = {}       # tipo inferido -> 1.ª linha com esse tipo
            for i, r in enumerate(res.rows):
                t = _infer_untyped(r.get(name))
                if t is not None:
                    seen.setdefault(t, i)
            if len(seen) > 1:
                conflicts.append((name, seen))
            elif seen:
                types[name] = next(iter(seen))
            elif name in protected:
                types[name] = "string"      # chave/partição só com nulos: fica, com o tipo de sempre
            else:
                omitted.append(name)        # sem tipo e só nulos: o tipo seria adivinhado; a coluna não é criada
        if conflicts:
            lines = []
            for name, seen in conflicts[:10]:
                ex = "; ".join(f"{t} ({_where_text(where(i))} valor {short_value(res.rows[i].get(name))})"
                               for t, i in list(seen.items())[:4])
                lines.append(f"coluna '{name}' sem tipo recebe valores de tipos diferentes: {ex}")
            raise TransformError(f"tabela '{spec.name}': " + " | ".join(lines)
                                 + ". Declare o type da coluna (os valores que não servirem são rejeitados por documento).")
        for name in omitted:
            self.report.add("warning", "untyped_all_null", "column_omitted",
                            "coluna sem tipo e só com nulos neste lote: não é criada (o tipo seria adivinhado)",
                            table=spec.name, column=name)
            for r in res.rows:
                r.pop(name, None)
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
            # chaves nulas já foram tratadas por linha (_rows_for); aqui só duplicados ENTRE documentos do lote
            if spec.dedupe:
                uniq: dict[tuple, int] = {}
                for i, r in enumerate(res.rows):
                    key = tuple(typed_key(r.get(k)) for k in spec.keys)
                    if key in uniq:
                        new = where(i)
                        self.report.add(
                            "warning", "duplicate_key", "row_replaced",
                            f"a mesma chave noutro documento do lote: fica a de {new.get('file')}"
                            + (f"#{new['document']}" if new.get("document") is not None else "")
                            + (f" {new['row']}" if new.get("row") else "") + " (a última, como no merge)",
                            table=spec.name, keys={k: r.get(k) for k in spec.keys}, **where(uniq[key]))
                    uniq[key] = i
                if len(uniq) < len(res.rows):
                    log.info("Tabela '%s': %d duplicado(s) removido(s)", spec.name, len(res.rows) - len(uniq))
                    keep = list(uniq.values())
                    res.rows = [res.rows[i] for i in keep]
                    if origins is not None:
                        res.origins = origins.select(keep)
        res.types = types


def _infer_untyped(v: Any) -> str | None:
    """Tipo inferido de um valor sem tipo declarado; listas sem valores não dizem nada (não criam conflitos)."""
    if isinstance(v, list) and all(x is None for x in v):
        return None
    return T.infer(v)


def untyped_problem(v: Any) -> str | None:
    """Valor que, numa coluna sem `type`, mudaria ao ser guardado: inteiros acima de 2^53 e números com mais
    algarismos do que um double (a coluna fica double), e listas com tipos misturados (ficariam texto)."""
    if v is None or isinstance(v, (bool, str)):
        return None
    if isinstance(v, int):
        if abs(v) > 2 ** 53:
            return (f"o inteiro {v} não cabe exactamente num double (uma coluna sem tipo fica double); declare "
                    f"type: bigint, decimal(...) ou string")
        return None
    if isinstance(v, (JsonFloat, Decimal)):
        exact = Decimal(v.text) if isinstance(v, JsonFloat) else v
        if exact.is_finite() and Decimal(repr(float(exact))) != exact:
            return (f"{exact} tem mais algarismos do que um double guarda (uma coluna sem tipo fica double); declare "
                    f"type: decimal(...) ou type: double com rounding: nearest")
        return None
    if isinstance(v, list):
        kinds = {typed_key(x)[0] for x in v if x is not None and not isinstance(x, (dict, list))}
        if len(kinds) > 1:
            return f"lista com valores de tipos diferentes ({', '.join(sorted(kinds))}): ficariam todos texto; declare o type"
        for x in v:
            if (problem := untyped_problem(x)):
                return problem
    return None


def _where_text(w: dict) -> str:
    out = w.get("file") or "?"
    if w.get("document") is not None:
        out += f"#{w['document']}"
    if w.get("row"):
        out += f" {w['row']}"
    return out


def _locator(spec: TableSpec, ctx: Context) -> str | None:
    """Posição da linha no documento: 'childItems[1]' ou 'orders[0].lines[2]' (None sem explode)."""
    if not spec.explode:
        return None
    return ".".join(f"{lv}[{'-' if idx is None else idx}]" for lv, (_, idx) in zip(spec.explode, ctx.stack[1:]))


def _source(col: ColumnSpec, ctx: Context) -> Any:
    """Valor de origem (sem templates: esses passam por Engine._source, que regista as partes em falta)."""
    if col.has_const:
        raw = col.const
    elif col.paths:
        raw = next((v for v in (resolve(ctx, p) for p in col.paths) if v not in (MISSING, None, "")), None)
    elif col.path:
        raw = resolve(ctx, col.path)
    else:
        raw = None
    return None if raw is MISSING else raw


def _key_columns(spec: TableSpec) -> list[str]:
    """Colunas que identificam a linha: keys (merge/dedupe) e replace_by."""
    return list(dict.fromkeys(spec.keys + spec.replace_by))


def _key_on_error(spec: TableSpec, names: list[str]) -> str:
    """Chave nula: 'skip_row' só se declarado (na coluna ou na tabela); de resto é erro ('null' não se aplica)."""
    cols = {c.name: c for c in spec.columns}
    declared = [cols[n].on_error for n in names if n in cols and cols[n].on_error]
    return "skip_row" if "skip_row" in declared or (not declared and spec.on_error == "skip_row") else "fail"


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
        if "_regex" in chk and not chk["_regex"].search(text_of(item)):
            return f"{item!r} não corresponde a /{chk['regex']}/"
        if "_in" in chk:
            key = typed_key(item, chk.get("ignore_case", False))
            if key not in chk["_in"]:
                other = [v for v in chk["in"] if typed_key(v)[0] != key[0] and text_of(v).lower() == text_of(item).lower()]
                if other:   # 2 (número) e '2' (texto): tipos diferentes, não se adivinha
                    return f"{item!r} ({key[0]}) só corresponde a {other[0]!r} na lista, que é de outro tipo"
                return f"{item!r} não está na lista {chk['in']}"
        if "min" in chk or "max" in chk:
            if isinstance(item, bool) or not isinstance(item, (int, float, Decimal)):
                return f"{item!r} não é numérico"          # antes: true passava como 1 e '5' (texto) como 5
            x = to_decimal(item)
            if "min" in chk and x < to_decimal(chk["min"]):
                return f"{item!r} < mínimo {chk['min']}"
            if "max" in chk and x > to_decimal(chk["max"]):
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


_TPL_RE = TEMPLATE_RE


def _canonical_part(path: str, t: str | None, v: Any) -> Any:
    """Parte da chave do document_id na forma canónica: texto tal como está; outros valores só com o tipo
    declarado (`{path: version, type: int}`), e já convertidos: 0, '0' e 0.0 dão o mesmo UUID."""
    if t is None:
        if isinstance(v, str):
            return v
        raise ValueError(f"o campo '{path}' é {type(v).__name__} ({v!r}): declare o tipo, ex.: "
                         f"{{path: {path}, type: int}}, para que 0, '0' e 0.0 dêem o mesmo UUID")
    return T.cast(v, t)


def _key_part(v: Any) -> Any:
    """Valor usado para identificar (document_id.from): uma projecção com elementos em falta não é aceite."""
    if isinstance(v, Projection):
        if v.missing:
            raise MissingElements(f"'{v.path}': {v.missing} de {len(v.slots)} elemento(s) sem o campo "
                                  f"(num template ou no document_id não se aceitam elementos em falta)")
        return list(v)
    return v


def _render(template: str, ctx: Context) -> tuple[str, list[str], int]:
    """-> (texto, partes em falta, nº de partes). Quem chama decide o que fazer com as partes em falta."""
    missing: list[str] = []

    def repl(m):
        v = _key_part(resolve(ctx, m.group(1)))
        if v is MISSING or v is None:
            missing.append(m.group(1))
            return ""
        return text_of(v)   # o mesmo texto das colunas string (booleano true/false, objecto em JSON, ...)

    return _TPL_RE.sub(repl, template), missing, len(_TPL_RE.findall(template))


def flatten(obj: dict, exclude: list[str], max_depth: int, lists: str, naming: str = "full") -> dict[str, Any]:
    """Achata um objecto: {a: {b: 1}} -> {a_b: 1} (naming=full) ou {b: 1} (naming=leaf)."""
    out, _, _, _ = flatten_sources(_leaves(obj, exclude, max_depth, lists), naming)
    return out


def flatten_sources(leaves: list[tuple[tuple[str, ...], Any]], naming: str,
                    fixed: Any = None) -> tuple[dict, dict, list, list]:
    """-> (coluna -> valor, coluna -> caminho de origem, colisões [(coluna, caminho, caminho)], fora do modelo).
    Dois campos diferentes que dão o mesmo nome de coluna (ex.: fossilGhgEmissions e fossil_ghg_emissions)
    são uma colisão: antes ficava o primeiro e o outro perdia-se sem aviso.

    `fixed` (LeafNames, de model.schema): os nomes leaf vêm do modelo e não dos campos deste documento (antes o
    mesmo campo ia para 'fossil' num documento e 'ghg_fossil' noutro). Um campo fora do modelo fica com o nome
    completo (estável) e é devolvido em `fora do modelo`; se esse nome for o de um campo do modelo, é colisão."""
    if fixed is None:
        names = column_names([p for p, _ in leaves], naming)
    else:
        names = {p: fixed.names.get(p) or sanitize("_".join(p)) for p, _ in leaves}
    out: dict[str, Any] = {}
    sources: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []
    outside: list[tuple[str, str]] = []
    for p, v in leaves:
        name, dotted = names[p], ".".join(p)
        if fixed is not None and p not in fixed.names:
            if name in fixed.owner:
                collisions.append((name, fixed.owner[name], dotted))
                continue
            outside.append((name, dotted))
        if name in out:
            collisions.append((name, sources[name], dotted))
            continue
        out[name], sources[name] = v, dotted
    return out, sources, collisions, outside


def _leaves(obj: dict, exclude: list[str], max_depth: int, lists: str) -> list[tuple[tuple[str, ...], Any]]:
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
                leaves.append((p, json_text(v) if lists == "json" else v))
            else:
                leaves.append((p, v))

    walk(obj, (), 0)
    return leaves


def _same_source(col: ColumnSpec | None, dotted: str, level: int) -> bool:
    """A coluna explícita lê o mesmo campo que o modo auto (substituição intencional, ex.: para dar um tipo)."""
    if col is None or not col.path:
        return False
    p = col.path.strip()
    for prefix in ("$item.",) + (("$root.",) if level == 0 else ()):
        if p.startswith(prefix):
            p = p[len(prefix):]
    return p == dotted


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
def unwrap(obj: Any, rule: dict, source: str | None = None, path: str = "", firsts: list | None = None) -> Any:
    """Substitui listas que na prática são 1:1 pelo seu único elemento.

    rule = {paths: set, all: bool, except: set, on_multiple: fail|first|keep}
    Caminhos sem índices: 'pcfAssessmentAndMethodology.pcfAssessmentInformation.time'.
    `firsts` (opcional) recebe (caminho, nº de elementos) de cada lista reduzida ao 1.º elemento (on_multiple: first).
    """
    if isinstance(obj, list):
        return [unwrap(x, rule, source, path, firsts) for x in obj]
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
                    if firsts is not None:
                        firsts.append((p, len(v)))
                    v = v[0]
        out[k] = unwrap(v, rule, source, p, firsts)
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
    """Gera tabelas automaticamente: 1 tabela raiz + 1 tabela por cada lista de objectos.

    Com `children: schema` (omissão com model.schema) as tabelas filhas são as listas do modelo mais as do lote:
    uma lista que ficou vazia em todos os documentos do lote continua a ter tabela, e o replace_by/overwrite dela
    corre. Com `naming: leaf` os nomes das tabelas e das colunas são fixados pelo modelo, não pelo lote."""
    from .pathcheck import Problem, leaf_names_for, model_list_paths, model_position  # noqa: PLC0415

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
    walker = sub.walker
    base = dict(auto=True, auto_naming=naming, auto_lists="array", auto_max_depth=max_depth, metadata_columns=meta_cols,
                partition_by=list(d.get("partition_by", []) or []), on_error=d.get("on_error", "fail"),
                missing_columns=d.get("missing_columns"), athena_options=dict(d.get("athena_options", {}) or {}))
    common = [c for c in sub.common_columns if c.name != id_col]
    root_leaf = leaf_names_for(walker, walker.root, exclude + [id_path], max_depth, "array") if naming == "leaf" else None
    specs = [TableSpec(name=prefix, columns=[ColumnSpec(id_col, path=id_path)] + common, keys=[id_col],
                       mode=mode, auto_exclude=exclude + [id_path], leaf_names=root_leaf, **base)]
    found: dict[tuple, None] = {}
    for doc in docs:
        _array_paths(doc.data, [], [], found, 0, max_depth)
    in_model: list[tuple[str, ...]] = []
    if walker is not None and (an.get("children") == "schema" or naming == "leaf"):
        in_model = sorted(lv for lv in model_list_paths(walker, max_depth) if not any(x in exclude for x in lv))
    batch = [lv for lv in found if not any(x in exclude for x in lv)]
    if an.get("children") == "schema":
        model_set = set(in_model)
        kept = in_model + [lv for lv in batch if lv not in model_set]     # lista fora do modelo: também entra
    else:
        kept = batch

    def flat(lv) -> tuple[str, ...]:
        return tuple(".".join(lv).split("."))

    if naming == "leaf":
        fixed = column_names([flat(lv) for lv in in_model], "leaf")
        owners = {n: k for k, n in fixed.items()}
        tnames = {}
        for lv in kept:
            key = flat(lv)
            tnames[key] = fixed.get(key) or sanitize("_".join(key))
            if key not in fixed and tnames[key] in owners:
                raise TransformError(f"auto_normalize: a lista '{'.'.join(key)}' (fora do modelo) e a lista "
                                     f"'{'.'.join(owners[tnames[key]])}' dão a mesma tabela '{prefix}_{tnames[key]}'; "
                                     f"exclua uma com auto_normalize.exclude")
    else:
        tnames = column_names([flat(lv) for lv in kept], naming)
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
        opts = dict(base)
        if naming == "leaf":
            try:
                pos = model_position(walker, list(levels))
            except Problem:
                pos = None
            if pos is None:
                opts["auto_naming"] = "full"     # lista fora do modelo: nomes completos (estáveis)
            else:
                opts["leaf_names"] = leaf_names_for(walker, pos, exclude, max_depth, "array")
        specs.append(TableSpec(
            name=sanitize(f"{prefix}_{tnames[flat(levels)]}"), columns=cols, explode=list(levels),
            keys=[id_col] + idx_cols, mode=child_mode, dedupe=True,
            replace_by=[id_col] if mode == "merge" else [], auto_exclude=exclude, auto_collision="prefix", **opts,
        ))
    return specs
