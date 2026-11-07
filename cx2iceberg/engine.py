"""Applies the configuration's rules to JSON documents and produces rows per table."""
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
    meta: dict[str, Any] = field(default_factory=dict)  # e.g. document_id
    index: int | None = None   # position in the file (JSON list or JSON Lines); None if the file is a single document
    error: str | None = None   # could not be read (e.g. invalid JSON, not UTF-8): the document is rejected
    notice: str | None = None  # nothing to process, but worth recording (e.g. file without documents)


class RowOrigins:
    """Which document and which explode position each row came from, in compact memory.

    A dict per row cost ~240 bytes (more than the row itself). Here integers are kept in arrays:
    8 bytes for the document number, 8 per explode level and 2 for the explode path (a table can be
    fed by rules with different explodes). File, position in the file and the text 'childItems[3]' are only
    rebuilt when a report event needs them.
    """

    ABSENT, EMPTY = -2, -1   # level this row does not have | empty explode with keep_empty

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
    check_failures: dict[str, int] = field(default_factory=dict)  # column -> number of values that failed the check
    origins: RowOrigins = field(default_factory=RowOrigins, repr=False)  # per row: document and position (compact)
    inferred: dict[str, str] = field(default_factory=dict, repr=False)   # final type of the inferred struct columns
    specs: list = field(default_factory=list, repr=False)                # every rule that feeds the table
    # replace_by: key of EVERY accepted document that went through the table, even without rows (a list that became
    # empty, every row excluded): the DELETE of the old rows comes from here, not from the rows (old children used to stay)
    replace_keys: dict[int, tuple] = field(default_factory=dict, repr=False)
    # accepted documents that went through the table: with mode overwrite and zero rows, the table becomes empty (that
    # is what the batch says); with no document, the batch says nothing about it and it is left alone
    documents: set[int] = field(default_factory=set, repr=False)


ISO_DATETIME = T.ISO_DATETIME
MAX_DEPTH = 64   # nesting levels; Catena-X models stay far below (PCF 9.0: ~12)
_SURROGATE = re.compile("[\ud800-\udfff]")
NUMERIC_TYPES = ("int", "bigint", "smallint", "tinyint", "double", "float")


@functools.lru_cache(maxsize=1024)
def _numeric_base(t: str) -> bool:
    base = t
    while base.startswith("array<"):
        base = base[len("array<"):-1]
    return base.startswith(NUMERIC_TYPES + ("decimal",))


def _numeric_texts(v: Any, t: str | None) -> list[str]:
    """Texts that will be read as numbers (numeric column or array of numbers): for the report."""
    if not t or not isinstance(v, (str, list)) or not _numeric_base(t):
        return []
    return [x for x in (v if isinstance(v, list) else [v]) if isinstance(x, str)]


def document_problem(data: Any) -> str | None:
    """Why a document cannot go on as it is, or None.

    Depth above MAX_DEPTH (protects the recursive functions and the whole load), non-finite numbers
    (NaN, inf: a document built in code or pasted into the studio) and text with lone surrogates
    ('\\ud800': passes the engine but cannot be written as UTF-8 — it would only fail at write time).
    Iterative walk; the path is only computed (2nd pass) when there is a problem.
    """
    def scan(with_path: bool) -> tuple[str, str] | None:
        stack: list[tuple[Any, int, str]] = [(data, 0, "")]
        while stack:
            obj, depth, path = stack.pop()
            if depth >= MAX_DEPTH:
                return f"more than {MAX_DEPTH} levels of nesting", path
            items = obj.items() if isinstance(obj, dict) else enumerate(obj)
            for k, v in items:
                if isinstance(k, str) and not k.isascii() and _SURROGATE.search(k):
                    return "key with a lone UTF-16 character (e.g. \\ud800), which cannot be written as UTF-8", path
                sub = "" if not with_path else (f"{path}.{k}" if path else str(k)) if isinstance(obj, dict) else f"{path}[{k}]"
                if isinstance(v, (dict, list)):
                    stack.append((v, depth + 1, sub))
                elif isinstance(v, str):
                    if not v.isascii() and _SURROGATE.search(v):
                        return "text with a lone UTF-16 character (e.g. \\ud800), which cannot be written as UTF-8", sub
                elif isinstance(v, float) and not math.isfinite(v):
                    return f"non-finite number ({v})", sub
        return None

    if not isinstance(data, (dict, list)):
        return None
    if scan(False) is None:
        return None
    reason, path = scan(True)
    path = path or "(root)"
    return f"{reason} at '{path if len(path) <= 120 else path[:120] + '…'}'"


class RowError(Exception):
    """The row is skipped (skip_row declared); the event is already in the report."""


class _Quiet(Exception):
    """Problem in a silent computation (replace_by key at document level)."""


class _ValueFailed(Exception):
    """Invalid value with no declared loss; the error is already in the report and the document is rejected."""


class TransformError(ValueError):
    pass


class TemplateIncomplete(ValueError):
    """Template with only some parts missing, and no template_missing declared."""

    reason = "template_missing"


class MissingElements(PathError):
    """A [*]/filter with elements that lack the field, in a column without `missing_elements` declared (or in a key)."""

    reason = "missing_elements"


class DocumentsRejected(TransformError):
    """One or more documents have errors. `results` has the rows of the others; `report` has the details."""

    def __init__(self, report: Report, results: dict[str, "TableResult"]):
        self.report, self.results = report, results
        msgs = report.error_messages(10)
        more = f"\n  … (+{report.errors - len(msgs)} error(s))" if report.errors > len(msgs) else ""
        super().__init__(f"{len(report.rejected)} document(s) rejected; none of their rows was written. Errors:\n  "
                         + "\n  ".join(msgs) + more)


# --- conditions: see conditions.py (validated at load time, compared with their type) --------


# --- submodel matching -------------------------------------------------
def matches(sub: SubmodelSpec, doc: Document) -> bool:
    return _match(sub.match, doc)


def _match(m: dict, doc: Document) -> bool:
    """Every criterion must pass; `any: [{...}, {...}]` passes if one of the blocks passes."""
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
        return False   # the document has another shape (e.g. another version of the model): no match


# --- engine ----------------------------------------------------------------
class Engine:
    """Applies the configuration to documents.

    Errors do not stop at the first one: each document with errors is rejected as a whole (none of its rows,
    in any table) and everything goes into `self.report`. `run()` raises `DocumentsRejected` if there are rejected
    documents; with `partial=True` it returns the rows of the others (the caller decides what to write).
    Configuration and batch errors (e.g. the error limit) always raise and return nothing.
    """

    def __init__(self, config: Config, match_all: bool | None = None, report: Report | None = None):
        self.config = config
        self.match_all = config.input.get("match_all", False) if match_all is None else match_all
        self.ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.validation: list[dict[str, Any]] = []   # documents that do not match the model's schema
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
                log.warning("No submodel in the configuration matches %s (semantic_id=%s)", doc.source_file, doc.semantic_id)
        return routed

    def run(self, docs: Iterable[Document], partial: bool = False) -> dict[str, TableResult]:
        settings = {"naive_timestamps": self.config.input.get("naive_timestamps", "fail")}
        try:
            with N.scope(settings) as notes:
                self._notes = notes       # notes from the conversions (declared losses, warnings) of each value
                results = self._run(docs)
        except ReportLimit as e:
            self.report.failure = str(e)
            err = TransformError(f"{e}. First errors:\n  " + "\n  ".join(self.report.error_messages(10)))
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
        # sequence number of each document in this load: identifies it even after unwrap/document_id
        docs = [dataclasses.replace(d, meta={**d.meta, "_seq": (rep.documents + i)}) for i, d in enumerate(docs)]
        rep.documents += len(docs)
        self._doc_info = {d.meta["_seq"]: (d.source_file, d.index) for d in docs}   # 1 entry per document
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
        self._notes.clear()           # notes from the match conditions do not describe stored values
        matched = {id(d) for ds in routed.values() for d in ds}
        skip_unmatched = self.config.input.get("on_unmatched", "fail") == "skip"
        for d in readable:
            if id(d) not in matched:
                msg = f"no submodel in the configuration matches (semantic_id={d.semantic_id})"
                if skip_unmatched:
                    self._doc_event(d, "declared", "no_submodel", "ignored", msg)
                else:
                    self._doc_event(d, "error", "no_submodel", "document_rejected",
                                    msg + "; declare input.on_unmatched: skip to ignore it")
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
                            f"auto_normalize.children: batch — child tables only from the lists present in this batch "
                            f"({children or 'none'}); those of a list that was empty in every document of the batch "
                            f"were neither deleted nor replaced", table=generated[0].name)
            if sub.document_id:
                tables = [_with_column(t, ColumnSpec(sub.document_id["column"], path="$meta.document_id", type="string"))
                          for t in tables]
            for spec in tables:
                res = results.setdefault(spec.name, TableResult(spec))
                if spec not in res.specs:
                    res.specs.append(spec)          # the configuration ensures they agree (mode, keys, types)
                for doc in sub_docs:
                    self._rows_for(spec, doc, res)
        for name, res in results.items():
            self._check_duplicates(res)   # repeated key in the same document: error (or on_duplicate declared)
            self._cast_inferred(res)      # may reject documents (a value that does not fit the inferred type)
        rejected = set(rep.rejected)
        for name, res in results.items():
            dropped = 0
            res.replace_keys = {d: k for d, k in res.replace_keys.items() if d not in rejected}   # rejected: deletes nothing
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
            except Exception as e:  # noqa: BLE001 - a table-level error (inferred types), not a document's
                raise TransformError(f"table '{name}': {e}") from e
            rep.tables[name] = {"rows": len(res.rows), "rows_from_rejected_documents": dropped}
        return results

    # --- report events --------------------------------------------
    def _doc_event(self, doc: Document, kind: str, reason: str, action: str, message: str) -> None:
        """Event at document level (no table/column)."""
        self.report.add(kind, reason, action, message, file=doc.source_file, document=doc.index,
                        doc_key=doc.meta.get("_seq"))

    def _event(self, kind: str, reason: str, action: str, message: str, spec: TableSpec, ctx: Context,
               column: str | None = None, value: Any = NO_VALUE) -> bool:
        """Event at row level: current document, explode position, table and column.

        Returns True if the event was kept as an example in the report (only those also go to the log).
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
            summary = "; ".join(f"{p['path'] or '(root)'}: {p['message']}" for p in problems[:3])
            more = f" (+{len(problems) - 3})" if len(problems) > 3 else ""
            msg = f"does not match the schema of '{sub.name}': {summary}{more}"
            if sub.validate == "fail":
                self._doc_event(d, "error", "schema", "document_rejected", msg)
                continue
            if sub.validate == "skip":
                log.warning("%s %s — document ignored", d.source_file, msg)
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
                                f"'{p}' has {n} elements; the first was used (unwrap on_multiple: first)")
            out.append(dataclasses.replace(d, data=data, meta={**d.meta, "_raw": d.data}))
        return out

    def _with_document_id(self, sub: SubmodelSpec, doc: Document) -> Document | None:
        rule = sub.document_id
        raw = doc.meta.get("_raw", doc.data)  # content as it arrived (before the unwrap)
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
                    msg = f"document_id: field(s) {missing} missing"
                    if rule["on_missing"] != "content":
                        self._doc_event(doc, "error", "document_id_key_missing", "document_rejected",
                                        msg + "; the UUID would change once the field appeared (duplicate rows). "
                                              "Declare document_id.on_missing: content to use the content")
                        return None
                    log.warning("%s in %s — the document's content was used", msg, doc.source_file)
                    self._doc_event(doc, "declared", "document_id_key_missing", "content_hash",
                                    msg + "; UUID computed from the content (on_missing: content)")
                else:
                    name = "key:" + "|".join(json.dumps(p, sort_keys=True, default=str) for p in parts)
            if name is None:
                name = "content:" + json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
            value = str(uuid.uuid5(ns, name))
        meta = {k: v for k, v in doc.meta.items() if k != "_raw"}
        return dataclasses.replace(doc, meta={**meta, "document_id": value})

    # --- row generation -----------------------------------------------
    def _contexts(self, ctx: Context, levels: list[str], keep_empty: bool) -> Iterator[Context]:
        if not levels:
            yield ctx
            return
        val = resolve(ctx, levels[0])
        if val is not MISSING and val is not None and not isinstance(val, (list, dict)):
            raise PathShapeError(f"explode '{levels[0]}': expected a list (or object), found {type(val).__name__} {val!r}")
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
                # notes outside a value (e.g. dates compared in the where) are not kept: without this they would pile
                # up, one per row, for the whole load
                self._notes.clear()
                try:
                    keep = evaluate(spec.where, ctx)
                    if not keep:
                        self._event("declared", "where_false", "row_not_generated", "row excluded by the table's where",
                                    spec, ctx)
                        continue
                    row = self._row(spec, ctx)
                except PathError as e:    # where path with the wrong shape: the table's on_error ('null' does not apply)
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
                    log.debug("Row skipped in %s: %s", spec.name, e)
                    continue
                except _ValueFailed:
                    continue
                if (null_keys := [k for k in _key_columns(spec) if row.get(k) is None]):
                    try:
                        self._problem(spec, null_keys[0], ctx, NO_VALUE,
                                      f"null key ({', '.join(null_keys)}): the row cannot be identified",
                                      _key_on_error(spec, null_keys), "null_key")
                    except RowError:
                        res.skipped += 1
                    except _ValueFailed:
                        pass
                    continue
                res.rows.append(row)
                res.origins.append(doc.meta.get("_seq", -1), tuple(spec.explode), tuple(idx for _, idx in ctx.stack[1:]))
        except PathError as e:
            # the explode does not fit the document's shape: no row of this table can be trusted -> document rejected
            self.report.add("error", e.reason, "document_rejected", str(e), table=spec.name, file=doc.source_file,
                            document=doc.index, doc_key=doc.meta.get("_seq"))

    def _replace_key(self, spec: TableSpec, doc: Document, root: Context, res: TableResult) -> bool:
        """replace_by key computed at document level (the columns come from $root/$meta, validated at load time).
        Without it, a document whose list became empty did not delete the old rows. False = document rejected."""
        ctx = root
        for _ in spec.explode:
            ctx = ctx.child({}, None)
        cols = {c.name: c for c in spec.columns}
        start = len(self._notes)
        self._quiet = True
        try:
            key = tuple(self._value_inner(spec, cols[name], ctx) for name in spec.replace_by)
            problem = None if all(v is not None for v in key) else \
                f"replace_by: {', '.join(n for n, v in zip(spec.replace_by, key) if v is None)} null in this document"
        except (_Quiet, RowError, _ValueFailed, ValueError) as e:
            problem = f"replace_by: the document's key could not be computed: {e}"
        finally:
            self._quiet = False
            del self._notes[start:]
        if problem:
            self.report.add("error", "null_key", "document_rejected", problem + " (the old rows would not be deleted)",
                            table=spec.name, file=doc.source_file, document=doc.index, doc_key=doc.meta.get("_seq"))
            return False
        res.replace_keys[doc.meta.get("_seq", -1)] = key
        return True

    def _row(self, spec: TableSpec, ctx: Context) -> dict[str, Any]:
        row: dict[str, Any] = {}
        failed = False  # carries on with the other columns so the report shows every error of the row
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
                                f"field '{dotted}' is not in the model: it goes to column '{name}' (full name; "
                                f"leaf names are fixed by model.schema)", spec, ctx, name)
                for name, first, second in collisions:
                    try:
                        self._problem(spec, name, ctx, NO_VALUE, f"fields '{first}' and '{second}' give the same column "
                                      f"'{name}'; exclude one with auto_exclude or declare explicit columns",
                                      "skip_row" if spec.on_error == "skip_row" else "fail", "auto_collision")
                    except _ValueFailed:
                        failed = True
                explicit_cols = {c.name: c for c in spec.columns}
                level = len(ctx.stack) - 1
                for name, val in fields.items():
                    if name in row:
                        if _same_source(explicit_cols.get(name), sources[name], level):
                            continue                 # the explicit column replaces this field (same source)
                        if spec.auto_collision == "prefix" and f"item_{name}" not in row:
                            name = f"item_{name}"
                        elif spec.auto_collision == "skip":
                            self._event("declared", "auto_collision", "auto_skipped",
                                        f"field '{sources[name]}' is left out: column '{name}' already exists (auto_collision: skip)",
                                        spec, ctx, name)
                            continue
                        else:
                            try:
                                self._problem(spec, name, ctx, NO_VALUE,
                                              f"field '{sources[name]}' gives column '{name}', which already exists with another "
                                              f"source; declare auto_collision: prefix (item_{name}) or skip",
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
                row["value"] = obj  # exploded list of scalars
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
        """A value that cannot be stored as it came: applies on_error and records the event.

        null -> set to null (declared); skip_row -> skips the row (declared); fail -> error, document rejected.
        """
        self._value_problem = True
        if getattr(self, "_quiet", False):
            raise _Quiet(message)
        if on_error == "null":
            if self._event("declared", reason, "nulled", message, spec, ctx, column, raw):
                log.warning("Value set to null — table '%s', column '%s', file '%s', value %r: %s",
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
            if not self._value_problem:   # a value set to null/skipped does not keep the notes of what was computed before
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
            raise  # configuration error (e.g. unknown operator): stops the load, it is not a problem with the value
        except Exception as e:  # noqa: BLE001 - errors in the data or in the user's rules
            return self._problem(spec, col.name, ctx, raw, str(e), on_error, getattr(e, "reason", "invalid_value"))
        if defaulted:
            self._event("declared", "missing_value", "default_applied", f"missing value; default {col.default!r} used",
                        spec, ctx, col.name, raw)
        if texts:
            if any(not t.strip(" \t\r\n") for t in texts):
                self._event("warning", "empty_text", "nulled", f"empty text in a {col.type} column: becomes null",
                            spec, ctx, col.name, raw)
            if any(t.strip(" \t\r\n") for t in texts):
                self._event("warning", "number_as_text", "kept", f"number received as text and read as {v!r}",
                            spec, ctx, col.name, raw)
        if col.required and v is None:
            # required: 'null' does not apply (setting a required value to null makes no sense) -> fails
            return self._problem(spec, col.name, ctx, raw, "required column is empty",
                                 "skip_row" if on_error == "skip_row" else "fail", "required_empty")
        if col.check:
            v = self._check(spec, col, v, ctx)
        if col.type is None and (problem := untyped_problem(v)):
            return self._problem(spec, col.name, ctx, v, problem, on_error, "untyped_value")
        return v

    def _source(self, spec: TableSpec, col: ColumnSpec, src: ColumnSpec, ctx: Context) -> Any:
        """Source of the value (`src` is the column or one of the `cases`).

        Template with ALL parts missing: null, like an absent field (nothing is lost). With only SOME
        missing, the present parts would be lost (null) or the value would be wrong ('P|', the same for every
        element without the part): error, unless `template_missing: empty` (empty parts) or `null` (counted).
        """
        if src.template is None:
            return _source(src, ctx)
        text, missing, total = _render(src.template, ctx)
        if not missing:
            return text
        if len(missing) == total:
            return None
        msg = f"template {src.template!r}: no value for {', '.join(missing)}"
        if col.template_missing == "empty":
            self._event("declared", "template_missing", "rendered_empty", msg + " (left empty)", spec, ctx, col.name)
            return text
        if col.template_missing == "null":
            self._event("declared", "template_missing", "nulled", msg + " (null value)", spec, ctx, col.name)
            return None
        raise TemplateIncomplete(msg + "; the present parts would be lost: declare template_missing: empty or null")

    def _elements(self, spec: TableSpec, col: ColumnSpec, ctx: Context, raw: Any) -> Any:
        """Result of a [*]/filter: if some elements lack the field, it only goes on with `missing_elements` declared.

        skip -> only the values found (as before); null -> one null per missing element. Without a declaration it is
        an error: first/last/join/count/sum/arrays would give different results depending on the interpretation.
        """
        if not isinstance(raw, Projection):
            return raw
        if raw.missing:
            msg = f"'{raw.path}': {raw.missing} of {len(raw.slots)} element(s) without the field"
            if col.missing_elements is None:
                raise MissingElements(f"{msg}; declare missing_elements: skip (ignore those elements) "
                                      f"or null (one null per element)")
            action = "element_skipped" if col.missing_elements == "skip" else "element_nulled"
            self._event("declared", "missing_elements", action, msg, spec, ctx, col.name, raw.slots)
            return list(raw) if col.missing_elements == "skip" else list(raw.slots)
        return list(raw)

    def _check_duplicates(self, res: TableResult) -> None:
        """The same key twice in the SAME document is a conflict in the data (e.g. the same child twice in a
        BOM, with different quantities): error, unless on_duplicate: last | first (counted)."""
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
            where = f"{res.origins.locator(first) or '(document)'} and {res.origins.locator(i) or '(document)'}"
            if spec.on_duplicate is None:
                self.report.add("error", "duplicate_key", "document_rejected",
                                f"the same key appears twice in the same document ({where}); declare on_duplicate: "
                                f"last | first if that is expected", table=spec.name, file=file, document=index,
                                row=res.origins.locator(i), keys=keys, doc_key=doc)
                continue
            loser = first if spec.on_duplicate == "last" else i
            if spec.on_duplicate == "last":
                seen[key] = i
            drop.add(loser)
            self.report.add("declared", "duplicate_key", "row_removed",
                            f"repeated key in the same document ({where}); the {'last' if spec.on_duplicate == 'last' else 'first'} one is kept",
                            table=spec.name, file=file, document=index, row=res.origins.locator(loser), keys=keys)
        if drop:
            keep = [i for i in range(len(res.rows)) if i not in drop]
            res.rows = [res.rows[i] for i in keep]
            res.origins = res.origins.select(keep)

    def _cast_inferred(self, res: TableResult) -> None:
        """`type: struct` / `array<struct>` columns: structure inferred from the rows (of the documents not rejected) and
        a cast of each value; a value that does not fit rejects its document (not the whole load)."""
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
                    v = v[0]  # 1:1 list
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
                    self.report.add("error", "invalid_value", "document_rejected", f"{e} (inferred type {final})",
                                    table=spec.name, column=col.name, file=file, document=index,
                                    row=res.origins.locator(i) if doc is not None else None,
                                    value=r.get(col.name), doc_key=doc)
                finally:
                    del self._notes[start:]

    def _check(self, spec: TableSpec, col: ColumnSpec, v: Any, ctx: Context) -> Any:
        try:
            problem = check_value(col.check, v, ctx)
        except PathError as e:    # required_if path with the wrong shape
            return self._problem(spec, col.name, ctx, v, str(e), col.on_error or spec.on_error, e.reason)
        if problem is None:
            return v
        res = getattr(self, "_res", None)
        if res is not None:
            res.check_failures[col.name] = res.check_failures.get(col.name, 0) + 1
        action = col.check["on_fail"]
        if action == "warn":
            if self._event("warning", "check", "kept", problem, spec, ctx, col.name, v):
                log.warning("check failed — table '%s', column '%s', file '%s': %s",
                            spec.name, col.name, ctx.meta.get("source_file"), problem)
            return v
        return self._problem(spec, col.name, ctx, v, problem, action, "check")

    # --- types, dedupe ----------------------------------------------------
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
            seen: dict[str, int] = {}       # inferred type -> 1st row with that type
            for i, r in enumerate(res.rows):
                t = _infer_untyped(r.get(name))
                if t is not None:
                    seen.setdefault(t, i)
            if len(seen) > 1:
                conflicts.append((name, seen))
            elif seen:
                types[name] = next(iter(seen))
            elif name in protected:
                types[name] = "string"      # key/partition with only nulls: kept, with the usual type
            else:
                omitted.append(name)        # untyped and only nulls: the type would be guessed; the column is not created
        if conflicts:
            lines = []
            for name, seen in conflicts[:10]:
                ex = "; ".join(f"{t} ({_where_text(where(i))} value {short_value(res.rows[i].get(name))})"
                               for t, i in list(seen.items())[:4])
                lines.append(f"untyped column '{name}' receives values of different types: {ex}")
            raise TransformError(f"table '{spec.name}': " + " | ".join(lines)
                                 + ". Declare the column's type (values that do not fit are rejected per document).")
        for name in omitted:
            self.report.add("warning", "untyped_all_null", "column_omitted",
                            "untyped column with only nulls in this batch: not created (the type would be guessed)",
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
            raise ConfigError(f"Table '{spec.name}': key columns {missing} do not exist. Columns: {list(types)}")
        if spec.keys:
            # null keys were already handled per row (_rows_for); here only duplicates BETWEEN documents of the batch
            if spec.dedupe:
                uniq: dict[tuple, int] = {}
                for i, r in enumerate(res.rows):
                    key = tuple(typed_key(r.get(k)) for k in spec.keys)
                    if key in uniq:
                        new = where(i)
                        self.report.add(
                            "warning", "duplicate_key", "row_replaced",
                            f"the same key in another document of the batch: the one from {new.get('file')}"
                            + (f"#{new['document']}" if new.get("document") is not None else "")
                            + (f" {new['row']}" if new.get("row") else "") + " is kept (the last one, as in merge)",
                            table=spec.name, keys={k: r.get(k) for k in spec.keys}, **where(uniq[key]))
                    uniq[key] = i
                if len(uniq) < len(res.rows):
                    log.info("Table '%s': %d duplicate(s) removed", spec.name, len(res.rows) - len(uniq))
                    keep = list(uniq.values())
                    res.rows = [res.rows[i] for i in keep]
                    if origins is not None:
                        res.origins = origins.select(keep)
        res.types = types


def _infer_untyped(v: Any) -> str | None:
    """Inferred type of a value with no declared type; lists without values say nothing (they create no conflicts)."""
    if isinstance(v, list) and all(x is None for x in v):
        return None
    return T.infer(v)


def untyped_problem(v: Any) -> str | None:
    """A value that would change when stored in a column without `type`: integers above 2^53 and numbers with more
    digits than a double holds (the column becomes double), and lists with mixed types (they would become text)."""
    if v is None or isinstance(v, (bool, str)):
        return None
    if isinstance(v, int):
        if abs(v) > 2 ** 53:
            return (f"the integer {v} does not fit exactly in a double (an untyped column becomes double); declare "
                    f"type: bigint, decimal(...) or string")
        return None
    if isinstance(v, (JsonFloat, Decimal)):
        exact = Decimal(v.text) if isinstance(v, JsonFloat) else v
        if exact.is_finite() and Decimal(repr(float(exact))) != exact:
            return (f"{exact} has more digits than a double holds (an untyped column becomes double); declare "
                    f"type: decimal(...) or type: double with rounding: nearest")
        return None
    if isinstance(v, list):
        kinds = {typed_key(x)[0] for x in v if x is not None and not isinstance(x, (dict, list))}
        if len(kinds) > 1:
            return f"list with values of different types ({', '.join(sorted(kinds))}): they would all become text; declare the type"
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
    """Position of the row in the document: 'childItems[1]' or 'orders[0].lines[2]' (None without explode)."""
    if not spec.explode:
        return None
    return ".".join(f"{lv}[{'-' if idx is None else idx}]" for lv, (_, idx) in zip(spec.explode, ctx.stack[1:]))


def _source(col: ColumnSpec, ctx: Context) -> Any:
    """Source value (no templates: those go through Engine._source, which records the missing parts)."""
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
    """Columns that identify the row: keys (merge/dedupe) and replace_by."""
    return list(dict.fromkeys(spec.keys + spec.replace_by))


def _key_on_error(spec: TableSpec, names: list[str]) -> str:
    """Null key: 'skip_row' only if declared (on the column or the table); otherwise it is an error ('null' does not apply)."""
    cols = {c.name: c for c in spec.columns}
    declared = [cols[n].on_error for n in names if n in cols and cols[n].on_error]
    return "skip_row" if "skip_row" in declared or (not declared and spec.on_error == "skip_row") else "fail"


def check_value(chk: dict, v: Any, ctx: Context) -> str | None:
    """Returns the description of the problem, or None if the value is valid."""
    if v is None or v == []:
        if chk.get("required"):
            return "required value missing"
        if "required_if" in chk and evaluate(chk["required_if"], ctx):
            return f"value required when {chk['required_if']}"
        return None
    for item in v if isinstance(v, list) else [v]:
        if item is None:
            continue
        if "_regex" in chk and not chk["_regex"].search(text_of(item)):
            return f"{item!r} does not match /{chk['regex']}/"
        if "_in" in chk:
            key = typed_key(item, chk.get("ignore_case", False))
            if key not in chk["_in"]:
                other = [v for v in chk["in"] if typed_key(v)[0] != key[0] and text_of(v).lower() == text_of(item).lower()]
                if other:   # 2 (number) and '2' (text): different types, no guessing
                    return f"{item!r} ({key[0]}) only matches {other[0]!r} in the list, which is of another type"
                return f"{item!r} is not in the list {chk['in']}"
        if "min" in chk or "max" in chk:
            if isinstance(item, bool) or not isinstance(item, (int, float, Decimal)):
                return f"{item!r} is not numeric"          # before: true passed as 1 and '5' (text) as 5
            x = to_decimal(item)
            if "min" in chk and x < to_decimal(chk["min"]):
                return f"{item!r} < minimum {chk['min']}"
            if "max" in chk and x > to_decimal(chk["max"]):
                return f"{item!r} > maximum {chk['max']}"
    return None


_VALIDATORS: dict[str, Any] = {}


def schema_validator(config: Config, sub: SubmodelSpec):
    """jsonschema validator for the submodel's SAMM schema (cached per file)."""
    try:
        import jsonschema
    except ImportError as e:  # pragma: no cover
        raise ConfigError("To validate documents install jsonschema (pip install jsonschema)") from e
    path = Path(os.path.normpath((config.base_dir or Path.cwd()) / sub.model["schema"]))
    key = str(path.resolve())
    if key not in _VALIDATORS:
        if not path.is_file():
            raise ConfigError(f"Submodel '{sub.name}': schema not found at {path}")
        schema = json.loads(path.read_text(encoding="utf-8"))
        _VALIDATORS[key] = jsonschema.validators.validator_for(schema)(schema)
    return _VALIDATORS[key]


_TYPE_NAMES = {"string": "text", "number": "number", "integer": "integer", "boolean": "true/false",
               "object": "object", "array": "list", "null": "null"}


def describe_error(e) -> dict[str, str]:
    """jsonschema error -> readable {path, message}."""
    path = ""
    for part in e.absolute_path:
        path += f"[{part}]" if isinstance(part, int) else (f".{part}" if path else str(part))
    v, val = e.validator, e.validator_value
    if v == "required":
        m = re.search(r"'([^']+)' is a required property", e.message)
        msg = f"required field '{m.group(1) if m else e.message}' is missing"
    elif v == "type":
        names = " or ".join(_TYPE_NAMES.get(t, t) for t in (val if isinstance(val, list) else [val]))
        msg = f"{json.dumps(e.instance, ensure_ascii=False, default=str)[:60]} should be {names}"
    elif v == "pattern":
        kind = "an ISO 8601 date/time" if "[0-9]{3,}" in str(val) and "T" in str(val) else "the expected format"
        msg = f"{json.dumps(e.instance, ensure_ascii=False)[:60]} is not {kind}"
    elif v == "enum":
        msg = f"{json.dumps(e.instance, ensure_ascii=False)[:60]} is not one of the allowed values ({', '.join(map(str, val))})"
    elif v in ("minimum", "exclusiveMinimum"):
        msg = f"{e.instance} is less than the minimum {val}"
    elif v in ("maximum", "exclusiveMaximum"):
        msg = f"{e.instance} is greater than the maximum {val}"
    elif v == "minItems":
        msg = f"the list must have at least {val} element(s)"
    elif v == "minLength":
        msg = "cannot be empty"
    elif v == "additionalProperties":
        msg = f"field not defined in the model: {e.message.split('(')[-1].rstrip(')')}"
    else:
        msg = e.message
    return {"path": path, "message": msg, "rule": v}


def _with_column(spec: TableSpec, col: ColumnSpec) -> TableSpec:
    """Copy of the table with the column first (if it is not there yet)."""
    if any(c.name == col.name for c in spec.columns):
        return spec
    return dataclasses.replace(spec, columns=[col] + spec.columns)


_TPL_RE = TEMPLATE_RE


def _canonical_part(path: str, t: str | None, v: Any) -> Any:
    """Part of the document_id key in canonical form: text as it is; other values only with the declared
    type (`{path: version, type: int}`), and already converted: 0, '0' and 0.0 give the same UUID."""
    if t is None:
        if isinstance(v, str):
            return v
        raise ValueError(f"field '{path}' is {type(v).__name__} ({v!r}): declare its type, e.g. "
                         f"{{path: {path}, type: int}}, so that 0, '0' and 0.0 give the same UUID")
    return T.cast(v, t)


def _key_part(v: Any) -> Any:
    """Value used for identification (document_id.from): a projection with missing elements is not accepted."""
    if isinstance(v, Projection):
        if v.missing:
            raise MissingElements(f"'{v.path}': {v.missing} of {len(v.slots)} element(s) without the field "
                                  f"(missing elements are not accepted in a template or in the document_id)")
        return list(v)
    return v


def _render(template: str, ctx: Context) -> tuple[str, list[str], int]:
    """-> (text, missing parts, number of parts). The caller decides what to do with the missing parts."""
    missing: list[str] = []

    def repl(m):
        v = _key_part(resolve(ctx, m.group(1)))
        if v is MISSING or v is None:
            missing.append(m.group(1))
            return ""
        return text_of(v)   # the same text as string columns (boolean true/false, object as JSON, ...)

    return _TPL_RE.sub(repl, template), missing, len(_TPL_RE.findall(template))


def flatten(obj: dict, exclude: list[str], max_depth: int, lists: str, naming: str = "full") -> dict[str, Any]:
    """Flattens an object: {a: {b: 1}} -> {a_b: 1} (naming=full) or {b: 1} (naming=leaf)."""
    out, _, _, _ = flatten_sources(_leaves(obj, exclude, max_depth, lists), naming)
    return out


def flatten_sources(leaves: list[tuple[tuple[str, ...], Any]], naming: str,
                    fixed: Any = None) -> tuple[dict, dict, list, list]:
    """-> (column -> value, column -> source path, collisions [(column, path, path)], outside the model).
    Two different fields that give the same column name (e.g. fossilGhgEmissions and fossil_ghg_emissions)
    are a collision: before, the first one stayed and the other was lost without a warning.

    `fixed` (LeafNames, from model.schema): leaf names come from the model, not from this document's fields (before,
    the same field went to 'fossil' in one document and 'ghg_fossil' in another). A field outside the model keeps
    its full (stable) name and is returned in `outside`; if that name is a model field's name, it is a collision."""
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
                    continue  # lists of objects -> child tables (explode / auto_normalize)
                leaves.append((p, json_text(v) if lists == "json" else v))
            else:
                leaves.append((p, v))

    walk(obj, (), 0)
    return leaves


def _same_source(col: ColumnSpec | None, dotted: str, level: int) -> bool:
    """The explicit column reads the same field as auto mode (an intentional override, e.g. to give it a type)."""
    if col is None or not col.path:
        return False
    p = col.path.strip()
    for prefix in ("$item.",) + (("$root.",) if level == 0 else ()):
        if p.startswith(prefix):
            p = p[len(prefix):]
    return p == dotted


def column_names(paths: list[tuple[str, ...]], naming: str = "full") -> dict[tuple[str, ...], str]:
    """Column names for paths. 'leaf' uses only the last field and adds
    the levels above only where there is a collision (e.g. production_stage_x vs packaging_stage_x)."""
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


# --- unwrap: 1:1 lists -> objects ---------------------------------------
def unwrap(obj: Any, rule: dict, source: str | None = None, path: str = "", firsts: list | None = None) -> Any:
    """Replaces lists that are 1:1 in practice with their single element.

    rule = {paths: set, all: bool, except: set, on_multiple: fail|first|keep}
    Paths without indices: 'pcfAssessmentAndMethodology.pcfAssessmentInformation.time'.
    `firsts` (optional) receives (path, number of elements) for each list reduced to its 1st element (on_multiple: first).
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
                    raise TransformError(f"unwrap: '{p}' has {len(v)} elements in '{source}' (expected 1). "
                                         f"Use on_multiple: first|keep or remove it from the unwrap.")
                elif rule["on_multiple"] == "first":
                    log.warning("unwrap: '%s' has %d elements in %s — the first was used", p, len(v), source)
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
    """Generates tables automatically: 1 root table + 1 table for each list of objects.

    With `children: schema` (the default with model.schema) the child tables are the model's lists plus the batch's:
    a list that was empty in every document of the batch still has a table, and its replace_by/overwrite
    runs. With `naming: leaf` the table and column names are fixed by the model, not by the batch."""
    from .pathcheck import Problem, leaf_names_for, model_list_paths, model_position  # noqa: PLC0415

    an = sub.auto_normalize or {}
    d = sub.defaults
    prefix = sanitize(an.get("table_prefix", sub.name))
    if "id_path" not in an and sub.document_id:  # no business id: the document's UUID links the tables
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
        kept = in_model + [lv for lv in batch if lv not in model_set]     # a list outside the model: also included
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
                raise TransformError(f"auto_normalize: list '{'.'.join(key)}' (outside the model) and list "
                                     f"'{'.'.join(owners[tnames[key]])}' give the same table '{prefix}_{tnames[key]}'; "
                                     f"exclude one with auto_normalize.exclude")
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
                opts["auto_naming"] = "full"     # a list outside the model: full (stable) names
            else:
                opts["leaf_names"] = leaf_names_for(walker, pos, exclude, max_depth, "array")
        specs.append(TableSpec(
            name=sanitize(f"{prefix}_{tnames[flat(levels)]}"), columns=cols, explode=list(levels),
            keys=[id_col] + idx_cols, mode=child_mode, dedupe=True,
            replace_by=[id_col] if mode == "merge" else [], auto_exclude=exclude, auto_collision="prefix", **opts,
        ))
    return specs
