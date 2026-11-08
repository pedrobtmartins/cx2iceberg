"""Load report: everything that did not reach the tables exactly as it came in the JSON.

Each event gives the table, column, file, document (position in the file), row (position in the explode),
keys, source value, what happened (`action`) and why (`reason`). There are three kinds:

    error      the document is rejected: none of its rows is written, in any table
    declared   a loss declared in the configuration (on_error: null, skip_row, default, where, ...): counted
    warning    no declared loss, but worth reviewing (check with on_fail: warn, duplicates removed, ...)

Bounded memory, whatever the size of the input:
    * exact counts per (kind, reason, action, table, column): the number of groups depends on the configuration;
    * only the first `samples` events of each group are kept in memory;
    * the full list can go to a JSON Lines file (`details_path`), written event by event;
    * after `max_errors` errors the load stops (and nothing is written).
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

VALUE_MAX = 200
NO_VALUE = object()

KIND_TITLES = {
    "error": "Errors (rejected documents)",
    "declared": "Losses declared in the configuration",
    "warning": "Warnings",
}
REASONS = {
    "invalid_value": "invalid value",
    "required_empty": "required value empty",
    "check": "check failed",
    "unreadable": "document could not be read",
    "invalid_document": "invalid document",
    "empty_file": "file without documents",
    "number_as_text": "number as text",
    "empty_text": "empty text",
    "path_shape": "path with the wrong shape",
    "filter_type": "filter compares different types",
    "missing_elements": "elements without the field",
    "rounded": "rounded (rounding declared)",
    "clamped": "limited by clamp",
    "naive_timestamp": "date/time without time zone",
    "timestamp_precision": "fraction below the microsecond",
    "unit_fallback": "quantity without unit",
    "nulls_skipped": "nulls skipped (nulls: skip)",
    "no_match": "regex without a match",
    "untyped_value": "value that would change in an untyped column",
    "untyped_all_null": "untyped column with only nulls",
    "column_added": "new column in the table",
    "column_widened": "column type widened in the table",
    "missing_columns": "table columns not in the configuration",
    "template_missing": "incomplete template",
    "auto_collision": "name collision in auto mode",
    "condition_type": "condition compares different types",
    "schema": "does not match the schema",
    "unwrap_multiple": "list with several elements in unwrap",
    "no_submodel": "no submodel matches",
    "document_id_key_missing": "document_id key field missing",
    "where_false": "excluded by where",
    "missing_value": "missing value",
    "duplicate_key": "duplicate key",
    "null_key": "null key",
    "outside_model": "field outside the model (auto_naming: leaf)",
    "children_from_batch": "child tables only from the batch's lists",
}
ACTIONS = {
    "document_rejected": "document rejected",
    "nulled": "value set to null",
    "row_skipped": "row skipped",
    "default_applied": "default applied",
    "kept": "kept",
    "ignored": "document ignored",
    "first_element": "first element used",
    "row_not_generated": "row not generated",
    "row_removed": "row removed",
    "content_hash": "UUID from the content",
    "element_skipped": "elements skipped",
    "element_nulled": "one null per element",
    "rounded": "value rounded",
    "clamped": "value limited",
    "assumed_utc": "read as UTC",
    "truncated": "truncated to the microsecond",
    "assumed_unit": "unit from `from`",
    "skipped": "skipped",
    "column_omitted": "column not created",
    "schema_change": "table schema changed",
    "row_replaced": "replaced by the next document's row",
    "auto_skipped": "auto field not included",
    "rendered_empty": "missing parts left empty",
    "assumed_zone": "read in the declared time zone",
    "assumed_midnight": "read as midnight",
    "full_name": "column with the full name",
    "not_deleted": "old rows not deleted",
}
WRITE_STATUS = {
    "pending": "pending",
    "written": "written",
    "failed": "FAILED",
    "not_written": "not written",
    "interrupted": "interrupted (state unknown)",
    "empty": "no rows",
}


class ReportLimit(Exception):
    """The maximum number of errors was reached: the load stops."""


def short_value(value: Any) -> str:
    try:
        text = repr(value)
    except Exception:  # noqa: BLE001 - a plugin object with a broken __repr__
        text = f"<{type(value).__name__}>"
    return text if len(text) <= VALUE_MAX else text[:VALUE_MAX] + "…"


def json_safe(value: Any) -> Any:
    """Strict JSON types only (json.dumps with allow_nan=False): the report must always be writable.

    Key values arrive already converted (datetime, Decimal, NaN, struct, map...): they become text.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k if isinstance(k, str) else str(json_safe(k)): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(v) for v in value]
    return short_value(value)


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


def _position(value: Any) -> int | str | None:
    return value if value is None or (isinstance(value, int) and not isinstance(value, bool)) else str(value)


def location(event: dict) -> str:
    """'f.json#2 childItems[1]' — the event's file, document and row."""
    out = event.get("file") or "(no file)"
    if event.get("document") is not None:
        out += f"#{event['document']}"
    if event.get("row"):
        out += f" {event['row']}"
    return out


def describe(event: dict) -> str:
    """Full message of an event (same format as the engine's error messages)."""
    parts = []
    if event.get("table"):
        parts.append(f"table '{event['table']}'")
    if event.get("column"):
        parts.append(f"column '{event['column']}'")
    if event.get("file"):
        doc = f" (document {event['document']})" if event.get("document") is not None else ""
        parts.append(f"file '{event['file']}'{doc}")
    if event.get("row"):
        parts.append(f"row {event['row']}")
    if event.get("keys"):
        parts.append(f"keys {event['keys']}")
    if "value" in event:
        parts.append(f"value {event['value']}")
    return f"{', '.join(parts)}: {event['message']}" if parts else event["message"]


class Report:
    def __init__(self, samples: int = 20, max_errors: int = 1000, details_path: str | None = None):
        self.samples = samples
        self.max_errors = max_errors
        self.details_path = details_path
        self.groups: dict[tuple, dict[str, Any]] = {}   # (kind, reason, action, table, column) -> {count, examples}
        self.rejected: dict[Any, dict[str, Any]] = {}    # document -> {file, document, errors}
        self.tables: dict[str, dict[str, int]] = {}
        self.documents = 0
        self.errors = 0
        self.interrupted = False
        self.failure: str | None = None                  # batch-level error (configuration, error limit, write)
        self.write: dict[str, Any] | None = None          # {target, tables: {name: {status, rows, error, ...}}}; None = nothing written
        self._details = None

    # --- recording ----------------------------------------------------------
    def add(self, kind: str, reason: str, action: str, message: str, *, table: str | None = None,
            column: str | None = None, file: str | None = None, document: int | None = None, row: str | None = None,
            keys: dict | None = None, value: Any = NO_VALUE, doc_key: Any = None) -> bool:
        """Record an event. Returns True if it was kept as an example (the first `samples` of its group)."""
        key = (kind, reason, action, table, column)
        group = self.groups.get(key)
        if group is None:
            group = self.groups[key] = {"count": 0, "examples": []}
        group["count"] += 1
        sampled = len(group["examples"]) < self.samples
        file, document = _text(file), _position(document)
        if sampled or self.details_path:
            # the full event is only built if it is kept; converted here already so the report
            # (JSON, log, Lambda response) never fails after the write
            event: dict[str, Any] = {"kind": kind, "reason": reason, "action": action, "table": _text(table),
                                     "column": _text(column), "file": file, "document": document, "row": _text(row),
                                     "keys": json_safe(keys), "message": str(message)}
            if value is not NO_VALUE:
                event["value"] = short_value(value)
            if sampled:
                group["examples"].append(event)
            if self.details_path:
                if self._details is None:
                    self._details = open(self.details_path, "w", encoding="utf-8")
                self._details.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n")
        if kind == "error":
            self.errors += 1
            if doc_key is not None:
                rej = self.rejected.setdefault(doc_key, {"file": file, "document": document, "errors": 0})
                rej["errors"] += 1
            if self.max_errors and self.errors >= self.max_errors:
                self.interrupted = True
                raise ReportLimit(f"load stopped after {self.errors} error(s) (--max-errors); nothing was written "
                                  f"and the counts are incomplete")
        return sampled

    # --- write -----------------------------------------------------------
    def start_write(self, target: str, rows: dict[str, int]) -> None:
        """target: athena | local. Every table starts 'pending' (or 'empty' with no rows)."""
        self.write = {"target": target, "tables": {
            name: {"status": "pending" if n else "empty", "rows": n, "error": None} for name, n in rows.items()}}

    def table_written(self, name: str, **extra: Any) -> None:
        self.write["tables"][name].update(status="written", **json_safe(extra))

    def write_failed(self, name: str, error: BaseException | str, **extra: Any) -> None:
        """Table `name` failed: the remaining ones become 'not_written' and the load becomes 'failed'."""
        tables = self.write["tables"]
        tables[name].update(status="failed", error=str(error), **json_safe(extra))
        for t in tables.values():
            if t["status"] == "pending":
                t["status"] = "not_written"
        self.failure = f"write failed on table '{name}': {error}"

    def write_blocked(self, reason: str) -> None:
        """The pre-check failed: no table was written."""
        if self.write:
            for t in self.write["tables"].values():
                if t["status"] == "pending":
                    t["status"] = "not_written"
        self.failure = reason

    def finish_write(self) -> None:
        """End of the write (always called): whatever is still 'pending' was interrupted midway."""
        if not self.write:
            return
        stuck = [n for n, t in self.write["tables"].items() if t["status"] == "pending"]
        for n in stuck:
            self.write["tables"][n]["status"] = "interrupted"
        if stuck and not self.failure:
            self.failure = f"write interrupted before it finished: {stuck}"

    def close(self) -> None:
        if self._details is not None:
            self._details.close()
            self._details = None

    # --- queries ---------------------------------------------------------
    @property
    def status(self) -> str:
        if self.failure or self.interrupted:
            return "failed"
        return "partial" if self.rejected else "ok"

    def count(self, kind: str | None = None, reason: str | None = None, action: str | None = None,
              table: str | None = None, column: str | None = None) -> int:
        want = {"kind": kind, "reason": reason, "action": action, "table": table, "column": column}
        total = 0
        for key, g in self.groups.items():
            fields = dict(zip(("kind", "reason", "action", "table", "column"), key))
            if all(v is None or fields[k] == v for k, v in want.items()):
                total += g["count"]
        return total

    def events(self, kind: str | None = None) -> list[dict]:
        """Events kept in memory (at most `samples` per group)."""
        return [e for key, g in self.groups.items() if kind in (None, key[0]) for e in g["examples"]]

    def error_messages(self, limit: int = 10) -> list[str]:
        return [describe(e) for e in self.events("error")[:limit]]

    def to_dict(self, examples: int | None = None, max_rejected: int | None = None) -> dict[str, Any]:
        """Report in strict JSON types. `examples`/`max_rejected` limit its size (e.g. the Lambda response)."""
        order = {"error": 0, "declared": 1, "warning": 2}
        groups = sorted(self.groups.items(), key=lambda kv: (order.get(kv[0][0], 9), -kv[1]["count"]))
        rejected = list(self.rejected.values())
        return {
            "status": self.status,
            "documents": self.documents,
            "rejected_total": len(rejected),
            "rejected_documents": rejected if max_rejected is None else rejected[:max_rejected],
            "interrupted": self.interrupted,
            "failure": _text(self.failure),
            "tables": self.tables,
            "write": self.write,
            "groups": [{"kind": k[0], "reason": k[1], "action": k[2], "table": k[3], "column": k[4],
                        "count": g["count"], "examples": g["examples"] if examples is None else g["examples"][:examples]}
                       for k, g in groups],
        }

    def render(self, examples: int = 3, max_rejected: int = 20) -> str:
        """Readable summary for the command line and the logs."""
        lines = [f"Load report: status={self.status} | documents={self.documents} | "
                 f"rejected={len(self.rejected)}"]
        if self.failure:
            lines.append(f"  failure: {self.failure}")
        if self.tables:
            parts = []
            for name, t in self.tables.items():
                extra = f" (+{t['rows_from_rejected_documents']} from rejected documents, not written)" \
                    if t.get("rows_from_rejected_documents") else ""
                parts.append(f"{name}={t['rows']}{extra}")
            lines.append("  rows: " + ", ".join(parts))
        if self.write:
            lines.append(f"Write ({self.write['target']}):")
            for name, t in self.write["tables"].items():
                extra = f" ({t['rows']} row(s))" if t["status"] == "written" else ""
                if t.get("replaced_documents") is not None:
                    extra += f"; old rows of {t['replaced_documents']} document(s) replaced (replace_by)"
                if t.get("emptied"):
                    extra += "; table emptied (overwrite: the batch's documents produce no rows)"
                err = f" — {t['error']}" if t.get("error") else ""
                lines.append(f"  {name}: {WRITE_STATUS.get(t['status'], t['status'])}{extra}{err}")
        else:
            lines.append("Write: none")
        if self.rejected:
            lines.append("Rejected documents (none of their rows is written):")
            for r in list(self.rejected.values())[:max_rejected]:
                lines.append(f"  {location(r)} — {r['errors']} error(s)")
            if len(self.rejected) > max_rejected:
                lines.append(f"  … and {len(self.rejected) - max_rejected} more")
        data = self.to_dict()["groups"]
        for kind, title in KIND_TITLES.items():
            groups = [g for g in data if g["kind"] == kind]
            if not groups:
                continue
            lines.append(f"{title}:")
            for g in groups:
                where = ".".join(x for x in (g["table"], g["column"]) if x) or "(document)"
                lines.append(f"  {where} — {REASONS.get(g['reason'], g['reason'])} → "
                             f"{ACTIONS.get(g['action'], g['action'])}: {g['count']}")
                for e in g["examples"][:examples]:
                    value = f" value {e['value']}" if "value" in e else ""
                    lines.append(f"      {location(e)}{value}: {e['message']}")
        return "\n".join(lines)
