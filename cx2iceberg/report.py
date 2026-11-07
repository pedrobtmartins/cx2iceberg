"""Relatório da carga: tudo o que não chegou às tabelas tal como veio no JSON.

Cada evento diz tabela, coluna, ficheiro, documento (posição no ficheiro), linha (posição no explode),
chaves, valor de origem, o que aconteceu (`action`) e porquê (`reason`). Há três tipos:

    error      o documento é rejeitado: nenhuma linha dele é escrita, em nenhuma tabela
    declared   perda declarada na configuração (on_error: null, skip_row, default, where, ...): contada
    warning    sem perda declarada, mas a rever (check com on_fail: warn, duplicados removidos, ...)

Memória limitada, seja qual for o tamanho da entrada:
    * contagens exactas por (tipo, motivo, acção, tabela, coluna): o nº de grupos depende da configuração;
    * só os primeiros `samples` eventos de cada grupo ficam em memória;
    * a lista completa pode ir para um ficheiro JSON Lines (`details_path`), escrito evento a evento;
    * ao fim de `max_errors` erros a carga é interrompida (e não se escreve nada).
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
    "error": "Erros (documentos rejeitados)",
    "declared": "Perdas declaradas na configuração",
    "warning": "Avisos",
}
REASONS = {
    "invalid_value": "valor inválido",
    "required_empty": "obrigatório vazio",
    "check": "check falhou",
    "unreadable": "não foi possível ler o documento",
    "invalid_document": "documento inválido",
    "empty_file": "ficheiro sem documentos",
    "number_as_text": "número em texto",
    "empty_text": "texto vazio",
    "path_shape": "caminho com a forma errada",
    "filter_type": "filtro compara tipos diferentes",
    "missing_elements": "elementos sem o campo",
    "rounded": "arredondado (rounding declarado)",
    "clamped": "limitado pelo clamp",
    "naive_timestamp": "data/hora sem fuso",
    "timestamp_precision": "fracção abaixo do microssegundo",
    "unit_fallback": "quantidade sem unidade",
    "nulls_skipped": "nulos ignorados (nulls: skip)",
    "no_match": "regex sem correspondência",
    "untyped_value": "valor que mudaria numa coluna sem tipo",
    "untyped_all_null": "coluna sem tipo só com nulos",
    "column_added": "coluna nova na tabela",
    "column_widened": "tipo da coluna alargado na tabela",
    "missing_columns": "colunas da tabela fora da configuração",
    "template_missing": "template incompleto",
    "auto_collision": "colisão de nomes no modo auto",
    "condition_type": "condição compara tipos diferentes",
    "schema": "não cumpre o schema",
    "unwrap_multiple": "lista com vários elementos no unwrap",
    "no_submodel": "nenhum submodelo corresponde",
    "document_id_key_missing": "campo da chave do document_id em falta",
    "where_false": "excluída pelo where",
    "missing_value": "valor em falta",
    "duplicate_key": "chave repetida",
    "null_key": "chave nula",
    "outside_model": "campo fora do modelo (auto_naming: leaf)",
    "children_from_batch": "tabelas filhas só das listas do lote",
}
ACTIONS = {
    "document_rejected": "documento rejeitado",
    "nulled": "valor anulado",
    "row_skipped": "linha ignorada",
    "default_applied": "default aplicado",
    "kept": "mantido",
    "ignored": "documento ignorado",
    "first_element": "usado o 1.º elemento",
    "row_not_generated": "linha não gerada",
    "row_removed": "linha removida",
    "content_hash": "UUID a partir do conteúdo",
    "element_skipped": "elementos ignorados",
    "element_nulled": "um nulo por elemento",
    "rounded": "valor arredondado",
    "clamped": "valor limitado",
    "assumed_utc": "lida como UTC",
    "truncated": "cortada ao microssegundo",
    "assumed_unit": "unidade do from",
    "skipped": "ignorados",
    "column_omitted": "coluna não criada",
    "schema_change": "esquema da tabela alterado",
    "row_replaced": "substituída pela do documento seguinte",
    "auto_skipped": "campo auto não incluído",
    "rendered_empty": "partes em falta vazias",
    "assumed_zone": "lida no fuso declarado",
    "assumed_midnight": "lida como meia-noite",
    "full_name": "coluna com o nome completo",
    "not_deleted": "linhas antigas não apagadas",
}
WRITE_STATUS = {
    "pending": "por escrever",
    "written": "escrita",
    "failed": "FALHOU",
    "not_written": "não escrita",
    "interrupted": "interrompida (estado desconhecido)",
    "empty": "sem linhas",
}


class ReportLimit(Exception):
    """Atingido o máximo de erros: a carga pára."""


def short_value(value: Any) -> str:
    try:
        text = repr(value)
    except Exception:  # noqa: BLE001 - objecto de um plugin com __repr__ partido
        text = f"<{type(value).__name__}>"
    return text if len(text) <= VALUE_MAX else text[:VALUE_MAX] + "…"


def json_safe(value: Any) -> Any:
    """Só tipos JSON estritos (json.dumps com allow_nan=False): o relatório tem de poder sair sempre.

    Os valores das chaves chegam já convertidos (datetime, Decimal, NaN, struct, map...): passam a texto.
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
    """'f.json#2 childItems[1]' — ficheiro, documento e linha do evento."""
    out = event.get("file") or "(sem ficheiro)"
    if event.get("document") is not None:
        out += f"#{event['document']}"
    if event.get("row"):
        out += f" {event['row']}"
    return out


def describe(event: dict) -> str:
    """Mensagem completa de um evento (formato das mensagens de erro do motor)."""
    parts = []
    if event.get("table"):
        parts.append(f"tabela '{event['table']}'")
    if event.get("column"):
        parts.append(f"coluna '{event['column']}'")
    if event.get("file"):
        doc = f" (documento {event['document']})" if event.get("document") is not None else ""
        parts.append(f"ficheiro '{event['file']}'{doc}")
    if event.get("row"):
        parts.append(f"linha {event['row']}")
    if event.get("keys"):
        parts.append(f"chaves {event['keys']}")
    if "value" in event:
        parts.append(f"valor {event['value']}")
    return f"{', '.join(parts)}: {event['message']}" if parts else event["message"]


class Report:
    def __init__(self, samples: int = 20, max_errors: int = 1000, details_path: str | None = None):
        self.samples = samples
        self.max_errors = max_errors
        self.details_path = details_path
        self.groups: dict[tuple, dict[str, Any]] = {}   # (kind, reason, action, table, column) -> {count, examples}
        self.rejected: dict[Any, dict[str, Any]] = {}    # documento -> {file, document, errors}
        self.tables: dict[str, dict[str, int]] = {}
        self.documents = 0
        self.errors = 0
        self.interrupted = False
        self.failure: str | None = None                  # erro ao nível do lote (configuração, limite de erros, escrita)
        self.write: dict[str, Any] | None = None          # {target, tables: {nome: {status, rows, error, ...}}}; None = nada escrito
        self._details = None

    # --- registo ----------------------------------------------------------
    def add(self, kind: str, reason: str, action: str, message: str, *, table: str | None = None,
            column: str | None = None, file: str | None = None, document: int | None = None, row: str | None = None,
            keys: dict | None = None, value: Any = NO_VALUE, doc_key: Any = None) -> bool:
        """Regista um evento. Devolve True se ficou guardado como exemplo (os primeiros `samples` do grupo)."""
        key = (kind, reason, action, table, column)
        group = self.groups.get(key)
        if group is None:
            group = self.groups[key] = {"count": 0, "examples": []}
        group["count"] += 1
        sampled = len(group["examples"]) < self.samples
        file, document = _text(file), _position(document)
        if sampled or self.details_path:
            # o evento completo só é construído se for guardado; convertido já aqui para o relatório
            # (JSON, log, resposta da Lambda) nunca falhar depois da escrita
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
                raise ReportLimit(f"carga interrompida ao fim de {self.errors} erro(s) (--max-errors); nada foi escrito "
                                  f"e as contagens estão incompletas")
        return sampled

    # --- escrita -----------------------------------------------------------
    def start_write(self, target: str, rows: dict[str, int]) -> None:
        """target: athena | local. Todas as tabelas começam 'pending' (ou 'empty' sem linhas)."""
        self.write = {"target": target, "tables": {
            name: {"status": "pending" if n else "empty", "rows": n, "error": None} for name, n in rows.items()}}

    def table_written(self, name: str, **extra: Any) -> None:
        self.write["tables"][name].update(status="written", **json_safe(extra))

    def write_failed(self, name: str, error: BaseException | str, **extra: Any) -> None:
        """A tabela `name` falhou: as que faltavam ficam 'not_written' e a carga fica 'failed'."""
        tables = self.write["tables"]
        tables[name].update(status="failed", error=str(error), **json_safe(extra))
        for t in tables.values():
            if t["status"] == "pending":
                t["status"] = "not_written"
        self.failure = f"escrita falhou na tabela '{name}': {error}"

    def write_blocked(self, reason: str) -> None:
        """A pré-verificação falhou: nenhuma tabela foi escrita."""
        if self.write:
            for t in self.write["tables"].values():
                if t["status"] == "pending":
                    t["status"] = "not_written"
        self.failure = reason

    def finish_write(self) -> None:
        """Fecho da escrita (sempre chamado): o que ficou 'pending' foi interrompido a meio."""
        if not self.write:
            return
        stuck = [n for n, t in self.write["tables"].items() if t["status"] == "pending"]
        for n in stuck:
            self.write["tables"][n]["status"] = "interrupted"
        if stuck and not self.failure:
            self.failure = f"escrita interrompida antes de terminar: {stuck}"

    def close(self) -> None:
        if self._details is not None:
            self._details.close()
            self._details = None

    # --- consulta ---------------------------------------------------------
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
        """Eventos guardados em memória (no máximo `samples` por grupo)."""
        return [e for key, g in self.groups.items() if kind in (None, key[0]) for e in g["examples"]]

    def error_messages(self, limit: int = 10) -> list[str]:
        return [describe(e) for e in self.events("error")[:limit]]

    def to_dict(self, examples: int | None = None, max_rejected: int | None = None) -> dict[str, Any]:
        """Relatório em tipos JSON estritos. `examples`/`max_rejected` limitam o tamanho (ex.: resposta da Lambda)."""
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
        """Resumo legível (português) para a linha de comandos e os logs."""
        lines = [f"Relatório da carga: estado={self.status} | documentos={self.documents} | "
                 f"rejeitados={len(self.rejected)}"]
        if self.failure:
            lines.append(f"  falha: {self.failure}")
        if self.tables:
            parts = []
            for name, t in self.tables.items():
                extra = f" (+{t['rows_from_rejected_documents']} de documentos rejeitados, não escritas)" \
                    if t.get("rows_from_rejected_documents") else ""
                parts.append(f"{name}={t['rows']}{extra}")
            lines.append("  linhas: " + ", ".join(parts))
        if self.write:
            lines.append(f"Escrita ({self.write['target']}):")
            for name, t in self.write["tables"].items():
                extra = f" ({t['rows']} linha(s))" if t["status"] == "written" else ""
                if t.get("replaced_documents") is not None:
                    extra += f"; linhas antigas de {t['replaced_documents']} documento(s) substituídas (replace_by)"
                if t.get("emptied"):
                    extra += "; tabela esvaziada (overwrite: os documentos do lote não dão linhas)"
                err = f" — {t['error']}" if t.get("error") else ""
                lines.append(f"  {name}: {WRITE_STATUS.get(t['status'], t['status'])}{extra}{err}")
        else:
            lines.append("Escrita: nenhuma")
        if self.rejected:
            lines.append("Documentos rejeitados (nenhuma linha deles é escrita):")
            for r in list(self.rejected.values())[:max_rejected]:
                lines.append(f"  {location(r)} — {r['errors']} erro(s)")
            if len(self.rejected) > max_rejected:
                lines.append(f"  … e mais {len(self.rejected) - max_rejected}")
        data = self.to_dict()["groups"]
        for kind, title in KIND_TITLES.items():
            groups = [g for g in data if g["kind"] == kind]
            if not groups:
                continue
            lines.append(f"{title}:")
            for g in groups:
                where = ".".join(x for x in (g["table"], g["column"]) if x) or "(documento)"
                lines.append(f"  {where} — {REASONS.get(g['reason'], g['reason'])} → "
                             f"{ACTIONS.get(g['action'], g['action'])}: {g['count']}")
                for e in g["examples"][:examples]:
                    value = f" valor {e['value']}" if "value" in e else ""
                    lines.append(f"      {location(e)}{value}: {e['message']}")
        return "\n".join(lines)
