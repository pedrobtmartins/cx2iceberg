"""cx2iceberg studio: construir transformações num browser, sem escrever YAML.

Servidor local (só 127.0.0.1), só biblioteca padrão. Lê e grava ficheiros apenas dentro de `root`.
O construtor trabalha sobre a configuração como dicionário (o mesmo formato do YAML); o YAML
é gerado ao gravar e fica visível em "Avançado".

    cx2iceberg studio --root . --open
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import threading
from datetime import date, datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from ..athena_types import INFER_STRUCT, athena_type, parse as parse_type
from ..catalog import Catalog, CatalogError
from ..config import load_yaml, ConfigError, from_dict, parse_short, parse_unwrap, sanitize
from ..engine import Document, Engine, TransformError, unwrap
from ..reader import loads as strict_loads, read_documents
from ..samm import field_tree, skeleton
from ..transforms import REGISTRY
from ..report import ACTIONS, KIND_TITLES, REASONS
from ..writer import WriteCheckError, check_offline, check_table, ddl

STATIC = Path(__file__).parent / "static"
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".taste-skill", ".pytest_cache", ".cx2iceberg"}
CONFIG_EXT = (".yaml", ".yml")
DATA_EXT = (".json", ".jsonl", ".ndjson")
MAX_ROWS = 200
MAX_DOC_CHARS = 400_000
HEADER = "# Gerado pelo cx2iceberg studio. Pode ser editado à mão (em \"Avançado\"); os comentários não são preservados.\n"


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ") if isinstance(v, datetime) else v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    return v


# --- valores e relatório para o browser -------------------------------------------------------------
_JS_SAFE = 2 ** 53     # acima disto um número JSON perde algarismos no JavaScript (9007199254740993 -> ...992)


def _float32_text(v: float) -> str:
    """O texto mais curto que volta ao mesmo float de 32 bits (o que o Athena mostra num `float`/real):
    1.100000023841858 -> 1.1."""
    import struct

    target = struct.pack("<f", v)
    for digits in range(1, 10):
        text = f"{v:.{digits}g}"
        if struct.pack("<f", float(text)) == target:
            return text
    return repr(v)


def _display(v: Any, tree: Any) -> Any:
    """Valor tal como vai ficar na tabela, numa forma que o browser não altera: inteiros grandes e decimais
    como texto, float (32 bits) pelo seu texto curto, datas em ISO. Antes o browser mostrava 9007199254740993
    como 9007199254740992 e um float 1.1 como 1.100000023841858."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v if -_JS_SAFE <= v <= _JS_SAFE else str(v)
    if isinstance(v, float):
        if not math.isfinite(v):
            return str(v)
        return float(_float32_text(v)) if tree == "float" else v
    if isinstance(tree, tuple) and tree != INFER_STRUCT:
        if tree[0] == "array" and isinstance(v, list):
            return [_display(x, tree[1]) for x in v]
        if tree[0] == "map" and isinstance(v, dict):
            return {str(k): _display(x, tree[2]) for k, x in v.items()}
        if tree[0] == "struct" and isinstance(v, dict):
            fields = dict(tree[1])
            return {k: _display(x, fields.get(k)) for k, x in v.items()}
    if isinstance(v, dict):
        return {str(k): _display(x, None) for k, x in v.items()}
    if isinstance(v, list):
        return [_display(x, None) for x in v]
    return _jsonable(v)


def _display_row(row: dict, types: dict[str, str]) -> dict:
    out = {}
    for c, t in types.items():
        try:
            tree = parse_type(t)
        except ValueError:
            tree = None
        out[c] = _display(row.get(c), tree)
    return out


def _report_view(report) -> dict:
    """O relatório da carga para o studio: o mesmo do CLI, com os nomes em português e até 5 exemplos por grupo."""
    d = _jsonable(report.to_dict(examples=5))
    for g in d["groups"]:
        g["kind_title"] = KIND_TITLES.get(g["kind"], g["kind"])
        g["reason_label"] = REASONS.get(g["reason"], g["reason"])
        g["action_label"] = ACTIONS.get(g["action"], g["action"])
    return d


_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env_lenient(text: str) -> str:
    """Como na configuração, mas uma variável em falta não é erro na pré-visualização."""
    return _ENV.sub(lambda m: os.environ.get(m.group(1), m.group(2) if m.group(2) is not None else m.group(0)), text)


def _expand_obj(v: Any) -> Any:
    if isinstance(v, str):
        return _expand_env_lenient(v)
    if isinstance(v, dict):
        return {k: _expand_obj(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_expand_obj(x) for x in v]
    return v


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


# --- configuração <-> modelo do construtor ---------------------------------
def _raw_column(name: str, spec: Any) -> dict:
    if isinstance(spec, str):
        return parse_short(spec, name)
    if spec is None:
        return {"path": name}
    if not isinstance(spec, dict):
        return {"const": spec}
    return dict(spec)


def _raw_columns(spec: Any) -> dict:
    if not spec:
        return {}
    if isinstance(spec, list):
        out = {}
        for c in spec:
            if isinstance(c, str):
                out[c] = {"path": c}
            else:
                (k, v), = c.items()
                out[k] = _raw_column(k, v)
        return out
    return {k: _raw_column(k, v) for k, v in spec.items()}


def normalize(raw: dict) -> dict:
    """Colunas sempre na forma de objecto ({path, type, transform, ...}) para o construtor."""
    cfg = json.loads(json.dumps(raw or {}, default=str))
    for s in cfg.get("submodels", []) or []:
        if "common_columns" in s:
            s["common_columns"] = _raw_columns(s["common_columns"])
        for t in s.get("tables", []) or []:
            t["columns"] = _raw_columns(t.get("columns"))
    return cfg


def _compact(col: dict) -> Any:
    col = {k: v for k, v in col.items() if v not in (None, "", [], {}) or k == "const"}
    if set(col) == {"path"}:
        return col["path"]
    return col


def render(cfg: dict) -> str:
    out = json.loads(json.dumps(cfg))
    for s in out.get("submodels", []) or []:
        if "common_columns" in s:
            s["common_columns"] = {k: _compact(v) for k, v in s["common_columns"].items()}
        for t in s.get("tables", []) or []:
            if "columns" in t:
                t["columns"] = {k: _compact(v) for k, v in t["columns"].items()}
    return HEADER + yaml.safe_dump(out, sort_keys=False, allow_unicode=True, width=120, default_flow_style=False)


def _find_id(data: Any) -> str | None:
    """Caminho do identificador do documento: catenaXId, id, ou o *.catenaXId / *.id mais curto (fora de listas)."""
    found: list[str] = []

    def walk(o: Any, path: str):
        if isinstance(o, dict):
            for k, v in o.items():
                p = f"{path}.{k}" if path else k
                if k in ("catenaXId", "id") and not isinstance(v, (dict, list)):
                    found.append(p)
                walk(v, p)

    walk(data, "")
    for cand in ("catenaXId", "id"):
        if cand in found:
            return cand
    return sorted(found, key=lambda p: (not p.endswith("catenaXId"), len(p)))[0] if found else None


def _single_lists(o: Any, path: str = "", out: list | None = None) -> list[str]:
    """Listas de objectos com exactamente 1 elemento no exemplo (candidatas a unwrap)."""
    out = [] if out is None else out
    if isinstance(o, dict):
        for k, v in o.items():
            p = f"{path}.{k}" if path else k
            if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                if len(v) == 1:
                    out.append(p)
                _single_lists(v[0], p, out)
            else:
                _single_lists(v, p, out)
    return out


def new_config(info: dict, root: Path, config_path: str) -> dict:
    """Configuração inicial para um submodelo do catálogo: id, UUID do documento, validação."""
    base = (root / config_path).parent
    rel = lambda p: os.path.relpath(root / p, base).replace(os.sep, "/")  # noqa: E731
    schema = json.loads((root / info["schema"]).read_text(encoding="utf-8"))
    example = json.loads((root / info["example"]).read_text(encoding="utf-8")) if info.get("example") else None
    if isinstance(example, list):
        example = example[0] if example else None
    paths = _single_lists(example) if isinstance(example, dict) else []
    rule = {"paths": paths} if paths else None
    data = unwrap(example if isinstance(example, dict) else skeleton(schema), parse_unwrap(rule, "unwrap")) if rule else (
        example if isinstance(example, dict) else skeleton(schema))
    id_path = _find_id(data)
    name = sanitize(info["aspect"])
    major = info["version"].split(".")[0]
    id_col = sanitize(id_path.split(".")[-1]) if id_path else None
    if id_col == "id":
        id_col = f"{name}_id"
    sub: dict[str, Any] = {
        "name": f"{name}_v{major}",
        "match": {"semantic_id": f"urn:samm:{info['model']}:{major}.*"},
        "model": {"id": info["model"], "version": info["version"], "schema": rel(info["schema"]),
                  **({"example": rel(info["example"])} if info.get("example") else {})},
        "validate": "warn",
    }
    if rule:
        sub["unwrap"] = rule
    sub["document_id"] = {"column": f"{name}_uuid", "from": [id_path]} if id_path else True
    table: dict[str, Any] = {"name": name}
    if id_path:
        table.update(mode="merge", keys=[id_col], columns={id_col: {"path": id_path, "type": "string"}})
    else:
        table.update(mode="append", columns={})
    sub["tables"] = [table]
    return {
        "target": {
            "database": "${ATHENA_DATABASE:-catenax}",
            "location": "${ICEBERG_LOCATION:-s3://my-bucket/warehouse}",
            "temp_path": "${ATHENA_TEMP:-s3://my-bucket/tmp}",
            "workgroup": "${ATHENA_WORKGROUP:-primary}",
            "region": "${AWS_REGION:-eu-west-1}",
        },
        "defaults": {"metadata_columns": ["source_file", "ingested_at"], "on_error": "fail"},
        "submodels": [sub],
    }


def _join(base: Path, rel: str) -> Path:
    """base/rel normalizado lexicalmente (funciona mesmo que pastas intermédias ainda não existam)."""
    return Path(os.path.normpath(base / rel))


_SAVE_LOCK = threading.Lock()


def _version(p: Path) -> str:
    """Versão de um ficheiro para detectar edições feitas fora do studio: o hash do conteúdo (não o mtime)."""
    return hashlib.sha256(p.read_bytes()).hexdigest()


class Studio:
    def __init__(self, root: str | Path, models_dir: str | Path | None = None):
        self.root = Path(root).resolve()
        self.catalog = Catalog(self.root, models_dir)

    # --- ficheiros ---------------------------------------------------------
    def safe(self, rel: str) -> Path:
        p = (self.root / rel).resolve()
        if p != self.root and self.root not in p.parents:
            raise ApiError(HTTPStatus.FORBIDDEN, "Caminho fora da pasta do studio")
        return p

    def rel(self, path: str | None) -> str | None:
        if path and path.startswith(str(self.root) + os.sep):
            return Path(path).relative_to(self.root).as_posix()
        return path

    def short(self, message: str) -> str:
        return message.replace(str(self.root) + os.sep, "")

    def _walk(self, exts: tuple[str, ...]) -> list[dict]:
        out = []
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith("."))
            for f in sorted(filenames):
                if f.endswith(exts):
                    p = Path(dirpath) / f
                    out.append({"path": p.relative_to(self.root).as_posix(), "size": p.stat().st_size})
        return out

    def files(self) -> dict:
        data = [f for f in self._walk(DATA_EXT) if not f["path"].endswith(("-schema.json", "/metadata.json"))]
        configs = [f for f in self._walk(CONFIG_EXT) if self._looks_like_config(f["path"])]
        return {"root": str(self.root), "configs": configs, "samples": data}

    def _looks_like_config(self, rel: str) -> bool:
        try:
            head = self.safe(rel).read_text(encoding="utf-8")[:20000]
        except (OSError, UnicodeDecodeError):
            return False
        return re.search(r"^submodels\s*:", head, re.MULTILINE) is not None

    def load(self, rel: str) -> dict:
        p = self.safe(rel)
        if not p.is_file():
            raise ApiError(HTTPStatus.NOT_FOUND, f"Ficheiro não encontrado: {rel}")
        text = p.read_text(encoding="utf-8")
        try:
            raw = load_yaml(text) or {}
        except yaml.YAMLError as e:
            return {"path": rel, "text": text, "version": _version(p), "config": None, "error": f"YAML inválido: {e}"}
        return {"path": rel, "text": text, "version": _version(p), "config": normalize(raw)}

    def parse(self, text: str) -> dict:
        try:
            raw = load_yaml(text) or {}
        except yaml.YAMLError as e:
            mark = getattr(e, "problem_mark", None)
            raise ApiError(HTTPStatus.BAD_REQUEST, f"YAML inválido{f' (linha {mark.line + 1})' if mark else ''}: "
                                                   f"{getattr(e, 'problem', None) or e}")
        if not isinstance(raw, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "A configuração tem de ser um mapa YAML")
        return {"config": normalize(raw)}

    def save(self, rel: str, text: str, version: str | None) -> dict:
        """Grava se o ficheiro ainda é o que o studio abriu (`version` = hash do conteúdo lido; None = ficheiro novo).
        Antes comparava-se o mtime: com a mesma marca de tempo (sistemas de ficheiros com resolução de 1-2 s,
        pastas partilhadas com Docker/WSL, `cp -p`, `rsync -t`) uma edição feita fora era apagada sem aviso."""
        p = self.safe(rel)
        if not rel.endswith(CONFIG_EXT):
            raise ApiError(HTTPStatus.BAD_REQUEST, "Só se gravam ficheiros .yaml/.yml")
        with _SAVE_LOCK:            # dois separadores a gravar ao mesmo tempo: verificação e escrita juntas
            if p.exists() and version is not None and _version(p) != version:
                raise ApiError(HTTPStatus.CONFLICT, "O ficheiro foi alterado fora do studio desde que o abriu. Recarregue-o.")
            if p.exists() and version is None:
                raise ApiError(HTTPStatus.CONFLICT, f"Já existe um ficheiro {rel}. Escolha outro nome.")
            p.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=p.parent, prefix=f".{p.name}.", suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, p)
            return {"path": rel, "version": _version(p)}

    # --- catálogo ----------------------------------------------------------
    def catalog_list(self, refresh: bool) -> dict:
        try:
            return {"models": self.catalog.models(refresh)}
        except CatalogError as e:
            raise ApiError(HTTPStatus.BAD_GATEWAY, str(e))

    def catalog_status(self, model: str, versions: list[str]) -> dict:
        try:
            return {"status": self.catalog.status(model, versions)}
        except CatalogError as e:
            raise ApiError(HTTPStatus.BAD_GATEWAY, str(e))

    def new(self, model: str, version: str, path: str | None) -> dict:
        try:
            info = self.catalog.download(model, version)
        except CatalogError as e:
            raise ApiError(HTTPStatus.BAD_GATEWAY, str(e))
        path = path or f"configs/{sanitize(info['aspect'])}_v{version.split('.')[0]}.yaml"
        if not path.endswith(CONFIG_EXT):
            raise ApiError(HTTPStatus.BAD_REQUEST, "O nome tem de terminar em .yaml")
        self.safe(path)
        cfg = new_config(info, self.root, path)
        return {"path": path, "config": cfg, "exists": self.safe(path).exists(), "example": info.get("example")}

    # --- campos do modelo --------------------------------------------------
    def _base(self, config_path: str | None) -> Path:
        return _join(self.root, config_path).parent if config_path else self.root

    def fields(self, cfg: dict, index: int, config_path: str | None) -> dict:
        subs = cfg.get("submodels") or []
        if index >= len(subs):
            raise ApiError(HTTPStatus.BAD_REQUEST, "Submodelo inexistente")
        sub = subs[index]
        model = sub.get("model") or {}
        if not model.get("schema"):
            return {"tree": None, "reason": "Este submodelo não tem um modelo SAMM associado (model.schema)."}
        base = self._base(config_path)
        schema_path = _join(base, model["schema"])
        if self.root not in schema_path.parents or not schema_path.is_file():
            return {"tree": None, "reason": f"Schema não encontrado: {model['schema']}"}
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        example = None
        if model.get("example"):
            ep = _join(base, model["example"])
            if self.root in ep.parents and ep.is_file():
                example = json.loads(ep.read_text(encoding="utf-8"))
                example = example[0] if isinstance(example, list) and example else example
        try:
            rule = parse_unwrap(sub.get("unwrap"), "unwrap")
        except ConfigError:
            rule = None
        view = example
        if rule and isinstance(example, dict):
            try:
                view = unwrap(example, {**rule, "on_multiple": "first"})
            except TransformError:
                view = example
        return {"tree": _jsonable(field_tree(schema, rule, view)), "urn": schema.get("x-samm-aspect-model-urn"),
                "example": self.rel(str(_join(base, model["example"]))) if model.get("example") else None}

    # --- pré-visualização --------------------------------------------------
    def preview(self, body: dict) -> dict:
        if body.get("config") is not None:
            raw = _expand_obj(body["config"])
        else:
            try:
                raw = load_yaml(_expand_env_lenient(body.get("text", ""))) or {}
            except yaml.YAMLError as e:
                return {"ok": False, "stage": "yaml", "error": str(e).split("\n")[0]}
        base = self._base(body.get("path"))
        try:
            cfg = from_dict(raw, base)
        except (ConfigError, ValueError) as e:
            return {"ok": False, "stage": "config", "error": str(e)}

        # os exemplos oficiais não trazem semantic id: usa-se o URN do schema do modelo
        urn_by_example: dict[str, str] = {}
        for s in cfg.submodels:
            if s.model and s.model.get("example"):
                try:
                    urn = json.loads(_join(base, s.model["schema"]).read_text(encoding="utf-8")).get("x-samm-aspect-model-urn")
                except (OSError, ValueError):
                    urn = None
                if urn:
                    urn_by_example[str(_join(base, s.model["example"]))] = urn

        samples = [str(self.safe(s)) for s in body.get("samples", [])]
        cap = _Capture()
        logger = logging.getLogger("cx2iceberg")
        logger.addHandler(cap)
        try:
            docs = []
            for d in read_documents(samples, cfg.input, semantic_id=body.get("semantic_id") or None):
                sid = d.semantic_id or urn_by_example.get(str(Path(d.source_file).resolve()))
                docs.append(dataclasses.replace(d, source_file=self.rel(d.source_file), semantic_id=sid))
            pasted = (body.get("pasted") or "").strip()
            if pasted:
                try:
                    data = strict_loads(pasted)          # o mesmo leitor estrito dos ficheiros
                except ValueError as e:
                    return {"ok": False, "stage": "pasted", "error": f"JSON colado inválido: {e}"}
                fallback = body.get("semantic_id") or (next(iter(urn_by_example.values()), None) if len(urn_by_example) == 1 else None)
                if isinstance(data, list):
                    docs += [Document(d, "(colado)", fallback, index=i) for i, d in enumerate(data)]
                else:
                    docs.append(Document(data, "(colado)", fallback))
            engine = Engine(cfg)
            routed = engine.route(docs)
            try:
                # partial: um documento rejeitado não esconde as linhas dos outros; aparece no relatório
                results = engine.run(docs, partial=True)
            except (TransformError, ConfigError) as e:
                return {"ok": False, "stage": "run", "error": self.short(str(e)),
                        "validation": engine.validation, "warnings": [self.short(m) for m in cap.messages],
                        "report": _report_view(engine.report)}
        finally:
            logger.removeHandler(cap)

        tables, write_check = [], []
        for name, res in results.items():
            try:                        # a mesma pré-verificação da escrita que o --dry-run faz
                check_table(res)
                if res.rows:
                    check_offline(res)
            except WriteCheckError as e:
                write_check.append(str(e))
            tables.append({
                "name": name, "mode": res.spec.mode, "keys": res.spec.keys, "partition_by": res.spec.partition_by,
                "explode": res.spec.explode,
                "columns": [{"name": c, "type": t, "athena": athena_type(t)} for c, t in res.types.items()],
                "rows": [_display_row(r, res.types) for r in res.rows[:MAX_ROWS]], "total": len(res.rows),
                "skipped": res.skipped, "check_failures": res.check_failures, "ddl": ddl(res, cfg.target),
            })
        unmatched = [d.source_file for d in docs if not any(d in routed.get(s.name, []) for s in cfg.submodels)]
        return {
            "ok": True,
            "summary": {"submodels": [s.name for s in cfg.submodels], "documents": len(docs),
                        "tables": len(tables), "rows": sum(t["total"] for t in tables), "unmatched": unmatched},
            "tables": tables,
            "validation": engine.validation,
            "warnings": list(dict.fromkeys(self.short(m) for m in cap.messages)),
            "report": _report_view(engine.report),
            "write_check": write_check,
        }

    def transforms(self) -> list[dict]:
        return [{"name": n, "doc": (REGISTRY[n].__doc__ or "").strip().split("\n")[0]} for n in sorted(REGISTRY)]


def make_handler(studio: Studio):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cx2iceberg-studio"

        def log_message(self, fmt, *args):  # silencioso
            pass

        def _send(self, status: int, body: bytes, ctype: str):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj: Any, status: int = 200):
            self._send(status, json.dumps(obj, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")

        def _guard(self):
            # Só pedidos da própria página (protege contra sites que tentem chamar o servidor local).
            origin = self.headers.get("Origin")
            host = self.headers.get("Host", "")
            if origin and urlparse(origin).netloc != host:
                raise ApiError(HTTPStatus.FORBIDDEN, "Origem não permitida")
            if not host.startswith(("127.0.0.1", "localhost")):
                raise ApiError(HTTPStatus.FORBIDDEN, "Host não permitido")

        def _handle(self, method: str):
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                self._guard()
                if method == "GET" and not url.path.startswith("/api/"):
                    return self._static(url.path)
                route = (method, url.path)
                b = self._body() if method in ("POST", "PUT") else {}
                if route == ("GET", "/api/files"):
                    return self._json(studio.files())
                if route == ("GET", "/api/catalog"):
                    return self._json(studio.catalog_list(q.get("refresh") == "1"))
                if route == ("GET", "/api/catalog/status"):
                    return self._json(studio.catalog_status(q.get("model", ""), q.get("versions", "").split(",")))
                if route == ("POST", "/api/new"):
                    return self._json(studio.new(b["model"], b["version"], b.get("path")))
                if route == ("POST", "/api/load"):
                    return self._json(studio.load(b["path"]))
                if route == ("POST", "/api/parse"):
                    return self._json(studio.parse(b.get("text", "")))
                if route == ("POST", "/api/render"):
                    return self._json({"yaml": render(b["config"])})
                if route == ("POST", "/api/fields"):
                    return self._json(studio.fields(b["config"], int(b.get("submodel", 0)), b.get("path")))
                if route == ("POST", "/api/preview"):
                    return self._json(studio.preview(b))
                if route == ("PUT", "/api/file"):
                    text = render(b["config"]) if b.get("config") is not None else b["text"]
                    return self._json(studio.save(b["path"], text, b.get("version")))
                if route == ("GET", "/api/transforms"):
                    return self._json(studio.transforms())
                raise ApiError(HTTPStatus.NOT_FOUND, "Rota desconhecida")
            except ApiError as e:
                self._json({"error": str(e)}, e.status)
            except (KeyError, ValueError, TypeError) as e:
                self._json({"error": f"Pedido inválido: {e}"}, HTTPStatus.BAD_REQUEST)

        def _static(self, path: str):
            name = "index.html" if path in ("/", "") else path.lstrip("/")
            p = (STATIC / name).resolve()
            if STATIC.resolve() not in p.parents or not p.is_file():
                raise ApiError(HTTPStatus.NOT_FOUND, "Não encontrado")
            ctype = {".html": "text/html", ".css": "text/css", ".js": "text/javascript"}.get(p.suffix, "application/octet-stream")
            self._send(200, p.read_bytes(), f"{ctype}; charset=utf-8")

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def do_PUT(self):
            self._handle("PUT")

    return Handler


def serve(root: str, port: int = 8765, open_browser: bool = False, models_dir: str | None = None):
    studio = Studio(root, models_dir)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), make_handler(studio))
    url = f"http://127.0.0.1:{httpd.server_address[1]}/"
    print(f"cx2iceberg studio em {url}  (pasta: {studio.root})  — Ctrl+C para parar")
    if open_browser:
        import webbrowser

        webbrowser.open(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
