"""cx2iceberg studio: ver, editar e pré-visualizar configurações num browser.

Servidor local (só 127.0.0.1), só biblioteca padrão. Lê e grava ficheiros apenas dentro de `root`.

    cx2iceberg studio --root . --port 8765
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from ..athena_types import athena_type
from ..config import ConfigError, from_dict
from ..engine import Document, Engine, TransformError, unwrap
from ..reader import read_documents
from ..transforms import REGISTRY
from ..writer import ddl

STATIC = Path(__file__).parent / "static"
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".taste-skill", ".pytest_cache"}
CONFIG_EXT = (".yaml", ".yml")
DATA_EXT = (".json", ".jsonl", ".ndjson")
MAX_ROWS = 200
MAX_DOC_CHARS = 400_000


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


_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env_lenient(text: str) -> str:
    """Como na configuração, mas uma variável em falta não é erro na pré-visualização."""
    return _ENV.sub(lambda m: os.environ.get(m.group(1), m.group(2) if m.group(2) is not None else m.group(0)), text)


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class Studio:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()

    # --- ficheiros ---------------------------------------------------------
    def safe(self, rel: str) -> Path:
        p = (self.root / rel).resolve()
        if p != self.root and self.root not in p.parents:
            raise ApiError(HTTPStatus.FORBIDDEN, "Caminho fora da pasta do studio")
        return p

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
        data = [f for f in self._walk(DATA_EXT) if not f["path"].endswith("-schema.json")]
        configs = [f for f in self._walk(CONFIG_EXT) if self._looks_like_config(f["path"])]
        return {"root": str(self.root), "configs": configs, "samples": data}

    def _looks_like_config(self, rel: str) -> bool:
        try:
            head = self.safe(rel).read_text(encoding="utf-8")[:20000]
        except (OSError, UnicodeDecodeError):
            return False
        return re.search(r"^submodels\s*:", head, re.MULTILINE) is not None

    def read(self, rel: str) -> dict:
        p = self.safe(rel)
        if not p.is_file():
            raise ApiError(HTTPStatus.NOT_FOUND, f"Ficheiro não encontrado: {rel}")
        return {"path": rel, "text": p.read_text(encoding="utf-8"), "mtime": p.stat().st_mtime}

    def save(self, rel: str, text: str, mtime: float | None) -> dict:
        p = self.safe(rel)
        if not rel.endswith(CONFIG_EXT):
            raise ApiError(HTTPStatus.BAD_REQUEST, "Só se gravam ficheiros .yaml/.yml")
        if p.exists() and mtime is not None and abs(p.stat().st_mtime - mtime) > 1e-6:
            raise ApiError(HTTPStatus.CONFLICT, "O ficheiro foi alterado fora do studio desde que o abriu. Recarregue-o.")
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=p.parent, prefix=f".{p.name}.", suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, p)
        return {"path": rel, "mtime": p.stat().st_mtime}

    # --- pré-visualização --------------------------------------------------
    def preview(self, body: dict) -> dict:
        text = body.get("text", "")
        samples = [str(self.safe(s)) for s in body.get("samples", [])]
        try:
            raw = yaml.safe_load(_expand_env_lenient(text)) or {}
        except yaml.YAMLError as e:
            mark = getattr(e, "problem_mark", None)
            return {"ok": False, "stage": "yaml", "error": str(e).split("\n")[0] if not mark else
                    f"{getattr(e, 'problem', None) or 'YAML inválido'} (linha {mark.line + 1})",
                    "line": mark.line + 1 if mark else None}
        if not isinstance(raw, dict):
            return {"ok": False, "stage": "config", "error": "A configuração tem de ser um mapa YAML (chave: valor)"}
        try:
            cfg = from_dict(raw)
        except (ConfigError, ValueError) as e:
            return {"ok": False, "stage": "config", "error": str(e), "line": _locate(text, str(e)) or _guess_line(text, str(e))}

        cap = _Capture()
        logger = logging.getLogger("cx2iceberg")
        logger.addHandler(cap)
        try:
            docs = [Document(d.data, self.rel(d.source_file), d.semantic_id, d.meta)
                    for d in read_documents(samples, cfg.input, semantic_id=body.get("semantic_id") or None)]
            pasted = (body.get("pasted") or "").strip()
            if pasted:
                try:
                    data = json.loads(pasted)
                except json.JSONDecodeError as e:
                    return {"ok": False, "stage": "pasted", "error": f"JSON colado inválido: {e}"}
                items = data if isinstance(data, list) else [data]
                docs += [Document(d, "(colado)", body.get("semantic_id") or cfg.input.get("semantic_id")) for d in items]
            engine = Engine(cfg)
            routed = engine.route(docs)
            try:
                results = engine.run(docs)
            except (TransformError, ConfigError) as e:
                return {"ok": False, "stage": "run", "error": self.short(str(e)),
                        "documents": self._documents(cfg, docs, routed), "warnings": [self.short(m) for m in cap.messages]}
        finally:
            logger.removeHandler(cap)

        tables = []
        for name, res in results.items():
            tables.append({
                "name": name,
                "mode": res.spec.mode,
                "keys": res.spec.keys,
                "partition_by": res.spec.partition_by,
                "explode": res.spec.explode,
                "columns": [{"name": c, "type": t, "athena": athena_type(t)} for c, t in res.types.items()],
                "rows": _jsonable(res.rows[:MAX_ROWS]),
                "total": len(res.rows),
                "skipped": res.skipped,
                "check_failures": res.check_failures,
                "ddl": ddl(res, cfg.target),
            })
        return {
            "ok": True,
            "summary": {"submodels": [s.name for s in cfg.submodels], "documents": len(docs),
                        "tables": len(tables), "rows": sum(t["total"] for t in tables)},
            "tables": tables,
            "documents": self._documents(cfg, docs, routed),
            "warnings": list(dict.fromkeys(self.short(m) for m in cap.messages)),
        }

    def _documents(self, cfg, docs, routed) -> list[dict]:
        out = []
        for d in docs:
            subs = [s for s in cfg.submodels if any(x is d for x in routed.get(s.name, []))]
            view = d.data
            if subs and subs[0].unwrap:
                try:
                    view = unwrap(d.data, subs[0].unwrap, d.source_file)
                except TransformError:
                    pass
            text = json.dumps(_jsonable(view), ensure_ascii=False)
            out.append({
                "source": d.source_file,
                "semantic_id": d.semantic_id,
                "submodels": [s.name for s in subs],
                "unwrapped": bool(subs and subs[0].unwrap),
                "data": _jsonable(view) if len(text) <= MAX_DOC_CHARS else None,
                "too_large": len(text) > MAX_DOC_CHARS,
            })
        return out

    def rel(self, path: str | None) -> str | None:
        if path and path.startswith(str(self.root) + os.sep):
            return Path(path).relative_to(self.root).as_posix()
        return path

    def short(self, message: str) -> str:
        return message.replace(str(self.root) + os.sep, "")

    def transforms(self) -> list[dict]:
        return [{"name": n, "doc": (REGISTRY[n].__doc__ or "").strip().split("\n")[0]} for n in sorted(REGISTRY)]


def _locate(text: str, message: str) -> int | None:
    """'submodels[0].tables[1].columns.x: …' -> linha do nó correspondente no YAML."""
    m = re.match(r"([A-Za-z_]\w*(?:\[\d+\])?(?:\.[\w$\-]+(?:\[\d+\])?)*)", message)
    if not m:
        return None
    try:
        node = yaml.compose(_expand_env_lenient(text))
    except yaml.YAMLError:
        return None
    line = None
    for seg in m.group(1).split("."):
        name, *idx = re.split(r"\[|\]", seg)
        if name and isinstance(node, yaml.MappingNode):
            hit = next(((k, v) for k, v in node.value if k.value == name), None)
            if hit is None:
                break
            line, node = hit[0].start_mark.line + 1, hit[1]
        for i in (int(x) for x in idx if x):
            if not isinstance(node, yaml.SequenceNode) or i >= len(node.value):
                return line
            node = node.value[i]
            line = node.start_mark.line + 1
    # "…tables[1]: mode 'x' inválido" -> a linha da chave citada, se existir nesse bloco
    key = re.match(r"\s*:?\s*([A-Za-z_]\w*)\b", message[m.end():])
    if key and isinstance(node, yaml.MappingNode):
        hit = next((k for k, _ in node.value if k.value == key.group(1)), None)
        if hit is not None:
            line = hit.start_mark.line + 1
    return line


def _guess_line(text: str, message: str) -> int | None:
    """Tenta encontrar no YAML a linha a que uma mensagem de erro se refere (pelo último nome citado)."""
    names = re.findall(r"'([A-Za-z0-9_.\-\[\]*$?=]+)'", message)
    lines = text.splitlines()
    for name in reversed(names):
        key = name.split(".")[-1]
        for i, line in enumerate(lines):
            if re.match(rf"\s*-?\s*{re.escape(key)}\s*:", line) or re.search(rf"name:\s*{re.escape(key)}\s*$", line):
                return i + 1
    return None


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
                if route == ("GET", "/api/files"):
                    return self._json(studio.files())
                if route == ("GET", "/api/file"):
                    return self._json(studio.read(q.get("path", "")))
                if route == ("PUT", "/api/file"):
                    b = self._body()
                    return self._json(studio.save(b["path"], b["text"], b.get("mtime")))
                if route == ("POST", "/api/preview"):
                    return self._json(studio.preview(self._body()))
                if route == ("GET", "/api/transforms"):
                    return self._json(studio.transforms())
                raise ApiError(HTTPStatus.NOT_FOUND, "Rota desconhecida")
            except ApiError as e:
                self._json({"error": str(e)}, e.status)
            except (KeyError, json.JSONDecodeError) as e:
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


def serve(root: str, port: int = 8765, open_browser: bool = False):
    studio = Studio(root)
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
