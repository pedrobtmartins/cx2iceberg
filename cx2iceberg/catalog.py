"""Catálogo de submodelos Catena-X (repositório eclipse-tractusx/sldt-semantic-models).

Lê a lista de modelos com uma única chamada à API do GitHub (árvore do repositório,
guardada em cache 24 h) ou a partir de um clone local (`models_dir`). Descarrega o schema
(`gen/<Aspect>-schema.json`), o exemplo (`gen/<Aspect>.json`) e o `metadata.json` de uma
versão para `<root>/models/<modelo>/<versão>/`, para ficarem junto às configurações.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = "eclipse-tractusx/sldt-semantic-models"
BRANCH = "main"
TREE_URL = f"https://api.github.com/repos/{REPO}/git/trees/{BRANCH}?recursive=1"
RAW_URL = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}/"
TTL = 24 * 3600
_GEN = re.compile(r"^(io\.catenax\.[^/]+)/(\d+\.\d+\.\d+)/gen/([^/]+)-schema\.json$")


class CatalogError(Exception):
    pass


def _version_key(v: str) -> tuple:
    return tuple(int(x) for x in v.split("."))


def humanize(model_id: str) -> str:
    """'io.catenax.single_level_bom_as_built' -> 'Single level bom as built'."""
    name = model_id.removeprefix("io.catenax.").replace(".", " · ").replace("_", " ")
    return name[:1].upper() + name[1:]


class Catalog:
    def __init__(self, root: str | Path, models_dir: str | Path | None = None):
        self.root = Path(root)
        self.models_dir = Path(models_dir) if models_dir else None
        self.cache = self.root / ".cx2iceberg"

    # --- fontes -----------------------------------------------------------
    def _http(self, url: str) -> bytes:
        headers = {"User-Agent": "cx2iceberg", "Accept": "application/vnd.github+json"}
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if token and url.startswith("https://api.github.com/"):
            headers["Authorization"] = f"Bearer {token}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 403 and "rate limit" in e.read().decode("utf-8", "replace").lower():
                raise CatalogError("Limite de pedidos à API do GitHub atingido. Defina GITHUB_TOKEN ou use um clone local "
                                   "(cx2iceberg studio --models-dir).") from e
            raise CatalogError(f"GitHub respondeu {e.code} para {url}") from e
        except urllib.error.URLError as e:
            raise CatalogError(f"Sem acesso ao GitHub ({e.reason}). Use um clone local com --models-dir.") from e

    def _paths(self, refresh: bool = False) -> list[str]:
        if self.models_dir:
            return [p.relative_to(self.models_dir).as_posix() for p in self.models_dir.glob("io.catenax.*/*/gen/*-schema.json")]
        cached = self.cache / "catalog-tree.json"
        if not refresh and cached.exists() and time.time() - cached.stat().st_mtime < TTL:
            return json.loads(cached.read_text())
        data = json.loads(self._http(TREE_URL))
        paths = [e["path"] for e in data.get("tree", []) if e.get("type") == "blob" and "/gen/" in e["path"]]
        self.cache.mkdir(parents=True, exist_ok=True)
        cached.write_text(json.dumps(paths))
        return paths

    def _read(self, rel: str) -> bytes:
        if self.models_dir:
            p = self.models_dir / rel
            if not p.is_file():
                raise CatalogError(f"Não existe no clone local: {rel}")
            return p.read_bytes()
        return self._http(RAW_URL + rel)

    # --- consulta ---------------------------------------------------------
    def models(self, refresh: bool = False) -> list[dict]:
        paths = set(self._paths(refresh))
        found: dict[str, dict] = {}
        for p in paths:
            m = _GEN.match(p)
            if not m:
                continue
            model, version, aspect = m.groups()
            entry = found.setdefault(model, {"id": model, "name": humanize(model), "versions": []})
            entry["versions"].append({
                "version": version,
                "aspect": aspect,
                "has_example": f"{model}/{version}/gen/{aspect}.json" in paths or self.models_dir is not None,
                "local": (self.root / "models" / model / version / f"{aspect}-schema.json").exists(),
            })
        for e in found.values():
            e["versions"].sort(key=lambda v: _version_key(v["version"]), reverse=True)
        return sorted(found.values(), key=lambda e: e["name"].lower())

    def _version(self, model: str, version: str) -> dict:
        for m in self.models():
            if m["id"] == model:
                for v in m["versions"]:
                    if v["version"] == version:
                        return v
        raise CatalogError(f"Submodelo desconhecido: {model} {version}")

    def status(self, model: str, versions: list[str]) -> dict[str, str | None]:
        out = {}
        for v in versions:
            if not re.fullmatch(r"\d+\.\d+\.\d+", v):
                continue
            local = self.root / "models" / model / v / "metadata.json"
            try:
                raw = local.read_bytes() if local.exists() else self._read(f"{model}/{v}/metadata.json")
                out[v] = json.loads(raw).get("status")
            except (CatalogError, ValueError):
                out[v] = None
        return out

    def download(self, model: str, version: str) -> dict:
        info = self._version(model, version)  # valida o par modelo/versão contra o catálogo
        aspect = info["aspect"]
        dest = self.root / "models" / model / version
        dest.mkdir(parents=True, exist_ok=True)
        files = {"schema": f"gen/{aspect}-schema.json", "example": f"gen/{aspect}.json", "metadata": "metadata.json"}
        out = {}
        for key, rel in files.items():
            target = dest / Path(rel).name
            if not target.exists():
                try:
                    target.write_bytes(self._read(f"{model}/{version}/{rel}"))
                except CatalogError:
                    if key == "schema":
                        raise
                    continue
            out[key] = target.relative_to(self.root).as_posix()
        schema = json.loads((dest / f"{aspect}-schema.json").read_text(encoding="utf-8"))
        out.update(model=model, version=version, aspect=aspect, urn=schema.get("x-samm-aspect-model-urn"))
        return out
