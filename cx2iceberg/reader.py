"""Leitura de submodelos: ficheiros, pastas, JSON Lines e S3.

Opções em `input:` na configuração:

    input:
      semantic_id: urn:samm:io.catenax.serial_part:3.0.0#SerialPart  # fixo para todos
      semantic_id_map:                       # ou por padrão de nome de ficheiro
        "*serial*": urn:samm:io.catenax.serial_part:3.0.0#SerialPart
      envelope:                              # quando o JSON vem embrulhado
        payload_path: payload
        semantic_id_path: semanticId
      list_is_document: false                # true: um JSON que é lista = 1 documento
      match_all: false                       # true: um documento pode alimentar vários submodelos

Um ficheiro que não se consegue ler não pára a carga: vira um documento rejeitado (com a razão) e os
restantes seguem. Em JSON Lines, a unidade é a linha. Não se adivinha a codificação: só UTF-8 (com ou sem
BOM). O JSON é lido em modo estrito: sem NaN/Infinity, sem chaves repetidas, sem números que o double não
representa (1e400, 1e-400); os floats guardam o texto original (ver numbers.JsonFloat).
"""
from __future__ import annotations

import fnmatch
import json
from pathlib import Path
from typing import Any, Iterator

from .engine import Document
from .numbers import json_float
from .paths import MISSING, PathError, resolve_obj
from .report import short_value

_EXT = (".json", ".jsonl", ".ndjson")


class ReadError(ValueError):
    """O conteúdo não pode ser lido sem perder ou adivinhar informação."""


# --- JSON estrito --------------------------------------------------------------
def _reject_constant(name: str):
    raise ReadError(f"{name} não é JSON válido (número não finito)")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict:
    out = dict(pairs)
    if len(out) != len(pairs):
        seen: dict[str, Any] = {}
        for k, v in pairs:
            if k in seen:
                raise ReadError(f"chave repetida {k!r} no mesmo objecto (valores {short_value(seen[k])} e "
                                f"{short_value(v)}): não se escolhe um deles")
            seen[k] = v
    return out


def loads(text: str) -> Any:
    """json.loads estrito (ver docstring do módulo). Lança ReadError/ValueError com a razão em português."""
    try:
        return json.loads(text, parse_float=json_float, parse_constant=_reject_constant, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as e:
        hint = ""
        if e.msg == "Extra data":
            hint = " (há mais de um documento JSON no ficheiro: para um por linha use a extensão .jsonl)"
        elif "\x00" in text:
            hint = " (o texto tem caracteres NUL: parece UTF-16 sem BOM; converta para UTF-8)"
        raise ReadError(f"JSON inválido: {e}{hint}") from None
    except RecursionError:
        raise ReadError("JSON demasiado aninhado") from None
    except ReadError:
        raise
    except ValueError as e:  # ex.: NumberError do json_float, inteiro com mais de 4300 dígitos
        if "Exceeds the limit" in str(e):
            raise ReadError("JSON inválido: número inteiro com mais de 4300 dígitos") from None
        raise ReadError(f"JSON inválido: {e}") from None


def _decode(raw: bytes, bom: bool = True) -> str:
    try:
        return raw.decode("utf-8-sig" if bom else "utf-8")
    except UnicodeDecodeError as e:
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            hint = "parece UTF-16"
        else:
            hint = "talvez Latin-1/Windows-1252"
        raise ReadError(f"não é UTF-8 válido: byte 0x{raw[e.start]:02x} na posição {e.start} ({hint}); "
                        f"converta o ficheiro para UTF-8 — a codificação não é adivinhada") from None


# --- origens -------------------------------------------------------------------
def _s3_objects(uri: str, session=None) -> Iterator[tuple[str, bytes | None, str | None]]:
    if session is None:                 # com uma sessão dada, o boto3 não é preciso
        import boto3

        session = boto3.Session()
    s3 = session.client("s3")
    bucket, _, prefix = uri[5:].partition("/")

    def get(key: str) -> tuple[str, bytes | None, str | None]:
        name = f"s3://{bucket}/{key}"
        try:
            return name, s3.get_object(Bucket=bucket, Key=key)["Body"].read(), None
        except Exception as e:  # noqa: BLE001 - um objecto ilegível não pára os outros
            return name, None, f"não foi possível ler o objecto: {e}"

    if prefix.endswith(_EXT):
        yield get(prefix)
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(_EXT):
                yield get(obj["Key"])


def _local_files(p: Path) -> Iterator[tuple[str, bytes | None, str | None]]:
    files = [f for f in sorted(p.rglob("*")) if f.is_file() and f.suffix in _EXT] if p.is_dir() else [p]
    for f in files:
        try:
            yield str(f), f.read_bytes(), None
        except OSError as e:  # permissões, ficheiro apagado entretanto...
            yield str(f), None, f"não foi possível ler o ficheiro: {e}"


# --- documentos ----------------------------------------------------------------
def read_documents(sources: list[str], opts: dict | None = None, semantic_id: str | None = None, session=None) -> Iterator[Document]:
    opts = opts or {}
    env = opts.get("envelope") or {}
    fixed = semantic_id or opts.get("semantic_id")
    sid_map = opts.get("semantic_id_map") or {}
    list_is_document = bool(opts.get("list_is_document"))
    for src in sources:
        files = _s3_objects(src, session) if src.startswith("s3://") else _local_files(Path(src))
        for name, raw, error in files:
            if error is not None:
                yield Document(None, name, fixed, error=error)
                continue
            yield from _file_documents(raw, name, env, fixed, sid_map, list_is_document)


def _file_documents(raw: bytes, name: str, env: dict, fixed: str | None, sid_map: dict,
                    list_is_document: bool) -> Iterator[Document]:
    def make(item: Any, index: int | None) -> Document:
        return _document(item, name, env, fixed, sid_map, index, list_is_document and index is None)

    if name.endswith((".jsonl", ".ndjson")):
        n = 0
        # separa em bytes por '\n': cada linha é descodificada sozinha (uma linha má não estraga as outras) e um
        # U+2028 dentro de um texto não parte a linha (str.splitlines partiria)
        for lineno, line in enumerate(raw.split(b"\n"), 1):
            if not line.strip():
                continue
            try:
                item = loads(_decode(line, bom=lineno == 1))
            except ReadError as e:
                yield Document(None, name, fixed, index=n, error=f"linha {lineno}: {e}")
            else:
                yield make(item, n)
            n += 1
        if n == 0:
            yield Document(None, name, fixed, notice="ficheiro sem documentos")
        return
    try:
        data = loads(_decode(raw))
    except ReadError as e:
        yield Document(None, name, fixed, error=str(e))
        return
    if isinstance(data, list) and not list_is_document:
        if not data:
            yield Document(None, name, fixed, notice="ficheiro sem documentos (lista vazia)")
        for i, item in enumerate(data):
            yield make(item, i)
    else:
        yield make(data, None)


def _document(item: Any, name: str, env: dict, fixed: str | None, sid_map: dict, index: int | None,
              list_ok: bool = False) -> Document:
    sid = fixed
    if not isinstance(item, dict) and not (list_ok and isinstance(item, list)):
        return Document(None, name, fixed, index=index, error=f"o documento não é um objecto JSON (é {_kind(item)})")
    if env:
        try:
            return _envelope(item, name, env, sid, sid_map, index)
        except PathError as e:
            return Document(None, name, sid, index=index, error=f"envelope: {e}")
    return _with_semantic_id(Document(item, name, sid, index=index), sid_map)


def _envelope(item: Any, name: str, env: dict, sid: str | None, sid_map: dict, index: int | None) -> Document:
    if env.get("semantic_id_path") and not sid:
        v = resolve_obj(item, env["semantic_id_path"])
        sid = None if v is MISSING else v
    payload = item
    if env.get("payload_path"):
        v = resolve_obj(item, env["payload_path"])
        if v is MISSING or v is None:
            return Document(None, name, sid, index=index, error=f"envelope sem payload em '{env['payload_path']}'")
        if not isinstance(v, dict):
            return Document(None, name, sid, index=index,
                            error=f"o payload em '{env['payload_path']}' não é um objecto JSON (é {_kind(v)})")
        payload = v
    return _with_semantic_id(Document(payload, name, sid, index=index), sid_map)


def _with_semantic_id(doc: Document, sid_map: dict) -> Document:
    if not doc.semantic_id:
        name = doc.source_file or ""
        base = name.rsplit("/", 1)[-1]
        doc.semantic_id = next((u for pat, u in sid_map.items() if fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(name, pat)), None)
    return doc


def _kind(v: Any) -> str:
    return {type(None): "null", bool: "booleano", str: "texto", list: "lista", dict: "objecto"}.get(
        type(v), "número" if isinstance(v, (int, float)) else type(v).__name__)
