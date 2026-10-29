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
"""
from __future__ import annotations

import fnmatch
import json
from pathlib import Path
from typing import Any, Iterator

from .engine import Document
from .paths import MISSING, resolve_obj

_EXT = (".json", ".jsonl", ".ndjson")


def _parse(text: str, name: str, list_is_document: bool) -> Iterator[Any]:
    if name.endswith((".jsonl", ".ndjson")):
        for line in text.splitlines():
            if line.strip():
                yield json.loads(line)
        return
    data = json.loads(text)
    if isinstance(data, list) and not list_is_document:
        yield from data
    else:
        yield data


def _s3_objects(uri: str, session=None) -> Iterator[tuple[str, str]]:
    import boto3

    s3 = (session or boto3.Session()).client("s3")
    bucket, _, prefix = uri[5:].partition("/")
    if prefix.endswith(_EXT):
        body = s3.get_object(Bucket=bucket, Key=prefix)["Body"].read().decode("utf-8")
        yield uri, body
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(_EXT):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
                yield f"s3://{bucket}/{obj['Key']}", body


def _local_files(p: Path) -> Iterator[tuple[str, str]]:
    if p.is_dir():
        for f in sorted(p.rglob("*")):
            if f.is_file() and f.suffix in _EXT:
                yield str(f), f.read_text(encoding="utf-8")
    else:
        yield str(p), p.read_text(encoding="utf-8")


def read_documents(sources: list[str], opts: dict | None = None, semantic_id: str | None = None, session=None) -> Iterator[Document]:
    opts = opts or {}
    env = opts.get("envelope") or {}
    fixed = semantic_id or opts.get("semantic_id")
    sid_map = opts.get("semantic_id_map") or {}
    for src in sources:
        files = _s3_objects(src, session) if src.startswith("s3://") else _local_files(Path(src))
        for name, text in files:
            for item in _parse(text, name, bool(opts.get("list_is_document"))):
                yield _document(item, name, env, fixed, sid_map)


def _document(item: Any, name: str, env: dict, fixed: str | None, sid_map: dict) -> Document:
    sid = fixed
    payload = item
    if env:
        if env.get("semantic_id_path") and not sid:
            v = resolve_obj(item, env["semantic_id_path"])
            sid = None if v is MISSING else v
        if env.get("payload_path"):
            v = resolve_obj(item, env["payload_path"])
            payload = None if v is MISSING else v
    if not sid:
        base = name.rsplit("/", 1)[-1]
        sid = next((u for pat, u in sid_map.items() if fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(name, pat)), None)
    return Document(payload, name, sid)
