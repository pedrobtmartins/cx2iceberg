"""Reading submodels: files, folders, JSON Lines and S3.

Options under `input:` in the configuration:

    input:
      semantic_id: urn:samm:io.catenax.serial_part:3.0.0#SerialPart  # fixed for all
      semantic_id_map:                       # or by file name pattern
        "*serial*": urn:samm:io.catenax.serial_part:3.0.0#SerialPart
      envelope:                              # when the JSON comes wrapped
        payload_path: payload
        semantic_id_path: semanticId
      list_is_document: false                # true: a JSON that is a list = 1 document
      match_all: false                       # true: a document can feed several submodels

A file that cannot be read does not stop the load: it becomes a rejected document (with the reason) and the
others go on. In JSON Lines, the unit is the line. The encoding is not guessed: UTF-8 only (with or without a
BOM). The JSON is read in strict mode: no NaN/Infinity, no repeated keys, no numbers the double cannot
represent (1e400, 1e-400); floats keep the original text (see numbers.JsonFloat).
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
    """The content cannot be read without losing or guessing information."""


# --- strict JSON --------------------------------------------------------------
def _reject_constant(name: str):
    raise ReadError(f"{name} is not valid JSON (non-finite number)")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict:
    out = dict(pairs)
    if len(out) != len(pairs):
        seen: dict[str, Any] = {}
        for k, v in pairs:
            if k in seen:
                raise ReadError(f"repeated key {k!r} in the same object (values {short_value(seen[k])} and "
                                f"{short_value(v)}): neither is picked")
            seen[k] = v
    return out


def loads(text: str) -> Any:
    """Strict json.loads (see the module docstring). Raises ReadError/ValueError with the reason."""
    try:
        return json.loads(text, parse_float=json_float, parse_constant=_reject_constant, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as e:
        hint = ""
        if e.msg == "Extra data":
            hint = " (there is more than one JSON document in the file: for one per line use the .jsonl extension)"
        elif "\x00" in text:
            hint = " (the text has NUL characters: it looks like UTF-16 without a BOM; convert it to UTF-8)"
        raise ReadError(f"Invalid JSON: {e}{hint}") from None
    except RecursionError:
        raise ReadError("JSON nested too deeply") from None
    except ReadError:
        raise
    except ValueError as e:  # e.g. NumberError from json_float, an integer with more than 4300 digits
        if "Exceeds the limit" in str(e):
            raise ReadError("Invalid JSON: integer with more than 4300 digits") from None
        raise ReadError(f"Invalid JSON: {e}") from None


def _decode(raw: bytes, bom: bool = True) -> str:
    try:
        return raw.decode("utf-8-sig" if bom else "utf-8")
    except UnicodeDecodeError as e:
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            hint = "looks like UTF-16"
        else:
            hint = "maybe Latin-1/Windows-1252"
        raise ReadError(f"not valid UTF-8: byte 0x{raw[e.start]:02x} at position {e.start} ({hint}); "
                        f"convert the file to UTF-8 — the encoding is not guessed") from None


# --- sources -------------------------------------------------------------------
def _s3_objects(uri: str, session=None) -> Iterator[tuple[str, bytes | None, str | None]]:
    if session is None:                 # with a given session, boto3 is not needed
        import boto3

        session = boto3.Session()
    s3 = session.client("s3")
    bucket, _, prefix = uri[5:].partition("/")

    def get(key: str) -> tuple[str, bytes | None, str | None]:
        name = f"s3://{bucket}/{key}"
        try:
            return name, s3.get_object(Bucket=bucket, Key=key)["Body"].read(), None
        except Exception as e:  # noqa: BLE001 - an unreadable object does not stop the others
            return name, None, f"could not read the object: {e}"

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
        except OSError as e:  # permissions, file deleted in the meantime...
            yield str(f), None, f"could not read the file: {e}"


# --- documents ----------------------------------------------------------------
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
        # split in bytes on '\n': each line is decoded on its own (a bad line does not spoil the others) and a
        # U+2028 inside a text does not split the line (str.splitlines would)
        for lineno, line in enumerate(raw.split(b"\n"), 1):
            if not line.strip():
                continue
            try:
                item = loads(_decode(line, bom=lineno == 1))
            except ReadError as e:
                yield Document(None, name, fixed, index=n, error=f"line {lineno}: {e}")
            else:
                yield make(item, n)
            n += 1
        if n == 0:
            yield Document(None, name, fixed, notice="file without documents")
        return
    try:
        data = loads(_decode(raw))
    except ReadError as e:
        yield Document(None, name, fixed, error=str(e))
        return
    if isinstance(data, list) and not list_is_document:
        if not data:
            yield Document(None, name, fixed, notice="file without documents (empty list)")
        for i, item in enumerate(data):
            yield make(item, i)
    else:
        yield make(data, None)


def _document(item: Any, name: str, env: dict, fixed: str | None, sid_map: dict, index: int | None,
              list_ok: bool = False) -> Document:
    sid = fixed
    if not isinstance(item, dict) and not (list_ok and isinstance(item, list)):
        return Document(None, name, fixed, index=index, error=f"the document is not a JSON object (it is {_kind(item)})")
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
            return Document(None, name, sid, index=index, error=f"envelope without a payload at '{env['payload_path']}'")
        if not isinstance(v, dict):
            return Document(None, name, sid, index=index,
                            error=f"the payload at '{env['payload_path']}' is not a JSON object (it is {_kind(v)})")
        payload = v
    return _with_semantic_id(Document(payload, name, sid, index=index), sid_map)


def _with_semantic_id(doc: Document, sid_map: dict) -> Document:
    if not doc.semantic_id:
        name = doc.source_file or ""
        base = name.rsplit("/", 1)[-1]
        doc.semantic_id = next((u for pat, u in sid_map.items() if fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(name, pat)), None)
    return doc


def _kind(v: Any) -> str:
    return {type(None): "null", bool: "boolean", str: "text", list: "list", dict: "object"}.get(
        type(v), "number" if isinstance(v, (int, float)) else type(v).__name__)
