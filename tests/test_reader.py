"""Step 2: reading — encoding, strict JSON, invalid documents; a bad file never stops the others."""
import json
import os
import sys
import types
from pathlib import Path

import pytest

from cx2iceberg import Document, Engine, from_dict, transform
from cx2iceberg.config import load
from cx2iceberg.reader import read_documents

EX = Path(__file__).parent.parent / "examples"
TABLE = [{"name": "t", "columns": {"id": "id", "v": "v ::double"}}]
OK = b'{"id": "ok", "v": 1}'


def load_dir(tmp_path, files, sub=None, opts=None, tables=TABLE):
    """Writes the files, reads the folder and runs the engine with partial=True. Returns (rows, report)."""
    for name, content in files.items():
        (tmp_path / name).write_bytes(content if isinstance(content, bytes) else content.encode())
    cfg = from_dict({"submodels": [{"name": "s", "tables": tables, **(sub or {})}]})
    eng = Engine(cfg)
    res = eng.run(list(read_documents([str(tmp_path)], opts)), partial=True)
    return res["t"].rows, eng.report


def rejected(rep):
    return {(Path(r["file"]).name, r["document"]) for r in rep.rejected.values()}


def messages(rep, kind="error"):
    return " | ".join(e["message"] for e in rep.events(kind))


# --- encoding: UTF-8 only, no guessing; the rest of the load goes on -----------------------
@pytest.mark.parametrize("content,fragment", [
    ('{"id": "Peça", "v": 2}'.encode("latin-1"), "byte 0xe7 at position 10 (maybe Latin-1"),
    ('{"id": "b", "v": 2}'.encode("cp1252") + "€".encode("cp1252"), "byte 0x80"),
    ('{"id": "b", "v": 2}'.encode("utf-16"), "byte 0xff at position 0 (looks like UTF-16)"),
    ('{"id": "b", "v": 2}'.encode("utf-16-le"), "NUL characters: it looks like UTF-16 without a BOM"),
    (b'{"id": "\xed\xa0\x80"}', "not valid UTF-8"),                 # surrogate encoded in UTF-8
    (b'{"id": "\xc0\xaf"}', "not valid UTF-8"),                    # overlong encoding
])
def test_invalid_utf8_rejects_only_that_file(tmp_path, content, fragment):
    rows, rep = load_dir(tmp_path, {"a.json": OK, "b.json": content, "c.json": b'{"id": "c", "v": 3}'})
    assert [r["id"] for r in rows] == ["ok", "c"]                     # before: a UnicodeDecodeError stopped everything
    assert rejected(rep) == {("b.json", None)} and fragment in messages(rep)


def test_utf8_bom_is_accepted(tmp_path):
    rows, rep = load_dir(tmp_path, {"a.json": b"\xef\xbb\xbf" + '{"id": "Peça", "v": 2}'.encode()})
    assert rows == [{"id": "Peça", "v": 2.0}] and not rep.rejected


def test_json_lines_reject_only_the_bad_line(tmp_path):
    content = b"\xef\xbb\xbf" + OK + b"\r\n" + '{"id": "é", "v": 2}'.encode("latin-1") + b"\n\n" + \
        '{"id": "c\u2028d", "v": 3}'.encode() + b"\n" + b'{"id": "e", "v": NaN}\n'
    rows, rep = load_dir(tmp_path, {"a.jsonl": content})
    assert [r["id"] for r in rows] == ["ok", "c\u2028d"]               # U+2028 does not split the line; CRLF and BOM accepted
    assert rejected(rep) == {("a.jsonl", 1), ("a.jsonl", 3)}
    assert "line 2: not valid UTF-8" in messages(rep) and "line 5: NaN" in messages(rep)


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root reads files without permission")
def test_unreadable_file_rejects_only_that_file(tmp_path):
    (tmp_path / "b.json").write_bytes(OK)
    (tmp_path / "b.json").chmod(0)
    try:
        rows, rep = load_dir(tmp_path, {"a.json": OK})
    finally:
        (tmp_path / "b.json").chmod(0o600)
    assert len(rows) == 1 and rejected(rep) == {("b.json", None)} and "could not read the file" in messages(rep)


def test_unreadable_s3_object_rejects_only_that_object():
    bodies = {"in/a.json": OK, "in/c.json": b'{"id": "c", "v": 3}'}

    class Client:
        def get_paginator(self, _):
            return types.SimpleNamespace(paginate=lambda **k: [{"Contents": [{"Key": "in/a.json"}, {"Key": "in/b.json"},
                                                                             {"Key": "in/c.json"}]}])

        def get_object(self, Bucket, Key):
            if Key not in bodies:
                raise PermissionError("AccessDenied (simulated)")
            return {"Body": types.SimpleNamespace(read=lambda: bodies[Key])}

    session = types.SimpleNamespace(client=lambda _: Client())
    docs = list(read_documents(["s3://bucket/in/"], session=session))
    assert [d.data["id"] for d in docs if d.error is None] == ["ok", "c"]
    assert [(d.source_file, d.error) for d in docs if d.error] == [("s3://bucket/in/b.json",
                                                                   "could not read the object: AccessDenied (simulated)")]


# --- strict JSON ----------------------------------------------------------------------
@pytest.mark.parametrize("content,fragment", [
    ('{"id": "b", "v": NaN}', "NaN is not valid JSON"),
    ('{"id": "b", "v": -Infinity}', "-Infinity is not valid JSON"),
    ('{"id": "b", "v": 2.0, "v": 20.0}', "repeated key 'v' in the same object (values 2.0 and 20.0)"),
    ('{"id": "b", "pcf": {"co2": 1, "x": [{"k": 1, "k": 2}]}}', "repeated key 'k'"),   # nested too
    ('{"id": "b", "v": 1e400}', "out of range for a double"),
    ('{"id": "b", "v": -1e400}', "out of range for a double"),
    ('{"id": "b", "v": 1e-400}', "too small for a double"),
    ('{"id": "b", "v": %s}' % ("9" * 5000), "more than 4300 digits"),                      # before: a ValueError stopped everything
    ("[" * 100000 + "]" * 100000, "nested too deeply"),                                 # before: a RecursionError stopped everything
    ('{"id": "b"}\n{"id": "c"}', "use the .jsonl extension"),
    ("", "Invalid JSON"),
])
def test_strict_json_rejects_the_document_with_a_reason(tmp_path, content, fragment):
    rows, rep = load_dir(tmp_path, {"a.json": OK, "b.json": content})
    assert [r["id"] for r in rows] == ["ok"] and rejected(rep) == {("b.json", None)}
    assert fragment in messages(rep)


def test_duplicate_key_with_the_same_value_is_still_rejected(tmp_path):
    _, rep = load_dir(tmp_path, {"b.json": '{"id": "b", "v": 2, "v": 2}'})
    assert "repeated key 'v'" in messages(rep)


# --- documents that cannot go on as they are -------------------------------------
def test_deep_documents_are_rejected_before_any_recursion(tmp_path):
    deep70 = '{"id": "b", "a": ' + '{"a": ' * 70 + "1" + "}" * 71
    deep995 = '{"id": "c", "x": ' + '[{"y": ' * 995 + "1" + "}]" * 995 + "}"           # json accepts it; the unwrap used to crash
    rows, rep = load_dir(tmp_path, {"a.json": OK, "b.json": deep70, "c.json": deep995}, sub={"unwrap": {"all": True}})
    assert [r["id"] for r in rows] == ["ok"] and rejected(rep) == {("b.json", None), ("c.json", None)}
    assert "more than 64 levels of nesting at 'a.a.a" in messages(rep)
    ok_63 = {"id": "d"}
    node = ok_63
    for _ in range(62):
        node["a"] = node = {}
    assert Engine(from_dict({"submodels": [{"name": "s", "tables": TABLE}]})).run([Document(ok_63, "d.json")])["t"].rows


def test_lone_surrogates_are_rejected_but_valid_pairs_are_kept(tmp_path):
    rows, rep = load_dir(tmp_path, {"a.json": '{"id": "smile \\ud83d\\ude00", "v": 1}',
                                    "b.json": '{"id": "b\\ud800", "v": 2}', "c.json": '{"id": "c", "\\udfff": 1}'})
    assert [r["id"] for r in rows] == ["smile \U0001F600"]                               # a valid pair: a single character
    assert rejected(rep) == {("b.json", None), ("c.json", None)}
    assert "text with a lone UTF-16 character" in messages(rep) and "key with a lone UTF-16 character" in messages(rep)


def test_documents_built_in_code_get_the_same_checks():
    eng = Engine(from_dict({"submodels": [{"name": "s", "tables": TABLE}]}))
    res = eng.run([Document({"id": "a", "v": 1.0}, "a"), Document({"id": "b", "v": float("nan")}, "b"),
                   Document({"id": "c", "x": [1, float("inf")]}, "c")], partial=True)
    assert [r["id"] for r in res["t"].rows] == ["a"]
    assert "non-finite number (nan) at 'v'" in messages(eng.report) and "non-finite number (inf) at 'x[1]'" in messages(eng.report)


@pytest.mark.parametrize("content,kind", [("null", "null"), ("42", "number"), ('"x"', "text"), ("true", "boolean")])
def test_top_level_value_must_be_an_object(tmp_path, content, kind):
    rows, rep = load_dir(tmp_path, {"a.json": OK, "b.json": content})
    assert len(rows) == 1 and f"the document is not a JSON object (it is {kind})" in messages(rep)   # before: a row of nulls


def test_non_object_items_in_a_list_file_are_rejected_individually(tmp_path):
    rows, rep = load_dir(tmp_path, {"a.json": '[{"id": "a", "v": 1}, null, {"id": "c", "v": 3}, [1]]'})
    assert [r["id"] for r in rows] == ["a", "c"] and rejected(rep) == {("a.json", 1), ("a.json", 3)}


def test_list_is_document_still_accepts_a_top_level_list(tmp_path):
    (tmp_path / "a.json").write_text('[{"id": "a"}]')
    docs = list(read_documents([str(tmp_path)], {"list_is_document": True}))
    assert docs[0].data == [{"id": "a"}] and docs[0].error is None


def test_empty_files_are_reported(tmp_path):
    rows, rep = load_dir(tmp_path, {"a.json": OK, "b.json": "[]", "c.jsonl": "\n  \n"})
    assert len(rows) == 1 and not rep.rejected
    assert rep.count(reason="empty_file", kind="warning") == 2                           # before: silence


def test_envelope_without_payload_is_rejected(tmp_path):
    env = {"envelope": {"payload_path": "payload", "semantic_id_path": "semanticId"}}
    files = {"a.json": '{"semanticId": "urn:x", "payload": {"id": "a", "v": 1}}',
             "b.json": '{"semanticId": "urn:x", "data": {"id": "b"}}',
             "c.json": '{"semanticId": "urn:x", "payload": "text"}'}
    rows, rep = load_dir(tmp_path, files, opts=env)
    assert [r["id"] for r in rows] == ["a"]                                               # before: a row of nulls
    assert "envelope without a payload at 'payload'" in messages(rep) and "is not a JSON object (it is text)" in messages(rep)


def test_studio_paste_uses_the_strict_reader(tmp_path):
    from cx2iceberg.studio.server import Studio

    cfg = {"submodels": [{"name": "s", "tables": TABLE}]}
    r = Studio(tmp_path).preview({"config": cfg, "samples": [], "pasted": '{"id": "a", "v": NaN}'})
    assert r["ok"] is False and r["stage"] == "pasted" and "NaN is not valid JSON" in r["error"]


# --- command line ------------------------------------------------------------------
def test_cli_with_a_latin1_file_loads_the_others_and_exits_2(tmp_path, capsys):
    from cx2iceberg.cli import main

    data = tmp_path / "data"
    data.mkdir()
    (data / "a.json").write_bytes(b'{"id": "A", "co2": 1.5}')
    (data / "b_latin1.json").write_bytes('{"id": "B", "name": "Peça", "co2": 2.0}'.encode("latin-1"))
    (data / "c.json").write_bytes(b'{"id": "C", "co2": 3.0}')
    (tmp_path / "c.yaml").write_text("submodels:\n  - name: s\n    tables:\n      - {name: t, columns: {id: id, co2: \"co2 ::double\"}}\n")
    with pytest.raises(SystemExit) as e:
        main(["run", "-c", str(tmp_path / "c.yaml"), str(data), "--dry-run"])
    out = capsys.readouterr()
    assert e.value.code == 2 and '"id": "A"' in out.out and '"id": "C"' in out.out
    assert "b_latin1.json — 1 error(s)" in out.err and "byte 0xe7" in out.err


# --- regression: the examples give exactly the same rows with the strict reader --------
@pytest.mark.parametrize("cfg_file,data", [("config.yaml", "data"), ("pcf_v7_to_v9.yaml", "data/pcf_7.json"),
                                           ("pcf_v7_to_v9.yaml", "data/pcf_9.json"), ("pcf9_flat.yaml", "data/pcf_9.json"),
                                           ("pcf9_struct.yaml", "data/pcf_9.json")])
def test_examples_are_unchanged_by_the_strict_reader(monkeypatch, cfg_file, data):
    import cx2iceberg.reader as R

    def run(strict: bool):
        if not strict:
            monkeypatch.setattr(R, "loads", json.loads)
        out = transform(load(EX / cfg_file), [str(EX / data)])
        monkeypatch.undo()
        return {n: (r.types, [{k: v for k, v in row.items() if k != "cx_ingested_at"} for row in r.rows])
                for n, r in out.items()}

    assert run(True) == run(False)                             # includes the document_id's content UUIDs
