"""Step 1: the load report, errors collected per document, exit codes and Lambda."""
import json
import sys
import types

import pytest

from cx2iceberg import ConfigError, Document, DocumentsRejected, Engine, TransformError, from_dict
from cx2iceberg.reader import read_documents
from cx2iceberg.report import VALUE_MAX, Report


def engine(tables, report=None, **sub):
    return Engine(from_dict({"submodels": [{"name": "s", "tables": tables, **sub}]}), report=report)


def docs(*items, file="f.json"):
    return [Document(d, file, "urn:x", index=i) for i, d in enumerate(items)]


HEAD_AND_LINES = [
    {"name": "head", "columns": {"id": "id", "n": "n ::int"}},
    {"name": "lines", "explode": "items", "columns": {"id": "$root.id", "q": "q ::double"}},
]


# --- errors: all collected, document rejected as a whole -------------------
def test_all_errors_are_collected_and_the_document_is_rejected_in_every_table():
    batch = docs({"id": "A", "n": 1, "items": [{"q": 1}]},
                 {"id": "B", "n": "x", "items": [{"q": 2}]},                  # error only in table head
                 {"id": "C", "n": 3, "items": [{"q": 3}, {"q": "1,5"}]})      # error only in the 2nd row of lines
    with pytest.raises(DocumentsRejected) as e:
        engine(HEAD_AND_LINES).run(batch)
    msg = str(e.value)
    assert "2 document(s) rejected" in msg and "column 'n'" in msg and "column 'q'" in msg  # both, not only the 1st

    eng = engine(HEAD_AND_LINES)
    res = eng.run(batch, partial=True)
    assert [r["id"] for r in res["head"].rows] == ["A"]
    assert [r["id"] for r in res["lines"].rows] == ["A"]          # the good rows of B and C come out too
    rep = eng.report
    assert rep.status == "partial" and len(rep.rejected) == 2
    assert rep.tables["lines"] == {"rows": 1, "rows_from_rejected_documents": 2}
    q = next(ev for ev in rep.events("error") if ev["column"] == "q")
    assert q == {**q, "kind": "error", "reason": "invalid_value", "action": "document_rejected", "table": "lines",
                 "file": "f.json", "document": 2, "row": "items[1]", "value": "'1,5'"}


def test_config_error_stops_the_load_even_with_on_error_null():
    bad = {"name": "t", "columns": {"v": {"cases": [{"when": {"path": "x", "op": "nope", "value": 1}, "path": "x"}],
                                          "on_error": "null"}}}
    with pytest.raises(ConfigError, match="nope"):          # before: silently set to null; now it does not even load
        engine([bad])


# --- declared losses and warnings: counted, with details ---------------------------
def test_declared_losses_are_reported_with_table_column_file_value_and_reason():
    cols = {"id": "id",
            "kg": {"path": "q", "type": "double", "on_error": "null"},
            "n": {"path": "n", "type": "int", "on_error": "skip_row"},
            "unit": {"path": "unit", "default": "kg"}}
    eng = engine([{"name": "t", "where": {"path": "id", "op": "ne", "value": "skip-me"}, "columns": cols}])
    res = eng.run(docs({"id": "a", "q": "x", "n": 1}, {"id": "b", "q": 1, "n": "y", "unit": "g"}, {"id": "skip-me"}))["t"]
    rep = eng.report
    assert [r["id"] for r in res.rows] == ["a"] and res.skipped == 1 and rep.status == "ok"
    nulled = rep.events("declared")
    by = {(e["reason"], e["action"], e["column"]): e for e in nulled}
    assert by[("invalid_value", "nulled", "kg")]["value"] == "'x'" and by[("invalid_value", "nulled", "kg")]["file"] == "f.json"
    assert by[("invalid_value", "row_skipped", "n")]["document"] == 1
    assert by[("missing_value", "default_applied", "unit")]["message"] == "missing value; default 'kg' used"
    assert rep.count(reason="where_false") == 1
    text = rep.render()
    assert "Losses declared" in text and "t.kg — invalid value → value set to null: 1" in text


def test_duplicates_null_keys_checks_and_unmatched_documents():
    # step 6: a repeated key IN THE SAME document, a null key and a document without a submodel went from warning to error;
    # a repeated key BETWEEN documents of the batch is still a warning (the last one is kept, as in merge)
    tables = [{"name": "bom", "explode": "childItems", "mode": "merge", "keys": ["parent", "child"],
               "columns": {"parent": "$root.id", "child": "c", "qty": "qty ::double",
                           "bpn": {"path": "bpn", "check": {"regex": "^BPNL"}}}}]
    eng = engine(tables, match={"semantic_id": "urn:x"})
    batch = docs({"id": "P", "childItems": [{"c": "C1", "qty": 2, "bpn": "BPNL1"}, {"c": "C1", "qty": 3, "bpn": "x"}]},
                 {"id": "N", "childItems": [{"qty": 5, "bpn": "BPNL2"}]},
                 {"id": "R", "childItems": [{"c": "C9", "qty": 1, "bpn": "BPNL3"}]},
                 {"id": "R", "childItems": [{"c": "C9", "qty": 2, "bpn": "x"}]})
    batch.append(Document({"id": "Q"}, "other.json", "urn:y"))
    res = eng.run(batch, partial=True)["bom"]
    assert [(r["parent"], r["child"], r["qty"]) for r in res.rows] == [("R", "C9", 2.0)]
    rep = eng.report
    dup = next(e for e in rep.events("error") if e["reason"] == "duplicate_key")
    assert dup["row"] == "childItems[1]" and dup["keys"] == {"parent": "P", "child": "C1"} and dup["document"] == 0
    assert rep.count(reason="null_key", kind="error") == 1 and rep.count(reason="no_submodel", kind="error") == 1
    replaced = next(e for e in rep.events("warning") if e["reason"] == "duplicate_key")
    assert replaced["action"] == "row_replaced" and replaced["document"] == 2
    assert rep.count(reason="check", action="kept") == 2      # the rejected document's one too: the report says it all


# --- errors at document level --------------------------------------------------
def test_invalid_json_rejects_only_its_file_or_line(tmp_path):
    (tmp_path / "good.json").write_text('{"id": "a", "n": 1}')
    (tmp_path / "bad.json").write_text('{"id": ')
    (tmp_path / "many.jsonl").write_text('{"id": "b", "n": 2}\n\n{"id": \n{"id": "c", "n": 3}\n')
    eng = engine([{"name": "t", "columns": {"id": "id", "n": "n ::int"}}])
    res = eng.run(list(read_documents([str(tmp_path)])), partial=True)["t"]
    assert sorted(r["id"] for r in res.rows) == ["a", "b", "c"]
    rejected = {(r["file"].rsplit("/", 1)[-1], r["document"]) for r in eng.report.rejected.values()}
    assert rejected == {("bad.json", None), ("many.jsonl", 1)}
    assert eng.report.count(reason="unreadable") == 2


def test_unwrap_and_schema_problems_are_per_document(tmp_path):
    tables = [{"name": "t", "columns": {"id": "id", "c": "block.c"}}]
    batch = docs({"id": "a", "block": [{"c": 1}]}, {"id": "b", "block": [{"c": 1}, {"c": 2}]})
    eng = engine(tables, unwrap={"paths": ["block"]})
    res = eng.run(batch, partial=True)["t"]
    assert [r["id"] for r in res.rows] == ["a"] and eng.report.count(reason="unwrap_multiple", kind="error") == 1
    eng = engine(tables, unwrap={"paths": ["block"], "on_multiple": "first"})
    assert len(eng.run(batch)["t"].rows) == 2 and eng.report.count(action="first_element", kind="declared") == 1

    (tmp_path / "s.json").write_text('{"type": "object", "required": ["id"]}')
    cfg = from_dict({"submodels": [{"name": "s", "model": {"schema": "s.json"}, "validate": "fail", "tables": tables}]},
                    tmp_path)
    eng = Engine(cfg)
    res = eng.run(docs({"id": "a"}, {"block": 1}), partial=True)["t"]
    assert [r["id"] for r in res.rows] == ["a"] and eng.report.count(reason="schema", kind="error") == 1


# --- bounded memory -----------------------------------------------------------
def test_report_memory_is_bounded_but_counts_are_exact(tmp_path):
    details = tmp_path / "d.jsonl"
    rep = Report(samples=3, details_path=str(details))
    eng = engine([{"name": "t", "columns": {"v": {"path": "v", "type": "double", "on_error": "null"}}}], report=rep)
    eng.run(docs(*[{"v": "x" * 1000} for _ in range(50)]))
    rep.close()
    group = rep.to_dict()["groups"][0]
    assert group["count"] == 50 and len(group["examples"]) == 3
    assert len(group["examples"][0]["value"]) == VALUE_MAX + 1          # repr cut + '…'
    lines = details.read_text().splitlines()
    assert len(lines) == 50 and json.loads(lines[49])["document"] == 49


def test_max_errors_interrupts_and_nothing_is_returned():
    rep = Report(max_errors=5)
    eng = engine([{"name": "t", "columns": {"v": "v ::int"}}], report=rep)
    with pytest.raises(TransformError, match="stopped after 5 error"):
        eng.run(docs(*[{"v": "x"} for _ in range(20)]), partial=True)
    assert rep.status == "failed" and rep.errors == 5 and rep.interrupted


# --- command line and Lambda ---------------------------------------------------
@pytest.fixture
def fake_wr(monkeypatch):
    calls = {"to_iceberg": [], "fail": set()}

    def to_iceberg(**k):
        if k["table"] in calls["fail"]:
            raise RuntimeError("simulated failure in Athena")
        calls["to_iceberg"].append((k["table"], k["df"].to_dict("records")))

    wr = types.SimpleNamespace(
        catalog=types.SimpleNamespace(create_database=lambda *a, **k: None, does_table_exist=lambda **k: False),
        athena=types.SimpleNamespace(to_iceberg=to_iceberg, start_query_execution=lambda *a, **k: None))
    monkeypatch.setitem(sys.modules, "awswrangler", wr)
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=lambda **k: object()))
    return calls


CLI_CFG = """
target: {database: db, temp_path: "s3://x/tmp", location: "s3://x/wh"}
submodels:
  - name: s
    tables:
      - name: t1
        columns: {id: id, co2: "co2 ::double"}
      - name: t2
        columns: {id: id}
"""


def _cli(tmp_path, data, *extra):
    from cx2iceberg.cli import main

    (tmp_path / "c.yaml").write_text(CLI_CFG)
    (tmp_path / "d.json").write_text(json.dumps(data))
    return main(["run", "-c", str(tmp_path / "c.yaml"), str(tmp_path / "d.json"), *extra])


def test_cli_partial_load_writes_valid_documents_and_exits_2(tmp_path, fake_wr, capsys):
    report = tmp_path / "r.json"
    with pytest.raises(SystemExit) as e:
        _cli(tmp_path, [{"id": "a", "co2": 1.5}, {"id": "b", "co2": "1,5"}], "--report", str(report))
    assert e.value.code == 2
    assert dict(fake_wr["to_iceberg"]) == {"t1": [{"id": "a", "co2": 1.5}], "t2": [{"id": "a"}]}   # nothing from 'b'
    err = capsys.readouterr().err
    assert "status=partial" in err and "d.json#1" in err and "value '1,5'" in err
    saved = json.loads(report.read_text())
    assert saved["status"] == "partial" and saved["rejected_documents"][0]["document"] == 1


def test_cli_ok_and_batch_failure(tmp_path, fake_wr, capsys):
    assert _cli(tmp_path, [{"id": "a", "co2": 1}]) is None                       # code 0
    assert "status=ok" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:
        _cli(tmp_path, [{"id": "a", "co2": "x"}, {"id": "b", "co2": "y"}], "--max-errors", "1")
    assert "stopped" in str(e.value.code) and len(fake_wr["to_iceberg"]) == 2  # t1 and t2 from the previous load, only


def test_cli_report_says_what_was_written_and_what_failed(tmp_path, fake_wr, capsys):
    # the report used to be saved before the write and said status ok / failure null with t1 already written
    fake_wr["fail"].add("t2")
    report = tmp_path / "r.json"
    with pytest.raises(SystemExit) as e:
        _cli(tmp_path, [{"id": "a", "co2": 1}], "--report", str(report))
    assert str(e.value.code).startswith("Write error: simulated failure")   # code 1, no traceback
    saved = json.loads(report.read_text())
    assert saved["status"] == "failed" and "table 't2'" in saved["failure"]
    assert {n: t["status"] for n, t in saved["write"]["tables"].items()} == {"t1": "written", "t2": "failed"}
    assert saved["write"]["tables"]["t2"]["error"] == "simulated failure in Athena"
    err = capsys.readouterr().err
    assert "t1: written (1 row(s))" in err and "t2: FAILED — simulated failure in Athena" in err


def test_lambda_is_all_or_nothing_and_returns_the_report(tmp_path, fake_wr, monkeypatch):
    import cx2iceberg.lambda_handler as L

    (tmp_path / "c.yaml").write_text(CLI_CFG)
    monkeypatch.setenv("CX2ICEBERG_CONFIG", str(tmp_path / "c.yaml"))
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "d.json"}}}]}
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: docs({"id": "a", "co2": 1}, {"id": "b", "co2": "x"}))
    with pytest.raises(DocumentsRejected):
        L.handler(event, None)
    assert fake_wr["to_iceberg"] == []                                           # nothing written
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: docs({"id": "a", "co2": 1}))
    out = L.handler(event, None)
    assert out["status"] == "ok" and out["written"] == {"t1": 1, "t2": 1} and out["report"]["documents"] == 1


def test_studio_preview_returns_the_report(tmp_path):
    from cx2iceberg.studio.server import Studio

    cfg = {"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {"id": "id", "n": "n ::int"}}]}]}
    r = Studio(tmp_path).preview({"config": cfg, "samples": [], "pasted": '[{"id": "a", "n": 1}, {"id": "b", "n": 1.5}]'})
    # the rejected document no longer hides the rest: the preview shows the other one's rows and the report
    assert r["ok"] is True and r["summary"]["rows"] == 1
    assert r["report"]["status"] == "partial" and r["report"]["rejected_documents"][0]["document"] == 1
    assert "is not an integer" in next(g for g in r["report"]["groups"] if g["kind"] == "error")["examples"][0]["message"]


# --- the report must always come out: strict JSON, even with keys of any type ------
def _strict(text):
    """json.loads that refuses NaN/Infinity (like the strict parsers that receive the Lambda response)."""
    def refuse(token):
        raise ValueError(f"non-JSON token: {token}")
    return json.loads(text, parse_constant=refuse)


def test_json_safe_covers_every_value_type():
    from datetime import date, datetime, time
    from decimal import Decimal

    from cx2iceberg.report import json_safe

    raw = {"ts": datetime(2024, 5, 1, 10), "d": date(2024, 5, 1), "t": time(1, 2), "dec": Decimal("1.50"),
           "nan": float("nan"), "inf": float("inf"), "ninf": float("-inf"), "arr": [Decimal("1"), None],
           "map": {datetime(2024, 1, 1): 1, 2: "x"}, "set": {1}, "obj": object()}
    out = json_safe(raw)
    assert _strict(json.dumps(out, allow_nan=False)) == out
    assert out["ts"] == "2024-05-01T10:00:00" and out["dec"] == "1.50" and out["nan"] == "NaN"
    assert out["ninf"] == "-Infinity" and out["map"] == {"2024-01-01T00:00:00": 1, "2": "x"}


def test_report_is_strict_json_with_timestamp_date_decimal_and_struct_keys(tmp_path):
    from pathlib import Path

    tables = [{"name": "t", "explode": "items", "mode": "merge", "keys": ["ts", "d", "dec", "arr", "st"],
               "columns": {"ts": "ts ::timestamp", "d": "d ::date", "dec": "dec ::decimal(5,2)", "arr": "arr ::array<int>",
                           "st": {"path": "st", "type": "struct<at:timestamp>"}, "v": "v ::double"}}]
    item = {"ts": "2024-05-01T10:00:00Z", "d": "2024-05-01", "dec": 1.5, "arr": [1], "st": {"at": "2024-05-01T10:00:00Z"}}
    details = tmp_path / "d.jsonl"
    rep = Report(details_path=str(details))
    eng = Engine(from_dict({"submodels": [{"name": "s", "tables": tables}]}), report=rep)
    # a repeated key between two documents of the batch (a warning, with the keys in the event)
    eng.run([Document({"items": [{**item, "v": 1}]}, Path("in") / "f.json", "urn:x", index=4),
             Document({"items": [{**item, "v": 2}]}, Path("in") / "f.json", "urn:x", index=5)])
    rep.close()
    out = _strict(json.dumps(rep.to_dict(), allow_nan=False))      # before: a TypeError (datetime) after the write
    dup = next(e for g in out["groups"] if g["reason"] == "duplicate_key" for e in g["examples"])
    assert dup["keys"] == {"ts": "2024-05-01T10:00:00", "d": "2024-05-01", "dec": "1.50", "arr": [1],
                           "st": {"at": "2024-05-01T10:00:00"}}
    assert dup["file"] == str(Path("in") / "f.json") and dup["document"] == 4
    assert "f.json#4" in rep.render()                              # before: Path + str -> TypeError
    assert all(_strict(line) for line in details.read_text().splitlines())


def test_lambda_response_is_built_before_the_write_and_is_strict_json(tmp_path, fake_wr, monkeypatch):
    import cx2iceberg.lambda_handler as L
    from cx2iceberg.report import Report as R

    (tmp_path / "c.yaml").write_text("""
target: {database: db, temp_path: "s3://x/tmp", location: "s3://x/wh"}
submodels:
  - name: s
    tables:
      - {name: readings, explode: items, columns: {part: "$root.id", co2: "co2 ::double"}}
      - {name: snapshots, explode: items, mode: merge, keys: [taken_at], columns: {taken_at: "at ::timestamp"}}
""")
    monkeypatch.setenv("CX2ICEBERG_CONFIG", str(tmp_path / "c.yaml"))
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "p.json"}}}]}
    at = "2024-05-01T10:00:00Z"
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: docs(
        {"id": "P", "items": [{"at": at, "co2": 1.5}]}, {"id": "Q", "items": [{"at": at, "co2": 2.0}]}))
    out = L.handler(event, None)     # a timestamp repeated between documents -> a warning with datetime in the keys
    assert _strict(json.dumps(out, allow_nan=False)) == out
    assert out["report"]["groups"][0]["reason"] == "duplicate_key" and out["report"]["groups"][0]["examples"] == []
    assert [t for t, _ in fake_wr["to_iceberg"]] == ["readings", "snapshots"]

    # if the report cannot be serialized, the invocation fails BEFORE writing (a retry does not duplicate)
    fake_wr["to_iceberg"].clear()
    monkeypatch.setattr(R, "to_dict", lambda self, **k: {"bad": object()})
    with pytest.raises(TypeError):
        L.handler(event, None)
    assert fake_wr["to_iceberg"] == []


# --- memory: the report must not cost memory per row ---------------------------
def _bom(n_docs, n_items):
    return docs(*[{"id": f"P{d}", "items": [{"c": f"C{d}-{i}", "kg": "x" if i % 10 == 0 else i} for i in range(n_items)]}
                  for d in range(n_docs)])


BOM_TABLE = [{"name": "t", "explode": "items", "mode": "merge", "keys": ["p", "c"],
              "columns": {"p": "$root.id", "c": "c", "kg": {"path": "kg", "type": "double", "on_error": "null"}}}]


def test_row_origins_cost_at_most_18_bytes_per_row():
    import gc
    import tracemalloc

    from cx2iceberg.engine import RowOrigins

    tracemalloc.start()
    res = engine(BOM_TABLE).run(_bom(200, 100))["t"]
    n = len(res.rows)
    gc.collect()                                           # only the origins' memory counts, not the load's garbage
    held = tracemalloc.get_traced_memory()[0]
    assert res.origins.nbytes() == n * (8 + 2 + 8)        # document + explode path + 1 level
    res.origins = RowOrigins()
    gc.collect()
    freed = held - tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    assert n == 20000 and freed / n < 24                   # before: dict + text per row, ~240 bytes


def test_report_memory_does_not_grow_with_events():
    import tracemalloc

    rep = Report()
    for i in range(1000):                                  # warm up: groups and examples already created
        rep.add("declared", "invalid_value", "nulled", "m", table="t", column="c", file="f", document=i, value="x" * 500)
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    for i in range(100_000):
        rep.add("declared", "invalid_value", "nulled", "m", table="t", column="c", file="f", document=i, value="x" * 500)
    grown = tracemalloc.get_traced_memory()[0] - before
    tracemalloc.stop()
    assert rep.count() == 101_000 and grown < 4096


def test_only_sampled_events_are_logged(caplog):
    rep = Report(samples=5)
    engine(BOM_TABLE, report=rep).run(_bom(5, 100))        # 50 values set to null
    assert rep.count(action="nulled") == 50
    assert sum("Value set to null" in m for m in caplog.messages) == 5


def test_row_positions_survive_rejection_dedupe_and_rules_with_different_explodes():
    cfg = from_dict({"submodels": [
        {"name": "a", "match": {"semantic_id": "urn:a"}, "tables": [
            {"name": "t", "explode": "items", "mode": "merge", "keys": ["k"], "columns": {"k": "k", "v": "v ::int"}}]},
        {"name": "b", "match": {"semantic_id": "urn:b"}, "tables": [
            {"name": "t", "explode": "orders[*].lines", "mode": "merge", "keys": ["k"], "columns": {"k": "k", "v": "v ::int"}}]},
    ]})
    eng = Engine(cfg)
    res = eng.run([Document({"items": [{"k": "x", "v": 1}, {"k": "bad", "v": "?"}]}, "a.json", "urn:a"),
                   Document({"items": [{"k": "y", "v": 1}]}, "a2.json", "urn:a"),
                   Document({"orders": [{"lines": [{"k": "z", "v": 2}]}, {"lines": [{"k": "y", "v": 3}]}]}, "b.json", "urn:b")],
                  partial=True)["t"]
    assert sorted((r["k"], r["v"]) for r in res.rows) == [("y", 3), ("z", 2)]   # a.json rejected; y: b.json is kept
    dup = next(e for e in eng.report.events("warning") if e["reason"] == "duplicate_key")
    assert (dup["file"], dup["row"]) == ("a2.json", "items[0]") and "b.json orders[1].lines[0]" in dup["message"]


# --- the write in the report -------------------------------------------------------------
def test_report_records_a_successful_write_and_a_dry_run(tmp_path, fake_wr):
    report = tmp_path / "r.json"
    _cli(tmp_path, [{"id": "a", "co2": 1}], "--report", str(report))
    saved = json.loads(report.read_text())
    assert saved["status"] == "ok" and saved["write"]["target"] == "athena"
    assert {n: (t["status"], t["rows"]) for n, t in saved["write"]["tables"].items()} == {"t1": ("written", 1), "t2": ("written", 1)}
    _cli(tmp_path, [{"id": "a", "co2": 1}], "--report", str(report), "--dry-run")
    assert json.loads(report.read_text())["write"] is None


def test_write_failure_after_replace_by_delete_is_reported(tmp_path, monkeypatch):
    import types as _t

    from cx2iceberg.writer import AthenaIcebergWriter

    sql = []
    wr = _t.SimpleNamespace(
        catalog=_t.SimpleNamespace(create_database=lambda *a, **k: None, does_table_exist=lambda **k: True,
                                  get_table_types=lambda **k: {}),   # the writer reads the catalog
        athena=_t.SimpleNamespace(to_iceberg=lambda **k: (_ for _ in ()).throw(RuntimeError("Parquet: decimal out of range")),
                                  start_query_execution=lambda q, **k: sql.append(q)))
    monkeypatch.setitem(sys.modules, "awswrangler", wr)
    monkeypatch.setitem(sys.modules, "boto3", _t.SimpleNamespace(Session=lambda **k: object()))
    rep = Report()
    res = engine([{"name": "child", "explode": "items", "replace_by": ["pid"], "columns": {"pid": "$root.id", "x": "x"}}],
                 report=rep).run(docs({"id": "P1", "items": [{"x": 1}]}))
    with pytest.raises(RuntimeError):
        AthenaIcebergWriter({"database": "db", "temp_path": "s3://x/t"}, report=rep).write(res)
    t = rep.to_dict()["write"]["tables"]["child"]
    assert sql and t["status"] == "failed" and t["deleted_before_failure"] is True
    assert "the old rows of these ids were deleted" in t["error"] and rep.status == "failed"


def test_interrupted_write_is_not_reported_as_ok():
    rep = Report()
    rep.start_write("athena", {"t1": 1, "t2": 1})
    rep.table_written("t1")
    rep.finish_write()                                     # e.g. KeyboardInterrupt / timeout halfway through t2
    assert rep.status == "failed" and rep.write["tables"]["t2"]["status"] == "interrupted"
    assert "t2: interrupted" in rep.render()


def test_lambda_logs_what_was_written_when_the_write_fails(tmp_path, fake_wr, monkeypatch, caplog):
    import cx2iceberg.lambda_handler as L

    (tmp_path / "c.yaml").write_text(CLI_CFG)
    monkeypatch.setenv("CX2ICEBERG_CONFIG", str(tmp_path / "c.yaml"))
    monkeypatch.setattr(L, "read_documents", lambda sources, opts: docs({"id": "a", "co2": 1}))
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "d.json"}}}]}
    out = L.handler(event, None)
    assert out["report"]["write"]["tables"]["t2"]["status"] == "written" and _strict(json.dumps(out, allow_nan=False))
    fake_wr["fail"].add("t2")
    with pytest.raises(RuntimeError):
        L.handler(event, None)
    assert "t1: written" in caplog.text and "t2: FAILED — simulated failure in Athena" in caplog.text
