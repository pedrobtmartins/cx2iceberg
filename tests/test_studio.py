import json
import os
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import yaml

from cx2iceberg.config import load
from cx2iceberg.engine import Document, Engine
from cx2iceberg.samm import field_tree, label
from cx2iceberg.studio.server import ApiError, Studio, make_handler, new_config, normalize, render

EX = Path(__file__).parent.parent / "examples"

CFG = """target: {database: db}
submodels:
  - name: s
    document_id: {from: id}
    tables:
      - name: t
        mode: merge
        keys: [id]
        columns:
          id: id
          n: n ::int
"""


@pytest.fixture
def root(tmp_path):
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "a.yaml").write_text(CFG)
    (tmp_path / "cfg" / "other.yaml").write_text("just: yaml\n")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "d.json").write_text(json.dumps([{"id": "A", "n": 1}, {"id": "B", "n": 2}]))
    (tmp_path / "data" / "x-schema.json").write_text("{}")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "hidden.yaml").write_text("submodels: []\n")
    return tmp_path


@pytest.fixture
def pcf_root(tmp_path):
    """Folder with PCF 9.0.0 as if it had come from the catalog (no network)."""
    d = tmp_path / "models" / "io.catenax.pcf" / "9.0.0"
    d.mkdir(parents=True)
    (d / "Pcf-schema.json").write_text((EX / "schemas" / "pcf_9.0.0-schema.json").read_text())
    (d / "Pcf.json").write_text((EX / "data" / "pcf_9.json").read_text())
    return tmp_path


PCF_INFO = {"model": "io.catenax.pcf", "version": "9.0.0", "aspect": "Pcf",
            "schema": "models/io.catenax.pcf/9.0.0/Pcf-schema.json", "example": "models/io.catenax.pcf/9.0.0/Pcf.json"}


def test_files_load_and_safety(root):
    s = Studio(root)
    f = s.files()
    assert [c["path"] for c in f["configs"]] == ["cfg/a.yaml"]          # configurations only, no hidden folders
    assert [d["path"] for d in f["samples"]] == ["data/d.json"]          # no *-schema.json
    cfg = s.load("cfg/a.yaml")["config"]
    assert cfg["submodels"][0]["tables"][0]["columns"]["n"] == {"path": "n", "type": "int"}   # compact form expanded
    with pytest.raises(ApiError):
        s.load("../outside.yaml")


def test_render_roundtrip():
    cfg = normalize(yaml.safe_load(CFG))
    text = render(cfg)
    again = normalize(yaml.safe_load(text))
    assert again == cfg
    assert "id: id\n" in text                                            # a column with only a path stays compact


def test_preview_with_config_dict(root):
    s = Studio(root)
    cfg = normalize(yaml.safe_load(CFG))
    r = s.preview({"config": cfg, "path": "cfg/a.yaml", "samples": ["data/d.json"]})
    assert r["ok"] and r["summary"]["rows"] == 2
    assert [c["name"] for c in r["tables"][0]["columns"]] == ["document_id", "id", "n"]
    cfg["submodels"][0]["tables"][0]["mode"] = "nope"
    bad = s.preview({"config": cfg, "samples": []})
    assert bad["stage"] == "config" and "nope" in bad["error"]
    # a rejected document no longer turns the preview into an error: it appears in the report, with the reason,
    # and the other documents' rows are still visible (before: only the error message)
    pasted = '[{"id": "Z", "n": 1.5}, {"id": "Y", "n": 2}]'
    part = s.preview({"config": normalize(yaml.safe_load(CFG)), "samples": [], "pasted": pasted})
    assert part["ok"] and part["report"]["status"] == "partial" and part["summary"]["rows"] == 1
    err = next(g for g in part["report"]["groups"] if g["kind"] == "error")
    assert err["column"] == "n" and err["reason_label"] == "invalid value" and "is not an integer" in err["examples"][0]["message"]
    assert part["report"]["rejected_documents"][0]["file"] == "(pasted)"


def test_preview_shows_values_as_they_are_written(root):
    # the browser (JavaScript numbers) showed 9007199254740993 as ...992 and a float 1.1 as 1.100000023841858
    cfg = {"submodels": [{"name": "s", "tables": [{"name": "t", "columns": {
        "big": "big ::bigint", "f": "f ::float", "d": "d ::decimal(5,2)", "xs": "xs ::array<bigint>",
        "fs": "fs ::array<float>"}}]}]}
    r = Studio(root).preview({"config": cfg, "samples": [], "pasted": '{"big": 9007199254740993, "f": 1.1, "d": 1.5, '
                                                                      '"xs": [9007199254740993, 1], "fs": [0.1, 2.5]}'})
    row = r["tables"][0]["rows"][0]
    assert row == {"big": "9007199254740993", "f": 1.1, "d": "1.50", "xs": ["9007199254740993", 1], "fs": [0.1, 2.5]}
    assert json.loads(json.dumps(row))["f"] == 1.1


def test_preview_runs_the_write_precheck(root):
    cfg = {"submodels": [{"name": "s", "tables": [{"name": "t", "partition_by": ["contry"], "columns": {"country": "c"}}]}]}
    r = Studio(root).preview({"config": cfg, "samples": [], "pasted": '{"c": "PT"}'})
    assert r["ok"] and "partition_by uses ['contry']" in r["write_check"][0]


def test_save_conflicts(root):
    s = Studio(root)
    f = s.load("cfg/a.yaml")
    r = s.save("cfg/a.yaml", CFG + "# x\n", f["version"])
    with pytest.raises(ApiError, match="changed outside"):
        s.save("cfg/a.yaml", CFG, f["version"])                        # old version
    with pytest.raises(ApiError, match="already exists"):
        s.save("cfg/a.yaml", CFG, None)                                # a new file with a name already taken
    s.save("cfg/a.yaml", CFG, r["version"])
    with pytest.raises(ApiError, match=".yaml"):
        s.save("data/d.json", "{}", None)


def test_external_edit_with_the_same_mtime_is_a_conflict(root):
    # before, the version was the mtime: with the same timestamp (FS with 1-2 s resolution, Docker/WSL, cp -p) an
    # edit made outside was overwritten by the studio without a warning; the previous test depended on the clock's resolution
    s = Studio(root)
    f = s.load("cfg/a.yaml")
    p = root / "cfg" / "a.yaml"
    st = p.stat()
    p.write_text(CFG + "# edited outside\n")
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns))
    with pytest.raises(ApiError, match="changed outside"):
        s.save("cfg/a.yaml", CFG + "# from the studio\n", f["version"])
    assert p.read_text().endswith("# edited outside\n")


def test_new_config_from_model_and_validation(pcf_root):
    cfg = new_config(PCF_INFO, pcf_root, "configs/pcf_v9.yaml")
    sub = cfg["submodels"][0]
    assert sub["model"]["schema"] == "../models/io.catenax.pcf/9.0.0/Pcf-schema.json"
    assert sub["validate"] == "warn" and sub["match"] == {"semantic_id": "urn:samm:io.catenax.pcf:9.*"}
    assert sub["document_id"]["from"] == ["pcfAssessmentAndMethodology.pcfAssessmentInformation.idAndVersion.id"]
    assert "attestationOfConformance" in sub["unwrap"]["paths"]       # a list with 1 element in the example
    s = Studio(pcf_root)
    ok = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [PCF_INFO["example"]]})
    assert ok["ok"] and ok["validation"] == [] and ok["summary"]["rows"] == 1   # the example uses the schema's URN
    doc = json.loads((pcf_root / PCF_INFO["example"]).read_text())
    doc["carbonContent"][0]["fossilCarbonContent"] = "lots"
    del doc["general"]
    bad = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [], "pasted": json.dumps(doc)})
    errs = {e["path"]: e["message"] for e in bad["validation"][0]["errors"]}
    assert errs[""] == "required field 'general' is missing"
    assert errs["carbonContent[0].fossilCarbonContent"] == '"lots" should be number'
    sub["validate"] = "skip"
    skipped = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [], "pasted": json.dumps(doc)})
    assert skipped["summary"]["rows"] == 0


def test_validate_fail_and_config_errors(pcf_root):
    (pcf_root / "configs").mkdir()
    cfg = new_config(PCF_INFO, pcf_root, "configs/pcf_v9.yaml")
    cfg["submodels"][0]["validate"] = "fail"
    p = pcf_root / "configs" / "pcf_v9.yaml"
    p.write_text(render(cfg))
    loaded = load(p)                                                   # paths relative to the configuration
    from cx2iceberg.engine import TransformError
    with pytest.raises(TransformError, match="does not match the schema"):
        Engine(loaded).run([Document({"x": 1}, "x.json", "urn:samm:io.catenax.pcf:9.0.0#Pcf")])
    from cx2iceberg import ConfigError, from_dict
    with pytest.raises(ConfigError, match="needs model.schema"):
        from_dict({"submodels": [{"validate": "warn", "tables": [{"name": "t", "auto": True}]}]})


def test_fields_tree(pcf_root):
    s = Studio(pcf_root)
    cfg = new_config(PCF_INFO, pcf_root, "configs/pcf_v9.yaml")
    tree = s.fields(cfg, 0, "configs/pcf_v9.yaml")["tree"]
    flat = {}

    def walk(ns):
        for n in ns:
            flat[n["path"]] = n
            walk(n.get("children") or [])

    walk(tree)
    fossil = flat["productLifeCycleStagesAndEmissions.productionStage.fossilGhgEmissions"]
    assert fossil["label"] == "Fossil GHG emissions" and fossil["type"] == "double" and fossil["example"] == 0.5
    assert flat["companyAndProductInformation.companyInformation.companyIds"]["kind"] == "values"
    assert flat["attestationOfConformance"]["single"] is True
    cfg["submodels"][0]["unwrap"]["paths"].remove("attestationOfConformance")   # it becomes a list with several
    tree2 = s.fields(cfg, 0, "configs/pcf_v9.yaml")["tree"]
    att = next(n for n in tree2 if n["key"] == "attestationOfConformance")
    assert att["single"] is False and att["children"][0]["path"].startswith("attestationOfConformance[*].")
    assert label("companyIds") == "Company IDs" and label("pcfExcludingBiogenicUptake") == "PCF excluding biogenic uptake"


def test_field_tree_marks_fields_missing_from_example():
    schema = json.loads((EX / "schemas" / "pcf_9.0.0-schema.json").read_text())
    tree = field_tree(schema, None, {"scopeOfPcfForm": [{"specVersion": "x"}]})
    scope = next(n for n in tree if n["key"] == "scopeOfPcfForm")
    kids = {c["key"]: c for c in scope["children"]}
    assert kids["specVersion"]["in_example"] and not kids["partialFullPcf"]["in_example"]


def test_catalog_with_local_clone(tmp_path):
    clone = tmp_path / "clone"
    v = clone / "io.catenax.pcf" / "9.0.0"
    (v / "gen").mkdir(parents=True)
    (v / "gen" / "Pcf-schema.json").write_text((EX / "schemas" / "pcf_9.0.0-schema.json").read_text())
    (v / "gen" / "Pcf.json").write_text((EX / "data" / "pcf_9.json").read_text())
    (v / "metadata.json").write_text('{"status": "release"}')
    s = Studio(tmp_path / "work", models_dir=clone)
    (tmp_path / "work").mkdir()
    models = s.catalog_list(False)["models"]
    assert models[0]["id"] == "io.catenax.pcf" and models[0]["versions"][0]["version"] == "9.0.0"
    assert s.catalog_status("io.catenax.pcf", ["9.0.0"])["status"] == {"9.0.0": "release"}
    n = s.new("io.catenax.pcf", "9.0.0", None)
    assert n["path"] == "configs/pcf_v9.yaml" and (tmp_path / "work" / "models/io.catenax.pcf/9.0.0/Pcf.json").exists()
    with pytest.raises(ApiError, match="Unknown submodel"):
        s.new("io.catenax.pcf", "../../etc", None)


def test_http_guard(root):
    from http.server import ThreadingHTTPServer

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(Studio(root)))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        assert b"cx2iceberg studio" in urllib.request.urlopen(base + "/").read()
        files = json.load(urllib.request.urlopen(base + "/api/files"))
        assert files["configs"][0]["path"] == "cfg/a.yaml"
        req = urllib.request.Request(base + "/api/files", headers={"Origin": "https://evil.example"})
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(req)
        assert e.value.code == 403
        put = urllib.request.Request(base + "/api/file", method="PUT", headers={"Content-Type": "application/json"},
                                     data=json.dumps({"path": "cfg/new.yaml", "config": normalize(yaml.safe_load(CFG))}).encode())
        assert json.load(urllib.request.urlopen(put))["path"] == "cfg/new.yaml"
        assert (root / "cfg" / "new.yaml").read_text().startswith("# Generated by cx2iceberg studio")
        # what the interface does: open (version), save with that version, and an old version gives 409
        def call(method, route, body):
            r = urllib.request.Request(base + route, method=method, headers={"Content-Type": "application/json"},
                                       data=json.dumps(body).encode())
            return json.load(urllib.request.urlopen(r))
        opened = call("POST", "/api/load", {"path": "cfg/new.yaml"})
        saved = call("PUT", "/api/file", {"path": "cfg/new.yaml", "config": opened["config"], "version": opened["version"]})
        assert saved["version"] and "mtime" not in saved
        with pytest.raises(urllib.error.HTTPError) as e:
            call("PUT", "/api/file", {"path": "cfg/new.yaml", "text": CFG + "# old\n", "version": "0" * 64})
        assert e.value.code == 409
    finally:
        httpd.shutdown()
