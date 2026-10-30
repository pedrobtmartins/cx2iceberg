import json
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
    """Pasta com o PCF 9.0.0 como se tivesse vindo do catálogo (sem rede)."""
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
    assert [c["path"] for c in f["configs"]] == ["cfg/a.yaml"]          # só configurações, sem pastas escondidas
    assert [d["path"] for d in f["samples"]] == ["data/d.json"]          # sem *-schema.json
    cfg = s.load("cfg/a.yaml")["config"]
    assert cfg["submodels"][0]["tables"][0]["columns"]["n"] == {"path": "n", "type": "int"}   # forma compacta expandida
    with pytest.raises(ApiError):
        s.load("../outside.yaml")


def test_render_roundtrip():
    cfg = normalize(yaml.safe_load(CFG))
    text = render(cfg)
    again = normalize(yaml.safe_load(text))
    assert again == cfg
    assert "id: id\n" in text                                            # coluna só com caminho fica compacta


def test_preview_with_config_dict(root):
    s = Studio(root)
    cfg = normalize(yaml.safe_load(CFG))
    r = s.preview({"config": cfg, "path": "cfg/a.yaml", "samples": ["data/d.json"]})
    assert r["ok"] and r["summary"]["rows"] == 2
    assert [c["name"] for c in r["tables"][0]["columns"]] == ["document_id", "id", "n"]
    cfg["submodels"][0]["tables"][0]["mode"] = "nope"
    bad = s.preview({"config": cfg, "samples": []})
    assert bad["stage"] == "config" and "nope" in bad["error"]
    run_err = s.preview({"config": normalize(yaml.safe_load(CFG)), "samples": [], "pasted": '{"id": "Z", "n": 1.5}'})
    assert run_err["stage"] == "run" and "não é inteiro" in run_err["error"]


def test_save_conflicts(root):
    s = Studio(root)
    f = s.load("cfg/a.yaml")
    r = s.save("cfg/a.yaml", CFG + "# x\n", f["mtime"])
    with pytest.raises(ApiError, match="alterado fora"):
        s.save("cfg/a.yaml", CFG, f["mtime"])                          # mtime antigo
    with pytest.raises(ApiError, match="Já existe"):
        s.save("cfg/a.yaml", CFG, None)                                # ficheiro novo com nome ocupado
    s.save("cfg/a.yaml", CFG, r["mtime"])
    with pytest.raises(ApiError, match=".yaml"):
        s.save("data/d.json", "{}", None)


def test_new_config_from_model_and_validation(pcf_root):
    cfg = new_config(PCF_INFO, pcf_root, "configs/pcf_v9.yaml")
    sub = cfg["submodels"][0]
    assert sub["model"]["schema"] == "../models/io.catenax.pcf/9.0.0/Pcf-schema.json"
    assert sub["validate"] == "warn" and sub["match"] == {"semantic_id": "urn:samm:io.catenax.pcf:9.*"}
    assert sub["document_id"]["from"] == ["pcfAssessmentAndMethodology.pcfAssessmentInformation.idAndVersion.id"]
    assert "attestationOfConformance" in sub["unwrap"]["paths"]       # lista com 1 elemento no exemplo
    s = Studio(pcf_root)
    ok = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [PCF_INFO["example"]]})
    assert ok["ok"] and ok["validation"] == [] and ok["summary"]["rows"] == 1   # o exemplo usa o URN do schema
    doc = json.loads((pcf_root / PCF_INFO["example"]).read_text())
    doc["carbonContent"][0]["fossilCarbonContent"] = "muito"
    del doc["general"]
    bad = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [], "pasted": json.dumps(doc)})
    errs = {e["path"]: e["message"] for e in bad["validation"][0]["errors"]}
    assert errs[""] == "falta o campo obrigatório 'general'"
    assert errs["carbonContent[0].fossilCarbonContent"] == '"muito" devia ser número'
    sub["validate"] = "skip"
    skipped = s.preview({"config": cfg, "path": "configs/pcf_v9.yaml", "samples": [], "pasted": json.dumps(doc)})
    assert skipped["summary"]["rows"] == 0


def test_validate_fail_and_config_errors(pcf_root):
    (pcf_root / "configs").mkdir()
    cfg = new_config(PCF_INFO, pcf_root, "configs/pcf_v9.yaml")
    cfg["submodels"][0]["validate"] = "fail"
    p = pcf_root / "configs" / "pcf_v9.yaml"
    p.write_text(render(cfg))
    loaded = load(p)                                                   # caminhos relativos à configuração
    from cx2iceberg.engine import TransformError
    with pytest.raises(TransformError, match="não cumpre o schema"):
        Engine(loaded).run([Document({"x": 1}, "x.json", "urn:samm:io.catenax.pcf:9.0.0#Pcf")])
    from cx2iceberg import ConfigError, from_dict
    with pytest.raises(ConfigError, match="precisa de model.schema"):
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
    cfg["submodels"][0]["unwrap"]["paths"].remove("attestationOfConformance")   # passa a lista com vários
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
    with pytest.raises(ApiError, match="desconhecido"):
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
        assert (root / "cfg" / "new.yaml").read_text().startswith("# Gerado pelo cx2iceberg studio")
    finally:
        httpd.shutdown()
