import json
import threading
import urllib.error
import urllib.request

import pytest

from cx2iceberg.studio.server import ApiError, Studio, make_handler

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


def test_files_and_safety(root):
    s = Studio(root)
    f = s.files()
    assert [c["path"] for c in f["configs"]] == ["cfg/a.yaml"]          # só configurações, sem pastas escondidas
    assert [d["path"] for d in f["samples"]] == ["data/d.json"]          # sem *-schema.json
    with pytest.raises(ApiError):
        s.read("../outside.yaml")


def test_preview_ok_and_errors(root):
    s = Studio(root)
    r = s.preview({"text": CFG, "samples": ["data/d.json"]})
    assert r["ok"] and r["summary"]["rows"] == 2
    t = r["tables"][0]
    assert [c["name"] for c in t["columns"]] == ["document_id", "id", "n"] and t["rows"][0]["n"] == 1
    assert r["documents"][0]["source"] == "data/d.json" and r["documents"][0]["submodels"] == ["s"]
    bad_yaml = s.preview({"text": CFG + "  oops: [", "samples": []})
    assert not bad_yaml["ok"] and bad_yaml["stage"] == "yaml" and bad_yaml["line"]
    bad_cfg = s.preview({"text": CFG.replace("mode: merge", "mode: nope"), "samples": []})
    assert bad_cfg["stage"] == "config" and CFG.splitlines()[bad_cfg["line"] - 1].strip() == "mode: merge"
    bad_run = s.preview({"text": CFG, "samples": [], "pasted": '{"id": "Z", "n": 1.5}'})
    assert bad_run["stage"] == "run" and "não é inteiro" in bad_run["error"]
    pasted = s.preview({"text": CFG, "samples": [], "pasted": '[{"id": "P", "n": 7}]'})
    assert pasted["tables"][0]["rows"][0]["id"] == "P"
    env = s.preview({"text": "target:\n  database: ${NAO_DEFINIDA}\n" + CFG.split("\n", 1)[1], "samples": []})
    assert env["ok"]                                                    # variável em falta não bloqueia


def test_save_conflict_and_type(root):
    s = Studio(root)
    f = s.read("cfg/a.yaml")
    r = s.save("cfg/a.yaml", CFG + "# x\n", f["mtime"])
    assert (root / "cfg" / "a.yaml").read_text().endswith("# x\n")
    with pytest.raises(ApiError, match="alterado fora"):
        s.save("cfg/a.yaml", CFG, f["mtime"])                          # mtime antigo
    s.save("cfg/a.yaml", CFG, r["mtime"])
    with pytest.raises(ApiError, match=".yaml"):
        s.save("data/d.json", "{}", None)
    s.save("novo/b.yaml", CFG, None)
    assert (root / "novo" / "b.yaml").exists()


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
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(base + "/../../etc/passwd")
        assert e.value.code in (403, 404)
    finally:
        httpd.shutdown()
