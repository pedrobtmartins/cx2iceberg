"use strict";

const $ = (s) => document.querySelector(s);
const LH = 20;          // altura de linha do editor (px), igual a --lh-code
const PAD = 12;         // padding superior do editor
const store = {
  get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* sem armazenamento */ } },
};

const state = {
  files: { configs: [], samples: [] },
  current: null,          // {path, original, mtime}
  samples: new Set(store.get("cx2.samples", null) || []),
  samplesKnown: store.get("cx2.samples", null) !== null,
  result: null,           // último resultado válido
  table: store.get("cx2.table", null),
  view: "tables",
  doc: 0,
  inflight: null,
  timer: 0,
};

const el = {
  yaml: $("#yaml"), gutter: $("#gutter"), errorBand: $("#error-band"), jumpBand: $("#jump-band"),
  status: $("#status"), statusText: $("#status-text"), statusLine: $("#status-line"),
  save: $("#save"), revert: $("#revert"), dirty: $("#dirty"), currentFile: $("#current-file"),
  configs: $("#configs"), samples: $("#samples"), pasted: $("#pasted"), semanticId: $("#semantic-id"),
  summary: $("#summary"), tableTabs: $("#table-tabs"), tableMeta: $("#table-meta"), grid: $("#grid"),
  docSelect: $("#doc-select"), docMeta: $("#doc-meta"), docTree: $("#doc-tree"),
  ddl: $("#ddl"), warnings: $("#warnings"), warnCount: $("#warn-count"), toast: $("#toast"),
  workspace: document.querySelector(".workspace"),
};

/* ------------------------------------------------------------------ API */
async function api(method, url, body, signal) {
  const res = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data.error || res.statusText), { status: res.status });
  return data;
}

function toast(msg, kind = "") {
  el.toast.textContent = msg;
  el.toast.className = `toast ${kind}`;
  el.toast.hidden = false;
  clearTimeout(toast.t);
  toast.t = setTimeout(() => { el.toast.hidden = true; }, kind === "error" ? 6000 : 3000);
}

/* ------------------------------------------------------------- ficheiros */
async function loadFiles() {
  state.files = await api("GET", "/api/files");
  if (!state.samplesKnown) {
    state.files.samples.forEach((f) => state.samples.add(f.path));
    state.samplesKnown = true;
  }
  const known = new Set(state.files.samples.map((f) => f.path));
  [...state.samples].forEach((p) => { if (!known.has(p)) state.samples.delete(p); });
  renderFiles();
}

function splitPath(p) {
  const i = p.lastIndexOf("/");
  return i < 0 ? ["", p] : [p.slice(0, i + 1), p.slice(i + 1)];
}

function renderFiles() {
  el.configs.replaceChildren(...state.files.configs.map((f) => {
    const li = document.createElement("li");
    const b = document.createElement("button");
    b.type = "button";
    const [dir, name] = splitPath(f.path);
    b.innerHTML = `<span class="dir"></span><span class="name"></span>`;
    b.querySelector(".dir").textContent = dir;
    b.querySelector(".name").textContent = name;
    b.setAttribute("aria-current", String(state.current?.path === f.path));
    b.addEventListener("click", () => openConfig(f.path));
    li.append(b);
    return li;
  }));
  if (!state.files.configs.length) {
    el.configs.innerHTML = `<li class="hint">Nenhum ficheiro .yaml com <code>submodels:</code> nesta pasta.</li>`;
  }
  el.samples.replaceChildren(...state.files.samples.map((f) => {
    const li = document.createElement("li");
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = state.samples.has(f.path);
    cb.addEventListener("change", () => {
      cb.checked ? state.samples.add(f.path) : state.samples.delete(f.path);
      store.set("cx2.samples", [...state.samples]);
      schedulePreview(0);
    });
    const span = document.createElement("span");
    span.textContent = f.path;
    label.append(cb, span);
    li.append(label);
    return li;
  }));
}

function isDirty() {
  return !!state.current && el.yaml.value !== state.current.original;
}

function updateDirty() {
  const d = isDirty();
  el.dirty.hidden = !d;
  el.save.disabled = !state.current || (!d && !state.current.isNew);
  el.revert.disabled = !d || state.current?.isNew;
  document.title = `${d ? "• " : ""}${state.current ? state.current.path + " — " : ""}cx2iceberg studio`;
}

async function openConfig(path) {
  if (isDirty() && !confirm("Há alterações por guardar. Abrir outro ficheiro e perdê-las?")) return;
  try {
    const f = await api("GET", `/api/file?path=${encodeURIComponent(path)}`);
    state.current = { path: f.path, original: f.text, mtime: f.mtime };
    store.set("cx2.last", f.path);
    setEditor(f.text);
    el.currentFile.textContent = f.path;
    renderFiles();
    showPane("editor");
    schedulePreview(0);
  } catch (e) {
    toast(e.message, "error");
  }
}

const TEMPLATE = `target:
  database: \${ATHENA_DATABASE:-catenax}
  location: \${ICEBERG_LOCATION:-s3://my-bucket/warehouse}
  temp_path: \${ATHENA_TEMP:-s3://my-bucket/tmp}

defaults:
  metadata_columns: true

submodels:
  - name: meu_submodelo
    # match: {semantic_id: "urn:samm:io.catenax.serial_part:3.*"}
    document_id: true
    tables:
      - name: minha_tabela
        auto: true
`;

function newConfig() {
  if (isDirty() && !confirm("Há alterações por guardar. Criar outra configuração e perdê-las?")) return;
  const path = prompt("Caminho do novo ficheiro (relativo à pasta do studio):", "configs/nova.yaml");
  if (!path) return;
  if (!/\.ya?ml$/.test(path)) { toast("O nome tem de terminar em .yaml ou .yml", "error"); return; }
  if (state.files.configs.some((f) => f.path === path)) { openConfig(path); return; }
  state.current = { path, original: "", mtime: null, isNew: true };
  el.currentFile.textContent = `${path} (novo)`;
  setEditor(TEMPLATE);
  renderFiles();
  showPane("editor");
  schedulePreview(0);
}

async function save() {
  if (!state.current) return;
  try {
    const r = await api("PUT", "/api/file", { path: state.current.path, text: el.yaml.value, mtime: state.current.mtime });
    const wasNew = state.current.isNew;
    state.current = { path: r.path, original: el.yaml.value, mtime: r.mtime };
    el.currentFile.textContent = r.path;
    updateDirty();
    toast(`Guardado: ${r.path}`);
    if (wasNew) loadFiles();
  } catch (e) {
    toast(e.message, "error");
  }
}

function revert() {
  if (!state.current || !isDirty()) return;
  setEditor(state.current.original);
  schedulePreview(0);
}

/* ---------------------------------------------------------------- editor */
function setEditor(text) {
  el.yaml.value = text;
  el.yaml.scrollTop = 0;
  updateGutter();
  updateDirty();
}

function updateGutter() {
  const n = el.yaml.value.split("\n").length;
  let s = "";
  for (let i = 1; i <= n; i++) s += i + "\n";
  el.gutter.textContent = s;
  syncScroll();
}

function bandY(line) {
  return PAD + (line - 1) * LH - el.yaml.scrollTop;
}

function syncScroll() {
  el.gutter.style.transform = `translateY(${-el.yaml.scrollTop}px)`;
  if (el.errorBand.dataset.line) el.errorBand.style.transform = `translateY(${bandY(+el.errorBand.dataset.line)}px)`;
  if (el.jumpBand.dataset.line) el.jumpBand.style.transform = `translateY(${bandY(+el.jumpBand.dataset.line)}px)`;
}

function markError(line) {
  if (line) {
    el.errorBand.dataset.line = line;
    el.errorBand.hidden = false;
  } else {
    delete el.errorBand.dataset.line;
    el.errorBand.hidden = true;
  }
  syncScroll();
}

function goToLine(line, flash = true) {
  const lines = el.yaml.value.split("\n");
  line = Math.max(1, Math.min(line, lines.length));
  let start = 0;
  for (let i = 0; i < line - 1; i++) start += lines[i].length + 1;
  const end = start + lines[line - 1].length;
  showPane("editor");
  el.yaml.focus({ preventScroll: true });
  el.yaml.setSelectionRange(start + (lines[line - 1].length - lines[line - 1].trimStart().length), end);
  el.yaml.scrollTop = Math.max(0, (line - 1) * LH - el.yaml.clientHeight / 3);
  if (flash) {
    el.jumpBand.dataset.line = line;
    el.jumpBand.hidden = false;
    el.jumpBand.style.opacity = "1";
    clearTimeout(goToLine.t);
    goToLine.t = setTimeout(() => { el.jumpBand.style.opacity = "0"; }, 1400);
  }
  syncScroll();
}

function insertAtCursor(text) {
  const { selectionStart: a, selectionEnd: b } = el.yaml;
  el.yaml.focus({ preventScroll: true });
  el.yaml.setRangeText(text, a, b, "end");
  onEdit();
}

function onEdit() {
  updateGutter();
  updateDirty();
  schedulePreview(350);
}

el.yaml.addEventListener("input", onEdit);
el.yaml.addEventListener("scroll", syncScroll);
el.yaml.addEventListener("keydown", (e) => {
  if (e.key !== "Tab") return;
  e.preventDefault();
  const v = el.yaml.value;
  const a = el.yaml.selectionStart, b = el.yaml.selectionEnd;
  const lineStart = v.lastIndexOf("\n", a - 1) + 1;
  if (a === b && !e.shiftKey) { el.yaml.setRangeText("  ", a, b, "end"); onEdit(); return; }
  const block = v.slice(lineStart, b);
  const changed = e.shiftKey ? block.replace(/^ {1,2}/gm, "") : block.replace(/^/gm, "  ");
  el.yaml.setRangeText(changed, lineStart, b, "select");
  onEdit();
});

/* ------------------------------------------------------ pré-visualização */
function schedulePreview(delay) {
  clearTimeout(state.timer);
  state.timer = setTimeout(runPreview, delay);
}

function setStatus(kind, text, line) {
  el.status.className = `status ${kind}`;
  el.statusText.textContent = text;
  if (line) {
    el.statusLine.hidden = false;
    el.statusLine.textContent = `Ir para a linha ${line}`;
    el.statusLine.onclick = () => goToLine(line);
  } else {
    el.statusLine.hidden = true;
  }
}

const STAGES = { yaml: "YAML inválido", config: "Configuração inválida", run: "Erro ao transformar", pasted: "JSON colado inválido" };

async function runPreview() {
  if (!state.current) return;
  state.inflight?.abort();
  const ctrl = new AbortController();
  state.inflight = ctrl;
  setStatus("busy", "A transformar os exemplos");
  try {
    const res = await api("POST", "/api/preview", {
      text: el.yaml.value,
      samples: [...state.samples],
      pasted: el.pasted.value,
      semantic_id: el.semanticId.value.trim(),
    }, ctrl.signal);
    if (ctrl.signal.aborted) return;
    if (res.ok) {
      state.result = res;
      markError(null);
      const s = res.summary;
      setStatus("ok", `Configuração válida · ${plural(s.submodels.length, "submodelo", "submodelos")}`);
      render(res, false);
    } else {
      markError(res.line);
      setStatus("error", `${STAGES[res.stage] || "Erro"}: ${res.error}`, res.line);
      render(state.result ? { ...state.result, warnings: res.warnings || state.result.warnings,
        documents: res.documents || state.result.documents } : null, true);
    }
  } catch (e) {
    if (e.name === "AbortError") return;
    setStatus("error", `Sem ligação ao studio: ${e.message}`);
  }
}

function plural(n, one, many) {
  return `${n} ${n === 1 ? one : many}`;
}

/* ---------------------------------------------------------------- render */
function render(res, stale) {
  if (!res) {
    el.summary.textContent = "Sem resultado";
    el.tableTabs.replaceChildren();
    el.grid.replaceChildren();
    el.tableMeta.textContent = "";
    return;
  }
  const s = res.summary;
  el.summary.textContent = `${plural(s.documents, "documento", "documentos")} → ${plural(s.tables, "tabela", "tabelas")}, ${plural(s.rows, "linha", "linhas")}`
    + (stale ? " (último resultado válido)" : "");
  el.summary.style.opacity = stale ? "0.55" : "1";
  renderTables(res.tables);
  renderDocuments(res.documents || []);
  el.ddl.textContent = res.tables.map((t) => t.ddl).join("\n\n") || "Sem tabelas.";
  const w = res.warnings || [];
  el.warnCount.textContent = w.length ? String(w.length) : "";
  el.warnings.replaceChildren(...(w.length ? w : ["Sem avisos."]).map((m) => {
    const li = document.createElement("li");
    li.textContent = m;
    if (!w.length) li.className = "none";
    return li;
  }));
}

function renderTables(tables) {
  if (!tables.length) {
    el.tableTabs.replaceChildren();
    el.tableMeta.textContent = "";
    el.grid.innerHTML = `<tbody><tr><td class="empty">Nenhuma tabela. Os exemplos selecionados correspondem a algum submodelo (match)?</td></tr></tbody>`;
    return;
  }
  if (!tables.some((t) => t.name === state.table)) state.table = tables[0].name;
  el.tableTabs.replaceChildren(...tables.map((t) => {
    const b = document.createElement("button");
    b.type = "button";
    b.setAttribute("role", "tab");
    b.setAttribute("aria-selected", String(t.name === state.table));
    b.innerHTML = `<span></span><span class="count"></span>`;
    b.firstChild.textContent = t.name;
    b.lastChild.textContent = t.total;
    b.addEventListener("click", () => {
      state.table = t.name;
      store.set("cx2.table", t.name);
      renderTables(tables);
      $("#grid-wrap").scrollTo({ left: 0, top: 0 });
    });
    return b;
  }));
  const t = tables.find((x) => x.name === state.table);
  const bits = [`<strong>${esc(t.mode)}</strong>`];
  if (t.keys.length) bits.push(`chave <strong>${t.keys.map(esc).join(", ")}</strong>`);
  if (t.explode.length) bits.push(`uma linha por elemento de <strong>${t.explode.map(esc).join(" → ")}</strong>`);
  if (t.partition_by.length) bits.push(`partições ${t.partition_by.map(esc).join(", ")}`);
  if (t.skipped) bits.push(`<span class="bad">${t.skipped} linha(s) ignorada(s)</span>`);
  const cf = Object.entries(t.check_failures || {});
  if (cf.length) bits.push(`<span class="bad">checks falhados: ${cf.map(([c, n]) => `${esc(c)} (${n})`).join(", ")}</span>`);
  if (t.total > t.rows.length) bits.push(`a mostrar ${t.rows.length} de ${t.total}`);
  el.tableMeta.innerHTML = bits.join(" · ");
  renderGrid(t);
}

const narrow = window.matchMedia("(max-width: 760px)");
narrow.addEventListener("change", () => { if (state.result) renderTables(state.result.tables); });

function colHeader(t, c) {
  const th = document.createElement("th");
  th.scope = narrow.matches ? "row" : "col";
  if (c.name.startsWith("cx_")) th.className = "meta-col";
  const b = document.createElement("button");
  b.type = "button";
  b.title = `${c.name}: ${c.type}`;
  b.innerHTML = `<span class="col-name"></span><span class="col-type"></span>`;
  b.firstChild.textContent = c.name;
  b.lastChild.textContent = c.type;
  b.addEventListener("click", () => jumpToRule(t.name, c.name));
  th.append(b);
  return th;
}

function renderGrid(t) {
  if (narrow.matches) return renderTransposed(t);
  el.grid.classList.remove("transposed");
  const thead = document.createElement("thead");
  const hr = document.createElement("tr");
  t.columns.forEach((c) => hr.append(colHeader(t, c)));
  thead.append(hr);
  const tbody = document.createElement("tbody");
  if (!t.rows.length) {
    tbody.innerHTML = `<tr><td class="empty" colspan="${t.columns.length}">Sem linhas para os exemplos selecionados.</td></tr>`;
  }
  t.rows.forEach((r) => {
    const tr = document.createElement("tr");
    t.columns.forEach((c) => tr.append(cell(r[c.name])));
    tbody.append(tr);
  });
  el.grid.replaceChildren(thead, tbody);
}

/* telemóvel: cada coluna numa linha, as linhas de dados lado a lado */
function renderTransposed(t) {
  el.grid.classList.add("transposed");
  const rows = t.rows.slice(0, 10);
  const tbody = document.createElement("tbody");
  if (!rows.length) {
    tbody.innerHTML = `<tr><td class="empty">Sem linhas para os exemplos selecionados.</td></tr>`;
  }
  if (rows.length) t.columns.forEach((c) => {
    const tr = document.createElement("tr");
    tr.append(colHeader(t, c));
    rows.forEach((r) => tr.append(cell(r[c.name])));
    tbody.append(tr);
  });
  el.grid.replaceChildren(tbody);
}

function cell(v) {
  const td = document.createElement("td");
  if (v === null || v === undefined) { td.className = "null"; td.textContent = "—"; return td; }
  if (typeof v === "number") { td.className = "num"; td.textContent = String(v); return td; }
  if (typeof v === "boolean") { td.textContent = String(v); return td; }
  if (typeof v === "object") {
    const flat = JSON.stringify(v);
    const simple = Array.isArray(v) && v.every((x) => x === null || typeof x !== "object");
    if (simple && flat.length <= 80) { td.textContent = flat; td.title = flat; return td; }
    td.className = "nested";
    const d = document.createElement("details");
    const s = document.createElement("summary");
    s.textContent = Array.isArray(v) ? `lista de ${v.length}` : `${Object.keys(v).length} campos`;
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify(v, null, 2);
    d.append(s, pre);
    td.append(d);
    return td;
  }
  td.textContent = String(v);
  td.title = String(v);
  return td;
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

/* --------------------------------------------- coluna -> regra no YAML */
function sanitize(name) {
  let s = String(name).replace(/^['"]|['"]$/g, "");
  s = s.replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2");
  s = s.replace(/[^0-9a-zA-Z_]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase().replace(/_+/g, "_");
  return /^\d/.test(s) ? `c_${s}` : s;
}

function jumpToRule(table, col) {
  const lines = el.yaml.value.split("\n");
  const keyRe = /^(\s*)(?:-\s+)?(['"]?[A-Za-z0-9_$.\-]+['"]?)\s*:/;
  const tableLines = lines.map((l, i) => [l, i]).filter(([l]) => {
    const m = l.match(/^\s*-?\s*name\s*:\s*(.+?)\s*$/);
    return m && sanitize(m[1]) === table;
  }).map(([, i]) => i);
  const findKey = (from, to) => {
    for (let i = from; i < to; i++) {
      const m = lines[i].match(keyRe);
      if (m && sanitize(m[2]) === col && m[2] !== "name") return i + 1;
    }
    return 0;
  };
  for (const start of tableLines) {
    let end = lines.length;
    for (let i = start + 1; i < lines.length; i++) {
      if (/^\s*-\s*name\s*:/.test(lines[i]) && indent(lines[i]) <= indent(lines[start])) { end = i; break; }
    }
    const hit = findKey(start + 1, end);
    if (hit) return goToLine(hit);
  }
  const docId = lines.findIndex((l) => new RegExp(`^\\s*column\\s*:\\s*['"]?${col}['"]?\\s*$`).test(l));
  if (docId >= 0) return goToLine(docId + 1);
  const anywhere = findKey(0, lines.length);
  if (anywhere) return goToLine(anywhere);
  if (col.startsWith("cx_")) return toast(`${col} é uma coluna de metadados (metadata_columns).`);
  toast(`${col} não tem regra própria: vem de auto: true ou de auto_normalize.`);
}

function indent(line) {
  return line.length - line.trimStart().length;
}

/* ------------------------------------------------------------ documento */
function renderDocuments(docs) {
  const prev = el.docSelect.value;
  el.docSelect.replaceChildren(...docs.map((d, i) => {
    const o = document.createElement("option");
    o.value = String(i);
    o.textContent = `${d.source || "documento"}${docs.filter((x) => x.source === d.source).length > 1 ? ` #${i + 1}` : ""}`;
    return o;
  }));
  if (!docs.length) {
    el.docMeta.textContent = "";
    el.docTree.innerHTML = `<p class="empty">Selecione dados de exemplo ou cole um JSON.</p>`;
    return;
  }
  el.docSelect.value = docs[+prev] ? prev : "0";
  renderTree(docs[+el.docSelect.value]);
}

function renderTree(d) {
  const sub = d.submodels.length ? `submodelo ${d.submodels.join(", ")}` : "não corresponde a nenhum submodelo";
  el.docMeta.textContent = `${sub}${d.semantic_id ? ` · ${d.semantic_id}` : ""}${d.unwrapped ? " · caminhos depois do unwrap" : ""}`;
  if (d.too_large) { el.docTree.innerHTML = `<p class="empty">Documento demasiado grande para mostrar.</p>`; return; }
  const root = document.createElement("ul");
  build(root, d.data, "");
  el.docTree.replaceChildren(root);
}

function build(ul, v, path) {
  if (Array.isArray(v)) {
    v.slice(0, 20).forEach((item, i) => {
      const li = document.createElement("li");
      const idx = document.createElement("span");
      idx.className = "idx";
      idx.textContent = `[${i}] `;
      li.append(idx);
      addValue(li, item, `${path}[*]`);
      ul.append(li);
    });
    if (v.length > 20) {
      const li = document.createElement("li");
      li.className = "more";
      li.textContent = `… mais ${v.length - 20}`;
      ul.append(li);
    }
    return;
  }
  Object.entries(v).forEach(([k, item]) => {
    const li = document.createElement("li");
    const p = path ? `${path}.${/^[A-Za-z0-9_$\-]+$/.test(k) ? k : `['${k}']`}` : k;
    const b = document.createElement("button");
    b.type = "button";
    b.className = "k";
    b.textContent = k;
    b.title = `Inserir ${p}`;
    b.addEventListener("click", () => { insertAtCursor(p); toast(`Inserido: ${p}`); });
    li.append(b, document.createTextNode(": "));
    addValue(li, item, p);
    ul.append(li);
  });
}

function addValue(li, v, path) {
  if (v !== null && typeof v === "object") {
    const n = Array.isArray(v) ? `lista de ${v.length}` : "";
    if (n) {
      const s = document.createElement("span");
      s.className = "z";
      s.textContent = n;
      li.append(s);
    }
    const ul = document.createElement("ul");
    build(ul, v, path);
    li.append(ul);
    return;
  }
  const s = document.createElement("span");
  s.className = v === null ? "z" : typeof v === "string" ? "s" : "n";
  const text = JSON.stringify(v);
  s.textContent = text.length > 140 ? text.slice(0, 137) + "…\"" : text;
  li.append(s);
}

el.docSelect.addEventListener("change", () => {
  const docs = state.result?.documents || [];
  if (docs[+el.docSelect.value]) renderTree(docs[+el.docSelect.value]);
});

/* ------------------------------------------------------------ vistas */
document.querySelectorAll(".views [data-view]").forEach((b) => b.addEventListener("click", () => {
  state.view = b.dataset.view;
  document.querySelectorAll(".views [data-view]").forEach((x) => x.setAttribute("aria-selected", String(x === b)));
  document.querySelectorAll(".view").forEach((v) => { v.hidden = v.dataset.view !== state.view; });
}));

function showPane(name) {
  el.workspace.dataset.show = name;
  document.querySelectorAll(".switcher [data-pane]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.pane === name)));
}
document.querySelectorAll(".switcher [data-pane]").forEach((b) => b.addEventListener("click", () => showPane(b.dataset.pane)));

/* ----------------------------------------------------------- arranque */
el.save.addEventListener("click", save);
el.revert.addEventListener("click", revert);
$("#new-config").addEventListener("click", newConfig);
el.pasted.addEventListener("input", () => schedulePreview(400));
el.semanticId.addEventListener("input", () => schedulePreview(400));
document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") { e.preventDefault(); if (!el.save.disabled) save(); }
});
window.addEventListener("beforeunload", (e) => { if (isDirty()) { e.preventDefault(); e.returnValue = ""; } });

(async () => {
  try {
    await loadFiles();
    const last = store.get("cx2.last", null);
    const first = state.files.configs.find((f) => f.path === last) || state.files.configs[0];
    if (first) await openConfig(first.path);
    updateGutter();
  } catch (e) {
    setStatus("error", `Não foi possível ler a pasta: ${e.message}`);
  }
})();
