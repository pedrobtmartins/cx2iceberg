"use strict";

/* cx2iceberg studio — construtor visual de transformações.
   O modelo de trabalho é a própria configuração (state.cfg, igual ao YAML com as colunas em
   forma de objecto); o YAML é gerado pelo servidor ao gravar e em "Avançado". */

const $ = (s) => document.querySelector(s);
const store = {
  get(k, d) { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* sem armazenamento */ } },
};

const state = {
  catalog: null, catalogError: null, expanded: null, statuses: {},
  files: { configs: [], samples: [] },
  cfg: null, path: null, mtime: null, isNew: false, dirty: false,
  si: 0, ti: 0, ci: null, pick: false,
  fields: null, fieldsKey: "", fieldIndex: {}, fieldsReason: "", example: null,
  samples: new Set(), result: null, last: null, timer: 0, inflight: null,
};

/* ------------------------------------------------------------ vocabulário */
const TYPES = [
  ["string", "Texto"], ["bigint", "Número inteiro"], ["double", "Número decimal"], ["timestamp", "Data e hora"],
  ["date", "Data"], ["boolean", "Sim / não"], ["array<string>", "Lista de textos"], ["array<double>", "Lista de números"],
  ["array<timestamp>", "Lista de datas"], ["json", "Texto JSON (bloco inteiro)"],
];
const MODES = [
  ["merge", "Actualizar as linhas com a mesma chave"],
  ["replace_doc", "Substituir as linhas do mesmo documento"],
  ["append", "Acrescentar sempre linhas novas"],
  ["overwrite", "Substituir a tabela toda em cada carga"],
];
/* "replace_doc" não é um modo do motor: é append + replace_by [UUID do documento] */
/* valor efectivo: tabela -> defaults do submodelo -> defaults da configuração */
const eff = (t, k, d) => t[k] ?? sub()?.defaults?.[k] ?? state.cfg?.defaults?.[k] ?? d;
const uiMode = (t) => (eff(t, "mode", "append") === "append" && eff(t, "replace_by", []).length ? "replace_doc" : eff(t, "mode", "append"));
function setMode(t, m) {
  delete t.replace_by;
  if (m === "replace_doc") {
    if (!sub().document_id) sub().document_id = true;
    t.mode = "append"; t.replace_by = [docIdCol()]; delete t.keys;
  } else {
    t.mode = m;
    if (m === "merge" && !(t.keys || []).length) t.keys = [docIdCol()].filter(Boolean);
    if (m !== "merge") delete t.keys;
  }
}
const UNITS = ["kilogram", "gram", "tonne", "metre", "centimetre", "millimetre", "litre", "cubicMetre", "squareMetre",
  "hourUnitOfTime", "minuteUnitOfTime", "secondUnitOfTime", "kilowattHour", "megajoule", "piece"];
const STEPS = {
  trim: { label: "Remover espaços nas pontas" },
  upper: { label: "Passar a maiúsculas" },
  lower: { label: "Passar a minúsculas" },
  urn_suffix: { label: "Tirar o prefixo URN (urn:uuid:…)" },
  prefix: { label: "Acrescentar texto antes", arg: "text" },
  suffix: { label: "Acrescentar texto depois", arg: "text" },
  replace: { label: "Substituir texto", arg: "pair" },
  map: { label: "Traduzir valores", arg: "pairs" },
  multiply: { label: "Multiplicar por", arg: "number" },
  divide: { label: "Dividir por", arg: "number" },
  round: { label: "Arredondar (casas decimais)", arg: "number" },
  clamp: { label: "Limitar entre mínimo e máximo", arg: "minmax" },
  unit_convert: { label: "Converter unidade para", arg: "unit" },
  first: { label: "Ficar com o primeiro da lista" },
  count: { label: "Contar os elementos da lista" },
  join: { label: "Juntar a lista num texto, separado por", arg: "text" },
  year: { label: "Ficar com o ano da data" },
  to_date: { label: "Ficar só com a data (sem hora)" },
  hash: { label: "Substituir por um código (anonimizar)" },
  uuid5: { label: "Gerar um UUID a partir do valor" },
};
const ON_FAIL = [["warn", "Avisar e manter o valor"], ["null", "Deixar a célula vazia"], ["skip_row", "Ignorar a linha"], ["fail", "Parar a carga"]];
const COL_KEYS = new Set(["path", "type", "transform", "default", "check", "const", "required", "on_error", "comment"]);

const typeLabel = (t) => (TYPES.find(([k]) => k === t) || [])[1] || (t?.startsWith("struct") ? "Estrutura (avançado)" : t || "Automático");

/* ------------------------------------------------------------------ util */
async function api(method, url, body, signal) {
  const res = await fetch(url, { method, headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined, signal });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data.error || res.statusText), { status: res.status });
  return data;
}
function toast(msg, kind = "") {
  const t = $("#toast");
  t.textContent = msg; t.className = `toast ${kind}`; t.hidden = false;
  clearTimeout(toast.t); toast.t = setTimeout(() => { t.hidden = true; }, kind === "error" ? 6500 : 3000);
}
function esc(s) { return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }
function h(tag, attrs = {}, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else e.setAttribute(k, v === true ? "" : v);
  }
  for (const c of kids.flat()) if (c != null && c !== false) e.append(c.nodeType ? c : document.createTextNode(String(c)));
  return e;
}
function sanitize(name) {
  let s = String(name).replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2");
  s = s.replace(/[^0-9a-zA-Z_]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase().replace(/_+/g, "_");
  return /^\d/.test(s) ? `c_${s}` : s || "coluna";
}
function short(v, n = 48) {
  if (v === undefined) return "";
  const s = typeof v === "string" ? v : JSON.stringify(v);
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

/* -------------------------------------------------------- acesso ao modelo */
const sub = () => state.cfg?.submodels?.[state.si];
const table = () => sub()?.tables?.[state.ti];
const col = () => (table() && state.ci != null ? table().columns?.[state.ci] : null);
const docIdCol = () => {
  const d = sub()?.document_id;
  return d ? (typeof d === "object" && d.column ? sanitize(d.column) : "document_id") : null;
};
function explodeOf(t) {
  if (!t?.explode) return null;
  return Array.isArray(t.explode) ? t.explode.join("[*].") : String(t.explode).replace(/\[\*\]$/, "");
}
function toTablePath(abs, t) {
  const E = explodeOf(t);
  if (!E) return abs;
  if (abs === E) return "$item";
  if (abs.startsWith(E + "[*].")) return abs.slice(E.length + 4);
  return "$root." + abs;
}
function toAbsPath(p, t) {
  if (!p) return p;
  const E = explodeOf(t);
  if (p.startsWith("$root.")) return p.slice(6);
  if (p === "$item") return E;
  if (E && !p.startsWith("$")) return `${E}[*].${p}`;
  return p;
}
function sourceText(c, t) {
  if (!c) return "";
  if (c.cases) return "Regra condicional (editar em Avançado)";
  if (c.template != null) return `Texto composto: ${c.template}`;
  if (c.paths) return `Primeiro com valor de ${c.paths.length} campos`;
  if ("const" in c) return c.const == null ? "Vazio (campo inexistente na origem)" : `Valor fixo: ${short(c.const)}`;
  if (c.path === "$index") return "Posição na lista";
  if (c.path === "$item") return "Cada valor da lista";
  if (c.path?.startsWith("$meta.")) return `Metadados: ${c.path.slice(6)}`;
  const f = state.fieldIndex[toAbsPath(c.path, t)];
  return f ? `${f.trail}` : c.path || "Sem origem";
}
function changed({ fields = false } = {}) {
  state.dirty = true;
  updateBar();
  if (fields) loadFields();
  renderAll();
  schedulePreview(300);
}

/* ------------------------------------------------------------- catálogo */
async function loadCatalog(refresh = false) {
  try {
    const r = await api("GET", `/api/catalog${refresh ? "?refresh=1" : ""}`);
    state.catalog = r.models; state.catalogError = null;
  } catch (e) {
    state.catalogError = e.message;
  }
  renderCatalog();
}
function renderCatalog() {
  const ul = $("#catalog");
  if (state.catalogError) {
    ul.replaceChildren(h("li", { class: "hint" }, state.catalogError, " ", h("button", { class: "link", type: "button", onclick: () => loadCatalog(true) }, "Tentar outra vez")));
    return;
  }
  if (!state.catalog) return;
  const q = $("#catalog-search").value.trim().toLowerCase().replace(/[\s_.-]+/g, " ");
  const list = state.catalog.filter((m) => !q || `${m.name} ${m.id}`.toLowerCase().replace(/[\s_.-]+/g, " ").includes(q));
  ul.replaceChildren(...list.slice(0, 80).map((m) => {
    const open = state.expanded === m.id;
    const li = h("li", {},
      h("button", { type: "button", "aria-expanded": String(open), onclick: () => toggleModel(m) },
        m.name, h("span", { class: "id" }, `${m.id} · ${m.versions.length} versõ${m.versions.length === 1 ? "o" : "es"}`)));
    if (open) {
      const st = state.statuses[m.id] || {};
      li.append(h("ul", { class: "versions" }, m.versions.map((v) => h("li", {},
        h("span", {}, v.version, " ", h("span", { class: `status ${st[v.version] || ""}` }, st[v.version] || (st._loading ? "…" : "")),
          v.has_example ? null : h("span", { class: "status" }, " sem exemplo")),
        h("button", { class: "btn btn-quiet btn-small", type: "button", onclick: () => startFrom(m, v) }, v.local ? "Abrir" : "Usar"),
      ))));
    }
    return li;
  }));
  if (!list.length) ul.replaceChildren(h("li", { class: "hint" }, "Nenhum submodelo com esse nome."));
}
async function toggleModel(m) {
  state.expanded = state.expanded === m.id ? null : m.id;
  renderCatalog();
  if (state.expanded && !state.statuses[m.id]) {
    state.statuses[m.id] = { _loading: true };
    renderCatalog();
    try {
      const r = await api("GET", `/api/catalog/status?model=${encodeURIComponent(m.id)}&versions=${m.versions.slice(0, 8).map((v) => v.version).join(",")}`);
      state.statuses[m.id] = r.status;
    } catch { state.statuses[m.id] = {}; }
    renderCatalog();
  }
}
async function startFrom(m, v) {
  if (state.dirty && !confirm("Há alterações por guardar. Começar outra configuração e perdê-las?")) return;
  try {
    let r = await api("POST", "/api/new", { model: m.id, version: v.version });
    if (r.exists) {
      if (confirm(`Já existe ${r.path}. Abrir essa configuração em vez de criar outra?`)) { openConfig(r.path); return; }
      const p = prompt("Nome do ficheiro para a nova configuração:", r.path.replace(/\.yaml$/, "_2.yaml"));
      if (!p) return;
      r = await api("POST", "/api/new", { model: m.id, version: v.version, path: p });
      if (r.exists) { toast(`Já existe ${p}.`, "error"); return; }
    }
    setConfig(r.config, r.path, null, true);
    if (r.example) state.samples = new Set([r.example]);
    state.dirty = true;
    updateBar();
    showPane("fields");
    toast(`Configuração nova para ${m.name} ${v.version}. Clique nos campos para criar colunas.`);
  } catch (e) { toast(e.message, "error"); }
}

/* ------------------------------------------------------------ ficheiros */
async function loadFiles() {
  state.files = await api("GET", "/api/files");
  renderConfigs();
  renderSamples();
}
function renderConfigs() {
  const ul = $("#configs");
  if (!state.files.configs.length) { ul.replaceChildren(h("li", { class: "hint" }, "Ainda não há configurações nesta pasta.")); return; }
  ul.replaceChildren(...state.files.configs.map((f) => {
    const i = f.path.lastIndexOf("/");
    return h("li", {}, h("button", { type: "button", "aria-current": String(state.path === f.path), onclick: () => openConfig(f.path) },
      h("span", { class: "dir" }, f.path.slice(0, i + 1)), f.path.slice(i + 1)));
  }));
}
function renderSamples() {
  $("#test-data").hidden = !state.cfg;
  $("#samples").replaceChildren(...state.files.samples.map((f) => h("li", {}, h("label", {},
    h("input", { type: "checkbox", checked: state.samples.has(f.path), onchange: (e) => {
      e.target.checked ? state.samples.add(f.path) : state.samples.delete(f.path);
      if (state.path) store.set(`cx2.samples:${state.path}`, [...state.samples]);
      schedulePreview(0);
    } }),
    h("span", {}, f.path, f.path === state.example ? " (exemplo oficial)" : "")))));
}
async function openConfig(path) {
  if (state.dirty && !confirm("Há alterações por guardar. Abrir outra configuração e perdê-las?")) return;
  try {
    const r = await api("POST", "/api/load", { path });
    if (!r.config) { setConfig(null, path, r.mtime, false); openAdvanced(r.text, r.error); return; }
    setConfig(r.config, path, r.mtime, false);
    showPane("fields");
  } catch (e) { toast(e.message, "error"); }
}
function setConfig(cfg, path, mtime, isNew) {
  Object.assign(state, { cfg, path, mtime, isNew, dirty: false, si: 0, ti: 0, ci: null, pick: false, result: null, last: null, fieldsKey: "" });
  const t = table();
  state.ci = t ? Object.keys(t.columns || {})[0] ?? null : null;
  state.samples = new Set(store.get(`cx2.samples:${path}`, []));
  store.set("cx2.last", path);
  updateBar();
  renderConfigs();
  loadFields();
  renderAll();
  schedulePreview(0);
}
function updateBar() {
  $("#current-file").textContent = state.path ? `${state.path}${state.isNew ? " (nova)" : ""}` : "Escolha um submodelo para começar";
  $("#dirty").hidden = !state.dirty;
  $("#save").disabled = !state.cfg || !state.dirty;
  $("#advanced").disabled = !state.path;
  document.title = `${state.dirty ? "• " : ""}${state.path ? state.path + " — " : ""}cx2iceberg studio`;
}
async function save() {
  if (!state.cfg || !state.path) return;
  try {
    const r = await api("PUT", "/api/file", { path: state.path, config: state.cfg, mtime: state.isNew ? null : state.mtime });
    Object.assign(state, { mtime: r.mtime, isNew: false, dirty: false });
    store.set(`cx2.samples:${state.path}`, [...state.samples]);
    updateBar();
    toast(`Guardado em ${r.path}`);
    loadFiles();
  } catch (e) { toast(e.message, "error"); }
}

/* --------------------------------------------------------------- campos */
async function loadFields() {
  const s = sub();
  if (!s) { state.fields = null; renderFields(); return; }
  const key = JSON.stringify([state.path, state.si, s.model, s.unwrap]);
  if (key === state.fieldsKey) return;
  state.fieldsKey = key;
  try {
    const r = await api("POST", "/api/fields", { config: state.cfg, submodel: state.si, path: state.path });
    state.fields = r.tree; state.fieldsReason = r.reason || ""; state.example = r.example || null;
    if (state.example && !state.samples.size) state.samples.add(state.example);
  } catch (e) { state.fields = null; state.fieldsReason = e.message; }
  state.fieldIndex = {};
  const walk = (nodes, trail) => (nodes || []).forEach((n) => {
    const t = trail ? `${trail} › ${n.label}` : n.label;
    state.fieldIndex[n.path] = { ...n, trail: t };
    walk(n.children, t);
  });
  walk(state.fields, "");
  renderAll();
  schedulePreview(0);
}
function usedPaths() {
  const t = table();
  return new Set(Object.values(t?.columns || {}).map((c) => toAbsPath(c.path, t)).filter(Boolean));
}
function renderFields() {
  const box = $("#fields");
  const s = sub();
  const head = $("#fields-title");
  head.replaceChildren();
  if (!s) { head.textContent = "Campos"; box.replaceChildren(h("p", { class: "empty" }, "Escolha um submodelo no catálogo ou abra uma configuração.")); return; }
  const subs = state.cfg.submodels;
  if (subs.length > 1) {
    head.append(h("select", { "aria-label": "Submodelo", onchange: (e) => { state.si = +e.target.value; state.ti = 0; state.ci = null; state.fieldsKey = ""; loadFields(); renderAll(); } },
      subs.map((x, i) => h("option", { value: i, selected: i === state.si }, x.name))));
  } else {
    head.textContent = s.model ? `Campos de ${s.model.id?.replace("io.catenax.", "") || s.name} ${s.model.version || ""}` : `Campos de ${s.name}`;
  }
  if (!state.fields) { box.replaceChildren(h("p", { class: "empty" }, state.fieldsReason || "A carregar…")); return; }
  const q = $("#field-search").value.trim().toLowerCase();
  const used = usedPaths();
  const t = table();
  const E = explodeOf(t);
  const out = [];
  if (state.pick) {
    out.push(h("div", { class: "pick-banner" }, `Escolha o campo de origem para “${state.ci}”.`,
      h("button", { class: "link", type: "button", onclick: () => { state.pick = false; renderAll(); } }, "Cancelar")));
  }
  const match = (n) => !q || n.label.toLowerCase().includes(q) || n.key.toLowerCase().includes(q) || (n.children || []).some(match);
  const build = (nodes) => h("ul", {}, nodes.filter(match).map((n) => {
    if (n.kind === "object" || n.kind === "list") {
      const meta = [];
      const acts = [];
      if (n.kind === "list") {
        if (n.single) {
          meta.push("um por documento");
          acts.push(h("button", { class: "link", type: "button", title: "Esta lista pode ter vários elementos", onclick: () => setSingle(n, false) }, "Pode ter vários"));
        } else {
          meta.push(`lista${n.count != null ? ` · ${n.count} no exemplo` : ""}`);
          acts.push(h("button", { class: "link", type: "button", onclick: () => childTable(n) }, E === n.path ? "Esta tabela já tem uma linha por elemento" : "Uma linha por elemento"));
          acts.push(h("button", { class: "link", type: "button", title: "Tratar como um único bloco", onclick: () => setSingle(n, true) }, "É sempre um só"));
        }
      }
      return h("li", { class: "group" },
        h("div", { class: "group-head", title: n.description || "" }, h("strong", {}, n.label), meta.length ? h("span", { class: "meta" }, meta.join(" · ")) : null,
          acts.length ? h("span", { class: "acts" }, acts) : null),
        build(n.children || []));
    }
    const isUsed = used.has(n.path);
    const ex = n.in_example ? short(n.example) : "sem valor no exemplo";
    return h("li", {}, h("button", { type: "button", class: `field${isUsed ? " used" : ""}`, title: n.description || n.label, onclick: () => addField(n) },
      h("span", { class: "name" }, n.label, n.required ? h("span", { class: "req" }, "obrigatório") : null),
      h("span", { class: "type" }, typeLabel(n.type)),
      h("span", { class: `ex${n.in_example ? "" : " none"}` }, ex)));
  }));
  out.push(build(state.fields));
  box.replaceChildren(...out);
}
function uniqueName(base, cols) {
  let n = base, i = 2;
  while (cols[n]) n = `${base}_${i++}`;
  return n;
}
function addField(n) {
  const t = table();
  if (!t) { toast("Crie primeiro uma tabela.", "error"); return; }
  t.columns = t.columns || {};
  const path = toTablePath(n.path, t);
  let type = n.type;
  if (path === "$item" && type?.startsWith("array<")) type = type.slice(6, -1);
  else if (path.includes("[*]") && !type?.startsWith("array")) type = `array<${type}>`;
  if (state.pick && col()) {
    const c = col();
    for (const k of ["const", "paths", "template", "cases"]) delete c[k];
    c.path = path;
    c.type = type;
    state.pick = false;
    changed();
    return;
  }
  const existing = Object.entries(t.columns).find(([, c]) => c.path === path);
  if (existing) { state.ci = existing[0]; renderAll(); showPane("table"); return; }
  const parent = n.path.split(/\.|\[\*\]\./).slice(-2, -1)[0];
  let name = sanitize(n.key);
  if (t.columns[name] && parent) name = sanitize(`${parent}_${n.key}`);
  name = uniqueName(name, t.columns);
  t.columns[name] = { path, type };
  state.ci = name;
  changed();
}
function unwrapRule(s) {
  let u = s.unwrap;
  if (!u) u = {};
  else if (u === "*" || u === true) u = { all: true };
  else if (Array.isArray(u)) u = { paths: u };
  else u = { ...u };
  return u;
}
function setSingle(n, single) {
  const s = sub();
  const raw = n.path.replace(/\[\*\]/g, "");
  const u = unwrapRule(s);
  if (u.all) {
    u.except = (u.except || []).filter((p) => p !== raw);
    if (!single) u.except.push(raw);
  } else {
    u.paths = (u.paths || []).filter((p) => p !== raw);
    if (single) u.paths.push(raw);
  }
  for (const k of ["paths", "except"]) if (u[k] && !u[k].length) delete u[k];
  if (Object.keys(u).length) s.unwrap = u; else delete s.unwrap;
  const from = single ? `${n.path}[*].` : `${n.path}.`;
  const to = single ? `${n.path}.` : `${n.path}[*].`;
  for (const t of s.tables) {
    if (single && explodeOf(t) === n.path) { delete t.explode; toast(`A tabela ${t.name} deixou de ter uma linha por elemento.`); }
    for (const c of Object.values(t.columns || {})) {
      const abs = toAbsPath(c.path, t);
      if (abs && abs.startsWith(from)) {
        c.path = toTablePath(to + abs.slice(from.length), t);
        if (c.type?.startsWith("array<") && single) c.type = c.type.slice(6, -1);
        else if (!single && c.type && !c.type.startsWith("array") && toTablePath(to + abs.slice(from.length), t).includes("[*]")) c.type = `array<${c.type}>`;
      }
    }
  }
  changed({ fields: true });
}
function childTable(n) {
  const s = sub();
  const existing = s.tables.findIndex((t) => explodeOf(t) === n.path);
  if (existing >= 0) { state.ti = existing; state.ci = Object.keys(s.tables[existing].columns || {})[0] ?? null; renderAll(); showPane("table"); return; }
  if (!s.document_id) s.document_id = true;
  const names = new Set(s.tables.map((t) => t.name));
  let name = sanitize(`${s.tables[0]?.name || s.name}_${n.key}`), i = 2;
  while (names.has(name)) name = `${sanitize(n.key)}_${i++}`;
  s.tables.push({ name, explode: n.path, mode: "append", replace_by: [docIdCol()], columns: { posicao: { path: "$index", type: "int" } } });
  state.ti = s.tables.length - 1;
  state.ci = "posicao";
  changed();
  toast(`Tabela ${name}: uma linha por cada elemento de “${n.label}”. Clique nos campos de dentro da lista para criar colunas.`);
}

/* --------------------------------------------------------------- tabela */
function renderTable() {
  const tabs = $("#table-tabs");
  const body = $("#table-body");
  const s = sub();
  if (!s) { tabs.replaceChildren(); body.replaceChildren(h("p", { class: "empty" }, "As tabelas aparecem aqui.")); return; }
  tabs.replaceChildren(...s.tables.map((t, i) => h("button", { type: "button", role: "tab", "aria-selected": String(i === state.ti),
    onclick: () => { state.ti = i; state.ci = Object.keys(t.columns || {})[0] ?? null; state.pick = false; renderAll(); } }, t.name)),
    h("button", { type: "button", class: "add", onclick: newTable }, "+ Nova tabela"));
  const t = table();
  if (!t) { body.replaceChildren(); return; }
  body.replaceChildren(renderSettings(t), renderColumns(t), renderEditor(t));
}
function newTable() {
  const s = sub();
  const names = new Set(s.tables.map((t) => t.name));
  let i = s.tables.length + 1, name = `tabela_${i}`;
  while (names.has(name)) name = `tabela_${++i}`;
  if (!s.document_id) s.document_id = true;
  s.tables.push({ name, mode: "merge", keys: [docIdCol()], columns: {} });
  state.ti = s.tables.length - 1; state.ci = null;
  changed();
}
function renderSettings(t) {
  const cols = Object.keys(t.columns || {});
  const did = docIdCol();
  const keyOpts = [...(did ? [did] : []), ...cols];
  const lists = Object.values(state.fieldIndex).filter((n) => (n.kind === "list" && !n.single) || n.kind === "values");
  const E = explodeOf(t);
  const setExplode = (val) => {
    const abs = Object.fromEntries(Object.entries(t.columns || {}).map(([k, c]) => [k, c.path && !c.path.startsWith("$") || c.path?.startsWith("$root.") || c.path === "$item" ? toAbsPath(c.path, t) : null]));
    if (val) t.explode = val; else delete t.explode;
    for (const [k, a] of Object.entries(abs)) if (a) t.columns[k].path = toTablePath(a, t);
    changed();
  };
  const keys = new Set(eff(t, "keys", []));
  return h("div", { class: "settings" },
    h("label", { for: "t-name" }, "Nome da tabela"),
    h("input", { id: "t-name", type: "text", value: t.name, onchange: (e) => { t.name = sanitize(e.target.value); changed(); } }),
    h("label", { for: "t-explode" }, "Uma linha por"),
    h("select", { id: "t-explode", onchange: (e) => setExplode(e.target.value) },
      h("option", { value: "", selected: !E }, "documento"),
      lists.map((n) => h("option", { value: n.path, selected: E === n.path }, `elemento de ${n.trail}`)),
      E && !lists.some((n) => n.path === E) ? h("option", { value: E, selected: true }, E) : null),
    h("label", { for: "t-mode" }, "Em cada carga"),
    h("select", { id: "t-mode", onchange: (e) => { setMode(t, e.target.value); changed(); } },
      MODES.map(([k, l]) => h("option", { value: k, selected: uiMode(t) === k }, l))),
    uiMode(t) === "merge" ? [h("span", { class: "lbl" }, "Chave"), h("div", { class: "keys" }, keyOpts.map((k) => h("label", {},
      h("input", { type: "checkbox", checked: keys.has(k), onchange: (e) => {
        const ks = new Set(eff(t, "keys", [])); e.target.checked ? ks.add(k) : ks.delete(k); t.keys = [...ks]; changed();
      } }), k === did ? `${k} (UUID do documento)` : k)))] : null,
    h("span", {}), h("div", {}, h("button", { class: "btn btn-danger btn-small", type: "button", onclick: () => {
      if (!confirm(`Remover a tabela ${t.name}?`)) return;
      sub().tables.splice(state.ti, 1); state.ti = Math.max(0, state.ti - 1); state.ci = null; changed();
    } }, "Remover tabela")));
}
function renderColumns(t) {
  const entries = Object.entries(t.columns || {});
  if (!entries.length) return h("p", { class: "empty" }, "Esta tabela ainda não tem colunas. Clique nos campos à esquerda para as criar.");
  return h("ul", { class: "cols" }, entries.map(([name, c]) => h("li", {}, h("button", { type: "button", "aria-current": String(name === state.ci),
    onclick: () => { state.ci = name; state.pick = false; renderAll(); } },
    h("span", { class: "cname" }, name), h("span", { class: "ctype" }, typeLabel(c.type)),
    h("span", { class: "csrc" }, `de ${sourceText(c, t)}${stepCount(c)}`)))));
}
function stepCount(c) {
  const n = steps(c).length + (c.default != null ? 1 : 0) + (c.check ? 1 : 0);
  return n ? ` · ${n} regra${n > 1 ? "s" : ""}` : "";
}
function steps(c) {
  const t = c.transform;
  return t == null ? [] : Array.isArray(t) ? t : [t];
}
function stepName(s) { return typeof s === "string" ? s : Object.keys(s)[0]; }
function stepArg(s) { return typeof s === "string" ? undefined : Object.values(s)[0]; }

function renderEditor(t) {
  const c = col();
  if (!c) return h("div");
  const name = state.ci;
  const advanced = Object.keys(c).filter((k) => !COL_KEYS.has(k));
  const f = state.fieldIndex[toAbsPath(c.path, t)];
  const rename = (e) => {
    const n = sanitize(e.target.value);
    if (!n || n === name) return;
    if (t.columns[n]) { toast(`Já existe a coluna ${n}.`, "error"); e.target.value = name; return; }
    t.columns = Object.fromEntries(Object.entries(t.columns).map(([k, v]) => [k === name ? n : k, v]));
    t.keys = (t.keys || []).map((k) => (k === name ? n : k));
    if (t.replace_by) t.replace_by = t.replace_by.map((k) => (k === name ? n : k));
    state.ci = n; changed();
  };
  const set = (k, v) => { if (v === "" || v == null) delete c[k]; else c[k] = v; changed(); };
  return h("div", { class: "editor" },
    h("h3", {}, `Coluna ${name}`),
    advanced.length ? h("p", { class: "advanced-note" }, "Esta coluna usa uma regra que o construtor não mostra (", advanced.join(", "), "). Pode alterá-la em Avançado.") : null,
    h("div", { class: "row" }, h("label", { for: "c-name" }, "Nome"), h("input", { id: "c-name", type: "text", value: name, onchange: rename })),
    h("div", { class: "row" }, h("span", { class: "lbl" }, "Origem"), h("div", { class: "src" },
      h("span", {}, sourceText(c, t)),
      c.path && !c.path.startsWith("$") ? h("span", { class: "path" }, c.path) : null,
      f?.description ? h("span", { class: "desc" }, f.description) : null,
      h("span", { class: "acts" },
        h("button", { class: "link", type: "button", onclick: () => { state.pick = true; renderAll(); showPane("fields"); } }, "Escolher outro campo"),
        h("button", { class: "link", type: "button", onclick: () => {
          const v = prompt("Valor fixo para esta coluna (deixe vazio para uma coluna sempre vazia):", c.const ?? "");
          if (v === null) return;
          for (const k of ["path", "paths", "template", "cases"]) delete c[k];
          c.const = v === "" ? null : v; changed();
        } }, "Usar um valor fixo")))),
    h("div", { class: "row" }, h("label", { for: "c-type" }, "Tipo"),
      h("select", { id: "c-type", onchange: (e) => set("type", e.target.value) },
        h("option", { value: "", selected: !c.type }, "Automático"),
        TYPES.map(([k, l]) => h("option", { value: k, selected: c.type === k }, l)),
        c.type && !TYPES.some(([k]) => k === c.type) ? h("option", { value: c.type, selected: true }, typeLabel(c.type)) : null)),
    h("div", { class: "row" }, h("label", { for: "c-default" }, "Se estiver vazio, usar"),
      h("input", { id: "c-default", type: "text", value: c.default ?? "", placeholder: "deixar vazio", onchange: (e) => set("default", e.target.value === "" ? null : coerce(e.target.value)) })),
    h("div", { class: "row" }, h("span", { class: "lbl" }, "Passos"), renderSteps(c)),
    h("div", { class: "row" }, h("span", { class: "lbl" }, "Validação"), renderCheck(c)),
    h("div", {}, h("button", { class: "btn btn-danger btn-small", type: "button", onclick: () => {
      delete t.columns[name]; t.keys = (t.keys || []).filter((k) => k !== name);
      state.ci = Object.keys(t.columns)[0] ?? null; changed();
    } }, "Remover coluna")));
}
function coerce(v) {
  if (v === "true") return true;
  if (v === "false") return false;
  return v.trim() !== "" && !Number.isNaN(Number(v)) ? Number(v) : v;
}
function renderSteps(c) {
  const list = steps(c);
  const write = (arr) => { if (arr.length) c.transform = arr; else delete c.transform; changed(); };
  const items = list.map((s, i) => {
    const n = stepName(s), a = stepArg(s), def = STEPS[n];
    const setArg = (v) => { const arr = [...list]; arr[i] = v === undefined ? n : { [n]: v }; write(arr); };
    let editor = null;
    if (def?.arg === "text") editor = h("input", { type: "text", value: a ?? "", onchange: (e) => setArg(e.target.value) });
    else if (def?.arg === "number") editor = h("input", { type: "number", step: "any", value: a ?? "", onchange: (e) => setArg(Number(e.target.value)) });
    else if (def?.arg === "unit") editor = h("select", { onchange: (e) => setArg({ to: e.target.value }) }, UNITS.map((u) => h("option", { value: u, selected: a?.to === u }, u)));
    else if (def?.arg === "minmax") editor = [
      h("input", { type: "number", step: "any", placeholder: "mínimo", value: a?.min ?? "", onchange: (e) => setArg({ ...a, min: e.target.value === "" ? undefined : Number(e.target.value) }) }),
      h("input", { type: "number", step: "any", placeholder: "máximo", value: a?.max ?? "", onchange: (e) => setArg({ ...a, max: e.target.value === "" ? undefined : Number(e.target.value) }) })];
    else if (def?.arg === "pair") editor = [
      h("input", { type: "text", placeholder: "procurar", value: a?.[0] ?? "", onchange: (e) => setArg([e.target.value, a?.[1] ?? ""]) }),
      h("input", { type: "text", placeholder: "substituir por", value: a?.[1] ?? "", onchange: (e) => setArg([a?.[0] ?? "", e.target.value]) })];
    else if (def?.arg === "pairs") editor = h("textarea", { placeholder: "valor antigo = valor novo\n(um por linha)", onchange: (e) => {
      const m = {};
      e.target.value.split("\n").forEach((l) => { const j = l.indexOf("="); if (j > 0) m[l.slice(0, j).trim()] = l.slice(j + 1).trim(); });
      m._ignore_case = true; setArg(m);
    } }, Object.entries(a || {}).filter(([k]) => !k.startsWith("_")).map(([k, v]) => `${k} = ${v}`).join("\n"));
    else if (!def && a !== undefined) editor = h("span", { class: "path" }, short(a, 60));
    return h("li", {}, h("span", { class: "sname" }, def ? def.label : n), editor,
      h("button", { class: "link rm", type: "button", onclick: () => write(list.filter((_, j) => j !== i)) }, "Remover"));
  });
  const add = h("select", { "aria-label": "Acrescentar passo", onchange: (e) => {
    const n = e.target.value; if (!n) return;
    const d = STEPS[n];
    const init = { text: "", number: 1, unit: { to: "kilogram" }, minmax: { min: 0 }, pair: ["", ""], pairs: { _ignore_case: true } }[d.arg];
    write([...list, d.arg ? { [n]: init } : n]);
  } }, h("option", { value: "" }, "+ Acrescentar passo…"), Object.entries(STEPS).map(([k, d]) => h("option", { value: k }, d.label)));
  return h("div", {}, items.length ? h("ul", { class: "steps" }, items) : null, add);
}
function renderCheck(c) {
  const chk = c.check || {};
  const write = (k, v) => {
    const n = { ...chk };
    if (v === "" || v == null || v === false) delete n[k]; else n[k] = v;
    const meaningful = Object.keys(n).filter((x) => x !== "on_fail");
    if (meaningful.length) c.check = n; else delete c.check;
    changed();
  };
  return h("div", { class: "checkset" },
    h("label", {}, h("input", { type: "checkbox", checked: !!chk.required, onchange: (e) => write("required", e.target.checked) }), "Obrigatório"),
    h("label", {}, "Mínimo", h("input", { type: "number", step: "any", value: chk.min ?? "", onchange: (e) => write("min", e.target.value === "" ? null : Number(e.target.value)) })),
    h("label", {}, "Máximo", h("input", { type: "number", step: "any", value: chk.max ?? "", onchange: (e) => write("max", e.target.value === "" ? null : Number(e.target.value)) })),
    h("label", {}, "Valores permitidos", h("input", { type: "text", placeholder: "separados por ;", value: (chk.in || []).join("; "),
      onchange: (e) => { const v = e.target.value.split(";").map((x) => x.trim()).filter(Boolean); write("in", v.length ? v : null); if (v.length) { c.check.ignore_case = true; } } })),
    h("label", {}, "Se falhar", h("select", { onchange: (e) => write("on_fail", e.target.value) },
      ON_FAIL.map(([k, l]) => h("option", { value: k, selected: (chk.on_fail || "warn") === k }, l)))));
}

/* ------------------------------------------------------ pré-visualização */
function schedulePreview(delay) {
  clearTimeout(state.timer);
  state.timer = setTimeout(runPreview, delay);
}
async function runPreview() {
  if (!state.cfg) return;
  state.inflight?.abort();
  const ctrl = new AbortController();
  state.inflight = ctrl;
  try {
    const r = await api("POST", "/api/preview", { config: state.cfg, path: state.path, samples: [...state.samples], pasted: $("#pasted").value }, ctrl.signal);
    if (ctrl.signal.aborted) return;
    renderResult(r);
  } catch (e) {
    if (e.name !== "AbortError") renderResult({ ok: false, stage: "net", error: `Sem ligação ao studio: ${e.message}` });
  }
}
const STAGE = { config: "A configuração ainda não está completa", run: "Erro ao transformar os dados de teste", pasted: "O documento colado não é JSON válido", net: "Erro" };
function renderResult(r) {
  const msg = $("#result-msg");
  const val = $("#validation");
  const v = r.validation || [];
  state.last = r;
  if (r.ok) state.result = r;
  const res = r.ok ? r : state.result;
  if (!r.ok) {
    msg.className = "result-msg"; msg.hidden = false;
    msg.replaceChildren(h("strong", {}, STAGE[r.stage] || "Erro"), ": ", friendly(r.error));
  } else {
    const notes = [];
    if (r.summary.unmatched?.length) notes.push(h("li", {}, `Não usados (não correspondem a nenhum submodelo): ${r.summary.unmatched.join(", ")}`));
    v.forEach((d) => d.errors.slice(0, 6).forEach((e) => notes.push(h("li", {}, `${d.source}: ${e.path || "(raiz)"}: ${e.message}`))));
    (r.warnings || []).filter((w) => !w.includes("não cumpre o schema")).slice(0, 6).forEach((w) => notes.push(h("li", {}, w)));
    msg.hidden = !notes.length;
    msg.className = "result-msg warn";
    msg.replaceChildren(h("strong", {}, "A rever"), h("ul", {}, notes));
  }
  if (!res) { $("#summary").textContent = "Sem resultado"; $("#grid").replaceChildren(); val.textContent = ""; return; }
  const s = res.summary;
  if (!s.documents) {
    $("#summary").textContent = "Sem dados de teste";
    $("#summary").style.opacity = "1";
    val.textContent = "Escolha dados de teste à esquerda ou cole um documento.";
    val.className = "validation";
    renderGrid(res.tables.find((x) => x.name === sanitize(table()?.name || "")) || res.tables[0]);
    return;
  }
  $("#summary").textContent = `${s.documents} documento${s.documents === 1 ? "" : "s"} de teste → ${s.rows} linha${s.rows === 1 ? "" : "s"}${r.ok ? "" : " (último resultado válido)"}`;
  $("#summary").style.opacity = r.ok ? "1" : "0.55";
  const hasModel = state.cfg?.submodels?.some((x) => x.model);
  if (!hasModel) { val.textContent = ""; val.className = "validation"; }
  else if (v.length) { val.textContent = `${v.length} documento${v.length === 1 ? "" : "s"} não cumpre${v.length === 1 ? "" : "m"} o schema`; val.className = "validation bad"; }
  else if (r.ok && s.documents) { val.textContent = "✓ Os dados de teste cumprem o schema"; val.className = "validation ok"; }
  else { val.textContent = ""; }
  const t = table();
  const tr = res.tables.find((x) => x.name === sanitize(t?.name || "")) || res.tables[0];
  renderGrid(tr);
}
function friendly(e) {
  return String(e || "")
    .replace(/submodels\[(\d+)\]\.tables\[(\d+)\]/g, (_, a, b) => `tabela ${state.cfg?.submodels?.[a]?.tables?.[b]?.name ?? b}`)
    .replace(/precisa de 'keys'/, "precisa de uma chave (escolha-a nas definições da tabela)");
}
function renderGrid(t) {
  const g = $("#grid");
  if (!t) { g.replaceChildren(h("tbody", {}, h("tr", {}, h("td", { class: "empty" }, "Sem linhas. Escolha dados de teste à esquerda.")))); return; }
  const cf = t.check_failures || {};
  const head = h("tr", {}, t.columns.map((c) => h("th", { scope: "col", class: c.name.startsWith("cx_") ? "meta-col" : null },
    h("button", { type: "button", onclick: () => { if (table()?.columns?.[c.name]) { state.ci = c.name; renderAll(); showPane("table"); } } },
      h("span", { class: "col-name" }, c.name, cf[c.name] ? ` ⚠ ${cf[c.name]}` : ""), h("span", { class: "col-type" }, typeLabel(c.type))))));
  const rows = t.rows.length ? t.rows.map((r) => h("tr", {}, t.columns.map((c) => cell(r[c.name]))))
    : [h("tr", {}, h("td", { class: "empty", colspan: t.columns.length }, "Sem linhas para os dados de teste."))];
  g.replaceChildren(h("thead", {}, head), h("tbody", {}, rows));
}
function cell(v) {
  if (v === null || v === undefined) return h("td", { class: "null" }, "vazio");
  if (typeof v === "number") return h("td", { class: "num" }, String(v));
  if (typeof v === "boolean") return h("td", {}, v ? "sim" : "não");
  if (typeof v === "object") {
    const flat = JSON.stringify(v);
    if (Array.isArray(v) && v.every((x) => x === null || typeof x !== "object") && flat.length <= 80) return h("td", { title: flat }, v.join(", "));
    return h("td", { class: "nested" }, h("details", {}, h("summary", {}, Array.isArray(v) ? `lista de ${v.length}` : `${Object.keys(v).length} campos`),
      h("pre", {}, JSON.stringify(v, null, 2))));
  }
  return h("td", { title: String(v) }, String(v));
}

/* ------------------------------------------------------------- avançado */
async function openAdvanced(text, error) {
  if (text == null) {
    try { text = (await api("POST", "/api/render", { config: state.cfg })).yaml; } catch (e) { toast(e.message, "error"); return; }
  }
  $("#adv-text").value = text;
  $("#adv-error").hidden = !error; $("#adv-error").textContent = error || "";
  $("#advanced-pane").hidden = false;
}
async function applyAdvanced() {
  try {
    const r = await api("POST", "/api/parse", { text: $("#adv-text").value });
    const keep = { path: state.path, mtime: state.mtime, isNew: state.isNew, samples: state.samples };
    setConfig(r.config, keep.path, keep.mtime, keep.isNew);
    state.samples = keep.samples;
    state.dirty = true; updateBar();
    $("#advanced-pane").hidden = true;
  } catch (e) { $("#adv-error").hidden = false; $("#adv-error").textContent = e.message; }
}

/* ---------------------------------------------------------------- geral */
function renderAll() {
  renderSamples();
  renderFields();
  renderTable();
  if (state.last) renderResult(state.last);
}
function showPane(name) {
  $(".workspace").dataset.show = name;
  document.querySelectorAll(".switcher [data-pane]").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.pane === name)));
}
document.querySelectorAll(".switcher [data-pane]").forEach((b) => b.addEventListener("click", () => showPane(b.dataset.pane)));
$("#catalog-search").addEventListener("input", renderCatalog);
$("#field-search").addEventListener("input", renderFields);
$("#pasted").addEventListener("input", () => schedulePreview(400));
$("#save").addEventListener("click", save);
$("#advanced").addEventListener("click", () => openAdvanced());
$("#adv-apply").addEventListener("click", applyAdvanced);
$("#adv-close").addEventListener("click", () => { $("#advanced-pane").hidden = true; });
document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") { e.preventDefault(); if (!$("#save").disabled) save(); }
  if (e.key === "Escape" && state.pick) { state.pick = false; renderAll(); }
});
window.addEventListener("beforeunload", (e) => { if (state.dirty) { e.preventDefault(); e.returnValue = ""; } });

(async () => {
  loadCatalog();
  try {
    await loadFiles();
    const last = store.get("cx2.last", null);
    if (last && state.files.configs.some((f) => f.path === last)) await openConfig(last);
  } catch (e) { toast(`Não foi possível ler a pasta: ${e.message}`, "error"); }
  renderAll();
})();
