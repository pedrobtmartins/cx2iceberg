"""Leitura e validação da configuração YAML."""
from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from . import athena_types as T
from . import transforms as TF
from .paths import split_explode

MODES = {"append", "overwrite", "overwrite_partitions", "merge"}
ON_ERROR = {"fail", "null", "skip_row"}
METADATA = ("source_file", "semantic_id", "ingested_at", "record_hash")


class ConfigError(ValueError):
    pass


@dataclass
class ColumnSpec:
    name: str
    path: str | None = None
    paths: list[str] | None = None          # coalesce: primeiro não-nulo
    template: str | None = None             # "{$root.catenaXId}-{key}"
    const: Any = None
    has_const: bool = False
    type: str | None = None                 # None = inferido
    transform: Any = None
    default: Any = None
    required: bool = False
    on_error: str | None = None
    comment: str | None = None
    cases: list[tuple[Any, "ColumnSpec"]] | None = None  # [(condição | None, origem)], a 1ª que passa ganha
    check: dict[str, Any] | None = None                  # validações depois do cast
    missing_elements: str | None = None    # [*]/filtro com elementos sem o campo: skip | null (sem isto: erro)
    rounding: str | None = None            # decimal: half_up, half_even, ...; double/float: nearest (sem isto: erro)
    template_missing: str | None = None    # template com só algumas partes em falta: erro | empty | null
    outside_model: bool = False            # o caminho não está no schema do modelo (não é verificado na carga)


@dataclass
class TableSpec:
    name: str
    columns: list[ColumnSpec]
    explode: list[str] = field(default_factory=list)
    keep_empty: bool = False                # explode "outer": gera linha mesmo com lista vazia
    where: Any = None
    keys: list[str] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    mode: str = "append"
    dedupe: bool = True
    auto: bool = False                      # achata automaticamente os campos escalares
    auto_exclude: list[str] = field(default_factory=list)
    auto_max_depth: int = 5
    auto_naming: str = "full"               # full (a_b_c) | leaf (c, sobe níveis só se colidir)
    auto_collision: str = "error"           # error | prefix (item_<nome>) | skip, quando o nome já existe com outra origem
    on_duplicate: str | None = None         # a mesma chave duas vezes no mesmo documento: erro | last | first
    missing_columns: str | None = None      # merge com colunas da tabela que a config não produz: erro | null
    auto_dates: bool = True                 # strings ISO-8601 -> timestamp no modo auto
    auto_lists: str = "array"               # array | json | skip  (listas de objectos -> json|skip)
    on_error: str = "fail"
    metadata_columns: list[str] = field(default_factory=list)
    replace_by: list[str] = field(default_factory=list)  # DELETE ... WHERE col IN (lote) antes de inserir
    location: str | None = None
    comment: str | None = None
    athena_options: dict[str, Any] = field(default_factory=dict)
    # auto_naming: leaf com model.schema: nomes fixados a partir do modelo (ver pathcheck.model_leaf_paths)
    leaf_names: Any = field(default=None, repr=False, compare=False)


@dataclass
class SubmodelSpec:
    name: str
    match: dict[str, Any]
    tables: list[TableSpec]
    auto_normalize: dict[str, Any] | None = None
    defaults: dict[str, Any] = field(default_factory=dict)
    common_columns: list[ColumnSpec] = field(default_factory=list)
    unwrap: dict[str, Any] | None = None
    document_id: dict[str, Any] | None = None  # UUID por documento, em todas as tabelas do submodelo
    model: dict[str, Any] | None = None        # {schema, example}: modelo SAMM de origem (caminhos relativos à config)
    validate: str = "off"                      # off | warn | skip | fail: valida cada documento contra o schema
    walker: Any = field(default=None, repr=False, compare=False)   # o schema do model, já lido (pathcheck)


@dataclass
class Config:
    target: dict[str, Any]
    submodels: list[SubmodelSpec]
    input: dict[str, Any]
    defaults: dict[str, Any]
    base_dir: Path | None = None               # pasta da configuração (para caminhos relativos)


# --- YAML 1.2 ------------------------------------------------------------------------
class YamlLoader(yaml.SafeLoader):
    """YAML 1.2 (core schema) e sem chaves repetidas.

    O PyYAML segue o YAML 1.1: `yes/no/on/off` viram booleanos (`map: {yes: Y, on: L}` ficava {True: 'L'} —
    duas traduções perdidas), `010` é 8 (octal) e `1:30` é 90 (sexagesimal). E uma chave repetida (ex.: a mesma
    coluna duas vezes) ficava com o último valor sem aviso. Aqui só `true/false` são booleanos, `010` é 10,
    `0o17` e `0x1A` são octal e hexadecimal, e uma chave repetida é erro. Datas sem aspas (`2025-01-01`)
    ficam texto, como no YAML 1.2: passam pelas mesmas regras de datas dos documentos.
    """


YamlLoader.yaml_implicit_resolvers = {
    k: [(tag, rx) for tag, rx in v if tag not in ("tag:yaml.org,2002:bool", "tag:yaml.org,2002:int",
                                                   "tag:yaml.org,2002:float", "tag:yaml.org,2002:null",
                                                   "tag:yaml.org,2002:timestamp")]
    for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()}
for _tag, _rx, _first in [
    ("tag:yaml.org,2002:bool", r"^(?:true|True|TRUE|false|False|FALSE)$", "tTfF"),
    ("tag:yaml.org,2002:null", r"^(?:~|null|Null|NULL|)$", ["~", "n", "N", ""]),
    ("tag:yaml.org,2002:int", r"^(?:[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+)$", "-+0123456789"),
    ("tag:yaml.org,2002:float", r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?|[-+]?\.(?:inf|Inf|INF)"
                                r"|\.(?:nan|NaN|NAN))$", "-+0123456789."),
]:
    YamlLoader.add_implicit_resolver(_tag, re.compile(_rx), list(_first))


def _yaml_int(loader, node):
    text = loader.construct_scalar(node)
    if text.startswith("0o"):
        return int(text[2:], 8)
    if text.startswith("0x"):
        return int(text[2:], 16)
    return int(text)                                   # '010' -> 10 (YAML 1.2)


def _yaml_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    seen = {}
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            hash(key)
        except TypeError:
            continue
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"chave repetida {key!r} (a anterior está na linha {seen[key] + 1})", key_node.start_mark)
        seen[key] = key_node.start_mark.line
    return loader.construct_mapping(node, deep=deep)


YamlLoader.add_constructor("tag:yaml.org,2002:int", _yaml_int)
YamlLoader.add_constructor("tag:yaml.org,2002:map", _yaml_mapping)


def load_yaml(text: str) -> Any:
    return yaml.load(text, Loader=YamlLoader)  # noqa: S506 - YamlLoader deriva do SafeLoader


def _flag(value: Any, where: str, default: bool) -> bool:
    """Opção booleana: só true/false. Antes era bool(valor): 'false' e 'no' (texto) davam True."""
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigError(f"{where}: use true ou false (recebeu {value!r})")
    return value


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand_env(text: str) -> str:
    def repl(m):
        val = os.environ.get(m.group(1), m.group(2))
        if val is None:
            raise ConfigError(f"Variável de ambiente '{m.group(1)}' não definida (use ${{{m.group(1)}:-valor}})")
        return val

    return _ENV_RE.sub(repl, text)


def sanitize(name: str) -> str:
    """Nome válido para Athena: minúsculas, [a-z0-9_], camelCase -> snake_case."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name))
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", s).strip("_").lower()
    s = re.sub(r"_+", "_", s)
    if not s:
        raise ConfigError(f"Nome inválido: '{name}'")
    return f"c_{s}" if s[0].isdigit() else s


def _raw_column_names(spec: Any) -> list[str]:
    if isinstance(spec, dict):
        return [str(k) for k in spec]
    if isinstance(spec, list):
        return [c if isinstance(c, str) else str(next(iter(c))) for c in spec]
    return []


def _check_keys(d: dict, allowed: set[str], where: str):
    unknown = set(d) - allowed
    if unknown:
        raise ConfigError(f"{where}: chave(s) desconhecida(s) {sorted(unknown)}. Permitidas: {sorted(allowed)}")


_COL_KEYS = {"path", "paths", "template", "const", "type", "transform", "default", "required", "on_error", "comment",
             "cases", "check", "missing_elements", "outside_model", "rounding", "template_missing"}
_SOURCES = ("path", "paths", "template", "const")
_CHECK_KEYS = {"regex", "min", "max", "in", "ignore_case", "required", "required_if", "on_fail"}
CHECK_ON_FAIL = {"warn", "null", "skip_row", "fail"}


def _condition(cond: Any, where: str) -> Any:
    """Valida uma condição (estrutura, operador, tipo do value, regex) ao carregar."""
    from .conditions import validate  # noqa: PLC0415

    try:
        validate(cond, where)
    except ValueError as e:
        raise ConfigError(str(e)) from None
    return cond


def parse_cases(name: str, spec: Any, where: str) -> list[tuple[Any, ColumnSpec]]:
    """cases: [{when: <condição>, path|paths|template|const: ..., transform: ...}, ..., {else: valor}]"""
    if not isinstance(spec, list) or not spec:
        raise ConfigError(f"{where}: 'cases' tem de ser uma lista não vazia")
    out = []
    for i, case in enumerate(spec):
        w = f"{where}[{i}]"
        if not isinstance(case, dict):
            raise ConfigError(f"{w}: cada caso é um objecto com when + origem, ou {{else: valor}}")
        if "else" in case:
            if len(case) != 1:
                raise ConfigError(f"{w}: 'else' não se combina com outras chaves (use {{path: ...}} sem 'when')")
            out.append((None, ColumnSpec(name, const=case["else"], has_const=True)))
            continue
        _check_keys(case, {"when", "transform", *_SOURCES}, w)
        if not any(k in case for k in _SOURCES):
            raise ConfigError(f"{w}: falta a origem do valor (path, paths, template ou const)")
        body = {k: v for k, v in case.items() if k != "when"}
        out.append((_condition(case.get("when"), f"{w}.when"), parse_column(name, body, w)))
    return out


def parse_check(spec: Any, where: str) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise ConfigError(f"{where}: 'check' tem de ser um objecto")
    _check_keys(spec, _CHECK_KEYS, where)
    chk = dict(spec)
    chk["ignore_case"] = _flag(chk.get("ignore_case"), f"{where}.ignore_case", False)
    chk.setdefault("on_fail", "warn")
    if chk["on_fail"] not in CHECK_ON_FAIL:
        raise ConfigError(f"{where}.on_fail deve ser um de {sorted(CHECK_ON_FAIL)}")
    if "regex" in chk:
        try:
            chk["_regex"] = re.compile(chk["regex"], re.IGNORECASE if chk.get("ignore_case") else 0)
        except re.error as e:
            raise ConfigError(f"{where}.regex inválido: {e}") from e
    if "in" in chk:
        from .transforms import typed_key  # noqa: PLC0415

        vals = chk["in"] if isinstance(chk["in"], list) else [chk["in"]]
        if not vals or any(v is None or isinstance(v, (list, dict)) for v in vals):
            raise ConfigError(f"{where}.in: use uma lista de valores simples")
        # comparação com tipo: 2 e 2.0 são iguais (antes '2.0' != '2'); o número 2 e o texto '2' não
        chk["_in"] = {typed_key(v, chk["ignore_case"]) for v in vals}
    for k in ("min", "max"):
        if k in chk and (isinstance(chk[k], bool) or not isinstance(chk[k], (int, float))):
            raise ConfigError(f"{where}.{k}: tem de ser um número")
    if "min" in chk and "max" in chk and chk["min"] > chk["max"]:
        raise ConfigError(f"{where}: min maior do que max")
    if "required_if" in chk:
        _condition(chk["required_if"], f"{where}.required_if")
    return chk


_SHORT_TYPE = re.compile(r"\s*::\s*([a-z0-9_<>(),\s]+?)\s*$", re.IGNORECASE)
_SHORT_TF = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\((.*)\))?\s*$")


def parse_short(spec: str, where: str) -> dict[str, Any]:
    """Forma compacta: 'caminho | transf | transf(arg) ::tipo'.

    Em YAML sem aspas, '::' tem de vir colado ao tipo ('::int'): ': ' iniciaria um mapa.
    """
    out: dict[str, Any] = {}
    m = _SHORT_TYPE.search(spec)
    if m:
        spec, out["type"] = spec[:m.start()], m.group(1)
    path, *tfs = [s.strip() for s in spec.split(" | ")]
    out["path"] = path
    chain = []
    for tf in tfs:
        m = _SHORT_TF.match(tf)
        if not m:
            raise ConfigError(f"{where}: transformação inválida '{tf}' (use nome ou nome(arg))")
        chain.append(m.group(1) if m.group(2) is None else {m.group(1): load_yaml(m.group(2))})
    if chain:
        out["transform"] = chain
    return out


def parse_column(name: str, spec: Any, where: str) -> ColumnSpec:
    col_name = sanitize(name)
    if isinstance(spec, str):
        spec = parse_short(spec, f"{where}.{name}")
    if spec is None:
        return ColumnSpec(col_name, path=name)  # coluna: ~  -> usa o próprio nome como caminho
    if not isinstance(spec, dict):
        return ColumnSpec(col_name, const=spec, has_const=True)
    _check_keys(spec, _COL_KEYS, f"{where}.{name}")
    sources = [k for k in ("path", "paths", "template", "const", "cases") if k in spec]
    if len(sources) > 1:
        raise ConfigError(f"{where}.{name}: use apenas um de path/paths/template/const/cases")
    col = ColumnSpec(
        col_name,
        path=spec.get("path"),
        paths=spec.get("paths"),
        template=spec.get("template"),
        const=spec.get("const"),
        has_const="const" in spec,
        type=T.normalize(spec["type"]) if spec.get("type") else None,
        transform=spec.get("transform"),
        default=spec.get("default"),
        required=_flag(spec.get("required"), f"{where}.{name}.required", False),
        on_error=spec.get("on_error"),
        comment=spec.get("comment"),
        cases=parse_cases(col_name, spec["cases"], f"{where}.{name}.cases") if "cases" in spec else None,
        check=parse_check(spec["check"], f"{where}.{name}.check") if "check" in spec else None,
        missing_elements=spec.get("missing_elements"),
        outside_model=_flag(spec.get("outside_model"), f"{where}.{name}.outside_model", False),
        rounding=spec.get("rounding"),
        template_missing=spec.get("template_missing"),
    )
    if col.template_missing is not None:
        has_template = col.template is not None or any(c.template is not None for _, c in col.cases or [])
        if col.template_missing not in ("empty", "null") or not has_template:
            raise ConfigError(f"{where}.{name}: template_missing aceita empty ou null, numa coluna com template")
    _check_rounding(col, f"{where}.{name}")
    if col.missing_elements not in (None, "skip", "null"):
        raise ConfigError(f"{where}.{name}: missing_elements deve ser skip ou null")
    try:
        TF.check_chain(col.transform)
    except ValueError as e:
        raise ConfigError(f"{where}.{name}: {e}") from e
    if not sources and col.transform is None:
        col.path = name
    if col.on_error and col.on_error not in ON_ERROR:
        raise ConfigError(f"{where}.{name}: on_error deve ser um de {sorted(ON_ERROR)}")
    return col


def _check_rounding(col: ColumnSpec, where: str) -> None:
    """`rounding` só faz sentido com um tipo que arredonda: decimal (modo) ou double/float (nearest)."""
    if col.rounding is None:
        return
    from .numbers import MODES  # noqa: PLC0415

    leaves = set(re.findall(r"decimal|double|float", col.type or ""))
    if not leaves:
        raise ConfigError(f"{where}: rounding só se aplica a colunas decimal, double ou float (type: {col.type})")
    allowed = set(MODES) if "decimal" in leaves else {"nearest"}
    if col.rounding not in allowed:
        raise ConfigError(f"{where}: rounding {col.rounding!r} inválido para {col.type}; use um de {sorted(allowed)}")


def parse_columns(spec: Any, where: str) -> list[ColumnSpec]:
    if spec is None:
        return []
    if isinstance(spec, list):  # lista de nomes = caminhos com o mesmo nome
        return [parse_column(c, None, where) if isinstance(c, str) else parse_column(*next(iter(c.items())), where) for c in spec]
    return [parse_column(k, v, where) for k, v in spec.items()]


_TABLE_KEYS = {
    "name", "columns", "explode", "keep_empty", "where", "keys", "partition_by", "mode", "dedupe",
    "auto", "auto_exclude", "auto_max_depth", "auto_lists", "auto_dates", "auto_collision", "auto_naming", "on_error", "metadata_columns",
    "location", "comment", "athena_options", "replace_by", "on_duplicate", "missing_columns",
}
# opções que a configuração decide: passadas em athena_options mudavam o modo, os tipos ou as chaves sem se ver
_RESERVED_ATHENA = {"df", "database", "table", "temp_path", "table_location", "partition_cols", "dtype", "mode",
                    "merge_cols", "merge_condition", "merge_match_nulls", "schema_evolution", "fill_missing_columns_in_df",
                    "keep_files", "workgroup", "boto3_session"}


def _metadata(value: Any) -> list[str]:
    if value is True:
        return list(METADATA)
    if not value:
        return []
    bad = set(value) - set(METADATA)
    if bad:
        raise ConfigError(f"metadata_columns desconhecidas: {sorted(bad)}. Permitidas: {METADATA}")
    return list(value)


def _check_write_options(mode: str, partition_by: list, athena_options: Any, missing_columns: Any, where: str) -> None:
    """Opções da escrita, iguais para tabelas explícitas e para as geradas pelo auto_normalize (a partir de defaults)."""
    if mode not in MODES:
        raise ConfigError(f"{where}: mode '{mode}' inválido. Use {sorted(MODES)}")
    if missing_columns not in (None, "null"):
        raise ConfigError(f"{where}: missing_columns só aceita null (em merge, anula as colunas da tabela que a "
                          f"configuração não produz)")
    if not isinstance(athena_options, dict):
        raise ConfigError(f"{where}.athena_options tem de ser um mapa")
    reserved = set(athena_options) & _RESERVED_ATHENA
    if reserved:
        raise ConfigError(f"{where}.athena_options: {sorted(reserved)} são decididas pela configuração "
                          f"(mode, keys, partition_by, tipos...) e não podem ser passadas aqui")
    if mode == "overwrite_partitions":
        if not partition_by:
            raise ConfigError(f"{where}: overwrite_partitions precisa de partition_by")
        transformed = [p for p in partition_by if "(" in p]
        if transformed:
            raise ConfigError(f"{where}: overwrite_partitions só funciona com partições por coluna, não {transformed} "
                              f"(o DELETE das partições usa os valores das colunas)")


def parse_table(spec: dict, defaults: dict, common: list[ColumnSpec], where: str) -> TableSpec:
    _check_keys(spec, _TABLE_KEYS, where)
    if "name" not in spec:
        raise ConfigError(f"{where}: falta 'name'")
    merged = {**defaults, **spec}
    explicit = parse_columns(spec.get("columns"), f"{where}.columns")
    seen_names: dict[str, str] = {}
    for raw_name, c in zip(_raw_column_names(spec.get("columns")), explicit):
        if c.name in seen_names:   # ex.: fossilGhg e fossil_ghg -> fossil_ghg: antes ficava só a última
            raise ConfigError(f"{where}.columns: '{seen_names[c.name]}' e '{raw_name}' dão a mesma coluna '{c.name}'")
        seen_names[c.name] = raw_name
    names = {c.name for c in explicit}
    columns = [c for c in common if c.name not in names] + explicit
    t = TableSpec(
        name=sanitize(spec["name"]),
        columns=columns,
        explode=split_explode(spec["explode"]) if spec.get("explode") else [],
        keep_empty=_flag(merged.get("keep_empty"), f"{where}.keep_empty", False),
        where=_condition(spec.get("where"), f"{where}.where"),
        keys=[sanitize(k) for k in merged.get("keys", []) or []],
        partition_by=list(merged.get("partition_by", []) or []),
        mode=merged.get("mode", "append"),
        dedupe=_flag(merged.get("dedupe"), f"{where}.dedupe", True),
        auto=_flag(merged.get("auto"), f"{where}.auto", False),
        auto_exclude=list(merged.get("auto_exclude", []) or []),
        auto_max_depth=int(merged.get("auto_max_depth", 5)),
        auto_lists=merged.get("auto_lists", "array"),
        auto_dates=_flag(merged.get("auto_dates"), f"{where}.auto_dates", True),
        auto_collision=merged.get("auto_collision", "error"),
        on_duplicate=merged.get("on_duplicate"),
        missing_columns=merged.get("missing_columns"),
        auto_naming=merged.get("auto_naming", "full"),
        on_error=merged.get("on_error", "fail"),
        metadata_columns=_metadata(merged.get("metadata_columns", False)),
        replace_by=[sanitize(k) for k in merged.get("replace_by", []) or []],
        location=spec.get("location"),
        comment=spec.get("comment"),
        athena_options={**defaults.get("athena_options", {}), **spec.get("athena_options", {})},
    )
    if t.mode not in MODES:
        raise ConfigError(f"{where}: mode '{t.mode}' inválido. Use {sorted(MODES)}")
    if t.mode == "merge" and not t.keys:
        raise ConfigError(f"{where}: mode 'merge' precisa de 'keys'")
    if t.on_error not in ON_ERROR:
        raise ConfigError(f"{where}: on_error deve ser um de {sorted(ON_ERROR)}")
    if t.auto_naming not in ("full", "leaf"):
        raise ConfigError(f"{where}: auto_naming deve ser full|leaf")
    if t.auto_lists not in ("array", "json", "skip"):
        raise ConfigError(f"{where}: auto_lists deve ser array|json|skip")
    if t.auto_collision not in ("error", "prefix", "skip"):
        raise ConfigError(f"{where}: auto_collision deve ser error|prefix|skip")
    if t.on_duplicate is not None:
        if t.on_duplicate not in ("last", "first"):
            raise ConfigError(f"{where}: on_duplicate deve ser last ou first")
        if not t.keys:
            raise ConfigError(f"{where}: on_duplicate precisa de keys")
    _check_write_options(t.mode, t.partition_by, t.athena_options, t.missing_columns, where)
    if t.mode == "merge" and not t.dedupe:
        raise ConfigError(f"{where}: mode merge com dedupe: false não é possível (o MERGE precisa de chaves únicas no lote)")
    return t


_SUBMODEL_KEYS = {"name", "match", "tables", "auto_normalize", "defaults", "common_columns", "unwrap", "document_id",
                  "model", "validate"}
VALIDATE_MODES = {"off", "warn", "skip", "fail"}
DOC_ID_STRATEGIES = {"key", "content", "random"}


def parse_document_id(spec: Any, where: str) -> dict[str, Any] | None:
    """document_id: true  |  {column: doc_id, from: [catenaXId], strategy: key|content|random, namespace: ...}"""
    if not spec:
        return None
    if spec is True:
        spec = {}
    if not isinstance(spec, dict):
        raise ConfigError(f"{where}: use true ou um objecto {{column, from, strategy, namespace}}")
    _check_keys(spec, {"column", "from", "strategy", "namespace", "on_missing"}, where)
    raw_src = spec.get("from")
    raw_src = [raw_src] if isinstance(raw_src, (str, dict)) else list(raw_src or [])
    src: list[tuple[str, str | None]] = []
    for i, item in enumerate(raw_src):
        # 'catenaXId' (texto) ou {path: version, type: int}: números e booleanos precisam de tipo
        if isinstance(item, str):
            src.append((item, None))
        elif isinstance(item, dict) and isinstance(item.get("path"), str) and set(item) <= {"path", "type"}:
            src.append((item["path"], T.normalize(item["type"]) if item.get("type") else None))
        else:
            raise ConfigError(f"{where}.from[{i}]: use um caminho ou {{path: ..., type: ...}}")
    if spec.get("on_missing", "fail") not in ("fail", "content"):
        raise ConfigError(f"{where}.on_missing deve ser fail ou content")
    strategy = spec.get("strategy", "key" if src else "content")
    if strategy not in DOC_ID_STRATEGIES:
        raise ConfigError(f"{where}.strategy deve ser um de {sorted(DOC_ID_STRATEGIES)}")
    if strategy == "key" and not src:
        raise ConfigError(f"{where}: strategy 'key' precisa de 'from' (caminhos dos campos que identificam o documento)")
    return {"column": sanitize(spec.get("column", "document_id")), "from": src, "strategy": strategy,
            "namespace": str(spec.get("namespace", "cx2iceberg")), "on_missing": spec.get("on_missing", "fail")}


def parse_unwrap(spec: Any, where: str) -> dict[str, Any] | None:
    """unwrap: [a.b, c]  |  "*"  |  {all: true, paths: [...], except: [...], on_multiple: fail|first|keep}"""
    if not spec:
        return None
    if spec == "*" or spec is True:
        spec = {"all": True}
    elif isinstance(spec, list):
        spec = {"paths": spec}
    _check_keys(spec, {"all", "paths", "except", "on_multiple"}, where)
    rule = {
        "all": _flag(spec.get("all"), f"{where}.all", False),
        "paths": set(spec.get("paths", []) or []),
        "except": set(spec.get("except", []) or []),
        "on_multiple": spec.get("on_multiple", "fail"),
    }
    if rule["on_multiple"] not in ("fail", "first", "keep"):
        raise ConfigError(f"{where}.on_multiple deve ser fail|first|keep")
    bad = [p for p in rule["paths"] | rule["except"] if "[" in p]
    if bad:
        raise ConfigError(f"{where}: caminhos de unwrap não levam índices: {bad}")
    return rule


_DEFAULT_KEYS = _TABLE_KEYS - {"name", "columns", "explode", "where", "location", "comment"}
_INPUT_KEYS = {"semantic_id", "semantic_id_map", "envelope", "list_is_document", "match_all", "naive_timestamps",
               "on_unmatched"}


def _parse_input(inp: Any) -> dict[str, Any]:
    """Valida `input:` (antes uma chave mal escrita, ex. naive_timestamp, era ignorada sem aviso)."""
    if not isinstance(inp, dict):
        raise ConfigError("input: tem de ser um objecto")
    _check_keys(inp, _INPUT_KEYS, "input")
    if "envelope" in inp:
        if not isinstance(inp["envelope"], dict):
            raise ConfigError("input.envelope: use {payload_path: ..., semantic_id_path: ...}")
        _check_keys(inp["envelope"], {"payload_path", "semantic_id_path"}, "input.envelope")
    for k in ("list_is_document", "match_all"):
        _flag(inp.get(k), f"input.{k}", False)
    if inp.get("on_unmatched", "fail") not in ("fail", "skip"):
        raise ConfigError("input.on_unmatched deve ser fail ou skip")
    from .dates import check_policy  # noqa: PLC0415

    try:
        check_policy(str(inp.get("naive_timestamps", "fail")))
    except ValueError as e:
        raise ConfigError(f"input.naive_timestamps: {e}") from None
    return dict(inp)


def _match_spec(m: Any, where: str) -> dict:
    """Valida o match: uma chave mal escrita (ex.: sematic_id) era ignorada e o submodelo aceitava tudo."""
    if not isinstance(m, dict):
        raise ConfigError(f"{where}: tem de ser um objecto")
    _check_keys(m, {"semantic_id", "file", "has_paths", "where", "any"}, where)
    if "any" in m:
        if not isinstance(m["any"], list) or not m["any"]:
            raise ConfigError(f"{where}.any: use uma lista de blocos de match")
        for i, x in enumerate(m["any"]):
            _match_spec(x, f"{where}.any[{i}]")
    if "where" in m:
        _condition(m["where"], f"{where}.where")
    return m


def _check_replace_by(subs: list) -> None:
    """replace_by: colunas definidas na tabela (ou a do document_id) e calculáveis ao nível do documento
    ($root., $meta.): a chave é precisa também quando o documento não gera linhas (lista que ficou vazia)."""
    from .pathcheck import column_paths  # noqa: PLC0415

    for sub in subs:
        doc_id = (sub.document_id or {}).get("column")
        for t in sub.tables:
            cols = {c.name: c for c in t.columns}
            for name in t.replace_by:
                if name == doc_id:
                    continue
                if name not in cols:
                    raise ConfigError(f"tabela '{t.name}': replace_by '{name}' não é uma coluna declarada da tabela")
                if not t.explode:
                    continue
                bad = [p for p, _ in column_paths(cols[name]) if not p.strip().startswith(("$root", "$meta"))]
                if bad:
                    raise ConfigError(f"tabela '{t.name}': a coluna '{name}' do replace_by depende do elemento do explode "
                                      f"({bad}); use caminhos $root./$meta. (identifica o documento, não a linha)")


_SHARED = ("mode", "keys", "partition_by", "replace_by", "dedupe", "on_duplicate", "location", "metadata_columns")


def _check_shared_tables(subs: list) -> None:
    """Uma tabela alimentada por várias regras (ex.: PCF v7 e v9) tem de ter o mesmo modo, chaves e partições,
    e o mesmo tipo em cada coluna comum. Antes valia a 1.ª regra em silêncio (um decimal da 2.ª virava double)."""
    seen: dict[str, tuple[str, TableSpec]] = {}
    problems = []
    for sub in subs:
        for t in sub.tables:
            where = f"'{sub.name}'.tables['{t.name}']"
            if t.name not in seen:
                seen[t.name] = (where, t)
                continue
            first_where, first = seen[t.name]
            for attr in _SHARED:
                if getattr(first, attr) != getattr(t, attr):
                    problems.append(f"tabela '{t.name}': {attr} {getattr(first, attr)!r} em {first_where} e "
                                    f"{getattr(t, attr)!r} em {where}")
            a = {c.name: (c.type, c.rounding) for c in first.columns}
            for c in t.columns:
                if c.name in a and a[c.name] != (c.type, c.rounding):
                    problems.append(f"tabela '{t.name}', coluna '{c.name}': tipo {a[c.name][0]!r} em {first_where} e "
                                    f"{c.type!r} em {where} (rounding {a[c.name][1]!r} / {c.rounding!r})")
    if problems:
        raise ConfigError("regras diferentes para a mesma tabela:\n  " + "\n  ".join(problems))


def load(path: str | Path) -> Config:
    text = _expand_env(Path(path).read_text(encoding="utf-8"))
    try:
        raw = load_yaml(text) or {}
    except yaml.YAMLError as e:
        hint = ""
        if "mapping values are not allowed" in str(e):
            hint = " Dica: na forma compacta escreva '::int' (sem espaço depois de '::') ou ponha o valor entre aspas."
        raise ConfigError(f"YAML inválido em {path}: {e}.{hint}") from e
    return from_dict(raw, Path(path).resolve().parent)


def _auto_normalize(an: dict, defaults: dict, model: Any, where: str) -> dict:
    """Valida o auto_normalize e resolve `children`: de onde vêm as tabelas filhas.

    schema: do model.schema (todas as listas do modelo, haja ou não dados no lote) — omissão com model.schema;
    batch: só das listas presentes no lote. Sem model.schema e com um modo que apaga ou substitui (merge: replace_by
    nas filhas; overwrite...), uma lista que ficou vazia em todos os documentos do lote não gerava a tabela e as
    linhas antigas ficavam: tem de ser declarado (children: batch), e fica contado em cada carga."""
    naming = an.get("naming", "full")
    if naming not in ("full", "leaf"):
        raise ConfigError(f"{where}.naming deve ser full ou leaf")
    if naming == "leaf" and not model:
        raise ConfigError(f"{where}: naming: leaf precisa de model.schema: sem ele os nomes das tabelas e colunas "
                          f"dependem das listas e campos presentes em cada lote/documento. Declare model.schema ou use "
                          f"naming: full")
    children = an.get("children")
    if children not in (None, "schema", "batch"):
        raise ConfigError(f"{where}.children deve ser schema ou batch")
    if children == "schema" and not model:
        raise ConfigError(f"{where}: children: schema precisa de model.schema")
    mode = defaults.get("mode", "merge")
    if children is None and not model and mode != "append":
        raise ConfigError(f"{where}: sem model.schema as tabelas filhas vêm só das listas presentes em cada lote: com "
                          f"mode {mode}, uma lista que ficou vazia em todos os documentos do lote não apagaria as linhas "
                          f"antigas. Declare model.schema (recomendado) ou auto_normalize.children: batch")
    return {**an, "children": children or ("schema" if model else "batch")}


def from_dict(raw: dict, base_dir: str | Path | None = None) -> Config:
    _check_keys(raw, {"target", "input", "defaults", "submodels", "plugins"}, "config")
    for mod in raw.get("plugins", []) or []:
        importlib.import_module(mod)
    target = raw.get("target", {}) or {}
    _check_keys(target, {"database", "location", "temp_path", "workgroup", "region", "profile", "create_database", "schema_evolution"}, "target")
    defaults = raw.get("defaults", {}) or {}
    _check_keys(defaults, _DEFAULT_KEYS, "defaults")
    inp = _parse_input(raw.get("input", {}) or {})
    for k in ("create_database", "schema_evolution"):
        if k in target:
            _flag(target[k], f"target.{k}", True)
    subs = []
    for i, s in enumerate(raw.get("submodels", []) or []):
        where = f"submodels[{i}]"
        _check_keys(s, _SUBMODEL_KEYS, where)
        name = s.get("name") or f"submodel_{i}"
        sdefaults = {**defaults, **(s.get("defaults") or {})}
        common = parse_columns(s.get("common_columns"), f"{where}.common_columns")
        tables = [parse_table(t, sdefaults, common, f"{where}.tables[{j}]") for j, t in enumerate(s.get("tables", []) or [])]
        an = s.get("auto_normalize")
        if an is True:
            an = {}
        if an is not None:
            _check_keys(an, {"table_prefix", "id_path", "id_column", "exclude", "max_depth", "naming", "children"},
                        f"{where}.auto_normalize")
            # as tabelas geradas só existem durante a carga: as opções de escrita dos defaults verificam-se aqui
            # (antes, defaults.athena_options: {mode: overwrite} passava e substituía a tabela raiz do merge)
            _check_write_options(sdefaults.get("mode", "merge"), list(sdefaults.get("partition_by", []) or []),
                                 sdefaults.get("athena_options", {}) or {}, sdefaults.get("missing_columns"),
                                 f"{where}.defaults (auto_normalize)")
            an = _auto_normalize(an, sdefaults, s.get("model"), f"{where}.auto_normalize")
        if not tables and an is None:
            raise ConfigError(f"{where}: defina 'tables' e/ou 'auto_normalize'")
        model = s.get("model")
        leafy = [t.name for t in tables if t.auto and t.auto_naming == "leaf"]
        if leafy and not model:
            raise ConfigError(f"{where}: auto_naming: leaf (tabelas {leafy}) precisa de model.schema: sem ele o nome de "
                              f"cada coluna depende dos campos presentes em cada documento (o mesmo campo ia para "
                              f"'fossil' num documento e 'ghg_fossil' noutro). Declare model.schema ou use auto_naming: full")
        if model is not None:
            if not isinstance(model, dict) or "schema" not in model:
                raise ConfigError(f"{where}.model: indique pelo menos 'schema' (e opcionalmente 'example')")
            _check_keys(model, {"schema", "example", "id", "version"}, f"{where}.model")
        validate = s.get("validate", "warn" if model else "off")
        if validate is True:
            validate = "warn"
        if validate is False:
            validate = "off"
        if validate not in VALIDATE_MODES:
            raise ConfigError(f"{where}.validate deve ser um de {sorted(VALIDATE_MODES)}")
        if validate != "off" and not model:
            raise ConfigError(f"{where}.validate precisa de model.schema")
        match = _match_spec(s.get("match") or {}, f"{where}.match")
        subs.append(SubmodelSpec(name, match, tables, an, sdefaults, common,
                                 parse_unwrap(s.get("unwrap"), f"{where}.unwrap"),
                                 parse_document_id(s.get("document_id"), f"{where}.document_id"),
                                 model, validate))
    if not subs:
        raise ConfigError("config: não há 'submodels'")
    _check_shared_tables(subs)
    _check_replace_by(subs)
    cfg = Config(target, subs, inp, defaults, Path(base_dir) if base_dir else None)
    from .pathcheck import check_config  # noqa: PLC0415 - evitar import circular
    check_config(cfg)   # caminhos, $parent/$index/$meta e (com model.schema) o modelo: tudo antes de ler dados
    return cfg
