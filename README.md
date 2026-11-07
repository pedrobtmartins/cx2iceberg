# cx2iceberg

Turns **Catena-X** submodels (value-only JSON: SerialPart, SingleLevelBomAsBuilt, PCF, Batch, …) into **Apache Iceberg tables in Amazon Athena**, through rules declared in a YAML file.

```
submodel JSON ──► match (semantic id / file / fields) ──► rules per table ──► Athena (Iceberg)
                                                           explode, filters,
                                                           transformations, types
```

## Quick start

```bash
pip install -e ".[aws,parquet]"
```

1. Generate a configuration. It is best to start from the **SAMM model's JSON Schema** (the `gen/` folder of each model in [eclipse-tractusx/sldt-semantic-models](https://github.com/eclipse-tractusx/sldt-semantic-models)), because it includes every field and type, even those that do not appear in an example. You can add example JSONs to it:

```bash
cx2iceberg init --schema Pcf-schema.json Pcf.json --name pcf9 --unwrap all --keep-list attestationOfConformance --naming leaf -o config.yaml
```

`init` options: `--unwrap all|a.b,c.d`, `--keep-list` (with `--unwrap all`, lists that really have several elements), `--naming full|leaf`, `--style flat|struct`, `--scalar-lists array|rows`, `--id-path`.

2. Review `config.yaml` (names, types, keys) and test without touching AWS:

```bash
cx2iceberg run -c config.yaml example.json --dry-run
```

3. Write to Athena:

```bash
cx2iceberg run -c config.yaml s3://bucket/input/serial_part/
```

Each `run` (and `ddl`) prints the **load report** on stderr: see [Load report](#load-report-and-exit-codes).

Other commands: `cx2iceberg validate -c config.yaml`, `cx2iceberg ddl -c config.yaml data/` (shows the Iceberg `CREATE TABLE`), `cx2iceberg transforms` (lists the transformations), `run --out folder --format csv|parquet|jsonl` (writes locally).

The file [examples/config.yaml](examples/config.yaml) covers almost every feature with SerialPart, BOM and PCF.

No value is changed or lost without a warning: an undeclared loss is an error, and a loss declared in the configuration is allowed and counted in the [load report](#load-report-and-exit-codes). The behaviour changes, and what to do with existing configurations, are in [CHANGELOG.md](CHANGELOG.md).

## Configuration structure

```yaml
target:                     # Athena destination
  database: catenax
  location: s3://bucket/warehouse      # each table in <location>/<table>/
  temp_path: s3://bucket/tmp
  workgroup: primary
  region: eu-west-1
  # profile, create_database (true), schema_evolution (true)

input:                      # optional: where the semantic id comes from
  semantic_id: urn:...                 # fixed, or
  semantic_id_map: {"*serial*": urn:...}   # by file name, or
  envelope: {payload_path: payload, semantic_id_path: semanticId}
  match_all: false                     # a document can feed several submodels
  on_unmatched: fail                   # document without a submodel: fail (rejected) | skip (ignored and counted)
  naive_timestamps: fail               # date/time without a time zone: fail | utc | IANA time zone (e.g. Europe/Berlin)

defaults:                   # applied to every table (can be overridden)
  metadata_columns: true
  on_error: fail

plugins: [my_module]        # optional: your own transformations

submodels:
  - name: serial_part
    match: {semantic_id: "urn:samm:io.catenax.serial_part:3.*"}
    defaults: {mode: merge}            # defaults for this submodel only
    common_columns:                    # columns repeated in every table of the submodel
      catenax_id: $root.catenaXId | urn_suffix
    tables: [...]
    auto_normalize: {...}              # optional: generates tables on its own
```

`${VAR}` or `${VAR:-default}` values are read from environment variables — the same configuration serves dev/qa/prod.

The YAML is read as **YAML 1.2**: only `true`/`false` are booleans (`yes`, `no`, `on`, `off` are text — in YAML 1.1, `map: {yes: Y, no: N, on: L, off: D}` kept only two translations), `010` is 10, `0o17` and `0x1A` are octal and hexadecimal, and a repeated key (for example the same column twice) is an error. Boolean options (`dedupe`, `keep_empty`, `required`, …) only accept `true`/`false`: before, `dedupe: "false"` was true. The keys of `input:` are validated (`naive_timestamps: fail | utc`, …).

### Reading the files

A file that cannot be read **does not stop the load**: it becomes a rejected document, with the reason, and the others go on (in JSON Lines the unit is the line). The JSON is read in strict mode, so that no value is swapped without a warning:

| Input | Result |
|---|---|
| not UTF-8 (Latin-1, Windows-1252, UTF-16…) | rejected, with the byte and the position; the encoding **is not guessed** (guessing would swap text). A UTF-8 BOM is accepted |
| `NaN`, `Infinity`, `-Infinity` | rejected (not JSON) |
| a repeated key in the same object (`{"co2": 2.0, "co2": 20.0}`) | rejected: neither value is picked |
| a number out of range for a double (`1e400`, `1e-400`) or an integer with more than 4300 digits | rejected |
| more than 64 levels of nesting | rejected |
| text with a lone surrogate (`"\ud800"`) | rejected (it cannot be written as UTF-8; it would only fail at write time) |
| a top level that is not an object (`null`, `42`), a list element that is not an object, an envelope without `payload` | rejected |
| several objects in a `.json` | rejected, with the suggestion to use `.jsonl` |
| a file without documents (`[]`, empty JSON Lines) | a warning in the report |

Numbers with more than 15 significant digits also keep the original text: a `decimal(38,20)` or `bigint` gets the exact value (e.g. `1234567890123456789.0` as `bigint` gives `1234567890123456789`, not `…768`).

### match — which rules apply to each JSON

| Key | Example | Meaning |
|---|---|---|
| `semantic_id` | `"urn:samm:io.catenax.serial_part:3.*"` | glob (or list) on the semantic id |
| `file` | `"*bom*.json"` | glob on the file name |
| `has_paths` | `[childItems]` | the paths must exist |
| `where` | `{path: specVersion, op: regex, value: "7\\."}` | any condition |

Without `match`, the submodel accepts everything. The first one that matches wins (unless `match_all`).

### Tables

```yaml
- name: bom_as_built_edges
  explode: childItems            # 1 row per element ("a[*].b[*]" for nested levels)
  keep_empty: false              # true = a row with nulls when the list is empty
  where: {path: hasAlternatives, op: eq, value: true}
  mode: merge                    # append | overwrite | overwrite_partitions | merge
  keys: [parent_id, child_id]    # keys of the MERGE and of deduplication
  replace_by: [parent_id]        # DELETE of the existing rows of those ids before inserting (with explode: $root/$meta)
  partition_by: [country, "day(created_on)", "bucket(16, child_id)"]
  dedupe: true                   # the same key in another document of the batch: the last one is kept (as in merge), with a warning
  on_duplicate: last             # the same key twice in the SAME document: error (default) | last | first
  on_error: fail                 # fail (rejects the document) | null | skip_row — see "Load report"
  metadata_columns: [source_file, ingested_at]   # or true
  auto: false                    # true = adds every scalar field automatically
  columns: {...}
  missing_columns: null          # merge: the table has columns the config does not produce -> NULL in them (default: error)
  athena_options: {}             # other awswrangler.athena.to_iceberg options (not those the config decides)
```

**Write modes**

| mode | Behaviour |
|---|---|
| `append` | inserts |
| `merge` | `MERGE INTO` by the `keys` (updates or inserts) — ideal for the digital twin |
| `overwrite` | replaces the whole table; if the submodel's documents in the batch produce no row, the table becomes empty (before, the old content stayed) |
| `overwrite_partitions` | replaces only the partitions present in the batch |
| `append` + `replace_by` | deletes the rows of the ids present and inserts again — ideal for **child tables** (lists that can shrink) |

`where` accepts `op`: `eq, ne, in, not_in, exists, missing, empty, not_empty, regex, gt, gte, lt, lte, contains`, and the combinations `{all: [...]}`, `{any: [...]}`, `{not: {...}}`.

Conditions (`where`, `cases.when`, `check.required_if`, `match.where`) are validated at load time (operator, type of the `value`, regex) and compare **with their type**: numbers as numbers (`1` and `1.0` are equal), dates in the schema's format as instants (`10:00+02:00` is before `09:30Z`), and, if one side is a date without a time, the date as it was written (`created >= "2025-01-01"`). Comparing different types is an error, with the evaluator's `on_error`: `{op: gt, value: "9"}` with the number 10, `{op: eq, value: 7}` with the text `"7"`, `gte 0` with `true` or `"n/a"`, `regex` on a number. Before, text was compared with text, and `10 > "9"` was false. In `match`, a different type counts as "no match".

Two key rules: a row with a null `keys` or `replace_by` column is an error (it cannot be identified; `on_error: skip_row` on the column skips the row and counts it); the same key twice in the same document is an error, unless `on_duplicate` (before, the last one silently stayed — the same child twice in a BOM lost a quantity). Keys compare with their type (`1` and `1.0` are the same key). `mode: merge` with `dedupe: false` is not accepted (MERGE needs unique keys).

In `auto` mode, two fields that give the same column name (`fossilGhgEmissions` and `fossil_ghg_emissions`) are an error; an explicit column with an auto field's name overrides it if it reads the same field, and from another source it is an error, unless `auto_collision: prefix` (`item_<name>`) or `skip` (counted).

**Writing**

The types written are the declared ones, never guessed from the data (before, a `bigint` with a null in the batch went through pandas as `float64` and `9007199254740993` became `...992`; a `tinyint` in a local Parquet file became `double`).

Before writing any table, and before any `replace_by` `DELETE`, **every** table of the batch is pre-checked; a problem stops the load without writing anything (`--dry-run` does the part that does not need AWS):

* conversion to the declared Arrow schema and the same conversion path awswrangler uses (if the awswrangler version has another API, that part is skipped with a warning);
* `partition_by` with columns that exist; names up to 255 characters; `replace_by` only on simple columns;
* comparison with the table in the Glue catalog, column by column:

| Table vs configuration | What happens |
|---|---|
| same type | writes |
| the table's type holds every value of the declared one (`int` → table `bigint`, `decimal(10,2)` → `decimal(12,3)`) | writes with the table's type, no `ALTER` |
| the declared one is a widening Iceberg does without changing the old rows (`int`→`bigint`, `decimal(p,s)`→`decimal(p2,s)`) | the table is widened, with a **warning** (an error with `target.schema_evolution: false`) |
| new column | added, with a **warning** (an error with `schema_evolution: false`) |
| another type (`double` in the table, `string` in the config; changing a decimal's scale) | **error**: the `ALTER` would fail after the `DELETE` |
| `float` on one side and `double` on the other | **error**: `1.1` accepted as a `float` (32 bits) is `1.100000023841858` and would read like that in a `double` column (in the new rows, or in the old ones if the table were widened). Declare the table's type |
| `merge` and the table has columns the configuration does not produce | **error**: the `MERGE` would put `NULL` in them in the updated rows; `missing_columns: null` declares it (counted). In an `auto` table (and in `auto_normalize`) the missing column is a field the batch's documents no longer have: `NULL` is the right value of the new version, and it is counted. In `append` the new rows get `NULL` in those columns, without touching the old ones |

`replace_by`: the `DELETE` covers **every** accepted document of the batch, including those that no longer produce rows (the list became empty, or the `where` excluded them all): before, their old rows stayed in the table. So, in a table with `explode`, the `replace_by` columns must be at document level (`$root.…`, `$meta.…` or the `document_id` column); a document without that value is rejected. Rejected documents delete nothing. The values are compared with the column's type in the table (`CAST('1.1' AS REAL)` for `float`: without it the `DELETE` found nothing).

`athena_options` does not accept what the configuration decides (`mode`, `dtype`, `merge_cols`, `partition_cols`, `schema_evolution`, …): before, it overrode it. `overwrite_partitions` only with partitions by column (not `day(…)`, `bucket(…)`). The same rules apply to the `defaults` of a submodel with `auto_normalize` (before, `athena_options: {mode: overwrite}` passed and replaced the `merge`'s root table), which also pass `missing_columns` to the generated tables. Two columns that give the same name (`fossilGhg` and `fossil_ghg`) are an error when the configuration is loaded (before, only one stayed).

Local files (`run --out`): **jsonl** with the numbers as they were read (decimal as a number, not text) and ISO 8601 dates; **csv** with null as an empty field without quotes and everything else in quotes (`""` is empty text), lists/objects as JSON and booleans as `true`/`false`; **parquet** with the declared schema.

### Columns

Compact form — `path | transformation | transformation(arg) ::type` (in unquoted YAML, `::` attached to the type):

```yaml
columns:
  catenax_id: catenaXId | urn_suffix
  part_id: localIdentifiers[?key=partInstanceId].value | first
  year: manufacturingInformation.date | year ::int
  bpn: businessPartner | upper | prefix(BPN-)
```

Full form:

```yaml
columns:
  quantity_kg:
    path: quantity                        # or paths: [a, b] (first non-null)
                                          # or template: "{$root.catenaXId}|{catenaXId}"
                                          # or const: 42
    transform: {unit_convert: {to: kilogram}}   # one or a list
    type: double
    default: 0
    required: true
    on_error: "null"
```

### Conditional values (`cases`)

The first case whose `when` condition passes gives the value; `else` is the default value. Each case can have its own source (`path`, `paths`, `template`, `const`) and its own `transform`. The conditions use the same operators as `where`.

```yaml
land_management_biogenic_co2_emissions:      # "0 if the v7 value is negative"
  cases:
    - when: {path: pcf.extTFS_luGhgEmissions, op: gte, value: 0}
      path: pcf.extTFS_luGhgEmissions
    - else: 0
  type: double
```

### Validations (`check`)

Runs after the cast. On a list, it validates each element. It compares with the type: `in: [1, 2, 3]` accepts `2.0`, but the text `"2"` does not match the number `2`; `min`/`max` only accept numbers (`true` and the text `"5"` are not numeric). `min`, `max` and `in` are validated at load time.

```yaml
company_ids:
  path: companyIds
  type: array<string>
  check:
    regex: "^urn:.*BPNL[0-9A-Z]{12}$"   # format
    min: 0                              # range (numbers)
    max: 100
    in: [cut-off, reverse cut-off]      # closed list of values (ignore_case: true to ignore case)
    required: true                      # required
    required_if: {path: pcf.massBalancingUsed, op: eq, value: true}   # required if...
    on_fail: warn                       # warn (default) | null | skip_row | fail
```

Failures appear in the load report (table, column, file, value) and in the `--dry-run` summary (`failed checks={column: n}`). With `on_fail: fail` the document is rejected.

### Paths

| Syntax | Result |
|---|---|
| `a.b.c` | nested field |
| `list[0]`, `list[-1]` | by index |
| `list[*].field` | all (a list) |
| `list[?key=manufacturerId].value` | filter (`=`, `!=`, `~` regex, `[?field]` exists) |
| `['key.with.dots']` | special key |
| `$root.catenaXId` | root of the document (useful inside `explode`) |
| `$parent.x`, `$parent.$parent.x` | explode level above |
| `$index`, `$parent.$index` | position in the list |
| `$item` | the element itself (lists of scalars) |
| `$meta.source_file` / `semantic_id` / `ingested_at` / `document_id` | metadata |

**An absent field and a wrong path are not the same thing.** A field that does not exist (or an intermediate object that is `null`) gives null, as always. A path that does not fit the document's shape is an **error** (with the column's `on_error`), because before it silently gave null:

| Path | Document | Result |
|---|---|---|
| `childItems.quantity` | `childItems` is a list | error: use `childItems[*].quantity` |
| `xs[?k=a][0].v` | the `[0]` applies to each result of the filter (an object) | error: for the 1st result use `xs[?k=a].v \| first` |
| `block[0].c` | `block` was unwrapped by the `unwrap` | error: remove the index |
| `name.first` | `name` is text | error |
| explode to a simple value | | error: the document is rejected |

In `match` (recognising the submodel) a path with another shape only counts as "no match", because it serves to tell versions apart.

**Filters with types.** `[?k=abc]` or `[?k='abc']` is text; `[?n=1]` is a number (`1` and `1.0` are equal); `[?b=true]` a boolean; `[?x=null]` null (or absent). Comparing different types (`[?v='1']` with the number 1) is an error, instead of never matching. In quotes the value may contain `=`, `~` and `]` (`[?key='a=b]c']`), and the operator is the first one that appears: `[?key~a=b]` is the regex `a=b`.

**Elements without the field (`missing_elements`).** With `[*]` or a filter, if some element does not have the field the column is an error, unless you declare what to do — `first`, `count`, `join`, `sum` and arrays would give different results depending on the interpretation:

```yaml
co2_per_stage:
  path: stages[*].co2
  missing_elements: null     # one null per missing element: [1.5, null, 2.0]
  # missing_elements: skip   # only the values found: [1.5, 2.0] (the old result)
  type: array<double>
```

Both are counted in the report. Missing elements are not accepted in a `template` or in `document_id.from`.

**Validation at load time.** Before any document is read, every path (columns, `paths`, `template`, `cases`, `where`, `check.required_if`, `explode`, `document_id.from`) is checked: syntax, `$parent` above the root, `$index` in a table without `explode`, unknown `$meta` keys. With `model.schema` on the submodel, each path is walked in the model's schema, with the `unwrap` already applied: a field that does not exist is an error with a suggestion (`field 'fossilGHGEmissions' does not exist … Did you mean 'fossilGhgEmissions'?`), as is a field asked of a list without `[*]`, an index on an unwrapped list, an `explode` that is not a list and `unwrap` paths that do not exist. For a field outside the schema (e.g. an extension), declare `outside_model: true` on the column.

### Transformations

| Group | Transformations |
|---|---|
| text | `trim upper lower snake_case prefix suffix replace regex_replace regex_extract substring split join to_string length` |
| Catena-X | `urn_suffix` (`urn:uuid:x`→`x`), `semantic_id_parts` (namespace/version/name), `unit_convert` (`{value, unit: "unit:gram"}`→kg, m, l, s, kWh, …) |
| values | `default null_if map const` (`map` accepts `_default`, `_null` and `_ignore_case: true`) |
| numbers | `to_number multiply divide add round abs clamp` (`round: {digits: 2, mode: half_up}`; `clamp: {min: 0}` / `{max: 0}`; `to_number: {decimal: ",", thousands: "."}`) |
| dates | `to_timestamp to_date date_format year month now today` (`to_timestamp: {unit: s}` for numbers) |
| lists | `first last single count distinct sort compact sum min max pluck` (`sum: {nulls: skip}`) |
| JSON | `to_json from_json keys get` |
| others | `to_bool hash uuid5 uuid4` |

No transformation changes or loses a value without saying so. Names and arguments are validated when the configuration is loaded.

| Transformation | Rule |
|---|---|
| `multiply divide add sum round clamp unit_convert` | exact arithmetic on the written value: `0.1 * 3` gives `0.3` (as float it gave `0.30000000000000004`); booleans are not numbers |
| `sum min max` | a null in the list is an error (the total would be partial); `{nulls: skip}` ignores and counts them |
| `min max sort` | numbers as numbers, ISO dates as instants (`10:00+02:00` is before `09:00Z`); numbers as text (`["9", "10"]`) and mixed types are errors |
| `round` | the mode is required (`half_up`, `half_even`, `half_down`, `up`, `down`, `ceiling`, `floor`): `0.125` gives `0.12` or `0.13` depending on the mode |
| `clamp` | each changed value is counted in the report |
| `single` | the list's only value; several different values are an error (`first` would silently pick one) |
| `distinct null_if map` | compare with their type: `true`, `1` and `"1"` are different (`1` and `1.0` equal). In `map`, a value that only matches a key of another type (the number `1` and the key `"1"`) is an error |
| `unit_convert` | a quantity without a unit is an error; `from:` is the unit to assume when the document does not have one (counted). Different `value`/`quantityNumber` and `unit`/`measurementUnit` in the same object are an error |
| `to_bool` | a value that is not a recognised boolean is an error (before: null). `yes`/`no`, `y`/`n`, `1`/`0` and the Portuguese `sim`/`não` are recognised |
| `regex_extract` | no match is an error; `{pattern: ..., no_match: null}` sets it to null and counts it |
| `join` | nulls in the list are an error (`""` and null would look the same); use `compact` first |
| `pluck get keys semantic_id_parts` | applied to something that does not fit (e.g. `get` on a list) are errors |
| `to_string join to_json` | the same text as `string` columns: `true`/`false`, JSON for objects, ISO dates; document numbers as they were written |
| `to_timestamp` | see "Dates" |

Your own transformations:

```python
# my_module.py  (declared in plugins: [my_module])
from cx2iceberg import register

@register("valid_bpn")
def valid_bpn(value, arg):
    return value if value and value.startswith("BPNL") and len(value) == 16 else None
```

### Types

`string tinyint smallint int bigint double float decimal(p,s) boolean timestamp date json array<T> map<K,V> struct<field:T,...>`, nestable at will
(`json` stores the serialized object in a string column; `tinyint` and `smallint` validate each value's range, but in the table they are `int`, because Iceberg has no 8/16-bit integers).

**Struct**: a dictionary with several levels in a single field, queryable with `column.field.subfield` in Athena.

```yaml
columns:
  life_cycle: { path: productLifeCycleStagesAndEmissions, type: struct }          # structure inferred from the data
  attestations: { path: attestationOfConformance, type: "array<struct>" }         # list of objects
  time:                                                                            # explicit structure
    path: pcfAssessmentAndMethodology.pcfAssessmentInformation.time
    type: "struct<reference_period_start:timestamp, referencePeriodEnd:timestamp>"
```

The struct's field names become snake_case and match the JSON's camelCase keys. Fields that are not in the type are ignored. A 1-element list is accepted as an object; with more than 1 it is an error.
Prefer **explicit** types (`init --style struct` generates them from the SAMM schema): with `type: struct`, the structure depends on each load's data, and Iceberg only lets you *add* fields to a struct, not change the type of an existing field. An object without fields (`{}`) becomes `json`, because Athena does not accept `struct<>`.
Without `type`, the type is inferred. Untyped numbers become **double**, because JSON does not tell `1` from `1.0` and Iceberg does not let you promote `bigint` to `double` later. But a value that would change in that inference rejects the document: an integer above 2^53 or a number with more digits than a double holds, and a list with mixed types (they would all become text). If the same untyped column gets different types in the batch (`1.5` in one document, `"n/a"` in another), the load fails with examples of each type: declare the `type`, and values that do not fit are then rejected only in their own document. An untyped column with only nulls in the batch is not created (the type would be guessed and would stay in the Iceberg table); there is a warning in the report.

#### Lossless casts

| Type | Rule |
|---|---|
| `tinyint smallint int bigint` | outside the type's range (`300` in a `tinyint`) is an error (before, it only failed at write time); decimal places are an error |
| `decimal(p,s)` | more decimal places than `s` is an error, unless the column has `rounding`; more digits than `p - s` before the decimal point is an error. `p` up to 38 |
| `double` | the number must come out of the double as it was written: `9007199254740993` or `0.12345678901234567890` are errors, unless `rounding: nearest` |
| `float` | it is a **32-bit** float (`real` in Athena): `1.23456789` would become `1.2345679` → error, unless `rounding: nearest`. For numbers with decimal places use `double` |
| every number | booleans are not numbers (`true` in a `double` was `1.0`) |
| `string` | document numbers as they were written, `true`/`false`, JSON for objects |
| `json` | text must be valid JSON (before, `"abc"` was stored as it was) |
| `map struct` | two keys that give the same key in the type (`"1"` and `"1.0"` in a `map<double,…>`; `fossilGhg` and `fossil_ghg` in a struct) are an error |

```yaml
value_kg:  {path: value, type: "decimal(12,3)", rounding: half_up}   # rounds and counts it in the report
ratio:     {path: r, type: double, rounding: nearest}                # accepts the nearest double
```

#### Dates

Date text only in the SAMM schema's format (`xsd:dateTime`): `2022-01-01T10:00:00`, optional fraction, `Z` or `±hh:mm` up to ±14:00; or a date `2022-01-01` (with an optional time zone). Python's date reader accepted much more (`2022-01-01 10:00`, `20220101T1000`, `+0200`, ISO weeks) and read `2022-01-01+02:00` — a date with a time zone — as 02:00 on that day. Other formats: `to_timestamp: {format: "%d/%m/%Y %H:%M %z"}`. `24:00:00` (end of the day, valid in the schema) is the instant 00:00 of the next day; for `date`, `year`, `month` and `date_format` it is ambiguous (`2022-12-31T24:00:00` is the end of 2022 or the start of 2023) and an error. Years outside 0001–9999 are not supported.

`timestamp` is an instant: with a time zone (`Z`, `+02:00`) it is converted to UTC. Without one (`2022-01-01T10:00:00`) the format is valid in the schema, but the instant is unknown: it is an error, unless you declare `input.naive_timestamps: utc` or the sender's IANA time zone (`Europe/Berlin`). With a time zone, times that do not exist or happen twice at a clock change are errors. A date without a time used as a timestamp (it becomes midnight) also needs that declaration. Each assumed value is counted. Only `timestamp` needs a time zone: dates, years and months use what was written. `date`, `to_date`, `year`, `month` and `date_format` use the date as it was written, in the document's time zone: `2023-07-11T01:00:00+02:00` is the 11th (in UTC it would be the 10th). A number is only a date with the unit declared (`to_timestamp: {unit: s}` or `{unit: ms}`); before, `year` of `2022` gave 1970. Fractions below the microsecond do not fit in an Iceberg timestamp: they are truncated and counted. `today` is the UTC date, like `now`.

#### Numbers as text

A number that arrives as text (`"2.5"`) is read **only** if it follows the format of JSON numbers (`123`, `-1.5`, `2e3`, with digits 0–9 and spaces around it); it is counted in the report as a warning. What is ambiguous or not a number is an error, with the reason:

| Text | Without a declared format | Why |
|---|---|---|
| `"1.234"`, `"12.345"`, `"1,234"` | error | it may be a thousands separator (1234) or a decimal (1.234) |
| `"1,5"`, `"1.234,5"` | error | comma: the format must be declared |
| `"1_000"`, `"+5"`, `"007"`, `".5"`, `"0x10"` | error | not the JSON format |
| `"١٢٣"`, `"１２３"` (non-ASCII digits) | error | Python silently accepted them |
| `"NaN"`, `"inf"`, `"1e400"`, `"1e-400"` | error | not finite / does not fit in a double |
| `""` or only spaces (numeric column) | null, with a warning | |

For numbers in another format, declare it with `to_number` (the same rule applies to `multiply`, `add`, `unit_convert`…):

```yaml
quantity: {path: qty, transform: {to_number: {decimal: ",", thousands: "."}}, type: double}   # "1.234,5" -> 1234.5
```

Thousands separators must be in groups of 3 (`"1.23,4"` is an error). Transformation names and arguments are validated when the configuration is loaded: a misspelled name stops the load straight away (before, with `on_error: null`, it set every value of the column to null).

### The UUID that links the tables (`document_id`)

When a submodel produces several tables (`explode`, `auto_normalize`), `document_id` adds to **all** of them the same column with one UUID per document, for the JOINs:

```yaml
submodels:
  - name: serial_part
    document_id:
      column: serial_part_uuid     # default: document_id
      from: catenaXId              # or a list: [id, {path: version, type: int}]
      # strategy: key | content | random
      # namespace: cx2iceberg      # changes the UUID space (e.g. one per environment)
      # on_missing: fail           # a from field missing: fail (default) | content
```

| strategy | UUID | Reprocessing the same document |
|---|---|---|
| `key` (with `from`) | uuid5 of the given fields | the same UUID, even if the content changes: `merge` updates the rows |
| `content` (`document_id: true`) | uuid5 of the whole content | the same UUID only if nothing changed |
| `random` | uuid4 | a new UUID on each load: only for `append` tables |

`key` is recommended. With `random`, reprocessing a file duplicates the child tables' rows.
The `from` paths apply **after** the `unwrap`. If a `from` field is missing in a document, the document is rejected: the UUID would change once the field appeared, and `merge` would duplicate the rows. With `on_missing: content` the content is used (counted).
`from` fields that are not text need a type: `{path: version, type: int}`. The value is converted before the UUID, so `0`, `"0"` and `0.0` give the same UUID (before, they gave three). With the same values and the same `namespace`, the UUID is the same across submodels: the same PCF (`id` + `version`) has the same UUID whether it comes as v7 or v9.

> **Migration (UUIDs already stored).** For text fields and integers the UUID did not change. It changed for documents in which a part of `from` came as text (`"0"`) or with decimal places (`0.0`) and is now declared as an integer: those documents now get the same UUID as the others. If tables already written have rows with the old UUID, reprocess those documents and delete the rows with the old UUID (in child tables with `replace_by`, the old rows are not deleted because the UUID is different).

`hash` and `uuid5` only accept text or objects/lists: a number on its own is an error (`1` and `1.0` would give different identities); use `to_string` or a `template` first. A `template` with **all** parts missing gives null, like an absent field. With only **some** missing it is an error: the present parts would be lost (null) or the value would be wrong (`"P|"`, the same for every element without the part). Declare `template_missing: empty` (missing parts left empty) or `template_missing: null` (a null value) on the column; both are counted.
In `auto_normalize` without `id_path`, the UUID becomes the key of the root table and of the child tables. It is also available in paths as `$meta.document_id`.

### 1:1 lists (`unwrap`)

Many Catena-X models (e.g. PCF 9.0) wrap each block in a single-element list: `pcfAssessmentAndMethodology[0].pcfAssessmentInformation[0].time[0].created`. The `unwrap` (at submodel level) treats those lists as objects **before** applying the rules, so the paths have no `[0]` and auto mode no longer creates 1-row tables.

```yaml
submodels:
  - name: pcf9
    unwrap:
      all: true                         # every list of objects...
      except: [attestationOfConformance] # ...except those that really have several elements
      on_multiple: fail                 # fail (default) | first (uses the 1st and warns) | keep (leaves the list)
    # or: unwrap: [general, carbonContent, productLifeCycleStagesAndEmissions.productionStage]
```

The paths take no indices and start at the root. With `all`, lists of simple values (e.g. `productIds`) are not touched. A list given explicitly that is empty becomes null.
If a list with more than one element shows up where one was expected, the load **fails** with the file and path concerned, so nothing is silently lost.

### Auto mode

* `auto: true` on a table: flattens every scalar field of the current object (`a.b.c` → `a_b_c`), lists of scalars become `array<...>`, ISO dates become `timestamp`. Explicit columns take precedence. Options: `auto_exclude`, `auto_max_depth`, `auto_lists: array|json|skip`, `auto_dates`, `auto_naming`.
* `auto_naming: leaf` (or `naming: leaf` in `auto_normalize`): short names. It uses only the last field (`fossil_ghg_emissions`) and adds levels above **only where there is a collision** (`production_stage_packaging_land_management_biogenic_co2_emissions` vs `packaging_stage_…`). **It needs `model.schema`**: collisions are computed over every field of the model (every `oneOf`/`anyOf` alternative), so each field always has the same name, in every document and every load. Before, they were computed from each document's fields: in the same load, `ghg.fossil` went to `fossil` in one document and to `ghg_fossil` in another (which also had `other.fossil`), and the column's sum was wrong without any warning. A field that is not in the model keeps its full name (`extra_fossil`) and is counted as a warning; if that name is a model field's name, it is an error (a collision). Without `model.schema`, `leaf` is a configuration error: use `full` or generate explicit columns with `init --naming leaf`.
* `auto_normalize` on a submodel: creates a root table and **one table for each list of objects** (at any depth), with the root's id and position indices as the key. The child tables use `replace_by` to stay in sync when lists shrink.
* Where the child tables come from (`auto_normalize.children`):
  * `schema` (the default with `model.schema`): every list of the model, with the `unwrap` applied, plus those that appear in the data. A list that was empty in **every** document of the batch still has a table, and the `DELETE` of the old rows runs. Cost: in `merge`, one `DELETE` per existing child table on each load (PCF 9.0 without `unwrap` has 27; with `unwrap: {all: true}`, 2).
  * `batch`: only the lists present in the batch. It always used to be like this, and a document whose list became empty (with no other document of the batch having that list) left the old rows in the child table. Without `model.schema`, and with a `mode` other than `append`, it must be declared; it is counted in the report on every load.

```yaml
model:
  schema: schemas/pcf_7.0.0-schema.json   # fixes the child tables and the leaf names
auto_normalize:
  table_prefix: pcf
  id_path: id          # default: catenaXId
  id_column: pcf_id
  exclude: [comment]
  naming: leaf         # full (default) | leaf (needs model.schema)
  children: schema     # schema (the default with model.schema) | batch
```

### Metadata columns

`metadata_columns: true` adds `cx_source_file`, `cx_semantic_id`, `cx_ingested_at` and `cx_record_hash` (sha256 of the row's data, useful to detect changes).

## Deep models: the PCF 9.0 case

PCF 9.0 has up to 5 levels and almost everything is a 1-element list. Without `unwrap`, `auto_normalize` generates **27 one-row tables**. There are two models that work well, both generated from the official schema and tested with the official example:

| | [examples/pcf9_flat.yaml](examples/pcf9_flat.yaml) | [examples/pcf9_struct.yaml](examples/pcf9_struct.yaml) |
|---|---|---|
| Tables | 1 main table (every 1:1 field as a column) + 8 child tables (one per real list) | 1 |
| Columns | ~140 simple columns | 12 (blocks as `struct`) |
| Query | `SELECT fossil_ghg_emissions FROM pcf9` | `SELECT product_life_cycle_stages_and_emissions.production_stage.fossil_ghg_emissions FROM pcf9` |
| Lists | tables `pcf9_product_ids`, `pcf9_attestation_of_conformance`, … (joined by `pcf9_id`) | `array<...>` inside the row (`CROSS JOIN UNNEST`) |
| Good for | BI/dashboards, tools that do not handle nested types | keeping the model's hierarchy, few columns |

Both can coexist: the tables are independent, so you can put both `tables:` in the same submodel.

### Lists to several rows (`explode`)

```yaml
- name: pcf9_product_ids                 # list of strings -> 1 row per value
  explode: companyAndProductInformation.productInformation.productIds
  keys: [pcf9_id, value]
  columns:
    pcf9_id: $root.pcfAssessmentAndMethodology.pcfAssessmentInformation.idAndVersion.id
    position: $index ::int
    value: $item

- name: bom_lines                        # nested lists: 1 row per element of the deepest level
  explode: "orders[*].lines[*]"
  columns:
    order_no: $parent.no
    order_pos: $parent.$index ::int
    sku: sku
```

`keep_empty: true` generates a row with nulls when the list is empty (like a LEFT JOIN). `replace_by: [id]` first deletes the old rows of the same document, so a list that shrank leaves nothing behind.

## Migration between versions of a model

[examples/pcf_v7_to_v9.yaml](examples/pcf_v7_to_v9.yaml) puts PCF 7.0.0 and PCF 9.0.0 in the same table, with V9 columns. It has a commented example of each kind of transformation a version mapping usually needs:

| | Transformation | How |
|---|---|---|
| 1 | rename a field | column name ≠ path |
| 2 | move to another level / structure | full path, `unwrap` |
| 3 | alternative names in the source (`x; extWBCSD_x`) | `paths: [a, b]` |
| 4 | a field that does not exist in the source → empty | `const: null` with `type` |
| 5 | a dropped field | do not map it |
| 6 | list of objects → list of values | `list[*].field` |
| 7 | nested lists flattened | `a[*].b[*].field` |
| 8 | simple value → list | `type: array<...>` |
| 9 | one field → several targets, conditionally; sign change | `cases`, `clamp`, `multiply: -1` |
| 10 | format (e.g. URN with BPN) | `check.regex` |
| 11 | range (0–100) | `check.min/max` |
| 12 | closed list of values, old values → new | `map` + `check.in` |
| 13 | conditionally required ("M if …") | `check.required_if` |
| 14 | default value | `default` |
| 15 | several versions in the same table | one submodel per version, the same table `name`, `match.any` by semantic id or `specVersion` |

With `keys: [id, version]`, a PCF migrated from v7 and the same PCF delivered as v9 end up **in the same row**: the last one processed wins. The rules that feed the same table must agree on `mode`, `keys`, `partition_by`, `replace_by`, `dedupe`, `on_duplicate`, `location`, `metadata_columns` and on the type (and `rounding`) of each common column; otherwise the configuration does not load (before, the 1st rule won: a `decimal` from the 2nd rule became `double`). The official V7 and V9 examples have the same `id`, so to compare them, run each one separately.

## Studio: build transformations without writing YAML

```bash
cx2iceberg studio --root . --open
```

Opens a local application at `http://127.0.0.1:8765`, made for people who do not know the configuration format:

1. **Choose a submodel** from the official catalog ([eclipse-tractusx/sldt-semantic-models](https://github.com/eclipse-tractusx/sldt-semantic-models)), with its versions and the status of each one (release, deprecated…). The studio downloads that version's schema and example to `models/<submodel>/<version>/` and creates an initial configuration with:
   * the document's identifier;
   * the UUID that links the tables;
   * validation against the schema;
   * the `unwrap` of the lists that have a single element in the example.
2. **Model fields:** every field of the schema, with a readable name, description, type, the "required" mark and the example's value. Fields the example does not have also appear, marked "no value in the example".
3. **Clicking a field creates a column.** Each column is set up in plain language:
   * name and type;
   * "if empty, use…";
   * steps: uppercase, remove the URN prefix, convert units, translate values, limit between minimum and maximum…;
   * validation: required, minimum and maximum, allowed values, and what to do if it fails.
4. **Lists:**
   * "One row per element" creates a child table, linked to the main one by the document's UUID;
   * "Always just one" or "Can have several" decides the `unwrap`.
5. **Live result** on the test data: the official example, files from the folder or a pasted document. It includes:
   * validation against the schema, with readable messages and the path of each problem;
   * the **load report**, the same as `run`'s: rejected documents (and why), declared losses and warnings, counted by table, column and reason, with examples. Columns with events have a ● in the grid's header. A rejected document does not hide the others' rows;
   * the write pre-check (the same as `--dry-run`'s), which warns if the write would be blocked;
   * the values as they will be in the table: big integers and decimals shown exactly (before, the browser showed `9007199254740993` as `…992`) and a `float` as Athena shows it (`1.1`, not `1.100000023841858`).
6. **Save** generates the YAML. Under **Advanced** you can see and edit the YAML, for rules the builder does not show (`cases`, `template`…). Those columns appear in the builder as an "advanced rule".

Without access to GitHub (proxy, closed network), use a local clone of the models repository:

```bash
cx2iceberg studio --models-dir ~/code/sldt-semantic-models
```

The catalog list comes from a single call to the GitHub API and is cached for 24 h in `.cx2iceberg/`. With `GITHUB_TOKEN` set, it uses that token.
On **Save**, the studio checks that the file is still the one it opened (by its content): if it was changed outside the studio, it does not save and asks you to reload. Before, it compared the modification time, which with the same timestamp (folders shared with Docker/WSL, file systems with 1–2 s resolution, `cp -p`) let it overwrite the edit made outside.
The server only accepts connections from `127.0.0.1` and only reads and writes inside `--root`. The preview never writes to Athena.

## Validating the documents against the model's schema

```yaml
submodels:
  - name: pcf_v9
    model:
      schema: ../models/io.catenax.pcf/9.0.0/Pcf-schema.json   # relative to the configuration
      example: ../models/io.catenax.pcf/9.0.0/Pcf.json
    validate: warn        # off | warn (the default with model) | skip | fail
```

Each document is validated **on input**, before any rule: types, required fields, enumerations, formats (for example the pattern of `Timestamp` dates) and limits. With `warn` the problem goes into the report and the document goes on. With `skip` the document is ignored (and counted). With `fail` the document is rejected.
On output, each column with a `type:` is converted to that type, with the column's `on_error`, and the `check` adds the business rules. There are two layers: the schema says whether the document is valid; the type and the `check` say whether the value fits the table.

## Load report and exit codes

No document is silently lost. Each load produces a report with the **table, column, file, document** (position in the file, in JSON lists and JSON Lines), **row** (position in the `explode`, e.g. `childItems[1]`), **value**, **what happened** and **why**. There are three kinds of event:

| Kind | When | Effect |
|---|---|---|
| error | an invalid value with `on_error: fail` (the default), an empty required value, a `check` with `on_fail: fail`, an unreadable file or invalid document (see "Reading the files"), `validate: fail`, an `unwrap` with several elements, a null key, a key repeated in the same document, a condition that compares different types, a document without a submodel, a `document_id` field missing, a `template` with missing parts, a document without the `replace_by` value | the **document is rejected**: none of its rows is written, in any table |
| declared loss | `on_error: null` / `skip_row`, a `default` applied, a row excluded by the `where`, `validate: skip`, `unwrap on_multiple: first`, `missing_elements`, `rounding`, `clamp`, `sum: {nulls: skip}`, `unit_convert` with `from`, `regex_extract` with `no_match: null`, `naive_timestamps`, `on_duplicate`, `template_missing: empty | null`, `auto_collision: skip`, `on_unmatched: skip`, `document_id.on_missing: content`, `missing_columns: null`, `auto_normalize.children: batch` | goes on, and is counted |
| warning | a field outside the model with `auto_naming: leaf`, a `check` with `on_fail: warn`, the same key in another document of the batch (the last one is kept), a number received as text, empty text in a numeric column, a file without documents, a fraction below the microsecond truncated, an untyped column with only nulls (not created), a new or widened column in the Athena table | goes on, and is counted |

The write pre-check (see "Writing") is not an error of one document: it stops the whole load before writing (in the report: `failure` with the problems and every table `not_written`; exit code `1`).

Errors do not stop at the first one: the load goes through every document and shows every error at once. A rejected document does not hold up the others: in a batch with files from several partners, the valid ones are written and the report says which files to ask for again.

```bash
cx2iceberg run -c config.yaml data/ --report report.json --report-details events.jsonl
```

| Option | |
|---|---|
| `--report FILE` | report as JSON: exact counts per table/column/reason and the first examples of each |
| `--report-details FILE` | every event, one per line, written as it goes (not kept in memory) |
| `--samples N` | examples kept per group (default 20) |
| `--max-errors N` | after N errors the load stops and nothing is written (default 1000) |

Only the counts (one per table/column/reason, which depends on the configuration and not on the data) and the first examples of each group are kept in memory; values are cut at 200 characters. The log only shows those examples, not one warning per value. Per row, the report only keeps which document and which `explode` position it came from (about 18 bytes), to be able to remove a rejected document's rows and say where a duplicate comes from.

The report is saved **at the end**, with the write result of each table: `written`, `failed` (with the error), `not_written` (it was not written because of an earlier failure), `empty` (no rows) or `interrupted` (the load was interrupted midway; state unknown). If a table with `replace_by` fails after the `DELETE`, the report says that the old rows of those ids were already deleted. With `--dry-run`, `write` is `null`.

**Exit codes** of `run` and `ddl`: `0` all good (there may be declared losses and warnings); `2` partial load: documents rejected, the others were written (or, with `--dry-run`, nothing was written); `1` failure (configuration, `--max-errors`, write): the report says which tables were written and which one failed.

## AWS Lambda

`cx2iceberg.lambda_handler.handler` processes S3 events (e.g. EDC payloads stored in a bucket). Set `CX2ICEBERG_CONFIG` (a path in the package or `s3://…/config.yaml`).

It is **all or nothing per invocation**: if any document of the event is rejected, nothing is written, the report goes to the log and the invocation fails. That way, a retry does not duplicate rows already written. Configure `MaximumRetryAttempts=0` and an *on-failure* destination (or DLQ): retrying does not fix errors in the data and, without a destination, AWS discards the event after the attempts. On success it returns `{"status", "written", "report"}`, as strict JSON. The response's `report` only has the counts (no examples) and the write result, to fit within AWS limits; the full report goes to the log (at `WARNING` level when it has warnings or declared losses, because Lambda's default level hides `INFO`). If the write fails, the log says which tables were written and which one failed.

After the write nothing can fail: the response is built and validated **before** writing. A failure after the write would leave the data written with the invocation marked as failed: AWS would retry (duplicate rows in `append` tables) or send the event to the *on-failure* destination, and reprocessing it would duplicate the rows again. A failure **during** the write (e.g. Athena fails on the 2nd table) still leaves the earlier tables written: hence `MaximumRetryAttempts=0`. Permissions needed: `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`, `glue:Get*/CreateTable/UpdateTable/CreateDatabase`, and `s3:GetObject/PutObject/DeleteObject/ListBucket` on the input, `location` and `temp_path` buckets.

## Programmatic use

```python
from cx2iceberg import load_config, transform, write_athena

cfg = load_config("config.yaml")
results = transform(cfg, ["data/"], semantic_id="urn:samm:io.catenax.serial_part:3.0.0#SerialPart")
write_athena(cfg, results)
```

`transform` raises `DocumentsRejected` if any document has errors (with `.report` and `.results`). To write only the valid documents:

```python
from cx2iceberg import Engine, read_documents

engine = Engine(cfg)
results = engine.run(list(read_documents(["data/"], cfg.input)), partial=True)
print(engine.report.render())        # or engine.report.to_dict()
```

## Limitations

* It reads the **value-only** format of Catena-X submodels. The full AAS format (`submodelElements`) is not converted.
* The SAMM schema does not say which lists are 1:1 (it has no `maxItems`), so the `unwrap` is your decision; the load fails if the decision is wrong for a document.
* `replace_by` runs a `DELETE` and then an `INSERT`: it is not atomic (the pre-check reduces, but does not remove, the risk of failing between the two; if it fails, the report says the `DELETE` was already done).
* `merge` never deletes: a row that is no longer produced (e.g. a child removed from the BOM) stays in the table. For lists that can shrink use `append` + `replace_by`.
* The pre-check compares column types; changes to the partition spec of an existing table are not detected.
* It processes the documents in memory. For very large volumes, call it in batches (e.g. one file or S3 prefix per run).

## Tests

```bash
pip install -e ".[dev]" && pytest
```

## License

The code is distributed under the [Apache-2.0](LICENSE) license. The schemas and some examples in `examples/` come from the Eclipse Tractus-X project and keep the CC-BY-4.0 license; see [NOTICE.md](NOTICE.md).
