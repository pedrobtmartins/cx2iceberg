# 0.3.0 — English

The project is now in English: error messages, the load report, CLI help and output, logs, the Studio interface, code comments, examples and documentation.

What did not change:

* the configuration format, the report's JSON keys and codes (`kind`, `reason`, `action`, `status`, …), exit codes and table output;
* `to_bool` still recognises the Portuguese `sim`/`não` (and `s`/`nao`) besides `yes`/`no`, `y`/`n`, `true`/`false` and `1`/`0`.

What may need attention:

* anything that matched the text of messages or logs (for example a log alert on `Valor anulado`) must use the English text (`Value set to null`);
* in the Studio, a new child table gets the position column `position` (before: `posicao`), a new table is called `table_N` (before: `tabela_N`), and a field name that sanitizes to nothing becomes `column` (before: `coluna`). Configurations already saved keep their names.

# 0.2.0 — Behaviour changes

A review of the path of each value, from the JSON to the Iceberg table, so that **no value is changed, lost or misread without a warning**. One rule for everything:

* **undeclared loss → error** (the document is rejected, or the load stops before writing);
* **loss declared in the configuration → allowed, and counted in the report**.

There is no compatibility mode. What no longer loads, or now fails, is in the last section, with what to do.

## Load report

| Before | Now |
|---|---|
| There was no report. Values set to null (`on_error: null`), skipped rows and applied `default`s were not recorded, or only in the log, one line per value | A report per table, column, file, document and row (`childItems[1]`), with three kinds of event: error, declared loss and warning. Options `--report`, `--report-details`, `--samples` and `--max-errors`. Only the counts and the first examples of each group are kept in memory |
| A document with an error stopped the whole load | The document is rejected, the others go on, and the report says which ones to ask for again. Exit codes: `0` ok, `2` partial, `1` failure. The Lambda is still all or nothing |
| The Lambda logged the report at `INFO`, invisible with the default `WARNING` level | `WARNING` when there are warnings or declared losses |

## Reading the files

| Before | Now |
|---|---|
| An unreadable file (not UTF-8, invalid JSON, too deeply nested, an integer with more than 4300 digits) stopped the whole load | It becomes a rejected document, with the reason, and the others go on |
| `NaN`/`Infinity` accepted; a repeated key (`{"co2": 2.0, "co2": 20.0}`) kept the last value | Rejected |
| Numbers with more than 15 digits lost precision (`1234567890123456789.0` → `…768`) | The original text is kept: `bigint` and `decimal` get the exact value |
| A top-level `null`/`42`, or a list element that is not an object, gave a row of nulls; an empty file gave no sign | Rejected; an empty file counts as a warning |

## Configuration

| Before | Now |
|---|---|
| YAML 1.1: `yes/no/on/off` were booleans (`map: {yes: Y, no: N, on: L, off: D}` kept two translations), `010` was octal, dates were converted, a repeated key kept the last value | YAML 1.2: only `true`/`false` are booleans, no date conversion, and a repeated key is an error |
| `dedupe: "false"` was true; a misspelled key in `input:` was ignored | Boolean options only accept `true`/`false`; unknown keys are errors |
| Transformation names and arguments only failed on the data (with `on_error: null`, they set the whole column to null) | Validated at load time |
| Malformed paths only failed on the data | Validated at load time. With `model.schema`, walked in the schema, with suggestions |
| Several rules for the same table: the 1st one won (a `decimal` from the 2nd became `double`) | They must agree on mode, keys, partitions and types |
| Two columns with the same name after normalization (`fossilGhg`, `fossil_ghg`): one stayed | Error |

## Paths and conditions

| Before | Now |
|---|---|
| A path with the wrong shape (`childItems.quantity` on a list, an index on an unwrapped list) gave null | Error, with the column's `on_error` |
| Filters compared text: `[?qty=1]` did not match `1.0`, `[?primary=true]` did not match `true`; `[?key~a=b]` was split at the `=` | Comparison with type; the operator is the first one that appears; different types are errors |
| `[*].field` with elements without the field ignored them | Error, unless `missing_elements: null` or `skip` is declared (counted) |
| Conditions (`where`, `cases`, `check`, `match`) compared text with text: `10 > "9"` was false, `"7" == 7` true, regex on numbers | Comparison with type (numbers, dates as instants); different types are errors. In `match` they count as "no match" |
| `check.min` accepted `true` as 1 and the text `"5"` as 5 | Numbers only |

## Transformations

| Before | Now |
|---|---|
| Float arithmetic: `0.1 * 3` gave `0.30000000000000004`; `round(2.675, 2)` gave `2.67` | Exact decimal arithmetic; `round` requires the rounding mode |
| `sum`/`min`/`max` with nulls gave a partial total; `max` of dates compared text; `["9", "10"]` sorted as text | Nulls are errors (or `nulls: skip`, counted); dates as instants; numbers as text are errors |
| `unit_convert` without a unit, an unrecognised `to_bool` and `regex_extract` without a match gave null | Error, unless declared (`from:`, `no_match: null`), and counted |
| `to_string(True)` gave `'True'`; `join` with nulls gave `'a,'` | `true`; nulls are errors |
| `distinct` and `null_if` mixed up `True`, `1` and `"1"` (`null_if: false` set `0` to null); `map` translated the number `1` with the key `"1"` | Comparison with type |
| `clamp` changed values without a record | Each changed value is counted |
| `hash`/`uuid5` of numbers: `1` and `1.0` gave different identities | Only text, objects or lists |

## Types and casts

| Before | Now |
|---|---|
| `true` in a number gave `1` | Error |
| Integers out of range and decimals with too many digits only failed at write time | An error in the cast, in the document |
| `decimal(10,2)` silently rounded (`1.255` → `1.26`, `0.004` → `0.00`) | Error, unless `rounding` is declared (counted) |
| `double` and `float` lost digits (`9007199254740993` → `…992`; `1.23456789` in a `float` → `1.2345679`) | Error, unless `rounding: nearest` |
| `json` stored `"abc"`; colliding `map`/`struct` keys lost one | Error |
| An untyped column with mixed values became text (`'1.5'`) | The load fails with examples; declare the `type` |
| `tinyint`/`smallint` went to Athena as they were (Iceberg does not have them) | In the table they are `int`; the range is still validated in the cast |

## Dates

| Before | Now |
|---|---|
| Python's reader accepted many formats and read `2022-01-01+02:00` as 02:00 | Only the schema's format (`xsd:dateTime`), or the declared `format` |
| A time without a time zone was treated as UTC without a warning | Error, unless `input.naive_timestamps: utc` or an IANA time zone (counted) |
| `date`/`year`/`date_format` used the day in UTC (`01:00+02:00` on the 11th gave the 10th) | They use the day as it was written |
| `year` of `2022` (a number) gave 1970 | A number is only a date with the unit declared |
| `24:00:00` was rejected | It is the instant 00:00 of the next day (and an error where it would be ambiguous) |

## Numbers as text

| Before | Now |
|---|---|
| `"1,5"` became `1.5`; `"1.234"` was read without a warning; non-ASCII digits were accepted | Only the JSON format is read (counted as a warning); what is ambiguous is an error. Other formats are declared with `to_number: {decimal: ",", thousands: "."}` |

## Keys, duplicates, identity and templates

| Before | Now |
|---|---|
| A row with a null key was written | Error (or `on_error: skip_row`, counted) |
| The same key twice in the same document: the last one stayed (a child repeated in a BOM lost its quantity) | Error, unless `on_duplicate: last/first` |
| `document_id` of `0`, `"0"` and `0.0` gave three UUIDs; a missing `from` field changed the UUID without a warning | Fields with a type; a missing field is an error (or `on_missing: content`) |
| A document without a submodel: a warning, and the load ended "ok" | Error, unless `input.on_unmatched: skip` (counted) |
| `template` with missing parts: the text kept empty parts | If all are missing, null. If only some are missing, an error (or `template_missing: empty/null`, counted) |

## Auto mode and `auto_normalize`

| Before | Now |
|---|---|
| Two auto fields with the same column name: the first one stayed | Error (or `auto_collision: prefix/skip`, counted) |
| `leaf` names computed from each document's fields: in the same load, `ghg.fossil` went to `fossil` in one document and to `ghg_fossil` in another | Computed from the model (`model.schema`, every `oneOf`/`anyOf` alternative), the same in every load. A field outside the model: its full name, with a warning |
| `auto_normalize`'s child tables came only from the batch's lists: a list that was empty in every document did not delete the old rows; with `naming: leaf` the table's name depended on the batch | With `model.schema`, every list of the model has a table, and the `DELETE` runs. Without a model, `children: batch` is declared (counted) |
| `defaults.athena_options: {mode: overwrite}` in an `auto_normalize` passed, and replaced the root table | Validated at load time, as for explicit tables |

## Writing

| Before | Now |
|---|---|
| Types guessed by pandas: a `bigint` with a null in the batch went through `float64` (`…993` → `…992`); a timestamp after 2262 failed in pandas 2 | Declared types (Int64, `datetime64[us]`, exact decimal) |
| A type mismatch with the table only showed up after the `replace_by` `DELETE` (old rows deleted, new ones not written); a `bigint` table could be narrowed to `int` | A pre-check of every table before any write or `DELETE` (Arrow, awswrangler, Glue catalog). A wider type in the table: written with that type. A widening Iceberg does without changing the old rows: a warning. `float` ↔ `double` and other types: an error |
| `replace_by` only deleted the documents that still had rows (an empty list, or everything excluded by the `where`: the old children stayed) | It deletes every accepted document of the batch; rejected documents delete nothing |
| A `DELETE` with `float` never matched (`1.1` double ≠ real) | Literals with the column's type in the table |
| `merge` put `NULL` in the table's columns the configuration does not produce | Error, unless `missing_columns: null`. In `auto` tables it is the right value (the field left the document), and it is counted |
| `overwrite` without rows left the old content, while the other tables were replaced | The table becomes empty, if the batch had documents of the submodel |
| `athena_options` could override `mode`/`dtype`/...; `overwrite_partitions` accepted `day(...)`; `partition_by` with a column that does not exist only failed in Athena | An error at load time or in the pre-check |
| Local CSV: null the same as `""`, lists as Python `repr`, `True`. JSONL: decimal as text. Local Parquet: guessed types | CSV with null unquoted and JSON for lists; exact JSONL; Parquet with the declared schema |

## Studio

| Before | Now |
|---|---|
| It only showed the first messages of the log; a rejected document hid the whole preview | The load report (rejected, declared losses, warnings) with a ● per column in the grid; the others' rows stay visible; the write pre-check |
| The browser showed `9007199254740993` as `…992` and a `float` 1.1 as `1.100000023841858` | Values shown as they will be in the table |
| On save, a conflict with an external edit was detected by the mtime: with the same timestamp (Docker/WSL, FS with 1–2 s resolution, `cp -p`), the external edit was overwritten | Detected by the content (hash) |

## What no longer loads, or now fails

Every case below was a silent loss or change of value. Now either the data is fixed, or the behaviour is declared in the configuration, and that behaviour is counted in the report.

| Situation | Becomes | What to do |
|---|---|---|
| `yes`/`no`/`on`/`off` used as booleans; repeated keys in the YAML; `dedupe: "false"` | error at load time | use `true`/`false`; remove the repeated key |
| A misspelled transformation or argument; an invalid path | error at load time | fix it |
| Times without a time zone; a date without a time used as a `timestamp` | document rejected | `input.naive_timestamps: utc` or the sender's IANA time zone |
| Numbers as text with a comma or a thousands separator | document rejected | `to_number: {decimal: ",", thousands: "."}` |
| `decimal`/`double`/`float` with more precision than the type | document rejected | `rounding` on the column, or a larger type |
| Conditions that compare different types (`value: "9"` with numbers) | document rejected | write the `value` with the right type |
| `[*].field` with elements without the field | document rejected | `missing_elements: null` or `skip` |
| `sum` with nulls; `unit_convert` without a unit; `regex_extract` without a match | document rejected | `sum: {nulls: skip}`, `from:`, `no_match: null` |
| A null key, or one repeated in the same document | document rejected | `on_error: skip_row`, `on_duplicate: last` |
| A document without a submodel | document rejected | `input.on_unmatched: skip` |
| `template` with only some parts | document rejected | `template_missing: empty` or `null` |
| `document_id.from` with untyped numbers, or a missing field | error at load time / document rejected | `{path: version, type: int}`; `on_missing: content`. See the note on migrating UUIDs in the README |
| `auto_naming: leaf` or `auto_normalize.naming: leaf` without `model.schema` | error at load time | declare `model.schema`, use `full`, or generate explicit columns with `init --naming leaf`. Tables already created with per-document leaf names may have columns with other names: the new columns appear as warnings at write time |
| `auto_normalize` without `model.schema` with a `mode` other than `append` | error at load time | `model.schema` (recommended) or `auto_normalize.children: batch` |
| `merge` on a table that has columns the configuration does not produce | the pre-check fails | `missing_columns: null`, or add the columns |
| A configuration type incompatible with the table (`float` ↔ `double`, `string` ↔ number, a decimal's scale) | the pre-check fails | declare the table's type |
| `athena_options` with `mode`, `dtype`, …; `overwrite_partitions` with `day(...)` | error at load time | use the configuration's options; partitions by column |
| Columns that give the same name (`fossilGhg`, `fossil_ghg`) | error at load time | rename one |

Updated examples: in `examples/config.yaml` the PCF submodel declares `model.schema` (PCF 7.0.0) and matches only `pcf:7.*`, and the folder's other files are ignored with `input.on_unmatched: skip`. Example [13] of `pcf_v7_to_v9.yaml` now compares dates (`created gte "2025-01-01"`), and `document_id.from` declares `{path: version, type: int}`.
