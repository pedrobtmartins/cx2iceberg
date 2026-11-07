# cx2iceberg

Transforma submodelos **Catena-X** (JSON value-only: SerialPart, SingleLevelBomAsBuilt, PCF, Batch, …) em **tabelas Apache Iceberg no Amazon Athena**, através de regras declaradas num YAML.

```
JSON do submodelo ──► match (semantic id / ficheiro / campos) ──► regras por tabela ──► Athena (Iceberg)
                                                                   explode, filtros,
                                                                   transformações, tipos
```

## Arranque rápido

```bash
pip install -e ".[aws,parquet]"
```

1. Gerar uma configuração. O melhor é partir do **JSON Schema do modelo SAMM** (pasta `gen/` de cada modelo em [eclipse-tractusx/sldt-semantic-models](https://github.com/eclipse-tractusx/sldt-semantic-models)), porque inclui todos os campos e tipos, mesmo os que não aparecem num exemplo. Pode juntar-lhe JSONs de exemplo:

```bash
cx2iceberg init --schema Pcf-schema.json Pcf.json --name pcf9 --unwrap all --keep-list attestationOfConformance --naming leaf -o config.yaml
```

Opções do `init`: `--unwrap all|a.b,c.d`, `--keep-list` (com `--unwrap all`, listas que têm mesmo vários elementos), `--naming full|leaf`, `--style flat|struct`, `--scalar-lists array|rows`, `--id-path`.

2. Rever o `config.yaml` (nomes, tipos, chaves) e testar sem tocar na AWS:

```bash
cx2iceberg run -c config.yaml exemplo.json --dry-run
```

3. Escrever no Athena:

```bash
cx2iceberg run -c config.yaml s3://bucket/entrada/serial_part/
```

Cada `run` (e `ddl`) mostra o **relatório da carga** em stderr: ver [Relatório da carga](#relatório-da-carga-e-códigos-de-saída).

Outros comandos: `cx2iceberg validate -c config.yaml`, `cx2iceberg ddl -c config.yaml dados/` (mostra o `CREATE TABLE` Iceberg), `cx2iceberg transforms` (lista as transformações), `run --out pasta --format csv|parquet|jsonl` (escreve localmente).

O ficheiro [examples/config.yaml](examples/config.yaml) cobre praticamente todas as funcionalidades com SerialPart, BOM e PCF.

Nenhum valor é alterado ou perdido sem aviso: uma perda não declarada é um erro, e uma perda declarada na configuração é permitida e fica contada no [relatório da carga](#relatório-da-carga-e-códigos-de-saída). As mudanças de comportamento, e o que fazer com configurações existentes, estão em [CHANGELOG.md](CHANGELOG.md).

## Estrutura da configuração

```yaml
target:                     # destino Athena
  database: catenax
  location: s3://bucket/warehouse      # cada tabela em <location>/<tabela>/
  temp_path: s3://bucket/tmp
  workgroup: primary
  region: eu-west-1
  # profile, create_database (true), schema_evolution (true)

input:                      # opcional: de onde vem o semantic id
  semantic_id: urn:...                 # fixo, ou
  semantic_id_map: {"*serial*": urn:...}   # por nome de ficheiro, ou
  envelope: {payload_path: payload, semantic_id_path: semanticId}
  match_all: false                     # um documento pode alimentar vários submodelos
  on_unmatched: fail                   # documento sem submodelo: fail (rejeitado) | skip (ignorado e contado)
  naive_timestamps: fail               # data/hora sem fuso: fail | utc | fuso IANA (ex.: Europe/Berlin)

defaults:                   # aplicado a todas as tabelas (sobreponível)
  metadata_columns: true
  on_error: fail

plugins: [meu_modulo]       # opcional: transformações próprias

submodels:
  - name: serial_part
    match: {semantic_id: "urn:samm:io.catenax.serial_part:3.*"}
    defaults: {mode: merge}            # defaults só deste submodelo
    common_columns:                    # colunas repetidas em todas as tabelas do submodelo
      catenax_id: $root.catenaXId | urn_suffix
    tables: [...]
    auto_normalize: {...}              # opcional: gera tabelas sozinho
```

Valores `${VAR}` ou `${VAR:-omissão}` são lidos de variáveis de ambiente — a mesma configuração serve dev/qa/prod.

O YAML é lido como **YAML 1.2**: só `true`/`false` são booleanos (`yes`, `no`, `on`, `off` são texto — no YAML 1.1, `map: {yes: Y, no: N, on: L, off: D}` ficava só com duas traduções), `010` é 10, `0o17` e `0x1A` são octal e hexadecimal, e uma chave repetida (por exemplo a mesma coluna duas vezes) é erro. As opções booleanas (`dedupe`, `keep_empty`, `required`, …) só aceitam `true`/`false`: antes `dedupe: "false"` era verdadeiro. As chaves de `input:` são validadas (`naive_timestamps: fail | utc`, …).

### Leitura dos ficheiros

Um ficheiro que não se consegue ler **não pára a carga**: fica como documento rejeitado, com a razão, e os restantes seguem (em JSON Lines a unidade é a linha). O JSON é lido em modo estrito, para nenhum valor ser trocado sem aviso:

| Entrada | Resultado |
|---|---|
| não é UTF-8 (Latin-1, Windows-1252, UTF-16…) | rejeitado, com o byte e a posição; a codificação **não é adivinhada** (adivinhar trocaria texto). BOM UTF-8 é aceite |
| `NaN`, `Infinity`, `-Infinity` | rejeitado (não é JSON) |
| chave repetida no mesmo objecto (`{"co2": 2.0, "co2": 20.0}`) | rejeitado: não se escolhe um dos valores |
| número fora do alcance do double (`1e400`, `1e-400`) ou inteiro com mais de 4300 dígitos | rejeitado |
| mais de 64 níveis de aninhamento | rejeitado |
| texto com um surrogate isolado (`"\ud800"`) | rejeitado (não pode ser gravado em UTF-8; falharia só na escrita) |
| topo que não é um objecto (`null`, `42`), elemento de lista que não é objecto, envelope sem `payload` | rejeitado |
| vários objectos num `.json` | rejeitado, com a sugestão de usar `.jsonl` |
| ficheiro sem documentos (`[]`, JSON Lines vazio) | aviso no relatório |

Números com mais de 15 algarismos significativos guardam também o texto original: um `decimal(38,20)` ou `bigint` recebe o valor exacto (ex.: `1234567890123456789.0` para `bigint` dá `1234567890123456789`, e não `…768`).

### match — que regras se aplicam a cada JSON

| Chave | Exemplo | Significado |
|---|---|---|
| `semantic_id` | `"urn:samm:io.catenax.serial_part:3.*"` | glob (ou lista) sobre o semantic id |
| `file` | `"*bom*.json"` | glob sobre o nome do ficheiro |
| `has_paths` | `[childItems]` | os caminhos têm de existir |
| `where` | `{path: specVersion, op: regex, value: "7\\."}` | condição arbitrária |

Sem `match`, o submodelo aceita tudo. O primeiro que corresponde ganha (a não ser com `match_all`).

### Tabelas

```yaml
- name: bom_as_built_edges
  explode: childItems            # 1 linha por elemento ("a[*].b[*]" para níveis aninhados)
  keep_empty: false              # true = linha com nulos quando a lista está vazia
  where: {path: hasAlternatives, op: eq, value: true}
  mode: merge                    # append | overwrite | overwrite_partitions | merge
  keys: [parent_id, child_id]    # chaves do MERGE e da deduplicação
  replace_by: [parent_id]        # DELETE das linhas existentes desses ids antes de inserir (com explode: $root/$meta)
  partition_by: [country, "day(created_on)", "bucket(16, child_id)"]
  dedupe: true                   # a mesma chave noutro documento do lote: fica o último (como no merge), com aviso
  on_duplicate: last             # a mesma chave duas vezes no MESMO documento: erro (omissão) | last | first
  on_error: fail                 # fail (rejeita o documento) | null | skip_row — ver "Relatório da carga"
  metadata_columns: [source_file, ingested_at]   # ou true
  auto: false                    # true = acrescenta todos os campos escalares automaticamente
  columns: {...}
  missing_columns: null          # merge: a tabela tem colunas que a config não produz -> NULL nelas (omissão: erro)
  athena_options: {}             # outras opções de awswrangler.athena.to_iceberg (não as que a config decide)
```

**Modos de escrita**

| mode | Comportamento |
|---|---|
| `append` | insere |
| `merge` | `MERGE INTO` pelas `keys` (actualiza ou insere) — ideal para o gémeo digital |
| `overwrite` | substitui a tabela inteira; se os documentos do submodelo no lote não derem nenhuma linha, a tabela fica vazia (antes ficava o conteúdo antigo) |
| `overwrite_partitions` | substitui só as partições presentes no lote |
| `append` + `replace_by` | apaga as linhas dos ids presentes e volta a inserir — ideal para **tabelas filhas** (listas que podem encolher) |

`where` aceita `op`: `eq, ne, in, not_in, exists, missing, empty, not_empty, regex, gt, gte, lt, lte, contains`, e combinações `{all: [...]}`, `{any: [...]}`, `{not: {...}}`.

As condições (`where`, `cases.when`, `check.required_if`, `match.where`) são validadas ao carregar (operador, tipo do `value`, regex) e comparam **com tipo**: números como números (`1` e `1.0` iguais), datas no formato do schema como instantes (`10:00+02:00` é antes de `09:30Z`), e, se um dos lados for uma data sem hora, a data tal como foi escrita (`created >= "2025-01-01"`). Comparar tipos diferentes é um erro, com o `on_error` de quem avalia: `{op: gt, value: "9"}` com o número 10, `{op: eq, value: 7}` com o texto `"7"`, `gte 0` com `true` ou `"n/a"`, `regex` sobre um número. Antes comparava-se texto com texto, e `10 > "9"` era falso. No `match`, um tipo diferente conta como "não corresponde".

Duas regras de chaves: uma linha com uma coluna de `keys` ou `replace_by` nula é um erro (não pode ser identificada; `on_error: skip_row` na coluna ignora a linha e conta); a mesma chave duas vezes no mesmo documento é um erro, a não ser com `on_duplicate` (antes ficava a última em silêncio — o mesmo filho duas vezes num BOM perdia uma quantidade). As chaves comparam com tipo (`1` e `1.0` são a mesma chave). `mode: merge` com `dedupe: false` não é aceite (o MERGE precisa de chaves únicas).

No modo `auto`, dois campos que dão o mesmo nome de coluna (`fossilGhgEmissions` e `fossil_ghg_emissions`) são um erro; uma coluna explícita com o nome de um campo auto substitui-o se ler o mesmo campo, e de outra origem é erro, a não ser com `auto_collision: prefix` (`item_<nome>`) ou `skip` (contado).

**Escrita**

Os tipos escritos são os declarados, nunca adivinhados dos dados (antes, um `bigint` com um nulo no lote passava pelo pandas como `float64` e `9007199254740993` ficava `...992`; um `tinyint` num Parquet local ficava `double`).

Antes de escrever qualquer tabela, e antes de qualquer `DELETE` do `replace_by`, **todas** as tabelas do lote são pré-verificadas; um problema pára a carga sem escrever nada (`--dry-run` faz a parte que não precisa da AWS):

* conversão para o schema Arrow declarado e o mesmo caminho de conversão que o awswrangler usa (se a versão do awswrangler tiver outra API, essa parte é saltada com aviso);
* `partition_by` com colunas que existem; nomes até 255 caracteres; `replace_by` só em colunas simples;
* comparação com a tabela no catálogo Glue, coluna a coluna:

| Tabela vs configuração | O que acontece |
|---|---|
| mesmo tipo | escreve |
| o tipo da tabela contém todos os valores do declarado (`int` → tabela `bigint`, `decimal(10,2)` → `decimal(12,3)`) | escreve com o tipo da tabela, sem `ALTER` |
| o declarado é um alargamento que o Iceberg faz sem mudar as linhas antigas (`int`→`bigint`, `decimal(p,s)`→`decimal(p2,s)`) | a tabela é alargada, com **aviso** (erro com `target.schema_evolution: false`) |
| coluna nova | acrescentada, com **aviso** (erro com `schema_evolution: false`) |
| outro tipo (`double` na tabela, `string` na config; mudar a escala de um decimal) | **erro**: o `ALTER` falharia já depois do `DELETE` |
| `float` de um lado e `double` do outro | **erro**: o `1.1` aceite como `float` (32 bits) é `1.100000023841858` e leria-se assim numa coluna `double` (nas linhas novas, ou nas antigas se a tabela fosse alargada). Declare o tipo da tabela |
| `merge` e a tabela tem colunas que a configuração não produz | **erro**: o `MERGE` poria `NULL` nelas nas linhas actualizadas; `missing_columns: null` declara-o (contado). Numa tabela `auto` (e no `auto_normalize`) a coluna em falta é um campo que os documentos do lote já não têm: `NULL` é o valor certo da nova versão, e fica contado. Em `append` as linhas novas ficam com `NULL` nessas colunas, sem mexer nas antigas |

`replace_by`: o `DELETE` abrange **todos** os documentos aceites do lote, também os que já não produzem linhas (a lista ficou vazia, ou o `where` excluiu todas): antes, as linhas antigas desses ficavam na tabela. Por isso, numa tabela com `explode`, as colunas do `replace_by` têm de ser de nível do documento (`$root.…`, `$meta.…` ou a coluna do `document_id`); um documento sem esse valor é rejeitado. Documentos rejeitados não apagam nada. Os valores são comparados com o tipo da coluna na tabela (`CAST('1.1' AS REAL)` para `float`: sem isso o `DELETE` não encontrava nada).

`athena_options` não aceita o que a configuração decide (`mode`, `dtype`, `merge_cols`, `partition_cols`, `schema_evolution`, …): antes passava por cima. `overwrite_partitions` só com partições por coluna (não `day(…)`, `bucket(…)`). As mesmas regras valem para os `defaults` de um submodelo com `auto_normalize` (antes `athena_options: {mode: overwrite}` passava e substituía a tabela raiz do `merge`), que também passam `missing_columns` às tabelas geradas. Duas colunas que dão o mesmo nome (`fossilGhg` e `fossil_ghg`) são um erro na carga da configuração (antes ficava só uma).

Ficheiros locais (`run --out`): **jsonl** com os números como foram lidos (decimal como número, não texto) e datas ISO 8601; **csv** com o nulo como campo vazio sem aspas e tudo o resto entre aspas (`""` é texto vazio), listas/objectos em JSON e booleanos `true`/`false`; **parquet** com o schema declarado.

### Colunas

Forma compacta — `caminho | transformação | transformação(arg) ::tipo` (em YAML sem aspas, `::` colado ao tipo):

```yaml
columns:
  catenax_id: catenaXId | urn_suffix
  part_id: localIdentifiers[?key=partInstanceId].value | first
  year: manufacturingInformation.date | year ::int
  bpn: businessPartner | upper | prefix(BPN-)
```

Forma completa:

```yaml
columns:
  quantity_kg:
    path: quantity                        # ou paths: [a, b] (primeiro não-nulo)
                                          # ou template: "{$root.catenaXId}|{catenaXId}"
                                          # ou const: 42
    transform: {unit_convert: {to: kilogram}}   # uma ou lista
    type: double
    default: 0
    required: true
    on_error: "null"
```

### Valores condicionais (`cases`)

O primeiro caso cuja condição `when` passa dá o valor; `else` é o valor por omissão. Cada caso pode ter a sua origem (`path`, `paths`, `template`, `const`) e a sua `transform`. As condições usam os mesmos operadores do `where`.

```yaml
land_management_biogenic_co2_emissions:      # "0 se o valor v7 for negativo"
  cases:
    - when: {path: pcf.extTFS_luGhgEmissions, op: gte, value: 0}
      path: pcf.extTFS_luGhgEmissions
    - else: 0
  type: double
```

### Validações (`check`)

Corre depois do cast. Numa lista, valida cada elemento. Compara com tipo: `in: [1, 2, 3]` aceita `2.0`, mas o texto `"2"` não corresponde ao número `2`; `min`/`max` só aceitam números (`true` e o texto `"5"` não são numéricos). `min`, `max` e `in` são validados ao carregar.

```yaml
company_ids:
  path: companyIds
  type: array<string>
  check:
    regex: "^urn:.*BPNL[0-9A-Z]{12}$"   # formato
    min: 0                              # intervalo (números)
    max: 100
    in: [cut-off, reverse cut-off]      # lista de valores fechada (ignore_case: true para ignorar maiúsculas)
    required: true                      # obrigatório
    required_if: {path: pcf.massBalancingUsed, op: eq, value: true}   # obrigatório se...
    on_fail: warn                       # warn (omissão) | null | skip_row | fail
```

As falhas aparecem no relatório da carga (tabela, coluna, ficheiro, valor) e no resumo do `--dry-run` (`checks falhados={coluna: n}`). Com `on_fail: fail` o documento é rejeitado.

### Caminhos

| Sintaxe | Resultado |
|---|---|
| `a.b.c` | campo aninhado |
| `lista[0]`, `lista[-1]` | por índice |
| `lista[*].campo` | todos (lista) |
| `lista[?key=manufacturerId].value` | filtro (`=`, `!=`, `~` regex, `[?campo]` existe) |
| `['chave.com.pontos']` | chave especial |
| `$root.catenaXId` | raiz do documento (útil dentro de `explode`) |
| `$parent.x`, `$parent.$parent.x` | nível de explode acima |
| `$index`, `$parent.$index` | posição na lista |
| `$item` | o próprio elemento (listas de escalares) |
| `$meta.source_file` / `semantic_id` / `ingested_at` / `document_id` | metadados |

**Campo ausente e caminho errado não são a mesma coisa.** Um campo que não existe (ou um objecto intermédio a `null`) dá nulo, como sempre. Um caminho que não tem a forma do documento é um **erro** (com o `on_error` da coluna), porque antes dava nulo em silêncio:

| Caminho | Documento | Resultado |
|---|---|---|
| `childItems.quantity` | `childItems` é uma lista | erro: use `childItems[*].quantity` |
| `xs[?k=a][0].v` | o `[0]` aplica-se a cada resultado do filtro (um objecto) | erro: para o 1.º resultado use `xs[?k=a].v \| first` |
| `block[0].c` | `block` foi desembrulhado pelo `unwrap` | erro: retire o índice |
| `name.first` | `name` é texto | erro |
| explode para um valor simples | | erro: o documento é rejeitado |

No `match` (reconhecer o submodelo) um caminho com outra forma conta apenas como "não corresponde", porque serve para distinguir versões.

**Filtros com tipos.** `[?k=abc]` ou `[?k='abc']` é texto; `[?n=1]` é número (`1` e `1.0` são iguais); `[?b=true]` booleano; `[?x=null]` nulo (ou ausente). Comparar tipos diferentes (`[?v='1']` com o número 1) é um erro, em vez de nunca corresponder. Entre aspas o valor pode ter `=`, `~` e `]` (`[?key='a=b]c']`), e o operador é o primeiro que aparece: `[?key~a=b]` é o regex `a=b`.

**Elementos sem o campo (`missing_elements`).** Com `[*]` ou um filtro, se algum elemento não tiver o campo a coluna dá erro, a não ser que se declare o que fazer — `first`, `count`, `join`, `sum` e os arrays dariam resultados diferentes consoante a interpretação:

```yaml
co2_por_etapa:
  path: stages[*].co2
  missing_elements: null     # um nulo por elemento em falta: [1.5, null, 2.0]
  # missing_elements: skip   # só os valores encontrados: [1.5, 2.0] (o resultado antigo)
  type: array<double>
```

Ambos ficam contados no relatório. Num `template` ou no `document_id.from` não se aceitam elementos em falta.

**Validação na carga.** Antes de ler qualquer documento, todos os caminhos (colunas, `paths`, `template`, `cases`, `where`, `check.required_if`, `explode`, `document_id.from`) são verificados: sintaxe, `$parent` acima da raiz, `$index` numa tabela sem `explode`, chaves de `$meta` desconhecidas. Com `model.schema` no submodelo, cada caminho é percorrido no schema do modelo, já com o `unwrap` aplicado: um campo que não existe dá erro com sugestão (`'fossilGHGEmissions' não existe … Quis dizer 'fossilGhgEmissions'?`), tal como um campo pedido a uma lista sem `[*]`, um índice numa lista desembrulhada, um `explode` que não é lista e caminhos do `unwrap` que não existem. Para um campo fora do schema (ex.: extensão), declare `outside_model: true` na coluna.

### Transformações

| Grupo | Transformações |
|---|---|
| texto | `trim upper lower snake_case prefix suffix replace regex_replace regex_extract substring split join to_string length` |
| Catena-X | `urn_suffix` (`urn:uuid:x`→`x`), `semantic_id_parts` (namespace/version/name), `unit_convert` (`{value, unit: "unit:gram"}`→kg, m, l, s, kWh, …) |
| valores | `default null_if map const` (`map` aceita `_default`, `_null` e `_ignore_case: true`) |
| números | `to_number multiply divide add round abs clamp` (`round: {digits: 2, mode: half_up}`; `clamp: {min: 0}` / `{max: 0}`; `to_number: {decimal: ",", thousands: "."}`) |
| datas | `to_timestamp to_date date_format year month now today` (`to_timestamp: {unit: s}` para números) |
| listas | `first last single count distinct sort compact sum min max pluck` (`sum: {nulls: skip}`) |
| JSON | `to_json from_json keys get` |
| outros | `to_bool hash uuid5 uuid4` |

Nenhuma transformação muda ou perde um valor sem o dizer. Os nomes e os argumentos são validados ao carregar a configuração.

| Transformação | Regra |
|---|---|
| `multiply divide add sum round clamp unit_convert` | contas exactas sobre o valor escrito: `0.1 * 3` dá `0.3` (em float dava `0.30000000000000004`); booleanos não são números |
| `sum min max` | um nulo na lista é erro (o total seria parcial); `{nulls: skip}` ignora-os e conta-os |
| `min max sort` | números como números, datas ISO como instantes (`10:00+02:00` é antes de `09:00Z`); números em texto (`["9", "10"]`) e tipos misturados dão erro |
| `round` | o modo é obrigatório (`half_up`, `half_even`, `half_down`, `up`, `down`, `ceiling`, `floor`): `0.125` dá `0.12` ou `0.13` conforme o modo |
| `clamp` | cada valor alterado fica contado no relatório |
| `single` | o único valor da lista; vários valores diferentes são erro (`first` escolheria um em silêncio) |
| `distinct null_if map` | comparam com tipo: `true`, `1` e `"1"` são diferentes (`1` e `1.0` iguais). Em `map`, um valor que só corresponde a uma chave de outro tipo (o número `1` e a chave `"1"`) é erro |
| `unit_convert` | quantidade sem unidade é erro; `from:` é a unidade a assumir quando o documento não a traz (contado). `value`/`quantityNumber` e `unit`/`measurementUnit` diferentes no mesmo objecto são erro |
| `to_bool` | um valor que não é booleano reconhecido é erro (antes: nulo) |
| `regex_extract` | sem correspondência é erro; `{pattern: ..., no_match: null}` anula e conta |
| `join` | nulos na lista são erro (`""` e nulo ficariam iguais); use `compact` antes |
| `pluck get keys semantic_id_parts` | aplicados a algo que não serve (ex.: `get` numa lista) são erro |
| `to_string join to_json` | texto igual ao das colunas `string`: `true`/`false`, JSON para objectos, datas ISO; números do documento como foram escritos |
| `to_timestamp` | ver "Datas" |

Transformações próprias:

```python
# meu_modulo.py  (declarado em plugins: [meu_modulo])
from cx2iceberg import register

@register("bpn_valido")
def bpn_valido(value, arg):
    return value if value and value.startswith("BPNL") and len(value) == 16 else None
```

### Tipos

`string tinyint smallint int bigint double float decimal(p,s) boolean timestamp date json array<T> map<K,V> struct<campo:T,...>`, aninháveis à vontade
(`json` grava o objecto serializado numa coluna string; `tinyint` e `smallint` validam o intervalo de cada valor, mas na tabela ficam `int`, porque o Iceberg não tem inteiros de 8/16 bits).

**Struct**: um dicionário com vários níveis num único campo, consultável com `coluna.campo.subcampo` no Athena.

```yaml
columns:
  life_cycle: { path: productLifeCycleStagesAndEmissions, type: struct }          # estrutura inferida dos dados
  attestations: { path: attestationOfConformance, type: "array<struct>" }         # lista de objectos
  time:                                                                            # estrutura explícita
    path: pcfAssessmentAndMethodology.pcfAssessmentInformation.time
    type: "struct<reference_period_start:timestamp, referencePeriodEnd:timestamp>"
```

Os nomes dos campos do struct passam a snake_case e casam com as chaves camelCase do JSON. Os campos que não estão no tipo são ignorados. Uma lista de 1 elemento é aceite como objecto; com mais de 1 dá erro.
Prefira tipos **explícitos** (o `init --style struct` gera-os a partir do schema SAMM): com `type: struct`, a estrutura depende dos dados de cada carga, e o Iceberg só deixa *acrescentar* campos a um struct, não mudar o tipo de um campo existente. Um objecto sem campos (`{}`) fica `json`, porque o Athena não aceita `struct<>`.
Sem `type`, o tipo é inferido. Números sem tipo ficam **double**, porque o JSON não distingue `1` de `1.0` e o Iceberg não deixa promover `bigint` para `double` depois. Mas um valor que mudaria nessa inferência rejeita o documento: um inteiro acima de 2^53 ou um número com mais algarismos do que um double guarda, e uma lista com tipos misturados (ficariam todos texto). Se a mesma coluna sem tipo recebe tipos diferentes no lote (`1.5` num documento, `"n/a"` noutro), a carga falha com exemplos de cada tipo: declare o `type`, e os valores que não servirem passam a ser rejeitados só no seu documento. Uma coluna sem tipo que no lote só tem nulos não é criada (o tipo seria adivinhado e ficaria na tabela Iceberg); fica um aviso no relatório.

#### Casts sem perdas

| Tipo | Regra |
|---|---|
| `tinyint smallint int bigint` | fora do intervalo do tipo (`300` num `tinyint`) é erro (antes só falhava na escrita); casas decimais são erro |
| `decimal(p,s)` | mais casas decimais do que `s` é erro, a não ser com `rounding` na coluna; mais algarismos do que `p - s` antes da vírgula é erro. `p` até 38 |
| `double` | o número tem de sair do double como foi escrito: `9007199254740993` ou `0.12345678901234567890` dão erro, a não ser com `rounding: nearest` |
| `float` | é um float de **32 bits** (`real` no Athena): `1.23456789` ficaria `1.2345679` → erro, a não ser com `rounding: nearest`. Para números com casas decimais use `double` |
| todos os números | booleanos não são números (`true` num `double` era `1.0`) |
| `string` | números do documento como foram escritos, `true`/`false`, JSON para objectos |
| `json` | texto tem de ser JSON válido (antes `"abc"` era guardado tal e qual) |
| `map struct` | duas chaves que dão a mesma chave no tipo (`"1"` e `"1.0"` num `map<double,…>`; `fossilGhg` e `fossil_ghg` num struct) é erro |

```yaml
valor_kg:  {path: valor, type: "decimal(12,3)", rounding: half_up}   # arredonda e conta no relatório
rácio:     {path: r, type: double, rounding: nearest}                # aceita o double mais próximo
```

#### Datas

Texto de datas só no formato do schema SAMM (`xsd:dateTime`): `2022-01-01T10:00:00`, fracção opcional, `Z` ou `±hh:mm` até ±14:00; ou uma data `2022-01-01` (com fuso opcional). O leitor de datas do Python aceitava muito mais (`2022-01-01 10:00`, `20220101T1000`, `+0200`, semanas ISO) e lia `2022-01-01+02:00` — uma data com fuso — como as 02:00 desse dia. Outros formatos: `to_timestamp: {format: "%d/%m/%Y %H:%M %z"}`. `24:00:00` (fim do dia, válido no schema) é o instante 00:00 do dia seguinte; para `date`, `year`, `month` e `date_format` é ambíguo (`2022-12-31T24:00:00` é fim de 2022 ou início de 2023) e dá erro. Anos fora de 0001–9999 não são suportados.

`timestamp` é um instante: com fuso (`Z`, `+02:00`) é convertido para UTC. Sem fuso (`2022-01-01T10:00:00`) o formato é válido no schema, mas o instante é desconhecido: dá erro, a não ser que se declare `input.naive_timestamps: utc` ou o fuso IANA do emissor (`Europe/Berlin`). Com um fuso, horas que não existem ou acontecem duas vezes na mudança de hora são erro. Uma data sem hora usada como timestamp (fica a meia-noite) também precisa dessa declaração. Cada valor assumido fica contado. Só o `timestamp` precisa de fuso: datas, anos e meses usam o que foi escrito. `date`, `to_date`, `year`, `month` e `date_format` usam a data tal como foi escrita, no fuso do documento: `2023-07-11T01:00:00+02:00` é o dia 11 (em UTC seria o dia 10). Um número só é data com a unidade declarada (`to_timestamp: {unit: s}` ou `{unit: ms}`); antes, `year` de `2022` dava 1970. Fracções abaixo do microssegundo não cabem num timestamp Iceberg: ficam cortadas e contadas. `today` é a data UTC, como `now`.

#### Números em texto

Um número que chega como texto (`"2.5"`) é lido **só** se seguir o formato dos números JSON (`123`, `-1.5`, `2e3`, com dígitos 0–9 e espaços à volta); fica contado no relatório como aviso. O que for ambíguo ou não for um número dá erro, com a razão:

| Texto | Sem formato declarado | Porquê |
|---|---|---|
| `"1.234"`, `"12.345"`, `"1,234"` | erro | pode ser separador de milhares (1234) ou decimal (1,234) |
| `"1,5"`, `"1.234,5"` | erro | vírgula: é preciso declarar o formato |
| `"1_000"`, `"+5"`, `"007"`, `".5"`, `"0x10"` | erro | não é o formato JSON |
| `"١٢٣"`, `"１２３"` (dígitos não-ASCII) | erro | o Python aceitava-os em silêncio |
| `"NaN"`, `"inf"`, `"1e400"`, `"1e-400"` | erro | não é finito / não cabe num double |
| `""` ou só espaços (coluna numérica) | nulo, com aviso | |

Para números com outro formato, declare-o com `to_number` (a mesma regra vale para `multiply`, `add`, `unit_convert`…):

```yaml
quantidade: {path: qtd, transform: {to_number: {decimal: ",", thousands: "."}}, type: double}   # "1.234,5" -> 1234.5
```

Os separadores de milhares têm de estar em grupos de 3 (`"1.23,4"` dá erro). Os nomes e os argumentos das transformações são validados ao carregar a configuração: um nome mal escrito pára a carga logo (antes, com `on_error: null`, anulava todos os valores da coluna).

### UUID que liga as tabelas (`document_id`)

Quando um submodelo dá origem a várias tabelas (`explode`, `auto_normalize`), `document_id` acrescenta a **todas** a mesma coluna com um UUID por documento, para os JOINs:

```yaml
submodels:
  - name: serial_part
    document_id:
      column: serial_part_uuid     # omissão: document_id
      from: catenaXId              # ou lista: [id, {path: version, type: int}]
      # strategy: key | content | random
      # namespace: cx2iceberg      # muda o espaço de UUIDs (ex.: um por ambiente)
      # on_missing: fail           # campo de from em falta: fail (omissão) | content
```

| strategy | UUID | Reprocessar o mesmo documento |
|---|---|---|
| `key` (com `from`) | uuid5 dos campos indicados | mesmo UUID, mesmo que o conteúdo mude: o `merge` actualiza as linhas |
| `content` (`document_id: true`) | uuid5 do conteúdo inteiro | mesmo UUID só se nada mudou |
| `random` | uuid4 | UUID novo em cada carga: só para tabelas `append` |

Recomenda-se `key`. Com `random`, reprocessar um ficheiro duplica as linhas das tabelas filhas.
Os caminhos de `from` contam **depois** do `unwrap`. Se um campo de `from` faltar num documento, o documento é rejeitado: o UUID mudaria quando o campo aparecesse, e o `merge` duplicaria as linhas. Com `on_missing: content` usa-se o conteúdo (contado).
Campos de `from` que não são texto precisam do tipo: `{path: version, type: int}`. O valor é convertido antes do UUID, por isso `0`, `"0"` e `0.0` dão o mesmo UUID (antes davam três). Com os mesmos valores e o mesmo `namespace`, o UUID é igual entre submodelos: o mesmo PCF (`id` + `version`) tem o mesmo UUID venha em v7 ou em v9.

> **Migração (UUIDs já gravados).** Para campos de texto e números inteiros o UUID não mudou. Mudou para documentos em que uma parte do `from` vinha como texto (`"0"`) ou com casas decimais (`0.0`) e agora é declarada como inteiro: esses documentos passam a ter o UUID dos restantes. Se tabelas já gravadas tiverem linhas com o UUID antigo, reprocesse esses documentos e apague as linhas com o UUID antigo (nas tabelas filhas com `replace_by`, as linhas antigas não são apagadas porque o UUID é outro).

`hash` e `uuid5` só aceitam texto ou objectos/listas: um número sozinho é erro (`1` e `1.0` dariam identidades diferentes); use `to_string` ou um `template` antes. Um `template` com **todas** as partes em falta dá nulo, como um campo ausente. Com só **algumas** em falta é um erro: as partes presentes perder-se-iam (nulo) ou o valor ficaria errado (`"P|"`, igual para todos os elementos sem a parte). Declare `template_missing: empty` (partes em falta vazias) ou `template_missing: null` (valor nulo) na coluna; ambos ficam contados.
No `auto_normalize` sem `id_path`, o UUID passa a ser a chave da tabela raiz e das tabelas filhas. Está também disponível em caminhos como `$meta.document_id`.

### Listas 1:1 (`unwrap`)

Muitos modelos Catena-X (ex.: PCF 9.0) embrulham cada bloco numa lista de um só elemento: `pcfAssessmentAndMethodology[0].pcfAssessmentInformation[0].time[0].created`. O `unwrap` (ao nível do submodelo) trata essas listas como objectos **antes** de aplicar as regras, por isso os caminhos ficam sem `[0]` e o modo automático deixa de criar tabelas de 1 linha.

```yaml
submodels:
  - name: pcf9
    unwrap:
      all: true                         # todas as listas de objectos...
      except: [attestationOfConformance] # ...menos as que têm mesmo vários elementos
      on_multiple: fail                 # fail (omissão) | first (usa o 1º e avisa) | keep (deixa a lista)
    # ou: unwrap: [general, carbonContent, productLifeCycleStagesAndEmissions.productionStage]
```

Os caminhos não levam índices e contam desde a raiz. Com `all`, as listas de valores simples (ex.: `productIds`) não são tocadas. Uma lista indicada explicitamente e vazia passa a nulo.
Se aparecer uma lista com mais de um elemento onde se esperava um, a carga **falha** com o ficheiro e o caminho em causa, para nada se perder em silêncio.

### Modo automático

* `auto: true` numa tabela: achata todos os campos escalares do objecto actual (`a.b.c` → `a_b_c`), listas de escalares viram `array<...>`, datas ISO viram `timestamp`. As colunas explícitas têm prioridade. Opções: `auto_exclude`, `auto_max_depth`, `auto_lists: array|json|skip`, `auto_dates`, `auto_naming`.
* `auto_naming: leaf` (ou `naming: leaf` no `auto_normalize`): nomes curtos. Usa só o último campo (`fossil_ghg_emissions`) e acrescenta níveis de cima **só onde há colisão** (`production_stage_packaging_land_management_biogenic_co2_emissions` vs `packaging_stage_…`). **Precisa de `model.schema`**: as colisões calculam-se sobre todos os campos do modelo (todas as alternativas de `oneOf`/`anyOf`), por isso cada campo tem sempre o mesmo nome, em todos os documentos e em todas as cargas. Antes calculavam-se com os campos de cada documento: na mesma carga, `ghg.fossil` ia para `fossil` num documento e para `ghg_fossil` noutro (que também tinha `other.fossil`), e a soma da coluna ficava errada sem nenhum aviso. Um campo que não está no modelo fica com o nome completo (`extra_fossil`) e é contado como aviso; se esse nome for o de um campo do modelo, é erro (colisão). Sem `model.schema`, `leaf` é um erro na configuração: use `full` ou gere colunas explícitas com `init --naming leaf`.
* `auto_normalize` num submodelo: cria uma tabela raiz e **uma tabela por cada lista de objectos** (a qualquer profundidade), com o id da raiz e índices de posição como chave. As tabelas filhas usam `replace_by` para ficarem sincronizadas quando listas encolhem.
* De onde vêm as tabelas filhas (`auto_normalize.children`):
  * `schema` (omissão com `model.schema`): todas as listas do modelo, já com o `unwrap`, mais as que aparecerem nos dados. Uma lista que ficou vazia em **todos** os documentos do lote continua a ter tabela, e o `DELETE` das linhas antigas corre. Custo: em `merge`, um `DELETE` por tabela filha existente em cada carga (o PCF 9.0 sem `unwrap` tem 27; com `unwrap: {all: true}`, 2).
  * `batch`: só as listas presentes no lote. Antes era sempre assim, e um documento cuja lista ficou vazia (sem outro documento do lote com essa lista) deixava as linhas antigas na tabela filha. Sem `model.schema`, e com um `mode` que não seja `append`, tem de ser declarado; fica contado no relatório em cada carga.

```yaml
model:
  schema: schemas/pcf_7.0.0-schema.json   # fixa as tabelas filhas e os nomes leaf
auto_normalize:
  table_prefix: pcf
  id_path: id          # omissão: catenaXId
  id_column: pcf_id
  exclude: [comment]
  naming: leaf         # full (omissão) | leaf (precisa de model.schema)
  children: schema     # schema (omissão com model.schema) | batch
```

### Colunas de metadados

`metadata_columns: true` acrescenta `cx_source_file`, `cx_semantic_id`, `cx_ingested_at` e `cx_record_hash` (sha256 dos dados da linha, útil para detectar alterações).

## Modelos profundos: o caso PCF 9.0

O PCF 9.0 tem até 5 níveis e quase tudo são listas de 1 elemento. Sem `unwrap`, o `auto_normalize` gera **27 tabelas de 1 linha**. Há dois modelos que funcionam bem, ambos gerados a partir do schema oficial e testados com o exemplo oficial:

| | [examples/pcf9_flat.yaml](examples/pcf9_flat.yaml) | [examples/pcf9_struct.yaml](examples/pcf9_struct.yaml) |
|---|---|---|
| Tabelas | 1 principal (todos os campos 1:1 como colunas) + 8 filhas (uma por lista real) | 1 |
| Colunas | ~140 colunas simples | 12 (blocos como `struct`) |
| Consulta | `SELECT fossil_ghg_emissions FROM pcf9` | `SELECT product_life_cycle_stages_and_emissions.production_stage.fossil_ghg_emissions FROM pcf9` |
| Listas | tabelas `pcf9_product_ids`, `pcf9_attestation_of_conformance`, … (join por `pcf9_id`) | `array<...>` dentro da linha (`CROSS JOIN UNNEST`) |
| Bom para | BI/dashboards, ferramentas que não lidam com tipos aninhados | manter a hierarquia do modelo, poucas colunas |

Os dois podem coexistir: as tabelas são independentes, por isso pode juntar ambos os `tables:` no mesmo submodelo.

### Listas para várias linhas (`explode`)

```yaml
- name: pcf9_product_ids                 # lista de strings -> 1 linha por valor
  explode: companyAndProductInformation.productInformation.productIds
  keys: [pcf9_id, value]
  columns:
    pcf9_id: $root.pcfAssessmentAndMethodology.pcfAssessmentInformation.idAndVersion.id
    position: $index ::int
    value: $item

- name: bom_lines                        # listas aninhadas: 1 linha por elemento do nível mais fundo
  explode: "orders[*].lines[*]"
  columns:
    order_no: $parent.no
    order_pos: $parent.$index ::int
    sku: sku
```

`keep_empty: true` gera uma linha com nulos quando a lista está vazia (equivale a um LEFT JOIN). `replace_by: [id]` apaga primeiro as linhas antigas do mesmo documento, para que uma lista que encolheu não deixe restos.

## Migração entre versões de um modelo

[examples/pcf_v7_to_v9.yaml](examples/pcf_v7_to_v9.yaml) põe PCF 7.0.0 e PCF 9.0.0 na mesma tabela, com colunas V9. Tem um exemplo comentado de cada tipo de transformação que um mapping de versões costuma pedir:

| | Transformação | Como |
|---|---|---|
| 1 | renomear campo | nome da coluna ≠ caminho |
| 2 | mudar de nível / estrutura | caminho completo, `unwrap` |
| 3 | nomes alternativos na origem (`x; extWBCSD_x`) | `paths: [a, b]` |
| 4 | campo inexistente na origem → vazio | `const: null` com `type` |
| 5 | campo descartado | não mapear |
| 6 | lista de objectos → lista de valores | `lista[*].campo` |
| 7 | listas aninhadas achatadas | `a[*].b[*].campo` |
| 8 | valor simples → lista | `type: array<...>` |
| 9 | um campo → vários destinos, condicional; troca de sinal | `cases`, `clamp`, `multiply: -1` |
| 10 | formato (ex.: URN com BPN) | `check.regex` |
| 11 | intervalo (0–100) | `check.min/max` |
| 12 | lista de valores fechada, valores antigos → novos | `map` + `check.in` |
| 13 | obrigatório condicional ("M if …") | `check.required_if` |
| 14 | valor por omissão | `default` |
| 15 | várias versões na mesma tabela | um submodelo por versão, mesmo `name` de tabela, `match.any` por semantic id ou `specVersion` |

Com `keys: [id, version]`, um PCF migrado de v7 e o mesmo PCF entregue em v9 ficam **na mesma linha**: ganha o último a ser processado. As regras que alimentam a mesma tabela têm de concordar em `mode`, `keys`, `partition_by`, `replace_by`, `dedupe`, `on_duplicate`, `location`, `metadata_columns` e no tipo (e `rounding`) de cada coluna comum; senão a configuração não carrega (antes valia a 1.ª regra: um `decimal` da 2.ª regra virava `double`). Os exemplos oficiais V7 e V9 têm o mesmo `id`, por isso, para os comparar, corra cada um em separado.

## Studio: construir transformações sem escrever YAML

```bash
cx2iceberg studio --root . --open
```

Abre em `http://127.0.0.1:8765` uma aplicação local, pensada para quem não conhece o formato da configuração:

1. **Escolher um submodelo** do catálogo oficial ([eclipse-tractusx/sldt-semantic-models](https://github.com/eclipse-tractusx/sldt-semantic-models)), com as versões e o estado de cada uma (release, deprecated…). O studio descarrega o schema e o exemplo dessa versão para `models/<submodelo>/<versão>/` e cria uma configuração inicial com:
   * o identificador do documento;
   * o UUID que liga as tabelas;
   * a validação contra o schema;
   * o `unwrap` das listas que no exemplo têm um só elemento.
2. **Campos do modelo:** todos os campos do schema, com nome legível, descrição, tipo, a marca "obrigatório" e o valor do exemplo. Os campos que o exemplo não traz aparecem também, marcados como "sem valor no exemplo".
3. **Clicar num campo cria uma coluna.** Cada coluna configura-se em linguagem simples:
   * nome e tipo;
   * "se estiver vazio, usar…";
   * passos: maiúsculas, tirar o prefixo URN, converter unidade, traduzir valores, limitar entre mínimo e máximo…;
   * validação: obrigatório, mínimo e máximo, valores permitidos, e o que fazer se falhar.
4. **Listas:**
   * "Uma linha por elemento" cria uma tabela filha, ligada à principal pelo UUID do documento;
   * "É sempre um só" ou "Pode ter vários" decide o `unwrap`.
5. **Resultado em tempo real** sobre os dados de teste: o exemplo oficial, ficheiros da pasta ou um documento colado. Inclui:
   * a validação contra o schema, com as mensagens em português e o caminho de cada problema;
   * o **relatório da carga**, o mesmo do `run`: documentos rejeitados (e porquê), perdas declaradas e avisos, contados por tabela, coluna e motivo, com exemplos. As colunas com eventos têm um ● no cabeçalho da grelha. Um documento rejeitado não esconde as linhas dos outros;
   * a pré-verificação da escrita (a mesma do `--dry-run`), que avisa se a escrita seria bloqueada;
   * os valores tal como vão ficar na tabela: inteiros grandes e decimais mostrados exactos (antes o browser mostrava `9007199254740993` como `…992`) e um `float` como o Athena o mostra (`1.1`, não `1.100000023841858`).
6. **Guardar** gera o YAML. Em **Avançado** vê-se e edita-se o YAML, para as regras que o construtor não mostra (`cases`, `template`…). Essas colunas aparecem no construtor como "regra avançada".

Sem acesso ao GitHub (proxy, rede fechada), use um clone local do repositório de modelos:

```bash
cx2iceberg studio --models-dir ~/code/sldt-semantic-models
```

A lista do catálogo vem de uma só chamada à API do GitHub e fica em cache 24 h em `.cx2iceberg/`. Com `GITHUB_TOKEN` definido, usa esse token.
Ao **Guardar**, o studio confirma que o ficheiro ainda é o que abriu (pelo conteúdo): se foi alterado fora do studio, não grava e pede para recarregar. Antes comparava a data de modificação, que com a mesma marca de tempo (pastas partilhadas com Docker/WSL, sistemas de ficheiros com resolução de 1–2 s, `cp -p`) deixava apagar a edição feita fora.
O servidor só aceita ligações de `127.0.0.1` e só lê e grava dentro de `--root`. A pré-visualização nunca escreve no Athena.

## Validar os documentos contra o schema do modelo

```yaml
submodels:
  - name: pcf_v9
    model:
      schema: ../models/io.catenax.pcf/9.0.0/Pcf-schema.json   # relativo à configuração
      example: ../models/io.catenax.pcf/9.0.0/Pcf.json
    validate: warn        # off | warn (omissão com model) | skip | fail
```

Cada documento é validado **à entrada**, antes de qualquer regra: tipos, campos obrigatórios, enumerações, formatos (por exemplo o padrão das datas `Timestamp`) e limites. Com `warn` o problema fica no relatório e o documento segue. Com `skip` o documento é ignorado (e contado). Com `fail` o documento é rejeitado.
À saída, cada coluna com `type:` é convertida para esse tipo, com o `on_error` da coluna, e o `check` acrescenta as regras de negócio. São duas camadas: o schema diz se o documento é válido; o tipo e o `check` dizem se o valor serve para a tabela.

## Relatório da carga e códigos de saída

Nenhum documento é perdido em silêncio. Cada carga produz um relatório com **tabela, coluna, ficheiro, documento** (posição no ficheiro, em listas JSON e JSON Lines), **linha** (posição no `explode`, ex.: `childItems[1]`), **valor**, **o que aconteceu** e **porquê**. Há três tipos de evento:

| Tipo | Quando | Efeito |
|---|---|---|
| erro | valor inválido com `on_error: fail` (omissão), obrigatório vazio, `check` com `on_fail: fail`, ficheiro ilegível ou documento inválido (ver "Leitura dos ficheiros"), `validate: fail`, `unwrap` com vários elementos, chave nula, chave repetida no mesmo documento, condição que compara tipos diferentes, documento sem submodelo, campo do `document_id` em falta, `template` com partes em falta, documento sem o valor do `replace_by` | o **documento é rejeitado**: nenhuma linha dele é escrita, em nenhuma tabela |
| perda declarada | `on_error: null` / `skip_row`, `default` aplicado, linha excluída pelo `where`, `validate: skip`, `unwrap on_multiple: first`, `missing_elements`, `rounding`, `clamp`, `sum: {nulls: skip}`, `unit_convert` com `from`, `regex_extract` com `no_match: null`, `naive_timestamps`, `on_duplicate`, `template_missing: empty | null`, `auto_collision: skip`, `on_unmatched: skip`, `document_id.on_missing: content`, `missing_columns: null`, `auto_normalize.children: batch` | segue, e fica contado |
| aviso | campo fora do modelo com `auto_naming: leaf`, `check` com `on_fail: warn`, a mesma chave noutro documento do lote (fica a última), número recebido como texto, texto vazio numa coluna numérica, ficheiro sem documentos, fracção abaixo do microssegundo cortada, coluna sem tipo só com nulos (não criada), coluna nova ou alargada na tabela do Athena | segue, e fica contado |

A pré-verificação da escrita (ver "Escrita") não é um erro de um documento: pára a carga inteira antes de escrever (no relatório: `failure` com os problemas e todas as tabelas `not_written`; código de saída `1`).

Os erros não param no primeiro: a carga percorre todos os documentos e mostra todos os erros de uma vez. Um documento rejeitado não trava os outros: num lote com ficheiros de vários parceiros, os válidos são escritos e o relatório diz quais os ficheiros a pedir de novo.

```bash
cx2iceberg run -c config.yaml dados/ --report relatorio.json --report-details eventos.jsonl
```

| Opção | |
|---|---|
| `--report FICHEIRO` | relatório em JSON: contagens exactas por tabela/coluna/motivo e os primeiros exemplos de cada |
| `--report-details FICHEIRO` | todos os eventos, um por linha, escritos à medida (não ficam em memória) |
| `--samples N` | exemplos guardados por grupo (omissão 20) |
| `--max-errors N` | ao fim de N erros a carga pára e nada é escrito (omissão 1000) |

Em memória ficam só as contagens (uma por tabela/coluna/motivo, o que depende da configuração e não dos dados) e os primeiros exemplos de cada grupo; os valores são cortados a 200 caracteres. No log aparecem só esses exemplos, não um aviso por valor. Por linha, o relatório guarda apenas de que documento e de que posição do `explode` ela veio (cerca de 18 bytes), para poder retirar as linhas de um documento rejeitado e dizer de onde vem um duplicado.

O relatório é gravado **no fim**, com o resultado da escrita de cada tabela: `written` (escrita), `failed` (com o erro), `not_written` (não chegou a ser escrita por causa de uma falha anterior), `empty` (sem linhas) ou `interrupted` (a carga foi interrompida a meio; estado desconhecido). Se uma tabela com `replace_by` falhar depois do `DELETE`, o relatório diz que as linhas antigas desses ids já foram apagadas. Com `--dry-run`, `write` é `null`.

**Códigos de saída** de `run` e `ddl`: `0` tudo bem (pode haver perdas declaradas e avisos); `2` carga parcial: documentos rejeitados, os restantes foram escritos (ou, com `--dry-run`, nada foi escrito); `1` falha (configuração, `--max-errors`, escrita): o relatório diz que tabelas ficaram escritas e qual falhou.

## AWS Lambda

`cx2iceberg.lambda_handler.handler` processa eventos S3 (ex.: payloads do EDC gravados num bucket). Defina `CX2ICEBERG_CONFIG` (caminho no pacote ou `s3://…/config.yaml`).

É **tudo ou nada por invocação**: se algum documento do evento for rejeitado, nada é escrito, o relatório vai para o log e a invocação falha. Assim, uma nova tentativa não duplica linhas já escritas. Configure `MaximumRetryAttempts=0` e um destino *on-failure* (ou DLQ): repetir não corrige erros nos dados e, sem destino, a AWS descarta o evento depois das tentativas. Com sucesso devolve `{"status", "written", "report"}`, em JSON estrito. O `report` da resposta tem só as contagens (sem exemplos) e o resultado da escrita, para caber nos limites da AWS; o relatório completo vai para o log (com nível `WARNING` quando tem avisos ou perdas declaradas, porque o nível por omissão da Lambda esconde o `INFO`). Se a escrita falhar, o log diz que tabelas ficaram escritas e qual falhou.

Depois da escrita nada pode falhar: a resposta é construída e validada **antes** de escrever. Uma falha depois da escrita deixaria os dados escritos com a invocação marcada como falhada: a AWS repetiria (linhas duplicadas nas tabelas `append`) ou mandaria o evento para o destino *on-failure*, e reprocessá-lo duplicaria as linhas outra vez. Uma falha **durante** a escrita (ex.: Athena falha na 2.ª tabela) continua a deixar as tabelas anteriores escritas: por isso `MaximumRetryAttempts=0`. Permissões necessárias: `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`, `glue:Get*/CreateTable/UpdateTable/CreateDatabase`, e `s3:GetObject/PutObject/DeleteObject/ListBucket` nos buckets de entrada, `location` e `temp_path`.

## Uso programático

```python
from cx2iceberg import load_config, transform, write_athena

cfg = load_config("config.yaml")
results = transform(cfg, ["dados/"], semantic_id="urn:samm:io.catenax.serial_part:3.0.0#SerialPart")
write_athena(cfg, results)
```

`transform` lança `DocumentsRejected` se algum documento tiver erros (com `.report` e `.results`). Para escrever só os documentos válidos:

```python
from cx2iceberg import Engine, read_documents

engine = Engine(cfg)
results = engine.run(list(read_documents(["dados/"], cfg.input)), partial=True)
print(engine.report.render())        # ou engine.report.to_dict()
```

## Limitações

* Lê o formato **value-only** dos submodelos Catena-X. O formato AAS completo (`submodelElements`) não é convertido.
* O schema SAMM não diz quais listas são 1:1 (não tem `maxItems`), por isso o `unwrap` é uma decisão sua; a carga falha se a decisão estiver errada para um documento.
* `replace_by` executa `DELETE` e depois `INSERT`: não é atómico (a pré-verificação reduz, mas não elimina, o risco de falhar entre os dois; se falhar, o relatório diz que o `DELETE` já foi feito).
* `merge` nunca apaga: uma linha que deixa de ser produzida (ex.: um filho retirado do BOM) fica na tabela. Para listas que podem encolher use `append` + `replace_by`.
* A pré-verificação compara tipos de colunas; mudanças na especificação de partições de uma tabela já existente não são detectadas.
* Processa os documentos em memória. Para volumes muito grandes, chame-o por lotes (ex.: um ficheiro ou prefixo S3 por execução).

## Testes

```bash
pip install -e ".[dev]" && pytest
```

## Licença

O código é distribuído sob a licença [Apache-2.0](LICENSE). Os schemas e alguns exemplos em `examples/` vêm do projeto Eclipse Tractus-X e mantêm a licença CC-BY-4.0; ver [NOTICE.md](NOTICE.md).
