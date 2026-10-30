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

Outros comandos: `cx2iceberg validate -c config.yaml`, `cx2iceberg ddl -c config.yaml dados/` (mostra o `CREATE TABLE` Iceberg), `cx2iceberg transforms` (lista as transformações), `run --out pasta --format csv|parquet|jsonl` (escreve localmente).

O ficheiro [examples/config.yaml](examples/config.yaml) cobre praticamente todas as funcionalidades com SerialPart, BOM e PCF.

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
  replace_by: [parent_id]        # DELETE das linhas existentes desses ids antes de inserir
  partition_by: [country, "day(created_on)", "bucket(16, child_id)"]
  dedupe: true                   # remove duplicados por keys (fica o último)
  on_error: fail                 # fail | null | skip_row
  metadata_columns: [source_file, ingested_at]   # ou true
  auto: false                    # true = acrescenta todos os campos escalares automaticamente
  columns: {...}
  athena_options: {}             # passados tal-e-qual a awswrangler.athena.to_iceberg
```

**Modos de escrita**

| mode | Comportamento |
|---|---|
| `append` | insere |
| `merge` | `MERGE INTO` pelas `keys` (actualiza ou insere) — ideal para o gémeo digital |
| `overwrite` | substitui a tabela inteira |
| `overwrite_partitions` | substitui só as partições presentes no lote |
| `append` + `replace_by` | apaga as linhas dos ids presentes e volta a inserir — ideal para **tabelas filhas** (listas que podem encolher) |

`where` aceita `op`: `eq, ne, in, not_in, exists, missing, empty, not_empty, regex, gt, gte, lt, lte, contains`, e combinações `{all: [...]}`, `{any: [...]}`, `{not: {...}}`.

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

Corre depois do cast. Numa lista, valida cada elemento.

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

As falhas aparecem no log, com o ficheiro, e no resumo do `--dry-run` (`checks falhados={coluna: n}`).

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
| `$meta.source_file` / `semantic_id` / `ingested_at` | metadados |

### Transformações

| Grupo | Transformações |
|---|---|
| texto | `trim upper lower snake_case prefix suffix replace regex_replace regex_extract substring split join to_string length` |
| Catena-X | `urn_suffix` (`urn:uuid:x`→`x`), `semantic_id_parts` (namespace/version/name), `unit_convert` (`{value, unit: "unit:gram"}`→kg, m, l, s, kWh, …) |
| valores | `default null_if map const` (`map` aceita `_default` e `_ignore_case: true`) |
| números | `to_number multiply divide add round abs clamp` (`clamp: {min: 0}` / `{max: 0}`) |
| datas | `to_timestamp to_date date_format year month now today` (ISO-8601 com fuso → UTC) |
| listas | `first last count distinct sort compact sum min max pluck` |
| JSON | `to_json from_json keys get` |
| outros | `to_bool hash uuid5 uuid4` |

Transformações próprias:

```python
# meu_modulo.py  (declarado em plugins: [meu_modulo])
from cx2iceberg import register

@register("bpn_valido")
def bpn_valido(value, arg):
    return value if value and value.startswith("BPNL") and len(value) == 16 else None
```

### Tipos

`string int bigint double float decimal(p,s) boolean timestamp date json array<T> map<K,V> struct<campo:T,...>`, aninháveis à vontade
(`json` grava o objecto serializado numa coluna string).

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
Sem `type`, o tipo é inferido. Números sem tipo ficam **double**, porque o JSON não distingue `1` de `1.0` e o Iceberg não deixa promover `bigint` para `double` depois. Um cast para `int` de um valor com casas decimais dá erro em vez de truncar.

### UUID que liga as tabelas (`document_id`)

Quando um submodelo dá origem a várias tabelas (`explode`, `auto_normalize`), `document_id` acrescenta a **todas** a mesma coluna com um UUID por documento, para os JOINs:

```yaml
submodels:
  - name: serial_part
    document_id:
      column: serial_part_uuid     # omissão: document_id
      from: catenaXId              # ou lista: [id, version]
      # strategy: key | content | random
      # namespace: cx2iceberg      # muda o espaço de UUIDs (ex.: um por ambiente)
```

| strategy | UUID | Reprocessar o mesmo documento |
|---|---|---|
| `key` (com `from`) | uuid5 dos campos indicados | mesmo UUID, mesmo que o conteúdo mude: o `merge` actualiza as linhas |
| `content` (`document_id: true`) | uuid5 do conteúdo inteiro | mesmo UUID só se nada mudou |
| `random` | uuid4 | UUID novo em cada carga: só para tabelas `append` |

Recomenda-se `key`. Com `random`, reprocessar um ficheiro duplica as linhas das tabelas filhas.
Os caminhos de `from` contam **depois** do `unwrap`. Se um campo de `from` faltar num documento, usa-se o conteúdo e fica um aviso no log.
Com os mesmos valores e o mesmo `namespace`, o UUID é igual entre submodelos. Por isso o mesmo PCF (`id` + `version`) tem o mesmo UUID venha em v7 ou em v9.
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
* `auto_naming: leaf` (ou `naming: leaf` no `auto_normalize`): nomes curtos. Usa só o último campo (`fossil_ghg_emissions`) e acrescenta níveis de cima **só onde há colisão** (`production_stage_packaging_land_management_biogenic_co2_emissions` vs `packaging_stage_…`). Como a colisão depende dos campos presentes, para um schema estável gere colunas explícitas com `init --naming leaf`.
* `auto_normalize` num submodelo: cria uma tabela raiz e **uma tabela por cada lista de objectos** (a qualquer profundidade), com o id da raiz e índices de posição como chave. As tabelas filhas usam `replace_by` para ficarem sincronizadas quando listas encolhem.

```yaml
auto_normalize:
  table_prefix: pcf
  id_path: id          # omissão: catenaXId
  id_column: pcf_id
  exclude: [comment]
  naming: leaf         # full (omissão) | leaf
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

Com `keys: [id, version]`, um PCF migrado de v7 e o mesmo PCF entregue em v9 ficam **na mesma linha**: ganha o último a ser processado. Os exemplos oficiais V7 e V9 têm o mesmo `id`, por isso, para os comparar, corra cada um em separado.

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
5. **Resultado em tempo real** sobre os dados de teste: o exemplo oficial, ficheiros da pasta ou um documento colado. Inclui a validação contra o schema, com as mensagens em português e o caminho de cada problema.
6. **Guardar** gera o YAML. Em **Avançado** vê-se e edita-se o YAML, para as regras que o construtor não mostra (`cases`, `template`…). Essas colunas aparecem no construtor como "regra avançada".

Sem acesso ao GitHub (proxy, rede fechada), use um clone local do repositório de modelos:

```bash
cx2iceberg studio --models-dir ~/code/sldt-semantic-models
```

A lista do catálogo vem de uma só chamada à API do GitHub e fica em cache 24 h em `.cx2iceberg/`. Com `GITHUB_TOKEN` definido, usa esse token.
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

Cada documento é validado **à entrada**, antes de qualquer regra: tipos, campos obrigatórios, enumerações, formatos (por exemplo o padrão das datas `Timestamp`) e limites. Com `warn` o problema fica no log e o documento segue. Com `skip` o documento é ignorado. Com `fail` a carga pára.
À saída, cada coluna com `type:` é convertida para esse tipo, com o `on_error` da coluna, e o `check` acrescenta as regras de negócio. São duas camadas: o schema diz se o documento é válido; o tipo e o `check` dizem se o valor serve para a tabela.

## AWS Lambda

`cx2iceberg.lambda_handler.handler` processa eventos S3 (ex.: payloads do EDC gravados num bucket). Defina `CX2ICEBERG_CONFIG` (caminho no pacote ou `s3://…/config.yaml`). Permissões necessárias: `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`, `glue:Get*/CreateTable/UpdateTable/CreateDatabase`, e `s3:GetObject/PutObject/DeleteObject/ListBucket` nos buckets de entrada, `location` e `temp_path`.

## Uso programático

```python
from cx2iceberg import load_config, transform, write_athena

cfg = load_config("config.yaml")
results = transform(cfg, ["dados/"], semantic_id="urn:samm:io.catenax.serial_part:3.0.0#SerialPart")
write_athena(cfg, results)
```

## Limitações

* Lê o formato **value-only** dos submodelos Catena-X. O formato AAS completo (`submodelElements`) não é convertido.
* O schema SAMM não diz quais listas são 1:1 (não tem `maxItems`), por isso o `unwrap` é uma decisão sua; a carga falha se a decisão estiver errada para um documento.
* `replace_by` executa `DELETE` e depois `INSERT`: não é atómico.
* Processa os documentos em memória. Para volumes muito grandes, chame-o por lotes (ex.: um ficheiro ou prefixo S3 por execução).

## Testes

```bash
pip install -e ".[dev]" && pytest
```
