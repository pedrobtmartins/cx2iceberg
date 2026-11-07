# 0.2.0 — Alterações de comportamento

Revisão do caminho de cada valor, do JSON à tabela Iceberg, para que **nenhum valor seja alterado, perdido ou mal lido sem aviso**. Uma regra para tudo:

* **perda não declarada → erro** (o documento é rejeitado, ou a carga pára antes de escrever);
* **perda declarada na configuração → permitida, e contada no relatório**.

Não há modo de compatibilidade. O que deixa de carregar, ou passa a falhar, está na última secção, com o que fazer.

## Relatório da carga

| Antes | Agora |
|---|---|
| Não havia relatório. Valores anulados (`on_error: null`), linhas ignoradas e `default` aplicados não ficavam registados, ou ficavam só no log, uma linha por valor | Relatório por tabela, coluna, ficheiro, documento e linha (`childItems[1]`), com três tipos de evento: erro, perda declarada e aviso. Opções `--report`, `--report-details`, `--samples` e `--max-errors`. Em memória ficam só as contagens e os primeiros exemplos de cada grupo |
| Um documento com erro parava a carga toda | O documento é rejeitado, os outros seguem, e o relatório diz quais pedir de novo. Códigos de saída: `0` ok, `2` parcial, `1` falha. A Lambda continua tudo ou nada |
| A Lambda registava o relatório em `INFO`, invisível com o nível por omissão `WARNING` | `WARNING` quando há avisos ou perdas declaradas |

## Leitura dos ficheiros

| Antes | Agora |
|---|---|
| Um ficheiro ilegível (não UTF-8, JSON inválido, aninhamento excessivo, inteiro com mais de 4300 dígitos) parava a carga toda | Fica como documento rejeitado, com a razão, e os outros seguem |
| `NaN`/`Infinity` aceites; chave repetida (`{"co2": 2.0, "co2": 20.0}`) ficava com o último valor | Rejeitado |
| Números com mais de 15 algarismos perdiam precisão (`1234567890123456789.0` → `…768`) | O texto original é guardado: `bigint` e `decimal` recebem o valor exacto |
| Topo `null`/`42`, ou um elemento de lista que não é objecto, dava uma linha de nulos; um ficheiro vazio não dava sinal | Rejeitado; ficheiro vazio conta como aviso |

## Configuração

| Antes | Agora |
|---|---|
| YAML 1.1: `yes/no/on/off` eram booleanos (`map: {yes: Y, no: N, on: L, off: D}` ficava com duas traduções), `010` era octal, datas eram convertidas, uma chave repetida ficava com o último valor | YAML 1.2: só `true`/`false` são booleanos, sem conversão de datas, e chave repetida é erro |
| `dedupe: "false"` era verdadeiro; uma chave mal escrita em `input:` era ignorada | Opções booleanas só aceitam `true`/`false`; chaves desconhecidas são erro |
| Nomes e argumentos de transformações só falhavam nos dados (com `on_error: null`, anulavam a coluna inteira) | Validados ao carregar |
| Caminhos mal formados só falhavam nos dados | Validados ao carregar. Com `model.schema`, percorridos no schema, com sugestões |
| Várias regras para a mesma tabela: valia a 1.ª (um `decimal` da 2.ª passava a `double`) | Têm de concordar em modo, chaves, partições e tipos |
| Duas colunas com o mesmo nome depois de normalizado (`fossilGhg`, `fossil_ghg`): ficava uma | Erro |

## Caminhos e condições

| Antes | Agora |
|---|---|
| Um caminho com a forma errada (`childItems.quantity` sobre uma lista, índice numa lista desembrulhada) dava nulo | Erro, com o `on_error` da coluna |
| Filtros comparavam texto: `[?qty=1]` não casava com `1.0`, `[?primary=true]` não casava com `true`; `[?key~a=b]` era partido no `=` | Comparação com tipo; o operador é o primeiro que aparece; tipos diferentes são erro |
| `[*].campo` com elementos sem o campo ignorava-os | Erro, a não ser com `missing_elements: null` ou `skip` declarado (contado) |
| Condições (`where`, `cases`, `check`, `match`) comparavam texto com texto: `10 > "9"` era falso, `"7" == 7` verdadeiro, regex sobre números | Comparação com tipo (números, datas como instantes); tipos diferentes são erro. No `match` contam como "não corresponde" |
| `check.min` aceitava `true` como 1 e o texto `"5"` como 5 | Só números |

## Transformações

| Antes | Agora |
|---|---|
| Contas em float: `0.1 * 3` dava `0.30000000000000004`; `round(2.675, 2)` dava `2.67` | Contas decimais exactas; `round` exige o modo de arredondamento |
| `sum`/`min`/`max` com nulos davam um total parcial; `max` de datas comparava texto; `["9", "10"]` ordenava como texto | Nulos são erro (ou `nulls: skip`, contado); datas como instantes; números em texto são erro |
| `unit_convert` sem unidade, `to_bool` não reconhecido e `regex_extract` sem correspondência davam nulo | Erro, a não ser declarado (`from:`, `no_match: null`), e contado |
| `to_string(True)` dava `'True'`; `join` com nulos dava `'a,'` | `true`; nulos são erro |
| `distinct` e `null_if` misturavam `True`, `1` e `"1"` (`null_if: false` anulava o `0`); `map` traduzia o número `1` pela chave `"1"` | Comparação com tipo |
| `clamp` alterava valores sem registo | Cada valor alterado fica contado |
| `hash`/`uuid5` de números: `1` e `1.0` davam identidades diferentes | Só texto, objectos ou listas |

## Tipos e casts

| Antes | Agora |
|---|---|
| `true` num número dava `1` | Erro |
| Inteiros fora do intervalo e decimais com demasiados algarismos só falhavam na escrita | Erro no cast, no documento |
| `decimal(10,2)` arredondava em silêncio (`1.255` → `1.26`, `0.004` → `0.00`) | Erro, a não ser com `rounding` declarado (contado) |
| `double` e `float` perdiam algarismos (`9007199254740993` → `…992`; `1.23456789` num `float` → `1.2345679`) | Erro, a não ser com `rounding: nearest` |
| `json` guardava `"abc"`; chaves de `map`/`struct` que colidiam perdiam uma | Erro |
| Coluna sem tipo com valores misturados ficava texto (`'1.5'`) | A carga falha com exemplos; declare o `type` |
| `tinyint`/`smallint` iam para o Athena tal e qual (o Iceberg não os tem) | Na tabela ficam `int`; o intervalo continua validado no cast |

## Datas

| Antes | Agora |
|---|---|
| O leitor do Python aceitava muitos formatos e lia `2022-01-01+02:00` como as 02:00 | Só o formato do schema (`xsd:dateTime`), ou o `format` declarado |
| Hora sem fuso era tratada como UTC sem aviso | Erro, a não ser com `input.naive_timestamps: utc` ou um fuso IANA (contado) |
| `date`/`year`/`date_format` usavam o dia em UTC (`01:00+02:00` do dia 11 dava dia 10) | Usam o dia tal como foi escrito |
| `year` de `2022` (número) dava 1970 | Um número só é data com a unidade declarada |
| `24:00:00` era rejeitado | É o instante 00:00 do dia seguinte (e erro onde seria ambíguo) |

## Números em texto

| Antes | Agora |
|---|---|
| `"1,5"` passava a `1.5`; `"1.234"` era lido sem aviso; dígitos não‑ASCII eram aceites | Só o formato JSON é lido (contado como aviso); o que for ambíguo é erro. Outros formatos declaram-se com `to_number: {decimal: ",", thousands: "."}` |

## Chaves, duplicados, identidade e templates

| Antes | Agora |
|---|---|
| Linha com chave nula era escrita | Erro (ou `on_error: skip_row`, contado) |
| A mesma chave duas vezes no mesmo documento: ficava a última (um filho repetido num BOM perdia a quantidade) | Erro, a não ser com `on_duplicate: last/first` |
| `document_id` de `0`, `"0"` e `0.0` dava três UUIDs; um campo do `from` em falta mudava o UUID sem aviso | Campos com tipo; campo em falta é erro (ou `on_missing: content`) |
| Documento sem submodelo: aviso, e a carga acabava "ok" | Erro, a não ser com `input.on_unmatched: skip` (contado) |
| `template` com partes em falta: o texto ficava com partes vazias | Se todas faltarem, nulo. Se só algumas faltarem, erro (ou `template_missing: empty/null`, contado) |

## Modo auto e `auto_normalize`

| Antes | Agora |
|---|---|
| Dois campos auto com o mesmo nome de coluna: ficava o primeiro | Erro (ou `auto_collision: prefix/skip`, contado) |
| Nomes `leaf` calculados com os campos de cada documento: na mesma carga, `ghg.fossil` ia para `fossil` num documento e para `ghg_fossil` noutro | Calculados com o modelo (`model.schema`, todas as alternativas `oneOf`/`anyOf`), iguais em todas as cargas. Campo fora do modelo: nome completo, com aviso |
| As tabelas filhas do `auto_normalize` vinham só das listas do lote: uma lista que ficou vazia em todos os documentos não apagava as linhas antigas; com `naming: leaf` o nome da tabela dependia do lote | Com `model.schema`, todas as listas do modelo têm tabela, e o `DELETE` corre. Sem modelo, declara-se `children: batch` (contado) |
| `defaults.athena_options: {mode: overwrite}` num `auto_normalize` passava, e substituía a tabela raiz | Validado ao carregar, como nas tabelas explícitas |

## Escrita

| Antes | Agora |
|---|---|
| Tipos adivinhados pelo pandas: um `bigint` com um nulo no lote passava por `float64` (`…993` → `…992`); um timestamp depois de 2262 falhava no pandas 2 | Tipos declarados (Int64, `datetime64[us]`, decimal exacto) |
| Uma incompatibilidade de tipo com a tabela só aparecia depois do `DELETE` do `replace_by` (linhas antigas apagadas, novas por escrever); uma tabela `bigint` podia ser estreitada para `int` | Pré-verificação de todas as tabelas antes de qualquer escrita ou `DELETE` (Arrow, awswrangler, catálogo Glue). Tipo mais largo na tabela: escreve-se com esse tipo. Alargamento que o Iceberg faz sem mudar as linhas antigas: aviso. `float` ↔ `double` e outros tipos: erro |
| `replace_by` só apagava os documentos que ainda tinham linhas (lista vazia, ou tudo excluído pelo `where`: os filhos antigos ficavam) | Apaga todos os documentos aceites do lote; documentos rejeitados não apagam nada |
| `DELETE` com `float` nunca correspondia (`1.1` double ≠ real) | Literais com o tipo da coluna na tabela |
| `merge` punha `NULL` nas colunas da tabela que a configuração não produz | Erro, a não ser com `missing_columns: null`. Em tabelas `auto` é o valor certo (o campo saiu do documento), e fica contado |
| `overwrite` sem linhas deixava o conteúdo antigo, enquanto as outras tabelas eram substituídas | A tabela fica vazia, se houve documentos do submodelo no lote |
| `athena_options` podia sobrepor `mode`/`dtype`/...; `overwrite_partitions` aceitava `day(...)`; `partition_by` com coluna inexistente só falhava no Athena | Erro ao carregar ou na pré-verificação |
| CSV local: nulo igual a `""`, listas em `repr` do Python, `True`. JSONL: decimal como texto. Parquet local: tipos adivinhados | CSV com nulo sem aspas e JSON para listas; JSONL exacto; Parquet com o schema declarado |

## Studio

| Antes | Agora |
|---|---|
| Só mostrava as primeiras mensagens do log; um documento rejeitado escondia a pré-visualização toda | Relatório da carga (rejeitados, perdas declaradas, avisos) com ● por coluna na grelha; as linhas dos restantes continuam visíveis; pré-verificação da escrita |
| O browser mostrava `9007199254740993` como `…992` e um `float` 1.1 como `1.100000023841858` | Valores mostrados tal como ficam na tabela |
| Ao gravar, o conflito com uma edição externa era detectado pelo mtime: com a mesma marca de tempo (Docker/WSL, FS com resolução de 1–2 s, `cp -p`), a edição externa era apagada | Detectado pelo conteúdo (hash) |

## O que deixa de carregar, ou passa a falhar

Todos os casos abaixo eram perdas ou mudanças de valor silenciosas. Agora ou se corrigem os dados, ou se declara o comportamento na configuração, e esse comportamento fica contado no relatório.

| Situação | Passa a | O que fazer |
|---|---|---|
| `yes`/`no`/`on`/`off` usados como booleanos; chaves repetidas no YAML; `dedupe: "false"` | erro ao carregar | usar `true`/`false`; remover a chave repetida |
| Transformação ou argumento mal escrito; caminho inválido | erro ao carregar | corrigir |
| Horas sem fuso; data sem hora usada como `timestamp` | documento rejeitado | `input.naive_timestamps: utc` ou o fuso IANA do emissor |
| Números em texto com vírgula ou separador de milhares | documento rejeitado | `to_number: {decimal: ",", thousands: "."}` |
| `decimal`/`double`/`float` com mais precisão do que o tipo | documento rejeitado | `rounding` na coluna, ou um tipo maior |
| Condições que comparam tipos diferentes (`value: "9"` com números) | documento rejeitado | escrever o `value` com o tipo certo |
| `[*].campo` com elementos sem o campo | documento rejeitado | `missing_elements: null` ou `skip` |
| `sum` com nulos; `unit_convert` sem unidade; `regex_extract` sem correspondência | documento rejeitado | `sum: {nulls: skip}`, `from:`, `no_match: null` |
| Chave nula, ou repetida no mesmo documento | documento rejeitado | `on_error: skip_row`, `on_duplicate: last` |
| Documento sem submodelo | documento rejeitado | `input.on_unmatched: skip` |
| `template` com só algumas partes | documento rejeitado | `template_missing: empty` ou `null` |
| `document_id.from` com números sem tipo, ou campo em falta | erro ao carregar / documento rejeitado | `{path: version, type: int}`; `on_missing: content`. Ver a nota de migração dos UUIDs no README |
| `auto_naming: leaf` ou `auto_normalize.naming: leaf` sem `model.schema` | erro ao carregar | declarar `model.schema`, usar `full`, ou gerar colunas explícitas com `init --naming leaf`. Tabelas já criadas com nomes leaf por documento podem ter colunas com outros nomes: as colunas novas aparecem como avisos na escrita |
| `auto_normalize` sem `model.schema` com `mode` que não seja `append` | erro ao carregar | `model.schema` (recomendado) ou `auto_normalize.children: batch` |
| `merge` numa tabela que tem colunas que a configuração não produz | pré-verificação falha | `missing_columns: null`, ou acrescentar as colunas |
| Tipo da configuração incompatível com a tabela (`float` ↔ `double`, `string` ↔ número, escala de decimal) | pré-verificação falha | declarar o tipo da tabela |
| `athena_options` com `mode`, `dtype`, …; `overwrite_partitions` com `day(...)` | erro ao carregar | usar as opções da configuração; partições por coluna |
| Colunas que dão o mesmo nome (`fossilGhg`, `fossil_ghg`) | erro ao carregar | renomear uma |

Exemplos actualizados: em `examples/config.yaml` o submodelo PCF declara `model.schema` (PCF 7.0.0) e corresponde só a `pcf:7.*`, e os outros ficheiros da pasta são ignorados com `input.on_unmatched: skip`. O exemplo [13] de `pcf_v7_to_v9.yaml` passou a comparar datas (`created gte "2025-01-01"`), e o `document_id.from` declara `{path: version, type: int}`.
