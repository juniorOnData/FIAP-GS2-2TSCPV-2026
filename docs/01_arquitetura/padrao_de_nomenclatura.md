# Padrão de nomenclatura

As convenções abaixo valem para todo objeto novo. A coluna "Situação" indica se o projeto já segue a regra ou se há divergência a corrigir, registrada no [plano de implantação](../07_implantacao/plano_de_implantacao.md).

## Regras gerais

1. Nomes técnicos em letras minúsculas, sem acento, com palavras separadas por sublinhado (`snake_case`). A exceção são as colunas da Gold, em maiúsculas com prefixo de tipo.
2. Nomes de tabelas e arquivos em inglês, que é o idioma já usado nas tabelas do projeto. Descrições, comentários e documentação em português.
3. Nenhum nome usa espaço, hífen ou caractere especial.

## Objetos

| Objeto | Padrão | Exemplo | Situação |
|---|---|---|---|
| Catálogo | `<organizacao>_<finalidade>` | `fiap_analytics` | Segue |
| Schema | Nome da camada | `bronze`, `silver`, `gold` | Segue |
| Volume | `<fonte>_<entidade>` | `cemaden_rainfall_readings` | Segue |
| Tabela Bronze | `<fonte>_<entidade>` | `geosampa_history_flooding_events` | Segue |
| Tabela Silver | `<fonte>_<entidade>` | `geosampa_flooding_events` | Segue desde o PR #14 |
| Dimensão Gold | `dim_<fonte>_<entidade>`; dimensão compartilhada `dim_<entidade>` | `dim_geosampa_subprefecture_area`, `dim_tempo` | Segue |
| Fato Gold | `fact_<fonte>_<entidade>` | `fact_geosampa_flooding_events` | Segue |
| Log de ingestão | `<fonte>_ingestion_log` | `sibh_ingestion_log` | Segue |
| Resultados de qualidade | `<fonte>_data_quality_log` | `geosampa_data_quality_log` | Segue |
| Notebook | `<NN>_<fonte>_<entidade>_<camada>`, com `NN` = ordem de execução na fonte | `03_sibh_measurements_landing` | Segue |
| Notebook de qualidade | `<NN>_<fonte>_<camada avaliada>_quality` | `02_geosampa_silver_quality` | Segue |
| Notebook Gold | `<NN>_<nome da tabela>` | `04_fact_geosampa_flooding_events` | Segue |
| Módulo Python | `<assunto>.py` em `src/common/` | `data_quality.py` | Segue |
| Arquivo SQL | Notebook `.sql` no formato de código fonte | `00_dim_tempo.sql` | Segue |
| Job (nome exibido) | `ChuvaSegura_<Fonte>` | `ChuvaSegura_GeoSampa` | Diverge: hoje `ChuvaSegura`, `ChuvaSegura - CEMADEN` e `ChuvaSegura - SIBH` |
| Job (chave do recurso no YAML) | `ChuvaSegura_<FONTE>` | `ChuvaSegura_SIBH` | Diverge só no GeoSampa (`ChuvaSegura`) |
| Arquivo de job | `jobs/chuvasegura_<fonte>.job.yml` | `jobs/chuvasegura_sibh.job.yml` | Segue |
| Tarefa (`task_key`) | `<fonte>_<entidade>_<camada>`; qualidade `quality_<camada>_<fonte>` | `sibh_measurements_bronze`, `quality_silver_geosampa` | Diverge no GeoSampa: `ingestion_history_flooding_events`, `silver_geosampa_flooding_events` |
| Branch Git | `<tipo>/<descricao>`, com tipo `feat`, `fix`, `docs` ou `chore` | `fix/geosampa-bronze-recarga` | Segue (hífen permitido no nome da branch, convenção do Git) |

Mudar o nome exibido ou a chave de um job já criado no workspace exige atualizar o job e os históricos de execução passam a ficar no job novo. Por isso a correção das divergências está planejada, e não foi feita neste PR.

## Colunas

| Camada | Padrão | Exemplos |
|---|---|---|
| Bronze | Nome original da fonte, sem alteração; colunas de controle começam com `_` | `codestacao`, `datahora`, `_source_file`, `_ingested_at` |
| Silver | `<prefixo>_<nome>` em minúsculas | `dt_ocorrencia`, `nr_latitude` |
| Gold | `<PREFIXO>_<NOME>` em maiúsculas | `COD_DIM_TEMPO`, `DSC_TIPO_OCORRENCIA` |
| Logs | Nome descritivo em português, minúsculas | `iniciado_em`, `registros` |

Prefixos da Silver:

| Prefixo | Uso |
|---|---|
| `nr_` | Número ou medida |
| `dt_` | Data |
| `dc_` | Descrição |
| `sg_` | Sigla |
| `nm_` | Nome |
| `cod_` | Código ou chave |
| `ds_`, `cd_`, `tp_` | Descrição, código e tipo nas tabelas de referência da Bronze |

Prefixos da Gold:

| Prefixo | Uso |
|---|---|
| `COD_` | Chave (substituta, estrangeira ou código) |
| `DSC_` | Descrição |
| `NOM_` | Nome |
| `SIG_` | Sigla |
| `NUM_` | Número ou medida |
| `DAT_` | Data ou data e hora |
| `FLG_` | Indicador verdadeiro ou falso |
| `TIP_` | Tipo |

## Parâmetros (widgets)

| Regra | Exemplo | Situação |
|---|---|---|
| `snake_case`, sem acento | `snapshot_date`, `min_month` | Segue |
| Mesmo idioma em todos os notebooks de uma fonte | SIBH: `mode`, `force`; CEMADEN: `modo`, `forcar_reprocesso` | Diverge entre fontes; padronizar quando a fonte for refatorada |
| Datas e meses em formato ISO (`AAAA-MM-DD`, `AAAA-MM`) | `min_month = 2020-01` | Diverge no CEMADEN (`mes_inicio = 201401`) |
| Valores booleanos com `dropdown` | `force`: `false` ou `true` | Segue |

## Identificadores

| Identificador | Formato | Exemplo |
|---|---|---|
| Notebook | `<FONTE>_<CAMADA>_<NNN>` | `GEO_SLV_001` |
| Regra de negócio | `RN_<FONTE>_<NNN>` | `RN_GEO_005` |
| Regra de qualidade | `DQ_<FONTE>_<CAMADA>_<NNN>` | `DQ_GEO_GLD_004` |
| Job | `JOB_CHUVASEGURA_<FONTE>` | `JOB_CHUVASEGURA_SIBH` |
| Etapa do notebook | `E<NN>`, na ordem das células de código | `E03` |

Siglas de fonte: `CEM` (CEMADEN), `SIB` (SIBH), `GEO` (GeoSampa), `SHR` (compartilhado). Siglas de camada: `LND` (Landing), `BRZ` (Bronze), `SLV` (Silver), `GLD` (Gold), `DQ` (Qualidade).

Os números são sequenciais dentro de cada combinação de fonte e camada e nunca são reaproveitados: um notebook ou regra descontinuado mantém o seu identificador.
