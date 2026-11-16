# ChuvaSegura: FIAP GS2 2TSCPV 2026

Pipeline de dados em Databricks que junta **chuva**, **nível de rios e córregos** e **histórico de alagamentos** da cidade de São Paulo (IBGE 3550308). O objetivo é cruzar as medições com as ocorrências de alagamento.

| Fonte | O que traz | Acesso |
|---|---|---|
| **CEMADEN** (API PED) | Cadastro de estações e leituras de chuva da rede CEMADEN | Login na SGAA (token) |
| **SIBH / SP Águas** | Cadastro de estações e medições de chuva e nível das demais instituições (SP Águas, SAISP, SABESP…) | API pública |
| **GeoSampa** | Ocorrências de alagamento, deslizamento, inundação e queda de árvore, além das subprefeituras | Arquivos baixados e enviados ao volume |

## Arquitetura

Tudo fica no catálogo `fiap_analytics` do Unity Catalog, em três camadas:

```mermaid
flowchart LR
  subgraph APIs
    PED[CEMADEN PED]
    SIBH[SIBH api/v2]
  end
  GPKG[Arquivos GeoSampa]
  subgraph bronze
    VOL[(Volumes: arquivos brutos<br/>JSON gzip / GPKG)]
    BT[Tabelas Bronze<br/>tudo em string]
    LOG[(Logs por origem)]
  end
  subgraph silver
    ST[Dados tipados e padronizados]
  end
  subgraph gold
    GT[Fatos e dimensões]
  end
  PED -- landing --> VOL
  SIBH -- landing --> VOL
  GPKG -- upload --> VOL
  VOL -- bronze --> BT
  BT --> ST --> GT
  VOL -.-> LOG
  BT -.-> LOG
```

- **Landing:** baixa das APIs e grava os arquivos no volume, exatamente como vieram, com um manifesto de controle (contagem e SHA256).
- **Bronze:** carrega os arquivos em tabelas Delta, com todas as colunas como texto e colunas de controle (`_source_file`, `_batch_id`, `_ingested_at`...). Campos novos da API vão para `_rescued_data`.
- **Silver:** dados tipados, padronizados e relacionados.
- **Gold:** modelo dimensional para análise.

Situação atual:

| Fonte | Landing | Bronze | Silver | Gold |
|---|:-:|:-:|:-:|:-:|
| CEMADEN | ✅ | ✅ | ⏳ | ⏳ |
| SIBH | ✅ | ✅ | ⏳ | ⏳ |
| GeoSampa | ✅ | ✅ | ✅ | ✅ |

## Estrutura do repositório

O número no início de cada arquivo indica a ordem de execução dentro da fonte.

```
jobs/                                             # definição dos jobs do Databricks (YAML)
├── chuvasegura_cemaden.job.yml
├── chuvasegura_geosampa.job.yml
└── chuvasegura_sibh.job.yml
src/
├── common/
│   └── ingestion_log.py                          # log de ingestão padrão (módulo Python)
├── shared/
│   └── transformations/gold/
│       └── 00_dim_tempo.sql                      # dimensão de tempo compartilhada pelas fatos
├── cemaden/
│   ├── ingestion/
│   │   ├── 01_cemaden_stations_landing.py
│   │   └── 01_cemaden_rainfall_readings_landing.py
│   └── transformations/bronze/
│       ├── 02_cemaden_stations_bronze.py
│       └── 02_cemaden_rainfall_readings_bronze.py
├── sibh/
│   ├── ingestion/
│   │   ├── 01_sibh_stations_landing.py
│   │   └── 03_sibh_measurements_landing.py
│   └── transformations/bronze/
│       ├── 02_sibh_stations_bronze.py
│       └── 04_sibh_measurements_bronze.py
└── geosampa/
    ├── ingestion/
    │   ├── 01_geosampa_subprefecture_area_bronze.sql
    │   └── 01_geosampa_history_flooding_events_bronze.py
    └── transformations/
        ├── silver/
        │   └── 02_geosampa_flooding_events_silver.sql
        └── gold/
            ├── 03_dim_geosampa_subprefecture_area.sql
            ├── 03_dim_geosampa_type_flooding_events.sql
            └── 04_fact_geosampa_flooding_events.sql
```

Os notebooks estão no formato de código-fonte do Databricks (`.py` e `.sql` começando com `Databricks notebook source`). O Databricks os abre como notebooks normais, com as células separadas.

## Pré-requisitos

1. **Workspace Databricks com Unity Catalog** e compute serverless ou um Databricks Runtime recente. O notebook da Gold do GeoSampa usa variáveis de sessão SQL e scripting SQL (`BEGIN … END`).
2. **Repositório clonado como Git folder** (*Workspace → Create → Git folder*). Os notebooks importam `src/common/ingestion_log.py` a partir da pasta do próprio notebook.
3. **Catálogo e schemas:**

   ```sql
   CREATE CATALOG IF NOT EXISTS fiap_analytics;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.bronze;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.silver;
   CREATE SCHEMA IF NOT EXISTS fiap_analytics.gold;
   ```

   Os volumes e as tabelas de log são criados pelos próprios notebooks.

4. **Credenciais do CEMADEN** num secret scope chamado `cemaden`, com as chaves `ped_email` e `ped_password` da conta cadastrada na SGAA/PED:

   ```bash
   databricks secrets create-scope cemaden
   databricks secrets put-secret cemaden ped_email
   databricks secrets put-secret cemaden ped_password
   ```

5. **Arquivos do GeoSampa:**
   - baixe os arquivos de ocorrências em GeoPackage (`.gpkg`), soltos ou dentro de um `.zip`;
   - mantenha o ano no nome do arquivo (por exemplo, `alagamentos_2024.gpkg`), porque é dele que a carga tira o ano de origem;
   - envie os arquivos para o volume:

   ```sql
   CREATE VOLUME IF NOT EXISTS fiap_analytics.bronze.geosampa_history_flooding_events;
   ```

   Depois, faça o upload em *Catalog → fiap_analytics → bronze → Volumes → geosampa_history_flooding_events → Upload*.

## Ordem de execução

### CEMADEN

São dois fluxos independentes. Cada um tem a landing e depois a Bronze.

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_cemaden_stations_landing` | `codibge` (3550308), `tipoestacao` (vazio = todos), `formato` (JSON) |
| 2 | `02_cemaden_stations_bronze` | `snapshot_date`: `pending` (padrão), `latest`, `all` ou `AAAA-MM-DD` |
| 1 | `01_cemaden_rainfall_readings_landing` | `modo`: `backfill` ou `incremental`; `mes_inicio` (201401); `mes_fim` (vazio = último mês fechado); `forcar_reprocesso`: `nao`/`sim` |
| 2 | `02_cemaden_rainfall_readings_bronze` | (sem widgets: carrega os meses pendentes segundo o log) |

- A PED aceita 12 chamadas por minuto, então o primeiro backfill desde 2014 leva horas. Se parar no meio, basta rodar de novo: o notebook continua de onde parou.
- Os cadastros (`*_stations_*`) são snapshots diários: rodar no mesmo dia substitui o snapshot do dia.

### SIBH

Os passos são sequenciais, porque a coleta das medições usa o cadastro da Bronze para escolher os sensores.

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_sibh_stations_landing` | `api_url`, `codibge_check` (3550308) |
| 2 | `02_sibh_stations_bronze` | `snapshot_date`: `pending` (padrão), `latest`, `all` ou `AAAA-MM-DD` |
| 3 | `03_sibh_measurements_landing` | `mode`: `probe`, `backfill` ou `incremental`; `group_type` (`minute`); `min_month` (2020-01); `excluded_owners` (CEMADEN); `min_interval_s` (1.0); `force` |
| 4 | `04_sibh_measurements_bronze` | `window`: `pending` (padrão), `all` ou `AAAA-MM` |

- Os sensores do CEMADEN ficam de fora do SIBH por padrão, porque já vêm pela PED.
- O modo `probe` faz duas chamadas de teste e mostra a resposta da API sem gravar nada.

### Dimensão de tempo (compartilhada)

`00_dim_tempo` cria e mantém a `fiap_analytics.gold.dim_tempo`: um dia por linha, de 2010 até o fim do ano seguinte, com chave `COD_DIM_TEMPO` no formato `yyyyMMdd`. É a única dimensão compartilhada entre as fontes. Toda fato do projeto usa essa chave, e fatos com granularidade menor que o dia (chuva por hora, por exemplo) guardam a hora na própria fato. O notebook roda no job `ChuvaSegura`, antes da fato do GeoSampa, e cada execução só acrescenta os dias que faltam.

### GeoSampa

| # | Notebook | Depende de |
|---|---|---|
| 1 | `01_geosampa_subprefecture_area_bronze` | — |
| 1 | `01_geosampa_history_flooding_events_bronze` | arquivos no volume |
| 2 | `02_geosampa_flooding_events_silver` | os dois passos 1 |
| 3 | `03_dim_geosampa_subprefecture_area` | subprefeituras (Bronze) |
| 3 | `03_dim_geosampa_type_flooding_events` | Silver |
| 4 | `04_fact_geosampa_flooding_events` | Silver, as duas dimensões e a `dim_tempo` |

- Arquivos que já foram carregados (mesmo SHA256) são pulados.
- A Silver converte a geometria (POINT ou POINT Z em WKB, conforme o ano do arquivo; SIRGAS 2000 / UTM 23S, EPSG 31983) em coordenadas UTM e em latitude e longitude. A conversão é uma UDF Python registrada no próprio notebook, e a última célula confere quantos pontos ficaram sem coordenada ou fora da área de São Paulo.
- A Gold recarrega só os anos de origem novos ou alterados.

## Jobs e workflows

Cada fonte tem um job, e a definição de cada um fica em `jobs/`, no formato YAML dos jobs do Databricks (`resources.jobs`). Separar por fonte faz com que uma falha numa API não trave as outras.

| Arquivo | Job | Agenda (America/Sao_Paulo) | Situação |
|---|---|---|---|
| `jobs/chuvasegura_geosampa.job.yml` | `ChuvaSegura` | todo dia às 09:00:30 | ativo |
| `jobs/chuvasegura_cemaden.job.yml` | `ChuvaSegura - CEMADEN` | todo dia às 07:00 | pausado até o backfill |
| `jobs/chuvasegura_sibh.job.yml` | `ChuvaSegura - SIBH` | todo dia às 07:30 | pausado até o backfill |

```mermaid
flowchart LR
  subgraph cemaden["ChuvaSegura - CEMADEN"]
    C1[01_cemaden_stations_landing] --> C2[02_cemaden_stations_bronze]
    C3[01_cemaden_rainfall_readings_landing<br/>modo = incremental] --> C4[02_cemaden_rainfall_readings_bronze]
  end
  subgraph sibh["ChuvaSegura - SIBH"]
    S1[01_sibh_stations_landing] --> S2[02_sibh_stations_bronze] --> S3[03_sibh_measurements_landing<br/>mode = incremental] --> S4[04_sibh_measurements_bronze]
  end
  subgraph geosampa["ChuvaSegura (GeoSampa)"]
    G1[01_geosampa_history_flooding_events_bronze] --> G3[02_geosampa_flooding_events_silver]
    G2[01_geosampa_subprefecture_area_bronze] --> G3
    G2 --> G4[03_dim_geosampa_subprefecture_area]
    G3 --> G4
    G3 --> G5[03_dim_geosampa_type_flooding_events]
    G3 --> G6[04_fact_geosampa_flooding_events]
    G4 --> G6
    G5 --> G6
    T0[00_dim_tempo] --> G6
  end
```

Todos os jobs:
- rodam em compute serverless (ambiente 6), com fila habilitada;
- apontam para os notebooks no Git folder `/Workspace/Users/rm562679@fiap.com.br/FIAP-GS2-2TSCPV-2026`. Se o repositório estiver clonado em outro lugar, troque esse prefixo nos três arquivos.

**Antes de ativar CEMADEN e SIBH:**
1. Rode uma vez, manualmente, a carga do histórico com o modo `backfill`:
   - CEMADEN: `01_cemaden_rainfall_readings_landing` com `modo = backfill`;
   - SIBH: `03_sibh_measurements_landing` com `mode = backfill`.
2. Rode as Bronzes correspondentes.
3. Mude `pause_status` para `UNPAUSED`, ou ative a agenda na interface.

O incremental diário cobre o mês atual e completa os meses parciais.

**Como aplicar os YAMLs:**
- **Job que já existe** (`ChuvaSegura`): troque o caminho de cada tarefa pelo do YAML. Em *Workflows → ChuvaSegura*, abra cada tarefa e edite o campo *Path*.
- **Job novo:** crie em *Workflows → Create job*, com as tarefas, dependências, parâmetros (`base_parameters`) e agenda do arquivo.
- **Com a Databricks CLI:** os arquivos já estão no formato de *resources* dos Databricks Asset Bundles. Para usar `databricks bundle deploy`, é preciso um `databricks.yml` na raiz que inclua `jobs/*.job.yml`. Para o bundle passar a gerenciar o `ChuvaSegura` já existente em vez de criar outro, use `databricks bundle deployment bind ChuvaSegura <job_id>`.

Sempre que um notebook mudar de nome ou de pasta, atualize o YAML no mesmo PR.

## Logs de ingestão e meses parciais

Cada origem tem a sua tabela de log, todas no mesmo formato, definido em `src/common/ingestion_log.py`:

- `fiap_analytics.bronze.cemaden_ingestion_log`
- `fiap_analytics.bronze.sibh_ingestion_log`
- `fiap_analytics.bronze.geosampa_ingestion_log`

Colunas: `batch_id, fonte, etapa, janela, status, registros, paginas, duplicados, arquivo, sha256, duracao_s, iniciado_em, finalizado_em, mensagem`.

| Status | Significado |
|---|---|
| `SUCESSO` | Carga completa |
| `PARCIAL` | Mês baixado antes de fechar. Será baixado de novo |
| `SEM_DADOS` | Período completo e sem registros |
| `FALHA` | Erro. A mensagem fica em `mensagem` e a próxima execução tenta de novo |

**Meses parciais:**
- Nas leituras do CEMADEN e nas medições do SIBH, um mês só é dado como completo se foi baixado **pelo menos 3 dias depois de terminar**, para pegar leituras que chegam atrasadas. Até lá ele fica `PARCIAL`.
- O incremental baixa de novo o mês atual, o mês anterior enquanto não estiver completo e qualquer mês que tenha ficado parcial.
- O backfill pula só os meses completos.

Exemplo de consulta:

```sql
SELECT fonte, etapa, janela, status, registros, finalizado_em, mensagem
FROM fiap_analytics.bronze.cemaden_ingestion_log
ORDER BY finalizado_em DESC
LIMIT 50;
```

## Tabelas geradas

| Camada | Tabela | Conteúdo |
|---|---|---|
| bronze | `cemaden_stations` | Snapshots diários do cadastro de estações do CEMADEN |
| bronze | `cemaden_rainfall_readings` | Leituras de chuva do CEMADEN, uma fatia por mês (`_window`) |
| bronze | `sibh_stations` | Snapshots diários do cadastro de estações do SIBH (estado inteiro) |
| bronze | `sibh_measurements` | Medições de chuva e nível do SIBH na capital, uma fatia por mês |
| bronze | `geosampa_history_flooding_events` | Ocorrências do GeoSampa, com a geometria em WKB |
| bronze | `geosampa_subprefecture_area` | Subprefeituras e agrupamentos (Anexo IX) |
| silver | `fact_geosampa_flooding_events` | Ocorrências com data, tipo e subprefeitura padronizados e localização (`nr_x_utm`, `nr_y_utm`, `nr_latitude`, `nr_longitude`, `geom_wkb`) |
| gold | `dim_tempo` | Dimensão de tempo diária, compartilhada por todas as fatos |
| gold | `dim_geosampa_subprefecture_area` | Dimensão de subprefeituras |
| gold | `dim_geosampa_type_flooding_events` | Dimensão de tipos de ocorrência |
| gold | `fact_geosampa_flooding_events` | Fato de ocorrências por data, tipo e subprefeitura |

## Desenvolvimento

- **Módulo de log:** depois de alterar `src/common/ingestion_log.py`, reinicie o Python do notebook (*Run → Restart Python*) para recarregar o módulo.
- **Fluxo de trabalho:** cada mudança vai numa branch própria e entra na `main` por pull request.

## Próximos passos

- Silver e Gold do CEMADEN e do SIBH, para cruzar chuva e nível com as ocorrências do GeoSampa.

## Fontes dos dados

- **CEMADEN:** dados da rede observacional do Centro Nacional de Monitoramento e Alertas de Desastres Naturais (CEMADEN/MCTI), via API PED.
- **SIBH:** Sistema de Informações Hidrológicas, Agência de Águas do Estado de São Paulo (SP Águas).
- **GeoSampa:** Prefeitura de São Paulo. Subprefeituras conforme o Anexo IX (ata 41/14).
