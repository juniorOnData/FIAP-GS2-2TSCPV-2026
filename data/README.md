# ChuvaSegura: pipeline de dados (Databricks)

> Esta pasta reúne a parte de dados do projeto: notebooks em `src/` e definições dos jobs em `jobs/`. A aplicação web (MVP) fica em [`../app`](../app). Os caminhos abaixo são relativos a esta pasta `data/`.

Pipeline de dados em Databricks que junta **chuva**, **nível de rios e córregos** e **histórico de alagamentos** da cidade de São Paulo (IBGE 3550308). O objetivo é cruzar as medições com as ocorrências de alagamento.

| Fonte | O que traz | Acesso |
|---|---|---|
| **CEMADEN** (API PED) | Cadastro de estações e leituras de chuva da rede CEMADEN | Login na SGAA (token) |
| **SIBH / SP Águas** | Cadastro de estações e medições de chuva e nível das demais instituições (SP Águas, SAISP, SABESP…) | API pública |
| **GeoSampa** | Ocorrências de alagamento, deslizamento, inundação e queda de árvore, além das subprefeituras | Arquivos baixados e enviados ao volume |
| **SGB / CPRM** | Carta de suscetibilidade a movimentos de massa e inundações do município (IPT/CPRM, 2015): áreas de suscetibilidade, bacias contribuintes e padrões de relevo | Download público (RIGEO) |
| **IBGE** | Censo 2022: malha de setores censitários com pessoas e domicílios, para estimar a população exposta | Download público (FTP do IBGE) |
| **OpenStreetMap** | Rede viária (extrato da Geofabrik), sob a licença ODbL, que exige atribuição | Download público (Geofabrik) |
| **INMET** | Dados meteorológicos horários das estações automáticas da capital (chuva, temperatura, umidade, pressão, vento e radiação), a partir de 2020 | Download público (portal do INMET) |

## Arquitetura

### Publicação para o site

O notebook `src/geosampa/export/06_export_geosampa_web.py` exporta a Gold do GeoSampa para o contrato estático de `../app/DATA_CONTRACT.md`. Ele valida a última execução Gold de qualidade e grava três arquivos no Volume `fiap_analytics.gold.geosampa_chuvasegura_web`. Para publicar no site estático, baixe-os para `app/data/gold/` e altere `DATA_ROOT` para `data/gold`. Essa primeira publicação contém ocorrências e o cadastro de subprefeituras; as demais fontes são acrescentadas em etapas posteriores.

Tudo fica no catálogo `fiap_analytics` do Unity Catalog, em três camadas:

```mermaid
flowchart LR
  subgraph APIs
    PED[CEMADEN PED]
    SIBH[SIBH api/v2]
  end
  GPKG[Arquivos GeoSampa]
  PUB[Downloads públicos<br/>SGB, IBGE, OSM, INMET]
  subgraph bronze
    VOL[(Volumes: arquivos brutos<br/>JSON gzip / GPKG / shapefile)]
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
  PUB -- landing --> VOL
  VOL -- bronze --> BT
  BT --> ST --> GT
  VOL -.-> LOG
  BT -.-> LOG
```

- **Landing:** baixa das APIs e dos portais e grava os arquivos no volume, exatamente como vieram, com um manifesto de controle (contagem e SHA256). Dos pacotes grandes (o da carta do SGB tem quase 1 GB), só os arquivos usados são lidos, por requisições HTTP Range.
- **Bronze:** carrega os arquivos em tabelas Delta, com todas as colunas como texto e colunas de controle (`_source_file`, `_batch_id`, `_ingested_at`...). Campos novos da API vão para `_rescued_data`.
- **Silver:** dados tipados, padronizados e relacionados.
- **Gold:** modelo dimensional para análise.

Situação atual:

| Fonte | Landing | Bronze | Silver | Gold |
|---|:-:|:-:|:-:|:-:|
| CEMADEN | ✅ | ✅ | ✅ | ✅ |
| SIBH | ✅ | ✅ | ✅ | ✅ |
| GeoSampa | ✅ | ✅ | ✅ | ✅ |
| SGB | ✅ | ✅ | ✅ | ✅ |
| IBGE | ✅ | ✅ | ✅ | ✅ |
| OpenStreetMap | ✅ | ✅ | ✅ | ✅ |
| INMET | ✅ | ✅ | ✅ | ✅ |

## Estrutura do repositório

O número no início de cada arquivo indica a ordem de execução dentro da fonte.

```
jobs/                                             # definição dos jobs do Databricks (YAML)
├── chuvasegura_cemaden.job.yml
├── chuvasegura_geosampa.job.yml
├── chuvasegura_geosampa_web_export.job.yml
├── chuvasegura_ibge.job.yml
├── chuvasegura_inmet.job.yml
├── chuvasegura_osm.job.yml
├── chuvasegura_sgb.job.yml
└── chuvasegura_sibh.job.yml
src/
├── common/
│   ├── arquivos.py                               # downloads com novas tentativas e extração de zips remotos (HTTP Range)
│   ├── conversoes.py                             # conversões de texto (datas, números, fuso) usadas nas Silvers
│   ├── data_quality.py                           # regras e execução das etapas de qualidade de dados
│   ├── geo.py                                    # shapefile, GeoPackage, WKB, UTM 23S <-> lat/lon, área e centroide
│   └── ingestion_log.py                          # log de ingestão padrão (módulo Python)
├── shared/
│   └── transformations/gold/
│       └── 00_dim_tempo.sql                      # dimensão de tempo compartilhada pelas fatos
├── cemaden/
│   ├── ingestion/
│   │   ├── 01_cemaden_stations_landing.py
│   │   └── 01_cemaden_rainfall_readings_landing.py
│   ├── quality/
│   │   ├── 03_cemaden_silver_quality.py
│   │   └── 05_cemaden_gold_quality.py
│   └── transformations/
│       ├── bronze/
│       │   ├── 02_cemaden_stations_bronze.py
│       │   └── 02_cemaden_rainfall_readings_bronze.py
│       ├── silver/
│       │   ├── 03_cemaden_stations_silver.py
│       │   └── 03_cemaden_rainfall_readings_silver.py
│       └── gold/
│           ├── 04_dim_cemaden_stations.sql
│           └── 05_fact_cemaden_rainfall_hourly.sql
├── sibh/
│   ├── ingestion/
│   │   ├── 01_sibh_stations_landing.py
│   │   └── 03_sibh_measurements_landing.py
│   ├── quality/
│   │   ├── 05_sibh_silver_quality.py
│   │   └── 07_sibh_gold_quality.py
│   └── transformations/
│       ├── bronze/
│       │   ├── 02_sibh_stations_bronze.py
│       │   └── 04_sibh_measurements_bronze.py
│       ├── silver/
│       │   ├── 05_sibh_stations_silver.py
│       │   └── 05_sibh_measurements_silver.py
│       └── gold/
│           ├── 06_dim_sibh_sensors.sql
│           ├── 07_fact_sibh_rainfall_hourly.sql
│           └── 07_fact_sibh_level_hourly.sql
├── geosampa/
│   ├── ingestion/
│   │   ├── 01_geosampa_subprefecture_area_bronze.sql
│   │   └── 01_geosampa_history_flooding_events_bronze.py
│   ├── quality/
│   │   ├── 02_geosampa_silver_quality.py
│   │   └── 04_geosampa_gold_quality.py
│   └── transformations/
│       ├── silver/
│       │   └── 02_geosampa_flooding_events_silver.sql
│       └── gold/
│           ├── 03_dim_geosampa_subprefecture_area.sql
│           ├── 03_dim_geosampa_type_flooding_events.sql
│           └── 04_fact_geosampa_flooding_events.sql
├── sgb/
│   ├── ingestion/
│   │   └── 01_sgb_susceptibility_landing.py
│   ├── quality/
│   │   ├── 03_sgb_silver_quality.py
│   │   └── 05_sgb_gold_quality.py
│   └── transformations/
│       ├── bronze/
│       │   └── 02_sgb_susceptibility_bronze.py
│       ├── silver/
│       │   └── 03_sgb_susceptibility_areas_silver.py
│       └── gold/
│           ├── 04_dim_sgb_susceptibility_class.sql
│           └── 05_dim_sgb_susceptibility_areas.sql
├── ibge/
│   ├── ingestion/
│   │   └── 01_ibge_census_tracts_landing.py
│   ├── quality/
│   │   ├── 03_ibge_silver_quality.py
│   │   └── 05_ibge_gold_quality.py
│   └── transformations/
│       ├── bronze/
│       │   └── 02_ibge_census_tracts_bronze.py
│       ├── silver/
│       │   └── 03_ibge_census_tracts_silver.py
│       └── gold/
│           ├── 04_dim_ibge_census_tracts.sql
│           └── 05_fact_ibge_census_tract_population.sql
├── osm/
│   ├── ingestion/
│   │   └── 01_osm_roads_landing.py
│   ├── quality/
│   │   ├── 03_osm_silver_quality.py
│   │   └── 05_osm_gold_quality.py
│   └── transformations/
│       ├── bronze/
│       │   └── 02_osm_roads_bronze.py
│       ├── silver/
│       │   └── 03_osm_roads_silver.py
│       └── gold/
│           ├── 04_dim_osm_road_class.sql
│           └── 05_dim_osm_roads.sql
└── inmet/
    ├── ingestion/
    │   └── 01_inmet_weather_landing.py
    ├── quality/
    │   ├── 03_inmet_silver_quality.py
    │   └── 05_inmet_gold_quality.py
    └── transformations/
        ├── bronze/
        │   └── 02_inmet_weather_bronze.py
        ├── silver/
        │   ├── 03_inmet_stations_silver.py
        │   └── 03_inmet_weather_readings_silver.py
        └── gold/
            ├── 04_dim_inmet_stations.sql
            └── 05_fact_inmet_weather_hourly.sql
```

Os notebooks estão no formato de código-fonte do Databricks (`.py` e `.sql` começando com `Databricks notebook source`). O Databricks os abre como notebooks normais, com as células separadas.

## Pré-requisitos

1. **Workspace Databricks com Unity Catalog** e compute serverless ou um Databricks Runtime recente. O notebook da Gold do GeoSampa usa variáveis de sessão SQL e scripting SQL (`BEGIN … END`).
2. **Repositório clonado como Git folder** (*Workspace → Create → Git folder*). Os notebooks importam os módulos de `src/common/` (log de ingestão, qualidade de dados e conversões) a partir da pasta do próprio notebook.
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

O cadastro de estações e as leituras de chuva seguem em paralelo até a Silver. Daí em diante, a qualidade da Silver barra a Gold.

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_cemaden_stations_landing` | `codibge` (3550308), `tipoestacao` (vazio = todos), `formato` (JSON) |
| 2 | `02_cemaden_stations_bronze` | `snapshot_date`: `pending` (padrão), `latest`, `all` ou `AAAA-MM-DD` |
| 1 | `01_cemaden_rainfall_readings_landing` | `modo`: `backfill` ou `incremental`; `mes_inicio` (202001, mínimo); `mes_fim` (vazio = último mês fechado); `forcar_reprocesso`: `nao`/`sim` |
| 2 | `02_cemaden_rainfall_readings_bronze` | (sem widgets: carrega os meses pendentes segundo o log) |
| 3 | `03_cemaden_stations_silver` | (sem widgets: recria a Silver a partir de todos os snapshots) |
| 3 | `03_cemaden_rainfall_readings_silver` | `window`: `pending` (padrão: meses recarregados na Bronze), `all` ou `AAAAMM` |
| 3 | `03_cemaden_silver_quality` (pasta `quality/`) | (sem widgets) |
| 4 | `04_dim_cemaden_stations` | (sem widgets) |
| 5 | `05_fact_cemaden_rainfall_hourly` | (sem widgets: recarrega os meses novos ou alterados) |
| 5 | `05_cemaden_gold_quality` (pasta `quality/`) | (sem widgets) |

- **Período:** só leituras a partir de janeiro de 2020, o mesmo início do SIBH. O histórico desde 2014 deixava a Bronze com cerca de 17 milhões de linhas. A landing não baixa meses anteriores (um `mes_inicio` menor é trocado por 202001), e a Bronze remove os que já estavam na tabela. A Silver e a Gold acompanham sozinhas, porque removem os meses que saíram da camada anterior. Os arquivos brutos antigos continuam no volume e podem ser apagados à mão (`ano=2014` a `ano=2019`).
- A PED aceita 12 chamadas por minuto, então o primeiro backfill desde 2020 leva algumas horas. Se parar no meio, basta rodar de novo: o notebook continua de onde parou.
- Os cadastros (`*_stations_*`) são snapshots diários: rodar no mesmo dia substitui o snapshot do dia.
- **Silver das estações:** uma linha por estação, com a versão do snapshot mais recente em que ela aparece. Estações que saíram do cadastro continuam na tabela (`fl_no_cadastro_atual = false`), para que as leituras antigas tenham estação.
- **Silver das leituras:** `datahora` da fonte está em UTC e vira `dt_hr_leitura_utc`; `dt_leitura`, `nr_hora` e `cod_dim_data` são calculados no fuso de São Paulo. `valor` vira `nr_chuva_mm` (chuva em mm no intervalo da leitura). Chuva negativa (falha do sensor) fica com `nr_chuva_mm` nulo e `fl_valor_invalido = true`; o valor recebido continua em `nr_chuva_mm_origem`. Leituras duplicadas (mesma estação, sensor e data e hora) ficam uma só, a da carga mais recente.
- **Gold:** a fato `fact_cemaden_rainfall_hourly` tem uma linha por estação, sensor, dia e hora (fuso de São Paulo), com a chuva acumulada, a maior leitura e o número de leituras da hora. Hora sem leitura não tem linha. A carga é incremental por mês, com o mesmo controle de assinatura da fato do GeoSampa.

### SIBH

Os passos são sequenciais, porque a coleta das medições usa o cadastro da Bronze para escolher os sensores.

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_sibh_stations_landing` | `api_url`, `codibge_check` (3550308) |
| 2 | `02_sibh_stations_bronze` | `snapshot_date`: `pending` (padrão), `latest`, `all` ou `AAAA-MM-DD` |
| 3 | `03_sibh_measurements_landing` | `mode`: `probe`, `backfill` ou `incremental`; `group_type` (`minute`); `min_month` (2020-01); `excluded_owners` (CEMADEN); `min_interval_s` (1.0); `force` |
| 4 | `04_sibh_measurements_bronze` | `window`: `pending` (padrão), `all` ou `AAAA-MM` |
| 5 | `05_sibh_stations_silver` | (sem widgets: recria a Silver a partir de todos os snapshots) |
| 5 | `05_sibh_measurements_silver` | `window`: `pending` (padrão: meses recarregados na Bronze), `all` ou `AAAA-MM` |
| 5 | `05_sibh_silver_quality` (pasta `quality/`) | (sem widgets) |
| 6 | `06_dim_sibh_sensors` | (sem widgets) |
| 7 | `07_fact_sibh_rainfall_hourly` e `07_fact_sibh_level_hourly` | (sem widgets: recarregam os meses novos ou alterados) |
| 7 | `07_sibh_gold_quality` (pasta `quality/`) | (sem widgets) |

- Os sensores do CEMADEN ficam de fora do SIBH por padrão, porque já vêm pela PED.
- O modo `probe` faz duas chamadas de teste e mostra a resposta da API sem gravar nada.
- **Silver dos sensores:** uma linha por sensor (`id` do cadastro). No SIBH, chuva e nível de um mesmo local têm `id` diferentes e o mesmo `cod_estacao`. Só a capital e os tipos 2 (`Chuva`) e 1 (`Nível`), com a versão do snapshot mais recente em que o sensor aparece.
- **Silver das medições:** `date` da fonte está em UTC e vira `dt_hr_medicao_utc`; `dt_medicao`, `nr_hora` e `cod_dim_data` ficam no fuso de São Paulo. `value` vira `nr_valor` (chuva em mm por medição, ou nível na unidade da fonte), e `dc_grandeza` diz qual dos dois é. Chuva negativa (falha do sensor) fica com `nr_valor` nulo e `fl_valor_invalido = true`; o valor recebido continua em `nr_valor_origem`. Medições duplicadas (mesmo sensor e data e hora) ficam uma só, inclusive entre meses: a API devolve as primeiras horas do mês seguinte junto com o mês pedido, e a medição fica só no mês da Bronze igual ao mês dela em UTC. Um mês reprocessado leva junto os vizinhos.
- **Gold:** uma dimensão de sensores e duas fatos por sensor, dia e hora (fuso de São Paulo): `fact_sibh_rainfall_hourly` (chuva somada, maior medição e número de medições) e `fact_sibh_level_hourly` (nível médio, mínimo, máximo e último da hora). As duas têm carga incremental por mês.

**Versão da transformação (Silvers do CEMADEN e do SIBH):** cada Silver guarda `chuvasegura.versao_transformacao` nas propriedades da tabela. Quando o código muda de versão, o modo `pending` dos jobs reprocessa todos os meses sozinho; não é preciso rodar `window = all` à mão.

### Dimensão de tempo (compartilhada)

`00_dim_tempo` cria e mantém a `fiap_analytics.gold.dim_tempo`: um dia por linha, de 2010 até o fim do ano seguinte, com chave `COD_DIM_TEMPO` no formato `yyyyMMdd`. É a única dimensão compartilhada entre as fontes. Toda fato do projeto usa essa chave, e fatos com granularidade menor que o dia (chuva por hora, por exemplo) guardam a hora na própria fato. O notebook roda no job `ChuvaSegura`, antes da fato do GeoSampa, e cada execução só acrescenta os dias que faltam.

### GeoSampa

| # | Notebook | Depende de |
|---|---|---|
| 1 | `01_geosampa_subprefecture_area_bronze` | — |
| 1 | `01_geosampa_history_flooding_events_bronze` | arquivos no volume |
| 2 | `02_geosampa_flooding_events_silver` | os dois passos 1 |
| 2 | `02_geosampa_silver_quality` (pasta `quality/`) | Silver |
| 3 | `03_dim_geosampa_subprefecture_area` | subprefeituras (Bronze) |
| 3 | `03_dim_geosampa_type_flooding_events` | qualidade da Silver |
| 4 | `04_fact_geosampa_flooding_events` | qualidade da Silver, as duas dimensões e a `dim_tempo` |
| 4 | `04_geosampa_gold_quality` (pasta `quality/`) | fato |

- Arquivos que já foram carregados (mesmo SHA256) são pulados.
- Um arquivo por ano no volume. Para corrigir um ano, troque o arquivo no volume pela versão nova e rode o job: a carga grava a versão nova e remove as linhas da anterior. Se houver dois arquivos do mesmo ano, a carga para e pede para deixar só um.
- A Silver converte a geometria (POINT ou POINT Z em WKB, conforme o ano do arquivo; SIRGAS 2000 / UTM 23S, EPSG 31983) em coordenadas UTM e em latitude e longitude. A conversão é uma UDF Python registrada no próprio notebook, e a última célula confere quantos pontos ficaram sem coordenada ou fora da área de São Paulo.
- A Gold recarrega só os anos de origem novos ou alterados.
- **Modelo da Gold (GeoSampa):** estrela com a fato `fact_geosampa_flooding_events` e as dimensões `dim_tempo`, `dim_geosampa_type_flooding_events` e `dim_geosampa_subprefecture_area`. Cada ocorrência é identificada por `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA` (número da ocorrência no arquivo do ano; no arquivo de 2026 ele vem na coluna `primaryindex`). Tipo ou subprefeitura não encontrados vão para o membro `-1` (não informado), então a fato nunca tem chave nula. As chaves estrangeiras são declaradas (informativas no Unity Catalog) e a fato usa liquid clustering por data e subprefeitura.

### SGB (carta de suscetibilidade)

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_sgb_susceptibility_landing` | `url` (pacote SIG da carta no RIGEO); `forcar`: `nao`/`sim` |
| 2 | `02_sgb_susceptibility_bronze` | `recarregar`: `nao`/`sim` |
| 3 | `03_sgb_susceptibility_areas_silver` | (sem widgets: recria a Silver a partir da Bronze) |
| 3 | `03_sgb_silver_quality` (pasta `quality/`) | (sem widgets) |
| 4 | `04_dim_sgb_susceptibility_class` | (sem widgets) |
| 5 | `05_dim_sgb_susceptibility_areas` | (sem widgets) |
| 5 | `05_sgb_gold_quality` (pasta `quality/`) | (sem widgets) |

- **Fonte:** *Carta de suscetibilidade a movimentos gravitacionais de massa e inundações: município de São Paulo - SP* (IPT/CPRM, 2015, escala 1:75.000), [registro no RIGEO](https://rigeo.sgb.gov.br/handle/doc/15216). O pacote SIG tem cerca de 970 MB, quase tudo fotos de campo. A landing lê do zip publicado só a pasta `12.Suscetibilidade` e a `04.PadrõesDeRelevo` (cerca de 30 MB) e grava esses arquivos, sem alteração, num zip por versão.
- **Carta estática:** se o pacote não mudou no RIGEO (mesmo ETag, tamanho e data), a landing não baixa nada e registra `SEM_DADOS`, e a Bronze não recarrega.
- **Camadas** (`cd_camada`): `inundacao` (1.166 polígonos), `bacia_inundacao` (802 bacias de drenagem que contribuem para as inundações; vão além do município, por todo o Alto Tietê), `enxurrada` (37), `movimento_massa` (2.370), `corrida_massa` (6) e `relevo` (211 polígonos de padrões de relevo). Classes `Alta`, `Média` e `Baixa` (grau 3, 2 e 1); enxurrada e corrida de massa não têm graduação.
- **Geometria:** a fonte está em SIRGAS 2000 / UTM 23S (EPSG 31983). A Silver grava a geometria em SIRGAS 2000 geográfico (EPSG 4674, longitude e latitude), o padrão das fontes geoespaciais do projeto, e calcula em UTM a área, o centroide e o envelope. As conversões estão em `src/common/geo.py`, em Python puro, e rodam no driver. A área calculada bate com a `Shape_Area` da fonte com diferença abaixo de 10⁻¹¹.
- **Declividade e relevo:** a declividade de cada classe (por exemplo, inundação Alta em declividades abaixo de 2° e movimento de massa Alta acima de 25°) e as características predominantes vêm dos quadros-síntese da carta, transcritos na dimensão de classes. O relevo entra como camada própria. O modelo digital de elevação e a declividade em raster ficam fora do projeto.
- **Áreas publicadas:** no movimento de massa, a soma dos polígonos de cada classe bate com o quadro-síntese (100,1, 231,2 e 1.196,2 km²). Na inundação, a soma (69,3, 88,0 e 138,8 km²) passa da área do quadro (52,0, 66,2 e 113,1 km²). A diferença está no próprio arquivo, por isso a qualidade compara com a `Shape_Area`.
- **Modelo da Gold (SGB):** `dim_sgb_susceptibility_class` (camada e classe, com grau, declividade, características e área do quadro-síntese) e `dim_sgb_susceptibility_areas` (dimensão espacial, uma linha por polígono, com a classe, a área, o centroide, o envelope e a geometria). As duas têm o membro `-1`. As áreas são a referência para o cruzamento com as ocorrências do GeoSampa: uma ocorrência pode cair em mais de uma área da mesma camada, e nesse caso vale o maior grau.

### IBGE (Censo 2022 por setor censitário)

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_ibge_census_tracts_landing` | `url` (malha com atributos de SP); `cod_ibge` (3550308, conferência); `forcar`: `nao`/`sim` |
| 2 | `02_ibge_census_tracts_bronze` | `cod_ibge` (3550308); `recarregar`: `nao`/`sim` |
| 3 | `03_ibge_census_tracts_silver` | (sem widgets: recria a Silver a partir da Bronze) |
| 3 | `03_ibge_silver_quality` (pasta `quality/`) | (sem widgets) |
| 4 | `04_dim_ibge_census_tracts` | (sem widgets) |
| 5 | `05_fact_ibge_census_tract_population` | (sem widgets) |
| 5 | `05_ibge_gold_quality` (pasta `quality/`) | (sem widgets) |

- **Fonte:** `SP_setores_CD2022.gpkg` da [malha com atributos](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/setores/gpkg/UF/SP/) dos *Agregados por Setores Censitários* do Censo 2022. Um GeoPackage do estado (cerca de 180 MB) traz o polígono de cada setor e as variáveis do universo V0001 a V0007 (pessoas, domicílios, domicílios particulares e coletivos, média de moradores, proporção de domicílios imputados e domicílios ocupados).
- **Recorte:** a landing guarda o arquivo do estado inteiro. A Bronze carrega só os 27.301 setores do município, porque o arquivo do estado tem mais de 100 mil setores.
- **Sem alteração na fonte**, a landing não baixa nada e registra `SEM_DADOS`, e a Bronze não recarrega. A conferência mensal existe porque o IBGE já republicou arquivos do Censo com correções.
- **Silver:** códigos `.` viram nulo; a situação detalhada e o tipo do setor ganham a descrição do dicionário de dados do IBGE; `fl_favela` marca os 3.875 setores de favelas e comunidades urbanas. A geometria já vem em SIRGAS 2000 geográfico (EPSG 4674) e é mantida. A área calculada em UTM fica a até 0,03% da área publicada pelo IBGE, e a densidade usa a área do IBGE.
- **Gold:** `dim_ibge_census_tracts` (dimensão espacial, um setor por linha, com distrito, favela, área, centroide, envelope e geometria) e `fact_ibge_census_tract_population` (pessoas e domicílios por setor na data de referência do Censo, 31/07/2022, chave `20220731` da `dim_tempo`). A soma dá 11.451.999 pessoas, a população oficial do município no Censo 2022.

### OpenStreetMap (rede viária)

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_osm_roads_landing` | `url` (extrato Sudeste da Geofabrik); `camada` (`gis_osm_roads_free_1`); `forcar`: `nao`/`sim` |
| 2 | `02_osm_roads_bronze` | `envelope` (`-46.84,-24.02,-46.35,-23.34`); `recarregar`: `nao`/`sim` |
| 3 | `03_osm_roads_silver` | (sem widgets: recria a Silver a partir da Bronze) |
| 3 | `03_osm_silver_quality` (pasta `quality/`) | (sem widgets) |
| 4 | `04_dim_osm_road_class` | (sem widgets) |
| 5 | `05_dim_osm_roads` | (sem widgets) |
| 5 | `05_osm_gold_quality` (pasta `quality/`) | (sem widgets) |

- **Licença:** dados © colaboradores do OpenStreetMap, sob a [Open Database License (ODbL 1.0)](https://www.openstreetmap.org/copyright). Todo mapa, painel ou relatório feito com estas tabelas precisa trazer a atribuição "© OpenStreetMap contributors". Se a base derivada for distribuída, ela também fica sob a ODbL. A atribuição está nos comentários das tabelas e no manifesto da landing.
- **Fonte:** extrato *free shapefile* da região Sudeste da Geofabrik (cerca de 2,2 GB, atualizado todo dia). A landing lê do zip publicado só a camada de vias (`gis_osm_roads_free_1`, cerca de 530 MB comprimidos) e grava, sem alteração, um zip por extrato. Sem alteração na fonte, registra `SEM_DADOS`.
- **Recorte:** a Bronze carrega as vias cujo envelope cruza o envelope do município (calculado a partir dos setores do IBGE, com margem de 0,01°). Entram as vias do município e as das bordas dos municípios vizinhos, cerca de 292 mil vias no extrato de novembro de 2026. O extrato completo continua no volume.
- **Silver:** sentido (`oneway`), velocidade máxima, ponte e túnel tipados. A geometria do OSM vem em WGS84 (EPSG 4326), que é tratado como igual ao SIRGAS 2000 (a diferença é menor que 1 m), e é gravada como EPSG 4674. O comprimento é calculado em UTM 23S.
- **Gold:** `dim_osm_road_class` (classes da Geofabrik com descrição em português, grupo, hierarquia viária e se a via é de veículos) e `dim_osm_roads` (dimensão espacial, uma via por linha, com nome, sentido, velocidade, ponte, túnel, comprimento, centroide, envelope e geometria). As duas têm o membro `-1`. A cada extrato novo, as vias que mudaram são atualizadas e as que saíram do OSM são removidas.

### INMET (dados meteorológicos horários)

| # | Notebook | Widgets |
|---|---|---|
| 1 | `01_inmet_weather_landing` | `modo`: `backfill` ou `incremental`; `ano_inicio` (2020, mínimo); `estacoes` (`A701,A771`); `forcar`: `nao`/`sim` |
| 2 | `02_inmet_weather_bronze` | (sem widgets: carrega os anos pendentes segundo o log) |
| 3 | `03_inmet_stations_silver` | (sem widgets: recria a Silver a partir da Bronze) |
| 3 | `03_inmet_weather_readings_silver` | `window`: `pending` (padrão), `all` ou `AAAA` |
| 3 | `03_inmet_silver_quality` (pasta `quality/`) | (sem widgets) |
| 4 | `04_dim_inmet_stations` | (sem widgets) |
| 5 | `05_fact_inmet_weather_hourly` | (sem widgets: recarrega os meses novos ou alterados) |
| 5 | `05_inmet_gold_quality` (pasta `quality/`) | (sem widgets) |

- **Por que entra:** o INMET traz variáveis que o CEMADEN e o SIBH não têm (temperatura, umidade, pressão, vento e radiação). Também mede chuva, numa rede independente.
- **Fonte:** dados históricos anuais do BDMEP (`https://portal.inmet.gov.br/uploads/dadoshistoricos/AAAA.zip`, cerca de 100 MB por ano, um CSV por estação automática do país). A landing lê do zip só os CSVs das estações da capital: A701 (São Paulo - Mirante) e A771 (São Paulo - Interlagos). A755 (Barueri) fica de fora porque é de outro município. O portal exige um User-Agent; os downloads usam o do projeto (`ChuvaSegura-FIAP/1.0`).
- **Período:** a partir de 2020, como o CEMADEN e o SIBH.
- **Ano parcial:** o INMET atualiza o zip do ano corrente mais ou menos uma vez por mês. Um ano só fica `SUCESSO` quando baixado a partir de fevereiro do ano seguinte; antes disso fica `PARCIAL` e o incremental confere de novo. Zip sem alteração na fonte e ano ainda não publicado entram como `SEM_DADOS`.
- **Bronze:** o CSV é lido em Python (texto em Latin-1, `;`, 8 linhas de cabeçalho da estação). As colunas ganham nomes curtos (`precipitacao_total_mm`, `temperatura_ar_c`...), e uma coluna desconhecida para a carga. Valores ficam em texto, com vírgula decimal e `-9999` para falta.
- **Silver:** `Data` e `Hora UTC` viram `dt_hr_medicao_utc`. As medidas do INMET valem para a hora que termina no horário da medição (a chuva das 13:00 UTC é a de 12:00 a 13:00 UTC). Por isso o dia e a hora no fuso de São Paulo saem do início da hora (`dt_hr_inicio_utc`), e a hora 9 da fato reúne o que aconteceu entre 09:00 e 09:59, como no CEMADEN e no SIBH. `-9999` vira nulo.
- **Gold:** `dim_inmet_stations` (membro `-1`) e `fact_inmet_weather_hourly`, uma linha por estação, dia e hora, com chuva, temperatura (média, máxima e mínima), ponto de orvalho, umidade, pressão, vento e radiação. A carga é incremental por mês, com a mesma assinatura das outras fatos.

## Jobs e workflows

Cada fonte tem um job, e a definição de cada um fica em `jobs/`, no formato YAML dos jobs do Databricks (`resources.jobs`). Separar por fonte faz com que uma falha numa API não trave as outras.

| Arquivo | Job | Agenda (America/Sao_Paulo) | Situação |
|---|---|---|---|
| `jobs/chuvasegura_geosampa.job.yml` | `ChuvaSegura` | todo dia às 09:00:30 | ativo |
| `jobs/chuvasegura_cemaden.job.yml` | `ChuvaSegura - CEMADEN` | todo dia às 07:00 | ativo |
| `jobs/chuvasegura_sibh.job.yml` | `ChuvaSegura - SIBH` | todo dia às 07:30 | ativo |
| `jobs/chuvasegura_sgb.job.yml` | `ChuvaSegura - SGB` | dia 1 de cada mês às 06:00 | ativo |
| `jobs/chuvasegura_ibge.job.yml` | `ChuvaSegura - IBGE` | dia 1 de cada mês às 06:30 | ativo |
| `jobs/chuvasegura_osm.job.yml` | `ChuvaSegura - OSM` | toda segunda-feira às 05:00 | ativo |
| `jobs/chuvasegura_inmet.job.yml` | `ChuvaSegura - INMET` | toda segunda-feira às 08:00 | pausado até o backfill |

```mermaid
flowchart LR
  subgraph cemaden["ChuvaSegura - CEMADEN"]
    C1[01_cemaden_stations_landing] --> C2[02_cemaden_stations_bronze] --> C5[03_cemaden_stations_silver]
    C3[01_cemaden_rainfall_readings_landing<br/>modo = incremental] --> C4[02_cemaden_rainfall_readings_bronze] --> C6[03_cemaden_rainfall_readings_silver]
    C5 --> CQ1{{03_cemaden_silver_quality}}
    C6 --> CQ1
    CQ1 --> C7[04_dim_cemaden_stations]
    CQ1 --> C8[05_fact_cemaden_rainfall_hourly]
    C7 --> C8
    CT[00_dim_tempo] --> C8
    C8 --> CQ2{{05_cemaden_gold_quality}}
  end
  subgraph sibh["ChuvaSegura - SIBH"]
    S1[01_sibh_stations_landing] --> S2[02_sibh_stations_bronze] --> S3[03_sibh_measurements_landing<br/>mode = incremental] --> S4[04_sibh_measurements_bronze]
    S2 --> S5[05_sibh_stations_silver]
    S4 --> S6[05_sibh_measurements_silver]
    S5 --> S6
    S5 --> SQ1{{05_sibh_silver_quality}}
    S6 --> SQ1
    SQ1 --> S7[06_dim_sibh_sensors]
    SQ1 --> S8[07_fact_sibh_rainfall_hourly]
    SQ1 --> S9[07_fact_sibh_level_hourly]
    S7 --> S8
    S7 --> S9
    ST[00_dim_tempo] --> S8
    ST --> S9
    S8 --> SQ2{{07_sibh_gold_quality}}
    S9 --> SQ2
  end
  subgraph geosampa["ChuvaSegura (GeoSampa)"]
    G1[01_geosampa_history_flooding_events_bronze] --> G3[02_geosampa_flooding_events_silver]
    G2[01_geosampa_subprefecture_area_bronze] --> G3
    G2 --> G4[03_dim_geosampa_subprefecture_area]
    G3 --> Q1{{02_geosampa_silver_quality}}
    Q1 --> G5[03_dim_geosampa_type_flooding_events]
    Q1 --> G6[04_fact_geosampa_flooding_events]
    G4 --> G6
    G5 --> G6
    T0[00_dim_tempo] --> G6
    G6 --> Q2{{04_geosampa_gold_quality}}
  end
  subgraph sgb["ChuvaSegura - SGB"]
    B1[01_sgb_susceptibility_landing] --> B2[02_sgb_susceptibility_bronze] --> B3[03_sgb_susceptibility_areas_silver]
    B3 --> BQ1{{03_sgb_silver_quality}}
    BQ1 --> B4[04_dim_sgb_susceptibility_class] --> B5[05_dim_sgb_susceptibility_areas]
    B5 --> BQ2{{05_sgb_gold_quality}}
  end
  subgraph ibge["ChuvaSegura - IBGE"]
    I1[01_ibge_census_tracts_landing] --> I2[02_ibge_census_tracts_bronze] --> I3[03_ibge_census_tracts_silver]
    I3 --> IQ1{{03_ibge_silver_quality}}
    IQ1 --> I4[04_dim_ibge_census_tracts] --> I5[05_fact_ibge_census_tract_population]
    IQ1 --> I5
    IT[00_dim_tempo] --> I5
    I5 --> IQ2{{05_ibge_gold_quality}}
  end
  subgraph osm["ChuvaSegura - OSM"]
    O1[01_osm_roads_landing] --> O2[02_osm_roads_bronze] --> O3[03_osm_roads_silver]
    O3 --> OQ1{{03_osm_silver_quality}}
    OQ1 --> O4[04_dim_osm_road_class] --> O5[05_dim_osm_roads]
    O5 --> OQ2{{05_osm_gold_quality}}
  end
  subgraph inmet["ChuvaSegura - INMET"]
    N1[01_inmet_weather_landing<br/>modo = incremental] --> N2[02_inmet_weather_bronze]
    N2 --> N3[03_inmet_stations_silver]
    N2 --> N4[03_inmet_weather_readings_silver]
    N3 --> NQ1{{03_inmet_silver_quality}}
    N4 --> NQ1
    NQ1 --> N5[04_dim_inmet_stations] --> N6[05_fact_inmet_weather_hourly]
    NQ1 --> N6
    NT[00_dim_tempo] --> N6
    N6 --> NQ2{{05_inmet_gold_quality}}
  end
```

Todos os jobs:
- rodam em compute serverless (ambiente 6), com fila habilitada;
- apontam para os notebooks no Git folder `/Workspace/Users/rm562679@fiap.com.br/FIAP-GS2-2TSCPV-2026`, na subpasta `data/src/`. Se o repositório estiver clonado em outro lugar, troque esse prefixo em todos os arquivos de `jobs/`.
- **Mudança de estrutura:** os notebooks saíram de `src/` para `data/src/`. Depois de atualizar o Git folder no Databricks para esta versão, os jobs já existentes no workspace precisam ter os caminhos das tarefas atualizados (iguais aos dos YAMLs); senão, falham por não encontrar o notebook.

**Antes de ativar CEMADEN, SIBH e INMET:**
1. Rode uma vez, manualmente, a carga do histórico com o modo `backfill`:
   - CEMADEN: `01_cemaden_rainfall_readings_landing` com `modo = backfill`;
   - SIBH: `03_sibh_measurements_landing` com `mode = backfill`;
   - INMET: `01_inmet_weather_landing` com `modo = backfill` (de 2020 até o ano corrente, poucos minutos).
2. Rode as Bronzes correspondentes.
3. Mude `pause_status` para `UNPAUSED`, ou ative a agenda na interface. O job do INMET vem com a agenda pausada por isso.

O incremental diário cobre o mês atual e completa os meses parciais (no INMET, o ano atual e os anos parciais).

**SGB, IBGE e OSM** não precisam de carga inicial: a primeira execução do job baixa a versão atual.

**Como aplicar os YAMLs:**
- **Job que já existe** (`ChuvaSegura`): troque o caminho de cada tarefa pelo do YAML. Em *Workflows → ChuvaSegura*, abra cada tarefa e edite o campo *Path*.
- **Job novo:** crie em *Workflows → Create job*, com as tarefas, dependências, parâmetros (`base_parameters`) e agenda do arquivo.
- **Com a Databricks CLI:** os arquivos já estão no formato de *resources* dos Databricks Asset Bundles. Para usar `databricks bundle deploy`, é preciso um `databricks.yml` (nesta pasta, incluindo `jobs/*.job.yml`, ou na raiz do repositório, incluindo `data/jobs/*.job.yml`). Para o bundle passar a gerenciar o `ChuvaSegura` já existente em vez de criar outro, use `databricks bundle deployment bind ChuvaSegura <job_id>`.

Sempre que um notebook mudar de nome ou de pasta, atualize o YAML no mesmo PR.

## Logs de ingestão e meses parciais

Cada origem tem a sua tabela de log, todas no mesmo formato, definido em `src/common/ingestion_log.py`:

- `fiap_analytics.bronze.cemaden_ingestion_log`
- `fiap_analytics.bronze.sibh_ingestion_log`
- `fiap_analytics.bronze.geosampa_ingestion_log`
- `fiap_analytics.bronze.sgb_ingestion_log`
- `fiap_analytics.bronze.ibge_ingestion_log`
- `fiap_analytics.bronze.osm_ingestion_log`
- `fiap_analytics.bronze.inmet_ingestion_log`

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

## Qualidade de dados

As etapas de qualidade usam o módulo `src/common/data_quality.py` e rodam como tarefas do job, depois da Silver e depois da Gold. O GeoSampa é a referência: as outras fontes vão seguir o mesmo padrão.

- **Regra:** uma consulta SQL que devolve `falhas` (linhas que violam a regra) e `total` (linhas avaliadas). A regra passa quando `falhas / total` não passa do `limite`. Com limite 0, nenhuma falha é aceita.
- **Severidade:**
  - `ERRO`: se falhar, a tarefa para e o job não segue. A Gold nunca é carregada a partir de uma Silver reprovada.
  - `AVISO`: só registra.
- **Regra que não consegue rodar** (tabela inexistente, SQL inválido) conta como falha, nunca como sucesso.
- **Regras prontas:** `nao_vazia`, `nao_nulo`, `condicao` (expressão verdadeira nas linhas que violam a regra), `unico`, `existe_em` (integridade referencial) e `consulta` (SQL livre).
- **Resultados:** ficam na tabela de qualidade de cada origem (`fiap_analytics.bronze.geosampa_data_quality_log`, `fiap_analytics.bronze.cemaden_data_quality_log`, `fiap_analytics.bronze.sibh_data_quality_log`, `fiap_analytics.bronze.sgb_data_quality_log`, `fiap_analytics.bronze.ibge_data_quality_log`, `fiap_analytics.bronze.osm_data_quality_log` e `fiap_analytics.bronze.inmet_data_quality_log`), uma linha por regra e execução, com tabela, regra, severidade, status (`SUCESSO`/`FALHA`), falhas, total, percentual e limite.

Regras do GeoSampa:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | tabela não vazia; mesmas linhas da Bronze; ano e `nr_id_ocorrencia` preenchidos e únicos por ano; data preenchida e entre 2010 e hoje; SRID 31983; ponto dentro da área de São Paulo | `name` repetido no ano; data fora do ano do arquivo; mais de 1% sem coordenada ou sem subprefeitura; mais de 5% do tipo "Outros" |
| Gold | fato não vazia; mesmas linhas da Silver por ano; ocorrência única; chaves existentes na `dim_tempo` e nas dimensões; ponto dentro da área; chaves únicas e membro `-1` nas dimensões; `dim_tempo` sem chave repetida nem dia faltando | mais de 1% das ocorrências com subprefeitura `-1`; ocorrências com tipo `-1` |

Regras do CEMADEN:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | estações: tabela não vazia, código único, latitude e longitude preenchidas e dentro da área de São Paulo; leituras: tabela não vazia, uma linha por chave distinta da Bronze, estação, sensor e data e hora preenchidos, data entre 2020 e agora, chuva não negativa | mais de 1% das leituras com data e hora fora do mês pedido à API (confere a premissa de UTC); mais de 1% sem valor; leituras acima de 50 mm; mais de 1% de leituras de estações fora do cadastro |
| Gold | fato não vazia; uma linha por estação, sensor, dia e hora; leituras e chuva iguais às da Silver em cada mês; chaves existentes na `dim_tempo` e na dimensão de estações; chuva da hora não negativa; chaves únicas e membro `-1` na dimensão | mais de 1% das horas com estação `-1`; horas acima de 120 mm |

Regras do SIBH:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | sensores: tabela não vazia, sensor único, grandeza, latitude e longitude preenchidas e dentro da área de São Paulo; medições: tabela não vazia, uma linha por chave distinta da Bronze, sensor, grandeza e data e hora preenchidos, data entre 2010 e agora, chuva não negativa | mais de 1% das medições com data e hora fora do mês pedido à API (confere a premissa de UTC); mais de 1% sem valor; chuva acima de 50 mm numa medição; mais de 1% de medições de sensores fora do cadastro |
| Gold | as duas fatos não vazias, com uma linha por sensor, dia e hora, medições iguais às da Silver em cada mês e chaves existentes na `dim_tempo` e na dimensão de sensores; chuva igual à da Silver em cada mês e não negativa; nível mínimo, médio e máximo coerentes; chaves únicas e membro `-1` na dimensão | mais de 1% das horas com sensor `-1`; horas de chuva acima de 120 mm; sensor de chuva na fato de nível |

Regras do SGB:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | tabela não vazia; mesmas feições da Bronze; as seis camadas presentes; feição única na camada; classe preenchida e, nas camadas graduadas, Alta, Média ou Baixa; geometria de origem em EPSG 31983 e preenchida; área maior que zero e igual à `Shape_Area` da fonte (0,01%); feições dentro da área de São Paulo (menos as bacias contribuintes); movimento de massa e relevo cobrindo a área do município (2%) | camada desconhecida; área de cada classe de movimento de massa diferente do quadro-síntese (1%) |
| Gold | dimensões não vazias; chaves únicas e membro `-1`; grau coerente com a classe; áreas e feições iguais às da Silver em cada camada; classe existente na dimensão de classes e nenhuma área com classe `-1`; geometria preenchida; centroide dentro do envelope | classe graduada sem características do quadro-síntese |

Regras do IBGE:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | tabela não vazia; mesmos setores da Bronze; código do setor preenchido, único, com 15 dígitos e do município 3550308; os 96 distritos presentes; tipo e situação descritos; pessoas preenchidas; contagens não negativas; domicílios = particulares + coletivos; ocupados até os particulares; proporção de imputados entre 0 e 1; geometria de origem em EPSG 4674 e preenchida; área maior que zero e igual à do IBGE (0,1%); setores dentro da área de São Paulo; soma das áreas igual à do município (1%) | população diferente da do Censo 2022 (11.451.999) |
| Gold | fato não vazia, com os mesmos setores, pessoas e domicílios da Silver; setor único por data; chaves existentes na `dim_tempo` e na dimensão de setores, sem setor `-1`; contagens não negativas; densidade coerente; dimensão não vazia, com chaves únicas, membro `-1` e os mesmos setores da Silver; geometria preenchida; centroide dentro do envelope | — |

Regras do OpenStreetMap:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | tabela não vazia; mesmas vias da Bronze; pelo menos 15 mil km de vias de veículos no recorte; `osm_id` preenchido e único; classe preenchida; geometria de origem em EPSG 4326, preenchida e linear; comprimento maior que zero; vias dentro da área de São Paulo; sentido `B`, `F` ou `T` | classe fora do esquema da Geofabrik; vias com mais de 50 km; velocidade fora de 5 a 130 km/h; mais de 20% das vias de veículos sem nome |
| Gold | dimensões não vazias; chaves únicas e membro `-1`; vias e extensão iguais às da Silver em cada classe; classe existente na dimensão; geometria preenchida; centroide dentro do envelope | vias com classe `-1` |

Regras do INMET:

| Etapa | ERRO (para o job) | AVISO (só registra) |
|---|---|---|
| Silver | estações: tabela não vazia, código único, latitude e longitude preenchidas e dentro da área de São Paulo; medições: tabela não vazia, uma linha por chave distinta da Bronze, estação preenchida e cadastrada, data e hora preenchidas, em hora cheia e entre 2020 e agora, chuva e vento não negativos | medição fora do ano do arquivo; mais de 10% sem chuva; horas acima de 100 mm; temperatura fora de -5 a 45 °C; umidade fora de 0 a 100%; pressão fora de 850 a 1000 mb |
| Gold | fato não vazia, com uma linha por estação, dia e hora, e as mesmas horas e chuva da Silver em cada mês; chaves existentes na `dim_tempo` e na dimensão de estações; chuva não negativa; no máximo 24 horas por estação e dia; dimensão com chaves únicas e membro `-1` | horas com estação `-1`; temperatura mínima acima da máxima |

Para incluir uma regra, acrescente-a à lista `regras` do notebook da etapa. Exemplo de consulta aos resultados:

```sql
SELECT executado_em, etapa, tabela, regra, severidade, status, falhas, total, percentual
FROM fiap_analytics.bronze.geosampa_data_quality_log
WHERE status = 'FALHA'
ORDER BY executado_em DESC;
```

## Tabelas geradas

| Camada | Tabela | Conteúdo |
|---|---|---|
| bronze | `cemaden_stations` | Snapshots diários do cadastro de estações do CEMADEN |
| bronze | `cemaden_rainfall_readings` | Leituras de chuva do CEMADEN a partir de 2020, uma fatia por mês (`_window`) |
| bronze | `sibh_stations` | Snapshots diários do cadastro de estações do SIBH (estado inteiro) |
| bronze | `sibh_measurements` | Medições de chuva e nível do SIBH na capital, uma fatia por mês |
| bronze | `geosampa_history_flooding_events` | Ocorrências do GeoSampa, com a geometria em WKB |
| bronze | `geosampa_subprefecture_area` | Subprefeituras e agrupamentos (Anexo IX) |
| bronze | `geosampa_data_quality_log` | Resultados das regras de qualidade do GeoSampa |
| bronze | `cemaden_data_quality_log` | Resultados das regras de qualidade do CEMADEN |
| bronze | `sibh_data_quality_log` | Resultados das regras de qualidade do SIBH |
| bronze | `sgb_susceptibility_areas` | Feições da carta de suscetibilidade do SGB, uma linha por feição de cada camada, com a geometria em WKB (EPSG 31983) |
| bronze | `sgb_data_quality_log` | Resultados das regras de qualidade do SGB |
| bronze | `ibge_census_tracts` | Setores censitários do Censo 2022 no município, atributos em texto e geometria em WKB (EPSG 4674) |
| bronze | `ibge_data_quality_log` | Resultados das regras de qualidade do IBGE |
| bronze | `osm_roads` | Vias do OpenStreetMap no recorte do município, atributos em texto e geometria em WKB (EPSG 4326) |
| bronze | `osm_data_quality_log` | Resultados das regras de qualidade do OpenStreetMap |
| bronze | `inmet_weather_readings` | Medições horárias do INMET a partir de 2020, em texto como no CSV, uma fatia por ano (`_window`) |
| bronze | `inmet_data_quality_log` | Resultados das regras de qualidade do INMET |
| silver | `sibh_stations` | Sensores de chuva e nível do SIBH na capital, uma linha por sensor, tipados |
| silver | `sibh_measurements` | Medições de chuva e nível tipadas e sem duplicidade, com data e hora em UTC e dia e hora no fuso de São Paulo |
| silver | `cemaden_stations` | Estações do CEMADEN no município, uma linha por estação, tipadas |
| silver | `cemaden_rainfall_readings` | Leituras de chuva tipadas e sem duplicidade, com data e hora em UTC e dia e hora no fuso de São Paulo |
| silver | `sgb_susceptibility_areas` | Áreas da carta de suscetibilidade, com camada e classe padronizadas, geometria em EPSG 4674, área, centroide e envelope |
| silver | `ibge_census_tracts` | Setores censitários tipados, com situação, tipo, distrito, favela, pessoas, domicílios, densidade, geometria, centroide e envelope |
| silver | `osm_roads` | Vias tipadas, com classe, nome, sentido, velocidade, ponte, túnel, geometria, comprimento, centroide e envelope |
| silver | `inmet_stations` | Estações automáticas do INMET na capital, uma linha por estação |
| silver | `inmet_weather_readings` | Medições horárias tipadas e sem duplicidade, com hora em UTC e dia e hora no fuso de São Paulo |
| silver | `geosampa_flooding_events` | Ocorrências com identificador (`nr_id_ocorrencia`), data, tipo e subprefeitura padronizados e localização (`nr_x_utm`, `nr_y_utm`, `nr_latitude`, `nr_longitude`, `geom_wkb`) |
| gold | `dim_tempo` | Dimensão de tempo diária, compartilhada por todas as fatos |
| gold | `dim_cemaden_stations` | Dimensão de estações do CEMADEN, com o membro `-1` (não informado) |
| gold | `fact_cemaden_rainfall_hourly` | Fato de chuva por estação, sensor, dia e hora |
| gold | `dim_sibh_sensors` | Dimensão de sensores do SIBH (chuva e nível), com o membro `-1` (não informado) |
| gold | `fact_sibh_rainfall_hourly` | Fato de chuva por sensor, dia e hora |
| gold | `fact_sibh_level_hourly` | Fato de nível por sensor, dia e hora (médio, mínimo, máximo e último) |
| gold | `dim_geosampa_subprefecture_area` | Dimensão de subprefeituras, com o membro `-1` (não informado) |
| gold | `dim_geosampa_type_flooding_events` | Dimensão de tipos de ocorrência, com o membro `-1` (não informado) |
| gold | `fact_geosampa_flooding_events` | Fato de ocorrências (uma linha por ocorrência) por data, tipo e subprefeitura, com latitude e longitude |
| gold | `dim_sgb_susceptibility_class` | Dimensão de classes da carta do SGB (camada e classe, com grau, declividade e características do quadro-síntese), com o membro `-1` |
| gold | `dim_sgb_susceptibility_areas` | Dimensão espacial das áreas da carta do SGB, uma linha por polígono, com classe, área, centroide, envelope e geometria |
| gold | `dim_ibge_census_tracts` | Dimensão espacial dos setores censitários do Censo 2022, com o membro `-1` |
| gold | `fact_ibge_census_tract_population` | Fato de pessoas e domicílios por setor censitário na data de referência do Censo 2022 |
| gold | `dim_osm_road_class` | Dimensão de classes de via do OpenStreetMap, com hierarquia viária, com o membro `-1` |
| gold | `dim_osm_roads` | Dimensão espacial das vias do OpenStreetMap, uma via por linha, com o membro `-1` |
| gold | `dim_inmet_stations` | Dimensão de estações do INMET, com o membro `-1` |
| gold | `fact_inmet_weather_hourly` | Fato de condições meteorológicas por estação, dia e hora |

## Desenvolvimento

- **Módulos de `src/common`:** depois de alterar `ingestion_log.py`, `data_quality.py`, `conversoes.py`, `geo.py` ou `arquivos.py`, reinicie o Python do notebook (*Run → Restart Python*) para recarregar o módulo.
- **Fluxo de trabalho:** cada mudança vai numa branch própria e entra na `main` por pull request.

## Próximos passos

- Cruzamento da chuva e do nível com as ocorrências do GeoSampa.

## Fontes dos dados

- **CEMADEN:** dados da rede observacional do Centro Nacional de Monitoramento e Alertas de Desastres Naturais (CEMADEN/MCTI), via API PED.
- **SIBH:** Sistema de Informações Hidrológicas, Agência de Águas do Estado de São Paulo (SP Águas).
- **GeoSampa:** Prefeitura de São Paulo. Subprefeituras conforme o Anexo IX (ata 41/14).
- **SGB:** Serviço Geológico do Brasil (SGB/CPRM) e Instituto de Pesquisas Tecnológicas (IPT). *Carta de suscetibilidade a movimentos gravitacionais de massa e inundações: município de São Paulo - SP*, 2015, escala 1:75.000. Disponível no RIGEO.
- **IBGE:** Instituto Brasileiro de Geografia e Estatística. Censo Demográfico 2022: Agregados por Setores Censitários (resultados do universo) e malha de setores censitários.
- **OpenStreetMap:** © colaboradores do OpenStreetMap. Dados disponíveis sob a Open Database License (ODbL 1.0), https://www.openstreetmap.org/copyright. Extrato da Geofabrik GmbH (download.geofabrik.de).
- **INMET:** Instituto Nacional de Meteorologia, Banco de Dados Meteorológicos (BDMEP), dados históricos das estações automáticas.
