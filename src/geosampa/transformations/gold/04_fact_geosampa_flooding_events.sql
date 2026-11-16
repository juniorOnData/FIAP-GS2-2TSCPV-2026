-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GEO_GLD_003 04_fact_geosampa_flooding_events
-- MAGIC
-- MAGIC ## 01. Identificação
-- MAGIC
-- MAGIC | Campo | Valor |
-- MAGIC |---|---|
-- MAGIC | Identificador | GEO_GLD_003 |
-- MAGIC | Nome | 04_fact_geosampa_flooding_events |
-- MAGIC | Projeto | ChuvaSegura |
-- MAGIC | Domínio de negócio | Ocorrências de alagamento (GeoSampa) |
-- MAGIC | Camada | Gold |
-- MAGIC | Linguagem | SQL |
-- MAGIC | Responsável técnico | juniorOnData |
-- MAGIC | Responsável funcional | A definir |
-- MAGIC | Status | Produção |
-- MAGIC | Versão | 1.5 |
-- MAGIC | Criticidade | Alta |
-- MAGIC | Caminho no repositório | `src/geosampa/transformations/gold/04_fact_geosampa_flooding_events.sql` |
-- MAGIC
-- MAGIC ## 02. Objetivo
-- MAGIC
-- MAGIC Carregar a fato de ocorrências, com uma linha por ocorrência, chaves para a `dim_tempo` e as dimensões do GeoSampa, latitude e longitude. A carga é incremental por ano de origem: só os anos novos ou alterados na Silver são recarregados.
-- MAGIC
-- MAGIC ## 03. Contexto funcional
-- MAGIC
-- MAGIC É a tabela consumida pelas análises e painéis de ocorrências e será a base do cruzamento com a chuva.
-- MAGIC
-- MAGIC ## 04. Arquitetura e fluxo
-- MAGIC
-- MAGIC | Posição | Item |
-- MAGIC |---|---|
-- MAGIC | Etapa anterior | `02_geosampa_silver_quality`, as duas dimensões do GeoSampa e `00_dim_tempo` |
-- MAGIC | Etapa atual | Atualiza `fiap_analytics.gold.fact_geosampa_flooding_events` |
-- MAGIC | Etapa posterior | `04_geosampa_gold_quality` e consumo analítico |
-- MAGIC
-- MAGIC ## 05. Dependências
-- MAGIC
-- MAGIC | Tipo | Item | Uso |
-- MAGIC |---|---|---|
-- MAGIC | Tabela | `fiap_analytics.silver.geosampa_flooding_events` | Ocorrências |
-- MAGIC | Tabela | `fiap_analytics.gold.dim_tempo` | Chave estrangeira de data |
-- MAGIC | Tabela | `fiap_analytics.gold.dim_geosampa_type_flooding_events` | Chave do tipo |
-- MAGIC | Tabela | `fiap_analytics.gold.dim_geosampa_subprefecture_area` | Chave da subprefeitura |
-- MAGIC | Objeto de sessão | View `stg_fact_geosampa_flooding_events`, views de resumo e variáveis `anos_silver` e `anos_recarga` | Criadas nas E02 e E03 e usadas nas E04 e E05 |
-- MAGIC | Permissão | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze` e `fiap_analytics.silver`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.gold` | Ler a Silver e as dimensões e gravar a fato |
-- MAGIC
-- MAGIC ## 06. Parâmetros de entrada
-- MAGIC
-- MAGIC Não aplicável. Não há widgets.
-- MAGIC
-- MAGIC ## 07. Fontes de dados
-- MAGIC
-- MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
-- MAGIC |---|---|---|---|---|---|---|
-- MAGIC | `fiap_analytics.silver.geosampa_flooding_events` | Delta | Silver do GeoSampa | Completa, com recarga por ano | Diária (job) | `_source_year` + `nr_id_ocorrencia` | Nenhum |
-- MAGIC | Dimensões da Gold | Delta | Gold | Consulta de chave | Diária (job) | Chaves naturais | Nenhum |
-- MAGIC
-- MAGIC ## 08. Processamento técnico
-- MAGIC
-- MAGIC | Etapa | Linguagem | Célula | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | E01 | SQL | Criação da tabela | `CREATE TABLE IF NOT EXISTS` com chave primária, chaves estrangeiras informativas e liquid clustering por data e subprefeitura. |
-- MAGIC | E02 | SQL | Staging | View temporária com as chaves das dimensões (`-1` quando não encontradas), identificador, coordenadas e data de ingestão. |
-- MAGIC | E03 | SQL | Anos a recarregar | Resumo por ano (linhas e assinatura `xxhash64`) na Silver e na Gold; variáveis com os anos da Silver e os anos novos ou alterados. |
-- MAGIC | E04 | SQL | Remoção | `DELETE` dos anos a recarregar e dos anos que saíram da Silver (só se a Silver tiver dados). |
-- MAGIC | E05 | SQL | Inserção | `INSERT` das linhas dos anos a recarregar. |
-- MAGIC
-- MAGIC ## 09. Regras de negócio
-- MAGIC
-- MAGIC | ID | Regra | Lógica | Comportamento esperado |
-- MAGIC |---|---|---|---|
-- MAGIC | RN_GEO_004 | Identificador da ocorrência | `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA`. | Uma linha por ocorrência. |
-- MAGIC | RN_GEO_008 | Membro não informado | `COALESCE` da chave da dimensão com `-1`. | Nenhuma chave de dimensão nula. |
-- MAGIC | RN_GEO_009 | Atualização por ano de origem | Um ano é recarregado quando é novo na Silver ou quando a contagem ou a assinatura do conteúdo diferem da Gold. Anos ausentes da Silver são removidos, desde que a Silver não esteja vazia. | A fato espelha a Silver ano a ano, sem reprocessar anos inalterados. |
-- MAGIC
-- MAGIC ## 10. Saídas de dados
-- MAGIC
-- MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `fiap_analytics.gold.fact_geosampa_flooding_events` | Delta (liquid clustering) | Uma linha por ocorrência | `COD_FATO_OCORRENCIA` (PK); `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA` (natural) | `DELETE` e `INSERT` por ano de origem |
-- MAGIC
-- MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
-- MAGIC
-- MAGIC ## 11. Qualidade dos dados
-- MAGIC
-- MAGIC Avaliada por `04_geosampa_gold_quality` (regras DQ_GEO_GLD_001 a DQ_GEO_GLD_009): fato não vazia, conciliação com a Silver por ano, ocorrência única, integridade referencial, ponto na área de São Paulo e proporção de membros `-1`.
-- MAGIC
-- MAGIC ## 12. Tratamento de erros
-- MAGIC
-- MAGIC | Situação | Comportamento | Registro | Recuperação |
-- MAGIC |---|---|---|---|
-- MAGIC | Falha entre o `DELETE` e o `INSERT` | Os anos removidos ficam sem linhas até a próxima execução | Saída da tarefa e regra DQ_GEO_GLD_002 | Rodar o notebook de novo: os anos ausentes na Gold são recarregados |
-- MAGIC | Chave estrangeira sem tabela de referência | O `CREATE TABLE` falha | Saída da tarefa | Rodar antes `00_dim_tempo` e as duas dimensões |
-- MAGIC
-- MAGIC ## 13. Segurança e governança
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
-- MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
-- MAGIC | Credenciais | Nenhuma |
-- MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze` e `fiap_analytics.silver`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.gold` |
-- MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
-- MAGIC
-- MAGIC ## 14. Monitoramento e operação
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `gold_geosampa_flooding_events` |
-- MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
-- MAGIC | SLA | A definir |
-- MAGIC | Métricas | Anos recarregados (saída da E03) e linhas por ano |
-- MAGIC | Logs | Resultados em `geosampa_data_quality_log` (etapa `gold`) |
-- MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
-- MAGIC | Reprocessamento | Rodar o notebook de novo. Para recriar, ver reprocessamento (`docs/04_operacao/reprocessamento.md`) |
-- MAGIC
-- MAGIC ## 15. Histórico de alterações
-- MAGIC
-- MAGIC | Versão | Data | Responsável | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | 1.0 | 2026-11-09 | juniorOnData | Criação da fato de ocorrências (PR #5) |
-- MAGIC | 1.1 | 2026-11-10 | juniorOnData | Correção do `CREATE TABLE IF NOT EXISTS` (PR #7) |
-- MAGIC | 1.2 | 2026-11-16 | juniorOnData | Regra única de carga incremental por ano (PR #8) |
-- MAGIC | 1.3 | 2026-11-16 | juniorOnData | Formato de código fonte e novo nome do arquivo (PR #9) |
-- MAGIC | 1.4 | 2026-11-16 | juniorOnData | Uma linha por ocorrência, membro `-1`, coordenadas, chaves estrangeiras e liquid clustering (PR #14) |
-- MAGIC | 1.5 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

-- COMMAND ----------

-- [SQL] E01 Criação da tabela
CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_geosampa_flooding_events (
  COD_FATO_OCORRENCIA    BIGINT    GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
    COMMENT 'Chave substituta numerica da linha',
  NUM_ANO_ORIGEM         INT       NOT NULL COMMENT 'Ano do arquivo de origem (controle da carga incremental)',
  NUM_ID_OCORRENCIA      BIGINT    NOT NULL COMMENT 'Numero da ocorrencia dentro do arquivo do ano (com NUM_ANO_ORIGEM, identifica a ocorrencia)',
  COD_DIM_TEMPO          BIGINT    NOT NULL COMMENT 'Data da ocorrencia no formato yyyyMMdd (dim_tempo)',
  COD_DIM_OCORRENCIA     BIGINT    NOT NULL COMMENT 'Chave da dimensao de tipos de ocorrencia (-1 = nao informado)',
  COD_DIM_SUBPREFEITURA  BIGINT    NOT NULL COMMENT 'Chave da dimensao de subprefeituras (-1 = nao informado)',
  NUM_LATITUDE           DOUBLE    COMMENT 'Latitude da ocorrencia em graus (SIRGAS 2000)',
  NUM_LONGITUDE          DOUBLE    COMMENT 'Longitude da ocorrencia em graus (SIRGAS 2000)',
  DAT_INGESTAO_BRONZE    TIMESTAMP COMMENT 'Data e hora da ingestao na Bronze',
  DAT_CARGA_FATO         TIMESTAMP COMMENT 'Data e hora da carga desta tabela',
  CONSTRAINT pk_fact_geosampa_flooding_events PRIMARY KEY (COD_FATO_OCORRENCIA),
  CONSTRAINT fk_fact_geosampa_dim_tempo FOREIGN KEY (COD_DIM_TEMPO)
    REFERENCES fiap_analytics.gold.dim_tempo (COD_DIM_TEMPO),
  CONSTRAINT fk_fact_geosampa_dim_ocorrencia FOREIGN KEY (COD_DIM_OCORRENCIA)
    REFERENCES fiap_analytics.gold.dim_geosampa_type_flooding_events (COD_DIM_OCORRENCIA),
  CONSTRAINT fk_fact_geosampa_dim_subprefeitura FOREIGN KEY (COD_DIM_SUBPREFEITURA)
    REFERENCES fiap_analytics.gold.dim_geosampa_subprefecture_area (COD_DIM_SUBPREFEITURA)
)
CLUSTER BY (COD_DIM_TEMPO, COD_DIM_SUBPREFEITURA)
COMMENT 'Fato de ocorrencias por tipo e subprefeitura de Sao Paulo (GeoSampa), carga incremental por ano';

-- COMMAND ----------

-- [SQL] E02 Staging
CREATE OR REPLACE TEMP VIEW stg_fact_geosampa_flooding_events AS
SELECT
  f._source_year                         AS NUM_ANO_ORIGEM,
  f.nr_id_ocorrencia                     AS NUM_ID_OCORRENCIA,
  f.cod_dim_data                         AS COD_DIM_TEMPO,
  COALESCE(d.COD_DIM_OCORRENCIA, -1)     AS COD_DIM_OCORRENCIA,
  COALESCE(a.COD_DIM_SUBPREFEITURA, -1)  AS COD_DIM_SUBPREFEITURA,
  f.nr_latitude                          AS NUM_LATITUDE,
  f.nr_longitude                         AS NUM_LONGITUDE,
  f._ingested_at                         AS DAT_INGESTAO_BRONZE
FROM
  fiap_analytics.silver.geosampa_flooding_events f
  LEFT JOIN fiap_analytics.gold.dim_geosampa_type_flooding_events d ON
    f.dc_tipo_ocorrencia = d.DSC_TIPO_OCORRENCIA
  LEFT JOIN fiap_analytics.gold.dim_geosampa_subprefecture_area a ON
    f.sg_subprefeitura = a.SIG_SUBPREFEITURA;

-- COMMAND ----------

-- [SQL] E03 Anos a recarregar
-- Resumo por ano de origem (linhas e assinatura do conteúdo) na Silver e na Gold.
-- Ano nulo (arquivo sem ano no nome) vira -1 para poder ser comparado.
CREATE OR REPLACE TEMP VIEW resumo_ano_silver AS
SELECT
  COALESCE(NUM_ANO_ORIGEM, -1) AS ANO,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
                    NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM stg_fact_geosampa_flooding_events
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW resumo_ano_gold AS
SELECT
  COALESCE(NUM_ANO_ORIGEM, -1) AS ANO,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
                    NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM fiap_analytics.gold.fact_geosampa_flooding_events
GROUP BY ALL;

DECLARE OR REPLACE VARIABLE anos_silver ARRAY<INT>;
DECLARE OR REPLACE VARIABLE anos_recarga ARRAY<INT>;

SET VAR anos_silver = (SELECT collect_set(ANO) FROM resumo_ano_silver);

-- Anos novos na Silver ou com conteúdo diferente do que está na Gold (arquivo recarregado, ajuste de dimensão etc.).
SET VAR anos_recarga = (
  SELECT collect_set(s.ANO)
  FROM resumo_ano_silver s
    LEFT JOIN resumo_ano_gold g ON s.ANO = g.ANO
  WHERE g.ANO IS NULL OR s.LINHAS <> g.LINHAS OR s.ASSINATURA <> g.ASSINATURA
);

SELECT anos_silver, anos_recarga;

-- COMMAND ----------

-- [SQL] E04 Remoção
-- Tira da Gold os anos que serão recarregados e os que não existem mais na Silver
-- (a segunda regra só vale se a Silver tiver dados, para não esvaziar a Gold por engano).
DELETE FROM fiap_analytics.gold.fact_geosampa_flooding_events
WHERE array_contains(anos_recarga, COALESCE(NUM_ANO_ORIGEM, -1))
   OR (size(anos_silver) > 0 AND NOT array_contains(anos_silver, COALESCE(NUM_ANO_ORIGEM, -1)));

-- COMMAND ----------

-- [SQL] E05 Inserção
INSERT INTO fiap_analytics.gold.fact_geosampa_flooding_events
  (NUM_ANO_ORIGEM, NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
   NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  NUM_ANO_ORIGEM, NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
  NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_geosampa_flooding_events
WHERE array_contains(anos_recarga, COALESCE(NUM_ANO_ORIGEM, -1));
