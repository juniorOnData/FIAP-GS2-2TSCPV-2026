-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04_fact_geosampa_flooding_events
-- MAGIC
-- MAGIC Fato de ocorrências por data, tipo e subprefeitura. Recarrega só os anos de origem novos ou alterados na Silver e remove os que saíram dela.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Uma linha por ocorrência. A ocorrência é identificada por `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA` (número da ocorrência dentro do arquivo do ano).
-- MAGIC 2. Chaves de dimensão nunca nulas: tipo ou subprefeitura não encontrados vão para o membro `-1` (não informado) da dimensão.
-- MAGIC 3. Latitude e longitude da ocorrência ficam na fato, para mapas e para o cruzamento com as estações de chuva.
-- MAGIC 4. Chaves estrangeiras declaradas para a `dim_tempo` e as dimensões do GeoSampa (informativas no Unity Catalog). Tabela com liquid clustering por data e subprefeitura.

-- MAGIC
-- MAGIC ## Como a carga incremental decide o que atualizar
-- MAGIC
-- MAGIC A view temporária de staging resolve as chaves das dimensões com `COALESCE(..., -1)` e prepara a linha que será gravada na fato. As views de resumo calculam, para cada ano, a quantidade de linhas e uma assinatura com `xxhash64` sobre os atributos analíticos.
-- MAGIC
-- MAGIC Um ano é recarregado quando ainda não existe na Gold, quando a contagem diverge ou quando a assinatura muda. Antes da inserção, o notebook remove os anos que serão recarregados e os que foram removidos da Silver. A exclusão de todos os anos não ocorre quando a Silver está vazia: essa condição protege a fato contra uma limpeza causada por uma carga sem dados.
-- MAGIC
-- MAGIC Como o identificador da ocorrência pode se repetir em arquivos de anos diferentes, a unicidade é verificada pela combinação `NUM_ANO_ORIGEM` + `NUM_ID_OCORRENCIA`.

-- COMMAND ----------

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

-- Tira da Gold os anos que serão recarregados e os que não existem mais na Silver
-- (a segunda regra só vale se a Silver tiver dados, para não esvaziar a Gold por engano).
DELETE FROM fiap_analytics.gold.fact_geosampa_flooding_events
WHERE array_contains(anos_recarga, COALESCE(NUM_ANO_ORIGEM, -1))
   OR (size(anos_silver) > 0 AND NOT array_contains(anos_silver, COALESCE(NUM_ANO_ORIGEM, -1)));

-- COMMAND ----------

INSERT INTO fiap_analytics.gold.fact_geosampa_flooding_events
  (NUM_ANO_ORIGEM, NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
   NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  NUM_ANO_ORIGEM, NUM_ID_OCORRENCIA, COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
  NUM_LATITUDE, NUM_LONGITUDE, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_geosampa_flooding_events
WHERE array_contains(anos_recarga, COALESCE(NUM_ANO_ORIGEM, -1));
