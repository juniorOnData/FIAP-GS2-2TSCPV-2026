-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04_fact_geosampa_flooding_events
-- MAGIC
-- MAGIC Fato de ocorrências por data, tipo e subprefeitura. Recarrega só os anos de origem novos ou alterados na Silver e remove os que saíram dela.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_geosampa_flooding_events (
  COD_FATO_OCORRENCIA    BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
    COMMENT 'Chave substituta numerica da linha',
  COD_DIM_TEMPO          BIGINT    COMMENT 'Data no formato yyyyMMdd',
  COD_DIM_OCORRENCIA     BIGINT    COMMENT 'Chave da dimensao de tipos de ocorrencia',
  COD_DIM_SUBPREFEITURA  BIGINT    COMMENT 'Chave da dimensao de subprefeituras',
  YEAR_PARTITION         STRING    COMMENT 'Ano da ocorrencia',
  MONTH_PARTITION        STRING    COMMENT 'Mes da ocorrencia',
  DAY_PARTITION          STRING    COMMENT 'Dia da ocorrencia',
  NUM_ANO_ORIGEM         INT       COMMENT 'Ano do arquivo de origem (controle da carga incremental)',
  DAT_INGESTAO_BRONZE    TIMESTAMP COMMENT 'Data e hora da ingestao na Bronze',
  DAT_CARGA_FATO         TIMESTAMP COMMENT 'Data e hora da carga desta tabela'
)
COMMENT 'Fato de ocorrencias por tipo e subprefeitura de Sao Paulo (GeoSampa), carga incremental por ano';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_fact_geosampa_flooding_events AS
SELECT
  f.cod_dim_data AS COD_DIM_TEMPO,
  d.COD_DIM_OCORRENCIA,
  a.COD_DIM_SUBPREFEITURA,
  CAST(YEAR(f.dt_ocorrencia) AS STRING)  AS YEAR_PARTITION,
  CAST(MONTH(f.dt_ocorrencia) AS STRING) AS MONTH_PARTITION,
  CAST(DAY(f.dt_ocorrencia) AS STRING)   AS DAY_PARTITION,
  f._source_year                         AS NUM_ANO_ORIGEM,
  f._ingested_at                         AS DAT_INGESTAO_BRONZE
FROM
  fiap_analytics.silver.fact_geosampa_flooding_events f
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
  SUM(CAST(xxhash64(COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM stg_fact_geosampa_flooding_events
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW resumo_ano_gold AS
SELECT
  COALESCE(NUM_ANO_ORIGEM, -1) AS ANO,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
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
  (COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
   YEAR_PARTITION, MONTH_PARTITION, DAY_PARTITION,
   NUM_ANO_ORIGEM, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  COD_DIM_TEMPO, COD_DIM_OCORRENCIA, COD_DIM_SUBPREFEITURA,
  YEAR_PARTITION, MONTH_PARTITION, DAY_PARTITION,
  NUM_ANO_ORIGEM, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_geosampa_flooding_events
WHERE array_contains(anos_recarga, COALESCE(NUM_ANO_ORIGEM, -1));

-- COMMAND ----------

BEGIN
  IF DAY(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) IN (1, 15) THEN
    OPTIMIZE fiap_analytics.gold.fact_geosampa_flooding_events
    ZORDER BY (COD_DIM_TEMPO, COD_DIM_SUBPREFEITURA);
  END IF;
END
