-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05_fact_cemaden_rainfall_hourly
-- MAGIC
-- MAGIC Fato de chuva por hora das estações do CEMADEN. Recarrega só os meses novos ou alterados na Silver e remove os que saíram dela.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Uma linha por estação, sensor, dia e hora no fuso de São Paulo (`COD_ESTACAO`, `COD_SENSOR`, `COD_DIM_TEMPO`, `NUM_HORA`). A hora 14 reúne as leituras de 14:00 a 14:59.
-- MAGIC 2. `NUM_CHUVA_MM` é a soma das leituras da hora; `NUM_CHUVA_MAX_LEITURA_MM`, a maior leitura; `NUM_LEITURAS`, quantas leituras houve. Hora sem leitura não tem linha (não significa chuva zero).
-- MAGIC 3. Estação fora do cadastro vai para o membro `-1` da dimensão de estações.
-- MAGIC 4. Carga incremental pelo mês da leitura (`NUM_ANO_MES`, fuso de São Paulo): um mês é recarregado quando é novo ou quando a contagem ou a assinatura do conteúdo diferem da Gold.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_cemaden_rainfall_hourly (
  COD_ESTACAO               STRING    NOT NULL COMMENT 'Codigo da estacao no CEMADEN',
  COD_SENSOR                STRING    NOT NULL COMMENT 'Sensor da estacao',
  COD_DIM_TEMPO             BIGINT    NOT NULL COMMENT 'Dia da leitura no fuso de Sao Paulo (yyyyMMdd)',
  NUM_HORA                  INT       NOT NULL COMMENT 'Hora da leitura no fuso de Sao Paulo (0 a 23)',
  COD_DIM_ESTACAO           BIGINT    NOT NULL COMMENT 'Chave da dimensao de estacoes (-1 = nao informado)',
  NUM_ANO_MES               INT       NOT NULL COMMENT 'Mes da leitura no fuso de Sao Paulo (yyyyMM), controle da carga incremental',
  NUM_CHUVA_MM              DOUBLE    COMMENT 'Chuva acumulada na hora, em mm',
  NUM_CHUVA_MAX_LEITURA_MM  DOUBLE    COMMENT 'Maior leitura da hora, em mm',
  NUM_LEITURAS              BIGINT    COMMENT 'Leituras na hora',
  DAT_INGESTAO_BRONZE       TIMESTAMP COMMENT 'Carga mais recente na Bronze entre as leituras da hora',
  DAT_CARGA_FATO            TIMESTAMP COMMENT 'Data e hora da carga desta tabela',
  CONSTRAINT pk_fact_cemaden_rainfall_hourly PRIMARY KEY (COD_ESTACAO, COD_SENSOR, COD_DIM_TEMPO, NUM_HORA),
  CONSTRAINT fk_fact_cemaden_dim_tempo FOREIGN KEY (COD_DIM_TEMPO)
    REFERENCES fiap_analytics.gold.dim_tempo (COD_DIM_TEMPO),
  CONSTRAINT fk_fact_cemaden_dim_estacao FOREIGN KEY (COD_DIM_ESTACAO)
    REFERENCES fiap_analytics.gold.dim_cemaden_stations (COD_DIM_ESTACAO)
)
CLUSTER BY (COD_DIM_TEMPO, COD_DIM_ESTACAO)
COMMENT 'Fato de chuva por hora das estacoes do CEMADEN no municipio de Sao Paulo, carga incremental por mes';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_fact_cemaden_rainfall_hourly AS
SELECT
  r.cod_estacao                                       AS COD_ESTACAO,
  r.cod_sensor                                        AS COD_SENSOR,
  r.cod_dim_data                                      AS COD_DIM_TEMPO,
  r.nr_hora                                           AS NUM_HORA,
  COALESCE(e.COD_DIM_ESTACAO, -1)                     AS COD_DIM_ESTACAO,
  CAST(date_format(r.dt_leitura, 'yyyyMM') AS INT)    AS NUM_ANO_MES,
  ROUND(SUM(r.nr_chuva_mm), 3)                        AS NUM_CHUVA_MM,
  MAX(r.nr_chuva_mm)                                  AS NUM_CHUVA_MAX_LEITURA_MM,
  COUNT(*)                                            AS NUM_LEITURAS,
  MAX(r._ingested_at)                                 AS DAT_INGESTAO_BRONZE
FROM
  fiap_analytics.silver.cemaden_rainfall_readings r
  LEFT JOIN fiap_analytics.gold.dim_cemaden_stations e ON r.cod_estacao = e.COD_ESTACAO
WHERE r.dt_hr_leitura_utc IS NOT NULL
GROUP BY r.cod_estacao, r.cod_sensor, r.cod_dim_data, r.nr_hora, COALESCE(e.COD_DIM_ESTACAO, -1),
         CAST(date_format(r.dt_leitura, 'yyyyMM') AS INT);

-- COMMAND ----------

-- Resumo por mês (linhas e assinatura do conteúdo) na Silver agregada e na Gold.
CREATE OR REPLACE TEMP VIEW resumo_mes_silver AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_ESTACAO, COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO,
                    NUM_CHUVA_MM, NUM_CHUVA_MAX_LEITURA_MM, NUM_LEITURAS, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM stg_fact_cemaden_rainfall_hourly
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW resumo_mes_gold AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_ESTACAO, COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO,
                    NUM_CHUVA_MM, NUM_CHUVA_MAX_LEITURA_MM, NUM_LEITURAS, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM fiap_analytics.gold.fact_cemaden_rainfall_hourly
GROUP BY ALL;

DECLARE OR REPLACE VARIABLE meses_silver ARRAY<INT>;
DECLARE OR REPLACE VARIABLE meses_recarga ARRAY<INT>;

SET VAR meses_silver = (SELECT collect_set(MES) FROM resumo_mes_silver);

-- Meses novos na Silver ou com conteúdo diferente do que está na Gold (mês recarregado, estação nova na dimensão etc.).
SET VAR meses_recarga = (
  SELECT collect_set(s.MES)
  FROM resumo_mes_silver s
    LEFT JOIN resumo_mes_gold g ON s.MES = g.MES
  WHERE g.MES IS NULL OR s.LINHAS <> g.LINHAS OR s.ASSINATURA <> g.ASSINATURA
);

SELECT size(meses_silver) AS meses_na_silver, size(meses_recarga) AS meses_a_recarregar, array_sort(meses_recarga) AS meses_recarga;

-- COMMAND ----------

-- Tira da Gold os meses que serão recarregados e os que não existem mais na Silver
-- (a segunda regra só vale se a Silver tiver dados, para não esvaziar a Gold por engano).
DELETE FROM fiap_analytics.gold.fact_cemaden_rainfall_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES)
   OR (size(meses_silver) > 0 AND NOT array_contains(meses_silver, NUM_ANO_MES));

-- COMMAND ----------

INSERT INTO fiap_analytics.gold.fact_cemaden_rainfall_hourly
  (COD_ESTACAO, COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_ANO_MES,
   NUM_CHUVA_MM, NUM_CHUVA_MAX_LEITURA_MM, NUM_LEITURAS, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  COD_ESTACAO, COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_ANO_MES,
  NUM_CHUVA_MM, NUM_CHUVA_MAX_LEITURA_MM, NUM_LEITURAS, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_cemaden_rainfall_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES);
