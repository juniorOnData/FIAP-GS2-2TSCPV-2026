-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07_fact_sibh_rainfall_hourly
-- MAGIC
-- MAGIC Fato de chuva por hora dos sensores do SIBH. Recarrega só os meses novos ou alterados na Silver e remove os que saíram dela.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Só medições de sensores de chuva (`dc_grandeza = 'Chuva'`).
-- MAGIC 2. Uma linha por sensor, dia e hora no fuso de São Paulo (`COD_SENSOR`, `COD_DIM_TEMPO`, `NUM_HORA`). A hora 14 reúne as medições de 14:00 a 14:59.
-- MAGIC 3. `NUM_CHUVA_MM` é a soma das medições da hora; `NUM_CHUVA_MAX_MEDICAO_MM`, a maior medição; `NUM_MEDICOES`, quantas medições houve. Hora sem medição não tem linha (não significa chuva zero).
-- MAGIC 4. Sensor fora do cadastro vai para o membro `-1` da dimensão de sensores.
-- MAGIC 5. Carga incremental pelo mês da medição (`NUM_ANO_MES`, fuso de São Paulo), pela contagem e pela assinatura do conteúdo, como nas outras fatos do projeto.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_sibh_rainfall_hourly (
  COD_SENSOR                STRING    NOT NULL COMMENT 'Identificador do sensor no SIBH',
  COD_DIM_TEMPO             BIGINT    NOT NULL COMMENT 'Dia da medicao no fuso de Sao Paulo (yyyyMMdd)',
  NUM_HORA                  INT       NOT NULL COMMENT 'Hora da medicao no fuso de Sao Paulo (0 a 23)',
  COD_DIM_SENSOR            BIGINT    NOT NULL COMMENT 'Chave da dimensao de sensores (-1 = nao informado)',
  NUM_ANO_MES               INT       NOT NULL COMMENT 'Mes da medicao no fuso de Sao Paulo (yyyyMM), controle da carga incremental',
  NUM_CHUVA_MM              DOUBLE    COMMENT 'Chuva acumulada na hora, em mm',
  NUM_CHUVA_MAX_MEDICAO_MM  DOUBLE    COMMENT 'Maior medicao da hora, em mm',
  NUM_MEDICOES              BIGINT    COMMENT 'Medicoes na hora',
  DAT_INGESTAO_BRONZE       TIMESTAMP COMMENT 'Carga mais recente na Bronze entre as medicoes da hora',
  DAT_CARGA_FATO            TIMESTAMP COMMENT 'Data e hora da carga desta tabela',
  CONSTRAINT pk_fact_sibh_rainfall_hourly PRIMARY KEY (COD_SENSOR, COD_DIM_TEMPO, NUM_HORA),
  CONSTRAINT fk_fact_sibh_rainfall_dim_tempo FOREIGN KEY (COD_DIM_TEMPO)
    REFERENCES fiap_analytics.gold.dim_tempo (COD_DIM_TEMPO),
  CONSTRAINT fk_fact_sibh_rainfall_dim_sensor FOREIGN KEY (COD_DIM_SENSOR)
    REFERENCES fiap_analytics.gold.dim_sibh_sensors (COD_DIM_SENSOR)
)
CLUSTER BY (COD_DIM_TEMPO, COD_DIM_SENSOR)
COMMENT 'Fato de chuva por hora dos sensores do SIBH (SP Aguas) no municipio de Sao Paulo, carga incremental por mes';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_fact_sibh_rainfall_hourly AS
SELECT
  m.cod_sensor                                        AS COD_SENSOR,
  m.cod_dim_data                                      AS COD_DIM_TEMPO,
  m.nr_hora                                           AS NUM_HORA,
  COALESCE(s.COD_DIM_SENSOR, -1)                      AS COD_DIM_SENSOR,
  CAST(date_format(m.dt_medicao, 'yyyyMM') AS INT)    AS NUM_ANO_MES,
  ROUND(SUM(m.nr_valor), 3)                           AS NUM_CHUVA_MM,
  MAX(m.nr_valor)                                     AS NUM_CHUVA_MAX_MEDICAO_MM,
  COUNT(*)                                            AS NUM_MEDICOES,
  MAX(m._ingested_at)                                 AS DAT_INGESTAO_BRONZE
FROM
  fiap_analytics.silver.sibh_measurements m
  LEFT JOIN fiap_analytics.gold.dim_sibh_sensors s ON m.cod_sensor = s.COD_SENSOR
WHERE m.dc_grandeza = 'Chuva' AND m.dt_hr_medicao_utc IS NOT NULL
GROUP BY m.cod_sensor, m.cod_dim_data, m.nr_hora, COALESCE(s.COD_DIM_SENSOR, -1),
         CAST(date_format(m.dt_medicao, 'yyyyMM') AS INT);

-- COMMAND ----------

-- Resumo por mês (linhas e assinatura do conteúdo) na Silver agregada e na Gold.
CREATE OR REPLACE TEMP VIEW resumo_mes_silver AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_SENSOR, NUM_CHUVA_MM,
                    NUM_CHUVA_MAX_MEDICAO_MM, NUM_MEDICOES, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM stg_fact_sibh_rainfall_hourly
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW resumo_mes_gold AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_SENSOR, NUM_CHUVA_MM,
                    NUM_CHUVA_MAX_MEDICAO_MM, NUM_MEDICOES, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM fiap_analytics.gold.fact_sibh_rainfall_hourly
GROUP BY ALL;

DECLARE OR REPLACE VARIABLE meses_silver ARRAY<INT>;
DECLARE OR REPLACE VARIABLE meses_recarga ARRAY<INT>;

SET VAR meses_silver = (SELECT collect_set(MES) FROM resumo_mes_silver);

-- Meses novos na Silver ou com conteúdo diferente do que está na Gold.
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
DELETE FROM fiap_analytics.gold.fact_sibh_rainfall_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES)
   OR (size(meses_silver) > 0 AND NOT array_contains(meses_silver, NUM_ANO_MES));

-- COMMAND ----------

INSERT INTO fiap_analytics.gold.fact_sibh_rainfall_hourly
  (COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_SENSOR, NUM_ANO_MES,
   NUM_CHUVA_MM, NUM_CHUVA_MAX_MEDICAO_MM, NUM_MEDICOES, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  COD_SENSOR, COD_DIM_TEMPO, NUM_HORA, COD_DIM_SENSOR, NUM_ANO_MES,
  NUM_CHUVA_MM, NUM_CHUVA_MAX_MEDICAO_MM, NUM_MEDICOES, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_sibh_rainfall_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES);
