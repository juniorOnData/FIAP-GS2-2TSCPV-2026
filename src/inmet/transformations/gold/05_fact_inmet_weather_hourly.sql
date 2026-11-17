-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05_fact_inmet_weather_hourly
-- MAGIC
-- MAGIC Fato de condições meteorológicas por hora das estações automáticas do INMET no município de São Paulo. Recarrega só os meses novos ou alterados na Silver e remove os que saíram dela.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Uma linha por estação, dia e hora no fuso de São Paulo (`COD_ESTACAO`, `COD_DIM_TEMPO`, `NUM_HORA`). A hora 9 é o intervalo de 09:00 a 09:59 (a medição do INMET às 13:00 UTC cobre 12:00 a 13:00 UTC, ou 09:00 a 10:00 em São Paulo), como nas fatos de chuva do CEMADEN e do SIBH.
-- MAGIC 2. Medidas: chuva da hora (mm), temperatura, temperatura máxima e mínima, ponto de orvalho, umidade relativa, pressão na estação, vento (velocidade, rajada e direção) e radiação global. Sem dado na fonte, a medida fica nula.
-- MAGIC 3. Estação fora do cadastro vai para o membro `-1` da dimensão.
-- MAGIC 4. Carga incremental pelo mês da medição (`NUM_ANO_MES`, fuso de São Paulo), com o mesmo controle de assinatura das outras fatos.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_inmet_weather_hourly (
  COD_ESTACAO               STRING    NOT NULL COMMENT 'Codigo da estacao no INMET',
  COD_DIM_TEMPO             BIGINT    NOT NULL COMMENT 'Dia no fuso de Sao Paulo (yyyyMMdd)',
  NUM_HORA                  INT       NOT NULL COMMENT 'Hora no fuso de Sao Paulo (0 a 23), inicio do intervalo medido',
  COD_DIM_ESTACAO           BIGINT    NOT NULL COMMENT 'Chave da dimensao de estacoes (-1 = nao informado)',
  NUM_ANO_MES               INT       NOT NULL COMMENT 'Mes no fuso de Sao Paulo (yyyyMM), controle da carga incremental',
  NUM_CHUVA_MM              DOUBLE    COMMENT 'Precipitacao total na hora, em mm',
  NUM_TEMPERATURA_C         DOUBLE    COMMENT 'Temperatura do ar (bulbo seco) no fim da hora, em graus Celsius',
  NUM_TEMPERATURA_MAX_C     DOUBLE    COMMENT 'Temperatura maxima na hora, em graus Celsius',
  NUM_TEMPERATURA_MIN_C     DOUBLE    COMMENT 'Temperatura minima na hora, em graus Celsius',
  NUM_TEMPERATURA_ORVALHO_C DOUBLE    COMMENT 'Temperatura do ponto de orvalho, em graus Celsius',
  NUM_UMIDADE_PCT           DOUBLE    COMMENT 'Umidade relativa do ar, em %',
  NUM_PRESSAO_MB            DOUBLE    COMMENT 'Pressao atmosferica ao nivel da estacao, em mb',
  NUM_VENTO_VELOCIDADE_MS   DOUBLE    COMMENT 'Velocidade do vento, em m/s',
  NUM_VENTO_RAJADA_MS       DOUBLE    COMMENT 'Rajada maxima do vento na hora, em m/s',
  NUM_VENTO_DIRECAO_GRAUS   DOUBLE    COMMENT 'Direcao do vento, em graus',
  NUM_RADIACAO_KJ_M2        DOUBLE    COMMENT 'Radiacao global na hora, em kJ/m2',
  DAT_HR_MEDICAO_UTC        TIMESTAMP COMMENT 'Horario da medicao na fonte, em UTC (fim do intervalo medido)',
  DAT_INGESTAO_BRONZE       TIMESTAMP COMMENT 'Data e hora da ingestao na Bronze',
  DAT_CARGA_FATO            TIMESTAMP COMMENT 'Data e hora da carga desta tabela',
  CONSTRAINT pk_fact_inmet_weather_hourly PRIMARY KEY (COD_ESTACAO, COD_DIM_TEMPO, NUM_HORA),
  CONSTRAINT fk_fact_inmet_dim_tempo FOREIGN KEY (COD_DIM_TEMPO)
    REFERENCES fiap_analytics.gold.dim_tempo (COD_DIM_TEMPO),
  CONSTRAINT fk_fact_inmet_dim_estacao FOREIGN KEY (COD_DIM_ESTACAO)
    REFERENCES fiap_analytics.gold.dim_inmet_stations (COD_DIM_ESTACAO)
)
CLUSTER BY (COD_DIM_TEMPO, COD_DIM_ESTACAO)
COMMENT 'Fato de condicoes meteorologicas por hora das estacoes automaticas do INMET no municipio de Sao Paulo, carga incremental por mes';

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW stg_fact_inmet_weather_hourly AS
SELECT
  m.cod_estacao                                      AS COD_ESTACAO,
  m.cod_dim_data                                     AS COD_DIM_TEMPO,
  m.nr_hora                                          AS NUM_HORA,
  COALESCE(e.COD_DIM_ESTACAO, -1)                    AS COD_DIM_ESTACAO,
  CAST(date_format(m.dt_medicao, 'yyyyMM') AS INT)   AS NUM_ANO_MES,
  m.nr_chuva_mm                                      AS NUM_CHUVA_MM,
  m.nr_temperatura_c                                 AS NUM_TEMPERATURA_C,
  m.nr_temperatura_max_c                             AS NUM_TEMPERATURA_MAX_C,
  m.nr_temperatura_min_c                             AS NUM_TEMPERATURA_MIN_C,
  m.nr_temperatura_orvalho_c                         AS NUM_TEMPERATURA_ORVALHO_C,
  m.nr_umidade_pct                                   AS NUM_UMIDADE_PCT,
  m.nr_pressao_mb                                    AS NUM_PRESSAO_MB,
  m.nr_vento_velocidade_ms                           AS NUM_VENTO_VELOCIDADE_MS,
  m.nr_vento_rajada_ms                               AS NUM_VENTO_RAJADA_MS,
  m.nr_vento_direcao_graus                           AS NUM_VENTO_DIRECAO_GRAUS,
  m.nr_radiacao_kj_m2                                AS NUM_RADIACAO_KJ_M2,
  m.dt_hr_medicao_utc                                AS DAT_HR_MEDICAO_UTC,
  m._ingested_at                                     AS DAT_INGESTAO_BRONZE
FROM fiap_analytics.silver.inmet_weather_readings m
  LEFT JOIN fiap_analytics.gold.dim_inmet_stations e ON m.cod_estacao = e.COD_ESTACAO
WHERE m.dt_hr_medicao_utc IS NOT NULL;

-- COMMAND ----------

-- Resumo por mês (linhas e assinatura do conteúdo) na Silver e na Gold.
CREATE OR REPLACE TEMP VIEW resumo_mes_silver AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_ESTACAO, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_CHUVA_MM, NUM_TEMPERATURA_C,
                    NUM_TEMPERATURA_MAX_C, NUM_TEMPERATURA_MIN_C, NUM_TEMPERATURA_ORVALHO_C, NUM_UMIDADE_PCT,
                    NUM_PRESSAO_MB, NUM_VENTO_VELOCIDADE_MS, NUM_VENTO_RAJADA_MS, NUM_VENTO_DIRECAO_GRAUS,
                    NUM_RADIACAO_KJ_M2, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM stg_fact_inmet_weather_hourly
GROUP BY ALL;

CREATE OR REPLACE TEMP VIEW resumo_mes_gold AS
SELECT
  NUM_ANO_MES AS MES,
  COUNT(*) AS LINHAS,
  SUM(CAST(xxhash64(COD_ESTACAO, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_CHUVA_MM, NUM_TEMPERATURA_C,
                    NUM_TEMPERATURA_MAX_C, NUM_TEMPERATURA_MIN_C, NUM_TEMPERATURA_ORVALHO_C, NUM_UMIDADE_PCT,
                    NUM_PRESSAO_MB, NUM_VENTO_VELOCIDADE_MS, NUM_VENTO_RAJADA_MS, NUM_VENTO_DIRECAO_GRAUS,
                    NUM_RADIACAO_KJ_M2, DAT_INGESTAO_BRONZE) AS DECIMAL(38, 0))) AS ASSINATURA
FROM fiap_analytics.gold.fact_inmet_weather_hourly
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
DELETE FROM fiap_analytics.gold.fact_inmet_weather_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES)
   OR (size(meses_silver) > 0 AND NOT array_contains(meses_silver, NUM_ANO_MES));

-- COMMAND ----------

INSERT INTO fiap_analytics.gold.fact_inmet_weather_hourly
  (COD_ESTACAO, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_ANO_MES, NUM_CHUVA_MM, NUM_TEMPERATURA_C,
   NUM_TEMPERATURA_MAX_C, NUM_TEMPERATURA_MIN_C, NUM_TEMPERATURA_ORVALHO_C, NUM_UMIDADE_PCT, NUM_PRESSAO_MB,
   NUM_VENTO_VELOCIDADE_MS, NUM_VENTO_RAJADA_MS, NUM_VENTO_DIRECAO_GRAUS, NUM_RADIACAO_KJ_M2,
   DAT_HR_MEDICAO_UTC, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO)
SELECT
  COD_ESTACAO, COD_DIM_TEMPO, NUM_HORA, COD_DIM_ESTACAO, NUM_ANO_MES, NUM_CHUVA_MM, NUM_TEMPERATURA_C,
  NUM_TEMPERATURA_MAX_C, NUM_TEMPERATURA_MIN_C, NUM_TEMPERATURA_ORVALHO_C, NUM_UMIDADE_PCT, NUM_PRESSAO_MB,
  NUM_VENTO_VELOCIDADE_MS, NUM_VENTO_RAJADA_MS, NUM_VENTO_DIRECAO_GRAUS, NUM_RADIACAO_KJ_M2,
  DAT_HR_MEDICAO_UTC, DAT_INGESTAO_BRONZE, current_timestamp()
FROM stg_fact_inmet_weather_hourly
WHERE array_contains(meses_recarga, NUM_ANO_MES);
