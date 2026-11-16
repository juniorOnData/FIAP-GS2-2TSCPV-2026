-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_dim_geosampa_type_flooding_events
-- MAGIC
-- MAGIC Dimensão de tipos de ocorrência. Só insere os tipos novos que aparecem na Silver.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.dim_geosampa_type_flooding_events (
    COD_DIM_OCORRENCIA BIGINT
      GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
      COMMENT 'Chave substituta numerica do tipo de ocorrência',
    DSC_TIPO_OCORRENCIA STRING NOT NULL COMMENT 'Descricao do tipo de ocorrência',
    DAT_CARGA_DIMENSAO TIMESTAMP COMMENT 'Data e hora em que o tipo entrou na dimensao'
  )
  COMMENT 'Dimensao de tipos de ocorrência (alagamento, deslizamento, inundacao e outros)';

-- COMMAND ----------

INSERT INTO fiap_analytics.gold.dim_geosampa_type_flooding_events (DSC_TIPO_OCORRENCIA, DAT_CARGA_DIMENSAO)
SELECT
  s.dc_tipo_ocorrencia,
  current_timestamp() AS dt_carga
FROM (
  SELECT DISTINCT dc_tipo_ocorrencia
  FROM fiap_analytics.silver.fact_geosampa_flooding_events
  WHERE dc_tipo_ocorrencia IS NOT NULL
) s
LEFT ANTI JOIN fiap_analytics.gold.dim_geosampa_type_flooding_events d
  ON s.dc_tipo_ocorrencia = d.DSC_TIPO_OCORRENCIA;

-- COMMAND ----------

SELECT * FROM fiap_analytics.gold.dim_geosampa_type_flooding_events
