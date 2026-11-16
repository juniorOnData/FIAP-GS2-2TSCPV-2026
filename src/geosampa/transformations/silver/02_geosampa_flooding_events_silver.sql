-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02_geosampa_flooding_events_silver
-- MAGIC
-- MAGIC Recria a Silver das ocorrências do GeoSampa: data tipada, tipo de ocorrência padronizado e subprefeitura relacionada pela sigla do GeoSampa (ou, se não houver, pela sigla do Anexo IX).

-- COMMAND ----------

CREATE OR REPLACE TABLE fiap_analytics.silver.fact_geosampa_flooding_events AS
  WITH history_flooding_events AS (
    SELECT
      CAST(data AS DATE) AS dt_ocorrencia,
      CAST(DATE_FORMAT(CAST(data AS DATE), "yyyyMMdd") AS BIGINT) AS cod_dim_data,
      CASE
        WHEN ocorrencia IN ("ALAGAMENTO") THEN "Alagamento"
        WHEN ocorrencia IN ("DESLIZAMENTO") THEN "Deslizamento"
        WHEN ocorrencia IN ("INUNDACAO") THEN "Inundação"
        WHEN ocorrencia IN ("QUEDA DE ARVORE") THEN "Queda de Árvore"
        ELSE "Outros"
      END AS dc_tipo_ocorrencia,
      LEFT(COALESCE(subprefeit, subpreit), 2) AS sg_subprefeitura,
      COALESCE(subprefeit, subpreit) AS nm_subprefeitura,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      fiap_analytics.bronze.geosampa_history_flooding_events
  ),
  ajuste_subprefecture AS (
    SELECT
      dt_ocorrencia,
      cod_dim_data,
      dc_tipo_ocorrencia,
      -- Só corrige variações de grafia. A troca da sigla do GeoSampa pela do Anexo IX (ex.: FO para FB)
      -- vem da coluna sg_subprefeitura_geosampa da tabela de subprefeituras.
      CASE
        WHEN sg_subprefeitura IN ("G ", "G-", "GU") THEN "G"
        WHEN sg_subprefeitura IN ("NA") THEN NULL
        ELSE sg_subprefeitura
      END AS sg_subprefeitura_geosampa,
      nm_subprefeitura,
      _source_file,
      _source_layer,
      _source_year,
      _source_sha256,
      _source_srs_id,
      _ingested_at
    FROM
      history_flooding_events
  )
  SELECT
    f.dt_ocorrencia,
    f.cod_dim_data,
    f.dc_tipo_ocorrencia,
    COALESCE(g.sg_subprefeitura, a.sg_subprefeitura, f.sg_subprefeitura_geosampa) AS sg_subprefeitura,
    COALESCE(g.nm_subprefeitura, a.nm_subprefeitura) AS nm_subprefeitura,
    f._source_file,
    f._source_layer,
    f._source_year,
    f._source_sha256,
    f._source_srs_id,
    f._ingested_at
  FROM
    ajuste_subprefecture f
      -- Primeiro pela sigla do GeoSampa; se não achar, pela sigla do Anexo IX (arquivos que já usam a sigla oficial).
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area g
        ON f.sg_subprefeitura_geosampa = g.sg_subprefeitura_geosampa
      LEFT JOIN fiap_analytics.bronze.geosampa_subprefecture_area a
        ON g.sg_subprefeitura IS NULL AND f.sg_subprefeitura_geosampa = a.sg_subprefeitura
