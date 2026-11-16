-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03_dim_geosampa_subprefecture_area
-- MAGIC
-- MAGIC Dimensão de subprefeituras, atualizada por `MERGE` a partir da Bronze. A chave substituta `COD_DIM_SUBPREFEITURA` não muda entre cargas.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.dim_geosampa_subprefecture_area (
  COD_DIM_SUBPREFEITURA        BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
    COMMENT 'Chave substituta numerica da subprefeitura',
  SIG_SUBPREFEITURA            STRING NOT NULL
    COMMENT 'Sigla da unidade conforme o Anexo IX (chave natural)',
  NOM_SUBPREFEITURA            STRING
    COMMENT 'Nome da unidade conforme o Anexo IX',
  SIG_SUBPREFEITURA_GEOSAMPA    STRING
    COMMENT 'Sigla usada nas ocorrencias do GeoSampa, quando difere do Anexo IX',
  DSC_ENDERECO                 STRING
    COMMENT 'Endereco da unidade',
  COD_AGRUPAMENTO              STRING
    COMMENT 'Agrupamento do Anexo IX (I, II, III ou IV)',
  TIP_UNIDADE                  STRING
    COMMENT 'SUBPREFEITURA ou SECRETARIA',
  DAT_CARGA_DIMENSAO           TIMESTAMP
    COMMENT 'Data e hora da primeira carga do registro',
  DAT_ULT_ATUALIZACAO          TIMESTAMP
    COMMENT 'Data e hora da ultima atualizacao do registro'
)
COMMENT 'Dimensao de subprefeituras de Sao Paulo, com agrupamentos, derivada da Bronze';

-- COMMAND ----------

MERGE INTO fiap_analytics.gold.dim_geosampa_subprefecture_area AS t
USING fiap_analytics.bronze.geosampa_subprefecture_area AS s
  ON t.SIG_SUBPREFEITURA = s.sg_subprefeitura
WHEN MATCHED AND (
     NOT (t.NOM_SUBPREFEITURA          <=> s.nm_subprefeitura)
  OR NOT (t.SIG_SUBPREFEITURA_GEOSAMPA <=> s.sg_subprefeitura_geosampa)
  OR NOT (t.DSC_ENDERECO               <=> s.ds_endereco)
  OR NOT (t.COD_AGRUPAMENTO            <=> s.cd_agrupamento)
  OR NOT (t.TIP_UNIDADE                <=> s.tp_unidade)
) THEN UPDATE SET
  NOM_SUBPREFEITURA          = s.nm_subprefeitura,
  SIG_SUBPREFEITURA_GEOSAMPA = s.sg_subprefeitura_geosampa,
  DSC_ENDERECO               = s.ds_endereco,
  COD_AGRUPAMENTO            = s.cd_agrupamento,
  TIP_UNIDADE                = s.tp_unidade,
  DAT_ULT_ATUALIZACAO        = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  SIG_SUBPREFEITURA, NOM_SUBPREFEITURA, SIG_SUBPREFEITURA_GEOSAMPA,
  DSC_ENDERECO, COD_AGRUPAMENTO, TIP_UNIDADE, DAT_CARGA_DIMENSAO, DAT_ULT_ATUALIZACAO
) VALUES (
  s.sg_subprefeitura, s.nm_subprefeitura, s.sg_subprefeitura_geosampa,
  s.ds_endereco, s.cd_agrupamento, s.tp_unidade, current_timestamp(), current_timestamp()
);

-- COMMAND ----------

SELECT * FROM fiap_analytics.gold.dim_geosampa_subprefecture_area
