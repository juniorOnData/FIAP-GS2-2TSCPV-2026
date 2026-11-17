-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05_fact_ibge_census_tract_population
-- MAGIC
-- MAGIC Fato de população e domicílios por setor censitário na data de referência do Censo 2022. É a base para estimar a população exposta: setores que cruzam áreas de suscetibilidade do SGB ou que concentram ocorrências do GeoSampa.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Uma linha por setor e data de referência (`COD_DIM_SETOR`, `COD_DIM_TEMPO`). A data é a do Censo 2022: meia-noite de 31/07 para 01/08/2022, chave `20220731` na `dim_tempo`.
-- MAGIC 2. Medidas do universo (V0001 a V0007), com a área do IBGE e a densidade em habitantes por km².
-- MAGIC 3. Setor fora da dimensão vai para o membro `-1`.
-- MAGIC 4. Carga por `MERGE`: atualiza o que mudou, insere o novo e remove, só da data do Censo 2022, os setores que saíram da Silver. Um censo futuro entra com outra data, sem apagar este.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.fact_ibge_census_tract_population (
  COD_DIM_SETOR                 BIGINT NOT NULL COMMENT 'Chave da dimensao de setores censitarios (-1 = nao informado)',
  COD_DIM_TEMPO                 BIGINT NOT NULL COMMENT 'Data de referencia do censo no formato yyyyMMdd (dim_tempo)',
  COD_SETOR                     STRING NOT NULL COMMENT 'Geocodigo do setor censitario do IBGE',
  NUM_PESSOAS                   BIGINT COMMENT 'Total de pessoas (V0001)',
  NUM_DOMICILIOS                BIGINT COMMENT 'Total de domicilios (V0002)',
  NUM_DOMICILIOS_PARTICULARES   BIGINT COMMENT 'Total de domicilios particulares (V0003)',
  NUM_DOMICILIOS_COLETIVOS      BIGINT COMMENT 'Total de domicilios coletivos (V0004)',
  NUM_DOMICILIOS_OCUPADOS       BIGINT COMMENT 'Total de domicilios particulares ocupados (V0007)',
  NUM_MEDIA_MORADORES           DOUBLE COMMENT 'Media de moradores em domicilios particulares ocupados (V0005)',
  NUM_PROP_DOMICILIOS_IMPUTADOS DOUBLE COMMENT 'Proporcao de domicilios particulares ocupados imputados, de 0 a 1 (V0006)',
  NUM_AREA_KM2                  DOUBLE COMMENT 'Area do setor em km2 (IBGE)',
  NUM_DENSIDADE_HAB_KM2         DOUBLE COMMENT 'Pessoas por km2',
  DAT_INGESTAO_BRONZE           TIMESTAMP COMMENT 'Data e hora da ingestao na Bronze',
  DAT_CARGA_FATO                TIMESTAMP COMMENT 'Data e hora da carga desta tabela',
  CONSTRAINT pk_fact_ibge_census_tract_population PRIMARY KEY (COD_SETOR, COD_DIM_TEMPO),
  CONSTRAINT fk_fact_ibge_dim_tempo FOREIGN KEY (COD_DIM_TEMPO)
    REFERENCES fiap_analytics.gold.dim_tempo (COD_DIM_TEMPO),
  CONSTRAINT fk_fact_ibge_dim_setor FOREIGN KEY (COD_DIM_SETOR)
    REFERENCES fiap_analytics.gold.dim_ibge_census_tracts (COD_DIM_SETOR)
)
CLUSTER BY (COD_DIM_TEMPO, COD_DIM_SETOR)
COMMENT 'Fato de populacao e domicilios por setor censitario do municipio de Sao Paulo (Censo 2022, IBGE)';

-- COMMAND ----------

-- Data de referência do Censo 2022 (20220731) fixa no código: é a mesma em todos os setores do arquivo.
CREATE OR REPLACE TEMP VIEW stg_fact_ibge_census_tract_population AS
SELECT
  COALESCE(d.COD_DIM_SETOR, -1)   AS COD_DIM_SETOR,
  CAST(20220731 AS BIGINT)        AS COD_DIM_TEMPO,
  s.cd_setor                      AS COD_SETOR,
  s.nr_pessoas                    AS NUM_PESSOAS,
  s.nr_domicilios                 AS NUM_DOMICILIOS,
  s.nr_domicilios_particulares    AS NUM_DOMICILIOS_PARTICULARES,
  s.nr_domicilios_coletivos       AS NUM_DOMICILIOS_COLETIVOS,
  s.nr_domicilios_ocupados        AS NUM_DOMICILIOS_OCUPADOS,
  s.nr_media_moradores            AS NUM_MEDIA_MORADORES,
  s.nr_prop_domicilios_imputados  AS NUM_PROP_DOMICILIOS_IMPUTADOS,
  s.nr_area_ibge_km2              AS NUM_AREA_KM2,
  s.nr_densidade_hab_km2          AS NUM_DENSIDADE_HAB_KM2,
  s._ingested_at                  AS DAT_INGESTAO_BRONZE
FROM fiap_analytics.silver.ibge_census_tracts s
  LEFT JOIN fiap_analytics.gold.dim_ibge_census_tracts d ON s.cd_setor = d.COD_SETOR;

-- COMMAND ----------

MERGE INTO fiap_analytics.gold.fact_ibge_census_tract_population AS t
USING stg_fact_ibge_census_tract_population AS s
  ON t.COD_SETOR = s.COD_SETOR AND t.COD_DIM_TEMPO = s.COD_DIM_TEMPO
WHEN MATCHED AND (
     NOT (t.COD_DIM_SETOR                 <=> s.COD_DIM_SETOR)
  OR NOT (t.NUM_PESSOAS                   <=> s.NUM_PESSOAS)
  OR NOT (t.NUM_DOMICILIOS                <=> s.NUM_DOMICILIOS)
  OR NOT (t.NUM_DOMICILIOS_PARTICULARES   <=> s.NUM_DOMICILIOS_PARTICULARES)
  OR NOT (t.NUM_DOMICILIOS_COLETIVOS      <=> s.NUM_DOMICILIOS_COLETIVOS)
  OR NOT (t.NUM_DOMICILIOS_OCUPADOS       <=> s.NUM_DOMICILIOS_OCUPADOS)
  OR NOT (t.NUM_MEDIA_MORADORES           <=> s.NUM_MEDIA_MORADORES)
  OR NOT (t.NUM_PROP_DOMICILIOS_IMPUTADOS <=> s.NUM_PROP_DOMICILIOS_IMPUTADOS)
  OR NOT (t.NUM_AREA_KM2                  <=> s.NUM_AREA_KM2)
  OR NOT (t.DAT_INGESTAO_BRONZE           <=> s.DAT_INGESTAO_BRONZE)
) THEN UPDATE SET
  COD_DIM_SETOR = s.COD_DIM_SETOR, NUM_PESSOAS = s.NUM_PESSOAS, NUM_DOMICILIOS = s.NUM_DOMICILIOS,
  NUM_DOMICILIOS_PARTICULARES = s.NUM_DOMICILIOS_PARTICULARES, NUM_DOMICILIOS_COLETIVOS = s.NUM_DOMICILIOS_COLETIVOS,
  NUM_DOMICILIOS_OCUPADOS = s.NUM_DOMICILIOS_OCUPADOS, NUM_MEDIA_MORADORES = s.NUM_MEDIA_MORADORES,
  NUM_PROP_DOMICILIOS_IMPUTADOS = s.NUM_PROP_DOMICILIOS_IMPUTADOS, NUM_AREA_KM2 = s.NUM_AREA_KM2,
  NUM_DENSIDADE_HAB_KM2 = s.NUM_DENSIDADE_HAB_KM2, DAT_INGESTAO_BRONZE = s.DAT_INGESTAO_BRONZE,
  DAT_CARGA_FATO = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  COD_DIM_SETOR, COD_DIM_TEMPO, COD_SETOR, NUM_PESSOAS, NUM_DOMICILIOS, NUM_DOMICILIOS_PARTICULARES,
  NUM_DOMICILIOS_COLETIVOS, NUM_DOMICILIOS_OCUPADOS, NUM_MEDIA_MORADORES, NUM_PROP_DOMICILIOS_IMPUTADOS,
  NUM_AREA_KM2, NUM_DENSIDADE_HAB_KM2, DAT_INGESTAO_BRONZE, DAT_CARGA_FATO
) VALUES (
  s.COD_DIM_SETOR, s.COD_DIM_TEMPO, s.COD_SETOR, s.NUM_PESSOAS, s.NUM_DOMICILIOS, s.NUM_DOMICILIOS_PARTICULARES,
  s.NUM_DOMICILIOS_COLETIVOS, s.NUM_DOMICILIOS_OCUPADOS, s.NUM_MEDIA_MORADORES, s.NUM_PROP_DOMICILIOS_IMPUTADOS,
  s.NUM_AREA_KM2, s.NUM_DENSIDADE_HAB_KM2, s.DAT_INGESTAO_BRONZE, current_timestamp()
)
WHEN NOT MATCHED BY SOURCE AND t.COD_DIM_TEMPO = 20220731 THEN DELETE;

-- COMMAND ----------

SELECT COD_DIM_TEMPO, COUNT(*) AS SETORES, SUM(NUM_PESSOAS) AS PESSOAS, SUM(NUM_DOMICILIOS) AS DOMICILIOS,
       COUNT_IF(COD_DIM_SETOR = -1) AS SETORES_SEM_DIMENSAO
FROM fiap_analytics.gold.fact_ibge_census_tract_population
GROUP BY ALL
