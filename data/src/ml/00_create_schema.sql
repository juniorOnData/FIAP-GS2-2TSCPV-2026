-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00_create_schema
-- MAGIC
-- MAGIC Cria o schema de Machine Learning do ChuvaSegura e os objetos de controle compartilhados pelos notebooks experimentais.
-- MAGIC A execução exige `USE CATALOG` e `CREATE SCHEMA` no catálogo `fiap_analytics`; a criação do Volume também exige `CREATE VOLUME`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS fiap_analytics.ml
COMMENT 'Tabelas, métricas e artefatos dos modelos experimentais do ChuvaSegura';

CREATE VOLUME IF NOT EXISTS fiap_analytics.ml.model_artifacts
COMMENT 'Artefatos versionados de modelos Spark ML do ChuvaSegura';

CREATE TABLE IF NOT EXISTS fiap_analytics.ml.model_runs (
  run_id STRING NOT NULL COMMENT 'Identificador desta execução de treino e score',
  model_id STRING NOT NULL COMMENT 'Identificador estável do modelo',
  model_version STRING NOT NULL COMMENT 'Versão do código/modelo',
  status STRING NOT NULL COMMENT 'Resultado da execução: SUCESSO ou FALHA',
  started_at TIMESTAMP COMMENT 'Início do treino em UTC',
  training_start TIMESTAMP COMMENT 'Início dos dados usados no treino',
  training_end TIMESTAMP COMMENT 'Fim dos dados usados no treino',
  test_start TIMESTAMP COMMENT 'Início do período temporal de teste',
  test_end TIMESTAMP COMMENT 'Fim do período temporal de teste',
  artifact_path STRING COMMENT 'Caminho versionado no Volume model_artifacts',
  details STRING COMMENT 'Parâmetros e observações da execução'
) USING DELTA
COMMENT 'Rastreio dos treinos e previsões experimentais do ChuvaSegura';

CREATE TABLE IF NOT EXISTS fiap_analytics.ml.model_metrics (
  run_id STRING NOT NULL COMMENT 'Execução do modelo',
  model_id STRING NOT NULL COMMENT 'Identificador estável do modelo',
  horizon_hours INT COMMENT 'Horizonte previsto em horas; nulo para modelos anuais',
  split STRING NOT NULL COMMENT 'Conjunto temporal: validation ou test',
  segment STRING COMMENT 'Recorte da métrica (por exemplo, sensor:123); nulo = conjunto inteiro',
  metric STRING NOT NULL COMMENT 'Nome da métrica',
  value DOUBLE COMMENT 'Valor calculado da métrica',
  computed_at TIMESTAMP COMMENT 'Instante de cálculo em UTC'
) USING DELTA
COMMENT 'Métricas de validação temporal dos modelos experimentais';

