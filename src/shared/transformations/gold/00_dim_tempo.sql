-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00_dim_tempo
-- MAGIC
-- MAGIC Dimensão de tempo compartilhada por todas as fatos do projeto (GeoSampa, CEMADEN e SIBH), com granularidade diária.
-- MAGIC
-- MAGIC Regras:
-- MAGIC
-- MAGIC 1. Chave `COD_DIM_TEMPO` no formato `yyyyMMdd` (BIGINT), a mesma já usada pelas fatos. A chave não muda entre cargas.
-- MAGIC 2. Período de 2010-01-01 até 31/12 do ano seguinte ao atual. Cada execução insere os dias que faltam, então o calendário avança sozinho na virada do ano.
-- MAGIC 3. Nomes de mês e de dia da semana em português. Semana no padrão ISO (segunda = 1).
-- MAGIC 4. Atributos para a análise de chuva: estação do ano no hemisfério sul (datas fixas aproximadas) e período chuvoso de São Paulo (outubro a março).
-- MAGIC 5. Fatos com granularidade menor que o dia (por exemplo, chuva por hora) usam esta chave junto com a hora na própria fato.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS fiap_analytics.gold.dim_tempo (
  COD_DIM_TEMPO        BIGINT    NOT NULL COMMENT 'Data no formato yyyyMMdd (chave da dimensão)',
  DAT_DIA              DATE      NOT NULL COMMENT 'Data',
  NUM_ANO              INT       COMMENT 'Ano',
  NUM_SEMESTRE         INT       COMMENT 'Semestre (1 ou 2)',
  NUM_TRIMESTRE        INT       COMMENT 'Trimestre (1 a 4)',
  NUM_MES              INT       COMMENT 'Mês (1 a 12)',
  NOM_MES              STRING    COMMENT 'Nome do mês (Janeiro a Dezembro)',
  SIG_MES              STRING    COMMENT 'Sigla do mês (Jan a Dez)',
  NUM_ANO_MES          INT       COMMENT 'Ano e mês no formato yyyyMM',
  DSC_ANO_MES          STRING    COMMENT 'Ano e mês no formato yyyy-MM',
  NUM_DIA_MES          INT       COMMENT 'Dia do mês',
  NUM_DIA_ANO          INT       COMMENT 'Dia do ano (1 a 366)',
  NUM_DIA_SEMANA       INT       COMMENT 'Dia da semana ISO (1 = segunda, 7 = domingo)',
  NOM_DIA_SEMANA       STRING    COMMENT 'Nome do dia da semana',
  SIG_DIA_SEMANA       STRING    COMMENT 'Sigla do dia da semana',
  NUM_SEMANA_ANO       INT       COMMENT 'Semana do ano ISO',
  FLG_FIM_DE_SEMANA    BOOLEAN   COMMENT 'Sábado ou domingo',
  DSC_ESTACAO_ANO      STRING    COMMENT 'Estação do ano no hemisfério sul (Verão, Outono, Inverno, Primavera), por datas fixas aproximadas',
  FLG_PERIODO_CHUVOSO  BOOLEAN   COMMENT 'Período chuvoso de São Paulo (outubro a março)',
  DAT_CARGA_DIMENSAO   TIMESTAMP COMMENT 'Data e hora em que o dia entrou na dimensão',
  CONSTRAINT pk_dim_tempo PRIMARY KEY (COD_DIM_TEMPO)
)
COMMENT 'Dimensão de tempo diária compartilhada pelas fatos do projeto ChuvaSegura';

-- COMMAND ----------

-- Período: de 2010-01-01 até 31/12 do ano seguinte ao atual.
CREATE OR REPLACE TEMP VIEW stg_dim_tempo AS
WITH dias AS (
  SELECT explode(sequence(DATE'2010-01-01', make_date(year(current_date()) + 1, 12, 31), INTERVAL 1 DAY)) AS DAT_DIA
)
SELECT
  CAST(date_format(DAT_DIA, 'yyyyMMdd') AS BIGINT)                                         AS COD_DIM_TEMPO,
  DAT_DIA,
  year(DAT_DIA)                                                                             AS NUM_ANO,
  CASE WHEN month(DAT_DIA) <= 6 THEN 1 ELSE 2 END                                           AS NUM_SEMESTRE,
  quarter(DAT_DIA)                                                                          AS NUM_TRIMESTRE,
  month(DAT_DIA)                                                                            AS NUM_MES,
  element_at(array('Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto',
                   'Setembro', 'Outubro', 'Novembro', 'Dezembro'), month(DAT_DIA))          AS NOM_MES,
  element_at(array('Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'),
             month(DAT_DIA))                                                                AS SIG_MES,
  CAST(date_format(DAT_DIA, 'yyyyMM') AS INT)                                               AS NUM_ANO_MES,
  date_format(DAT_DIA, 'yyyy-MM')                                                           AS DSC_ANO_MES,
  day(DAT_DIA)                                                                              AS NUM_DIA_MES,
  dayofyear(DAT_DIA)                                                                        AS NUM_DIA_ANO,
  weekday(DAT_DIA) + 1                                                                      AS NUM_DIA_SEMANA,
  element_at(array('Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira',
                   'Sábado', 'Domingo'), weekday(DAT_DIA) + 1)                              AS NOM_DIA_SEMANA,
  element_at(array('Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'), weekday(DAT_DIA) + 1) AS SIG_DIA_SEMANA,
  weekofyear(DAT_DIA)                                                                       AS NUM_SEMANA_ANO,
  weekday(DAT_DIA) >= 5                                                                     AS FLG_FIM_DE_SEMANA,
  -- Estações por datas fixas aproximadas (o início real varia um ou dois dias de um ano para o outro).
  CASE
    WHEN month(DAT_DIA) * 100 + day(DAT_DIA) >= 1221 OR month(DAT_DIA) * 100 + day(DAT_DIA) < 320 THEN 'Verão'
    WHEN month(DAT_DIA) * 100 + day(DAT_DIA) < 621 THEN 'Outono'
    WHEN month(DAT_DIA) * 100 + day(DAT_DIA) < 923 THEN 'Inverno'
    ELSE 'Primavera'
  END                                                                                       AS DSC_ESTACAO_ANO,
  month(DAT_DIA) IN (10, 11, 12, 1, 2, 3)                                                   AS FLG_PERIODO_CHUVOSO
FROM dias;

-- COMMAND ----------

-- Insere os dias novos e corrige os atributos de um dia já existente se a regra de cálculo mudar.
MERGE INTO fiap_analytics.gold.dim_tempo AS t
USING stg_dim_tempo AS s
  ON t.COD_DIM_TEMPO = s.COD_DIM_TEMPO
WHEN MATCHED AND (
     NOT (t.NOM_MES             <=> s.NOM_MES)
  OR NOT (t.SIG_MES             <=> s.SIG_MES)
  OR NOT (t.NOM_DIA_SEMANA      <=> s.NOM_DIA_SEMANA)
  OR NOT (t.SIG_DIA_SEMANA      <=> s.SIG_DIA_SEMANA)
  OR NOT (t.DSC_ESTACAO_ANO     <=> s.DSC_ESTACAO_ANO)
  OR NOT (t.FLG_PERIODO_CHUVOSO <=> s.FLG_PERIODO_CHUVOSO)
) THEN UPDATE SET
  NOM_MES             = s.NOM_MES,
  SIG_MES             = s.SIG_MES,
  NOM_DIA_SEMANA      = s.NOM_DIA_SEMANA,
  SIG_DIA_SEMANA      = s.SIG_DIA_SEMANA,
  DSC_ESTACAO_ANO     = s.DSC_ESTACAO_ANO,
  FLG_PERIODO_CHUVOSO = s.FLG_PERIODO_CHUVOSO
WHEN NOT MATCHED THEN INSERT (
  COD_DIM_TEMPO, DAT_DIA, NUM_ANO, NUM_SEMESTRE, NUM_TRIMESTRE, NUM_MES, NOM_MES, SIG_MES,
  NUM_ANO_MES, DSC_ANO_MES, NUM_DIA_MES, NUM_DIA_ANO, NUM_DIA_SEMANA, NOM_DIA_SEMANA, SIG_DIA_SEMANA,
  NUM_SEMANA_ANO, FLG_FIM_DE_SEMANA, DSC_ESTACAO_ANO, FLG_PERIODO_CHUVOSO, DAT_CARGA_DIMENSAO
) VALUES (
  s.COD_DIM_TEMPO, s.DAT_DIA, s.NUM_ANO, s.NUM_SEMESTRE, s.NUM_TRIMESTRE, s.NUM_MES, s.NOM_MES, s.SIG_MES,
  s.NUM_ANO_MES, s.DSC_ANO_MES, s.NUM_DIA_MES, s.NUM_DIA_ANO, s.NUM_DIA_SEMANA, s.NOM_DIA_SEMANA, s.SIG_DIA_SEMANA,
  s.NUM_SEMANA_ANO, s.FLG_FIM_DE_SEMANA, s.DSC_ESTACAO_ANO, s.FLG_PERIODO_CHUVOSO, current_timestamp()
);

-- COMMAND ----------

-- Conferência: um dia por chave e nenhum buraco no período.
SELECT
  COUNT(*)                                            AS dias,
  COUNT(DISTINCT COD_DIM_TEMPO)                       AS chaves_distintas,
  MIN(DAT_DIA)                                        AS primeiro_dia,
  MAX(DAT_DIA)                                        AS ultimo_dia,
  datediff(MAX(DAT_DIA), MIN(DAT_DIA)) + 1 - COUNT(*) AS dias_faltando
FROM fiap_analytics.gold.dim_tempo;
