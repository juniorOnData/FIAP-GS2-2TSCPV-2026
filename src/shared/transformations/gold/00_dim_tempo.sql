-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SHR_GLD_001 00_dim_tempo
-- MAGIC
-- MAGIC ## 01. Identificação
-- MAGIC
-- MAGIC | Campo | Valor |
-- MAGIC |---|---|
-- MAGIC | Identificador | SHR_GLD_001 |
-- MAGIC | Nome | 00_dim_tempo |
-- MAGIC | Projeto | ChuvaSegura |
-- MAGIC | Domínio de negócio | Calendário (compartilhado) |
-- MAGIC | Camada | Gold |
-- MAGIC | Linguagem | SQL |
-- MAGIC | Responsável técnico | juniorOnData |
-- MAGIC | Responsável funcional | A definir |
-- MAGIC | Status | Produção |
-- MAGIC | Versão | 1.1 |
-- MAGIC | Criticidade | Alta |
-- MAGIC | Caminho no repositório | `src/shared/transformations/gold/00_dim_tempo.sql` |
-- MAGIC
-- MAGIC ## 02. Objetivo
-- MAGIC
-- MAGIC Criar e manter a dimensão de tempo diária compartilhada por todas as fatos do projeto, com atributos de calendário em português e atributos para a análise de chuva.
-- MAGIC
-- MAGIC ## 03. Contexto funcional
-- MAGIC
-- MAGIC É a única dimensão compartilhada entre as fontes. Permite que ocorrências e medições de chuva sejam comparadas pela mesma data.
-- MAGIC
-- MAGIC ## 04. Arquitetura e fluxo
-- MAGIC
-- MAGIC | Posição | Item |
-- MAGIC |---|---|
-- MAGIC | Etapa anterior | Nenhuma |
-- MAGIC | Etapa atual | Atualiza `fiap_analytics.gold.dim_tempo` |
-- MAGIC | Etapa posterior | `04_fact_geosampa_flooding_events` e as futuras fatos de CEMADEN e SIBH |
-- MAGIC
-- MAGIC ## 05. Dependências
-- MAGIC
-- MAGIC | Tipo | Item | Uso |
-- MAGIC |---|---|---|
-- MAGIC | Permissão | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze` e `fiap_analytics.silver`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.gold` | Criar e atualizar a dimensão |
-- MAGIC
-- MAGIC ## 06. Parâmetros de entrada
-- MAGIC
-- MAGIC Não aplicável. O período é calculado a partir da data atual; não há widgets.
-- MAGIC
-- MAGIC ## 07. Fontes de dados
-- MAGIC
-- MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
-- MAGIC |---|---|---|---|---|---|---|
-- MAGIC | Calendário gerado com `sequence` e `explode` | SQL | Gerado no notebook | Completa | Diária (job) | `COD_DIM_TEMPO` | Nenhum |
-- MAGIC
-- MAGIC ## 08. Processamento técnico
-- MAGIC
-- MAGIC | Etapa | Linguagem | Célula | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | E01 | SQL | Criação da tabela | `CREATE TABLE IF NOT EXISTS` com chave primária `COD_DIM_TEMPO`. |
-- MAGIC | E02 | SQL | Staging do calendário | View temporária com um dia por linha e todos os atributos. |
-- MAGIC | E03 | SQL | Atualização | `MERGE`: insere os dias novos e corrige atributos de dias existentes se a regra de cálculo mudar. |
-- MAGIC | E04 | SQL | Conferência | Conta dias, chaves distintas e dias faltando. |
-- MAGIC
-- MAGIC ## 09. Regras de negócio
-- MAGIC
-- MAGIC | ID | Regra | Lógica | Comportamento esperado |
-- MAGIC |---|---|---|---|
-- MAGIC | RN_SHR_001 | Período do calendário | De 2010-01-01 até 31/12 do ano seguinte ao atual. | Na virada do ano, o calendário avança sozinho. |
-- MAGIC | RN_SHR_002 | Estação do ano | Verão de 21/12 a 19/03, Outono de 20/03 a 20/06, Inverno de 21/06 a 22/09, Primavera de 23/09 a 20/12 (datas fixas aproximadas). | `DSC_ESTACAO_ANO` preenchida para todo dia. |
-- MAGIC | RN_SHR_003 | Período chuvoso | Meses de outubro a março. | `FLG_PERIODO_CHUVOSO` verdadeiro nesses meses. |
-- MAGIC | RN_SHR_004 | Semana e dia da semana | Padrão ISO: segunda-feira = 1, domingo = 7. | `NUM_DIA_SEMANA` e `NUM_SEMANA_ANO` no padrão ISO. |
-- MAGIC
-- MAGIC ## 10. Saídas de dados
-- MAGIC
-- MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
-- MAGIC |---|---|---|---|---|
-- MAGIC | `fiap_analytics.gold.dim_tempo` | Delta | Um dia por linha | `COD_DIM_TEMPO` (PK, `yyyyMMdd`) | `MERGE` |
-- MAGIC
-- MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
-- MAGIC
-- MAGIC ## 11. Qualidade dos dados
-- MAGIC
-- MAGIC Conferência na última célula e regras DQ_GEO_GLD_016 (chave única) e DQ_GEO_GLD_017 (nenhum dia faltando), executadas por `04_geosampa_gold_quality`.
-- MAGIC
-- MAGIC ## 12. Tratamento de erros
-- MAGIC
-- MAGIC | Situação | Comportamento | Registro | Recuperação |
-- MAGIC |---|---|---|---|
-- MAGIC | Falha no `MERGE` | A tarefa falha e a fato do GeoSampa não roda | Saída da tarefa | Rodar o notebook de novo (idempotente) |
-- MAGIC
-- MAGIC ## 13. Segurança e governança
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Classificação dos dados | Pública (dados abertos da fonte; confirmar os termos de uso) |
-- MAGIC | Dados sensíveis | Nenhum. As ocorrências têm coordenadas de logradouro, sem identificação de pessoas |
-- MAGIC | Credenciais | Nenhuma |
-- MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; SELECT em `fiap_analytics.bronze` e `fiap_analytics.silver`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.gold` |
-- MAGIC | Políticas aplicáveis | Termos de uso dos dados abertos da Prefeitura de São Paulo (confirmar) |
-- MAGIC
-- MAGIC ## 14. Monitoramento e operação
-- MAGIC
-- MAGIC | Item | Valor |
-- MAGIC |---|---|
-- MAGIC | Job e tarefa | `ChuvaSegura` (`jobs/chuvasegura_geosampa.job.yml`), tarefa `dim_tempo` |
-- MAGIC | Frequência | Diária, 09:00:30 (America/Sao_Paulo) |
-- MAGIC | SLA | A definir |
-- MAGIC | Métricas | Dias e dias faltando (conferência) |
-- MAGIC | Logs | Resultados em `geosampa_data_quality_log` (etapa `gold`) |
-- MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
-- MAGIC | Reprocessamento | Rodar o notebook de novo (idempotente) |
-- MAGIC
-- MAGIC ## 15. Histórico de alterações
-- MAGIC
-- MAGIC | Versão | Data | Responsável | Descrição |
-- MAGIC |---|---|---|---|
-- MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação da dimensão de tempo compartilhada (PR #11) |
-- MAGIC | 1.1 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

-- COMMAND ----------

-- [SQL] E01 Criação da tabela
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

-- [SQL] E02 Staging do calendário
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

-- [SQL] E03 Atualização
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

-- [SQL] E04 Conferência
-- Conferência: um dia por chave e nenhum buraco no período.
SELECT
  COUNT(*)                                            AS dias,
  COUNT(DISTINCT COD_DIM_TEMPO)                       AS chaves_distintas,
  MIN(DAT_DIA)                                        AS primeiro_dia,
  MAX(DAT_DIA)                                        AS ultimo_dia,
  datediff(MAX(DAT_DIA), MIN(DAT_DIA)) + 1 - COUNT(*) AS dias_faltando
FROM fiap_analytics.gold.dim_tempo;
