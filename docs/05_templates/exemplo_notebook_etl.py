# Databricks notebook source
# MAGIC %md
# MAGIC # EXM_SLV_001 01_exemplo_rainfall_daily_silver
# MAGIC
# MAGIC > **Exemplo fictício** do padrão de documentação. As tabelas do schema `sandbox` não existem no projeto e o notebook não faz parte de nenhum job.
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | EXM_SLV_001 |
# MAGIC | Nome | 01_exemplo_rainfall_daily_silver |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Pluviometria (exemplo) |
# MAGIC | Camada | Silver |
# MAGIC | Linguagem | Python e SQL |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Desenvolvimento |
# MAGIC | Versão | 1.0 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `docs/05_templates/exemplo_notebook_etl.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Consolidar as leituras de chuva de um dia em um acumulado diário por subprefeitura, descartando leituras inválidas, classificando a intensidade da chuva e gravando o resultado de forma idempotente na tabela `fiap_analytics.sandbox.exemplo_chuva_diaria`.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC O acumulado diário por subprefeitura é a medida usada para comparar a chuva com as ocorrências de alagamento do mesmo dia e da mesma região.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | Carga das leituras na Bronze (`fiap_analytics.sandbox.exemplo_leituras_chuva`) |
# MAGIC | Etapa atual | Consolida um dia de leituras em `fiap_analytics.sandbox.exemplo_chuva_diaria` |
# MAGIC | Etapa posterior | Fato de chuva na Gold (exemplo) |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Tabela | `fiap_analytics.sandbox.exemplo_leituras_chuva` | Leituras de chuva a cada 15 minutos |
# MAGIC | Tabela | `fiap_analytics.sandbox.exemplo_estacoes` | Subprefeitura de cada estação |
# MAGIC | Módulo | `src/common/data_quality.py` | Regras de qualidade antes da gravação |
# MAGIC | Objeto de sessão | View `parametros_execucao` (criada na E02 em Python, lida na E03 em SQL) | Data de referência da execução |
# MAGIC | Objeto de sessão | View `stg_chuva_diaria` (criada na E03 em SQL, avaliada na E04 em Python e gravada na E05 em SQL) | Resultado consolidado antes da gravação |
# MAGIC | Permissão | SELECT nas tabelas de origem; CREATE TABLE e MODIFY em `fiap_analytics.sandbox` | Ler a origem e gravar o destino |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC | Nome | Tipo | Obrigatório | Descrição | Valor padrão | Exemplo |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `data_referencia` | text (AAAA-MM-DD) | Não | Dia a consolidar; vazio usa o dia anterior no fuso de São Paulo | (vazio) | 2026-11-15 |
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | `fiap_analytics.sandbox.exemplo_leituras_chuva` | Delta | Bronze de leituras (exemplo) | Por janela diária | Diária | `cod_estacao` + `dt_hr_leitura` | Dia de `data_referencia` no fuso de São Paulo |
# MAGIC | `fiap_analytics.sandbox.exemplo_estacoes` | Delta | Cadastro de estações (exemplo) | Completa | Diária | `cod_estacao` | Estações ativas |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Parâmetros | Widget `data_referencia` e cálculo do dia anterior quando vazio |
# MAGIC | E02 | PY | Configuração | Imports, constantes e criação da view `parametros_execucao` com a data de referência |
# MAGIC | E03 | SQL | Consolidação | View `stg_chuva_diaria`: filtra leituras válidas do dia, soma por estação e calcula a média por subprefeitura e a classificação |
# MAGIC | E04 | PY | Qualidade | Regras de qualidade sobre a view; para o notebook se uma regra ERRO falhar |
# MAGIC | E05 | SQL | Gravação | Cria a tabela se não existir e grava o dia com `MERGE` |
# MAGIC | E06 | PY | Conferência | Compara as linhas gravadas com a view |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC | ID | Regra | Lógica | Comportamento esperado |
# MAGIC |---|---|---|---|
# MAGIC | RN_EXM_001 | Leitura válida | `valor_mm` preenchido, maior ou igual a 0 e menor que 200 em 15 minutos | Leituras inválidas são descartadas e contadas na qualidade |
# MAGIC | RN_EXM_002 | Acumulado da subprefeitura | Soma das leituras válidas de cada estação no dia; depois a média das estações da subprefeitura | Uma linha por subprefeitura e dia |
# MAGIC | RN_EXM_003 | Classificação da chuva | Menos de 5 mm: Fraca; de 5 a 25 mm: Moderada; de 25 a 50 mm: Forte; 50 mm ou mais: Muito forte | Uma classificação para cada linha |
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | `fiap_analytics.sandbox.exemplo_chuva_diaria` | Delta | Uma linha por subprefeitura e dia | `dt_referencia` + `sg_subprefeitura` | `MERGE` pela chave |
# MAGIC
# MAGIC As colunas estão nos comentários da tabela (E05). Num notebook real, elas também entram no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Antes da gravação (E04): resultado não vazio (ERRO), subprefeitura preenchida (ERRO), uma linha por subprefeitura (ERRO), acumulado não negativo (ERRO) e subprefeitura com menos de duas estações (AVISO, até 10%). As leituras descartadas pela RN_EXM_001 aparecem na saída da E03, como a diferença entre `leituras_lidas` e `leituras_validas`. Duplicidade é impossível na gravação, porque o `MERGE` usa a chave.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | `data_referencia` em formato inválido | Para na E01 com `ValueError` | Saída da tarefa | Corrigir o parâmetro |
# MAGIC | Nenhuma leitura no dia | A regra de tabela não vazia reprova e o notebook para antes de gravar | Tabela de resultados de qualidade | Conferir a carga da Bronze e rodar de novo |
# MAGIC | Falha na gravação | O `MERGE` é atômico: nada é gravado | Saída da tarefa | Rodar de novo com a mesma data |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública |
# MAGIC | Dados sensíveis | Nenhum |
# MAGIC | Credenciais | Nenhuma |
# MAGIC | Permissões necessárias | SELECT nas tabelas de origem; CREATE TABLE e MODIFY em `fiap_analytics.sandbox` |
# MAGIC | Políticas aplicáveis | Não aplicável (dados fictícios) |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | Não aplicável (exemplo fora dos jobs) |
# MAGIC | Frequência | Diária, se fosse agendado |
# MAGIC | SLA | Não aplicável |
# MAGIC | Métricas | Subprefeituras gravadas e leituras descartadas |
# MAGIC | Logs | `fiap_analytics.sandbox.exemplo_data_quality_log` |
# MAGIC | Alertas | Não aplicável |
# MAGIC | Reprocessamento | Rodar de novo com a mesma `data_referencia` (idempotente) |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-16 | juniorOnData | Criação do exemplo do padrão de documentação (PR #15) |

# COMMAND ----------

# [PY] E01 Parâmetros
from datetime import date, datetime, timedelta, timezone

dbutils.widgets.text("data_referencia", "", "Dia a consolidar (AAAA-MM-DD, vazio = ontem)")

_param = dbutils.widgets.get("data_referencia").strip()
if _param:
    DATA_REFERENCIA = date.fromisoformat(_param)  # ValueError se o formato estiver errado
else:
    # O dia anterior é calculado no fuso de São Paulo (UTC-3), onde as leituras são interpretadas.
    DATA_REFERENCIA = (datetime.now(timezone.utc) - timedelta(hours=3)).date() - timedelta(days=1)
print(f"Data de referência: {DATA_REFERENCIA}")

# COMMAND ----------

# [PY] E02 Configuração
import os
import sys
import uuid

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "src", "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
sys.path.append(os.path.join(_src, "src"))
from common.data_quality import DataQuality, condicao, nao_nulo, nao_vazia, unico  # noqa: E402

DESTINO = "fiap_analytics.sandbox.exemplo_chuva_diaria"
DQ_TABLE = "fiap_analytics.sandbox.exemplo_data_quality_log"
BATCH_ID = str(uuid.uuid4())

# A data vai para o SQL por uma view de uma linha, declarada na seção 05 como objeto de sessão.
spark.createDataFrame([(DATA_REFERENCIA,)], "dt_referencia date").createOrReplaceTempView("parametros_execucao")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [SQL] E03 Consolidação
# MAGIC CREATE OR REPLACE TEMP VIEW stg_chuva_diaria AS
# MAGIC WITH leituras_do_dia AS (
# MAGIC   SELECT l.cod_estacao, l.valor_mm, p.dt_referencia
# MAGIC   FROM fiap_analytics.sandbox.exemplo_leituras_chuva l
# MAGIC   CROSS JOIN parametros_execucao p
# MAGIC   WHERE to_date(from_utc_timestamp(l.dt_hr_leitura, 'America/Sao_Paulo')) = p.dt_referencia
# MAGIC ),
# MAGIC acumulado_estacao AS (
# MAGIC   -- RN_EXM_001: só leituras válidas entram na soma
# MAGIC   SELECT cod_estacao, dt_referencia, SUM(valor_mm) AS nr_chuva_mm
# MAGIC   FROM leituras_do_dia
# MAGIC   WHERE valor_mm IS NOT NULL AND valor_mm >= 0 AND valor_mm < 200
# MAGIC   GROUP BY cod_estacao, dt_referencia
# MAGIC )
# MAGIC SELECT
# MAGIC   a.dt_referencia,
# MAGIC   e.sg_subprefeitura,
# MAGIC   ROUND(AVG(a.nr_chuva_mm), 1) AS nr_chuva_mm,   -- RN_EXM_002
# MAGIC   COUNT(*) AS nr_estacoes,
# MAGIC   CASE                                           -- RN_EXM_003
# MAGIC     WHEN AVG(a.nr_chuva_mm) < 5 THEN 'Fraca'
# MAGIC     WHEN AVG(a.nr_chuva_mm) < 25 THEN 'Moderada'
# MAGIC     WHEN AVG(a.nr_chuva_mm) < 50 THEN 'Forte'
# MAGIC     ELSE 'Muito forte'
# MAGIC   END AS dc_classificacao_chuva
# MAGIC FROM acumulado_estacao a
# MAGIC JOIN fiap_analytics.sandbox.exemplo_estacoes e
# MAGIC   ON a.cod_estacao = e.cod_estacao AND e.fl_ativa
# MAGIC GROUP BY a.dt_referencia, e.sg_subprefeitura;
# MAGIC
# MAGIC -- Leituras lidas e válidas no dia: a diferença é o que a RN_EXM_001 descartou.
# MAGIC SELECT
# MAGIC   COUNT(*)                                                                  AS leituras_lidas,
# MAGIC   COUNT_IF(l.valor_mm IS NOT NULL AND l.valor_mm >= 0 AND l.valor_mm < 200) AS leituras_validas,
# MAGIC   (SELECT COUNT(*) FROM stg_chuva_diaria)                                   AS subprefeituras
# MAGIC FROM fiap_analytics.sandbox.exemplo_leituras_chuva l
# MAGIC CROSS JOIN parametros_execucao p
# MAGIC WHERE to_date(from_utc_timestamp(l.dt_hr_leitura, 'America/Sao_Paulo')) = p.dt_referencia;

# COMMAND ----------

# [PY] E04 Qualidade
VIEW = "stg_chuva_diaria"
regras = [
    nao_vazia(VIEW),
    nao_nulo(VIEW, "sg_subprefeitura"),
    unico(VIEW, ["dt_referencia", "sg_subprefeitura"], nome="subprefeitura_unica_no_dia"),
    condicao("chuva_nao_negativa", VIEW, "Acumulado maior ou igual a zero", "nr_chuva_mm < 0"),
    condicao("subprefeitura_com_uma_estacao", VIEW, "Subprefeituras medidas por uma única estação",
             "nr_estacoes < 2", severidade="AVISO", limite=0.10),
]
dq = DataQuality(spark, DQ_TABLE, "exemplo", "silver", BATCH_ID)
resultados = dq.executar(regras)
display(dq.resultados_df(resultados))
dq.validar(resultados)  # para aqui se alguma regra ERRO falhou: nada é gravado

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [SQL] E05 Gravação
# MAGIC CREATE TABLE IF NOT EXISTS fiap_analytics.sandbox.exemplo_chuva_diaria (
# MAGIC   dt_referencia          DATE    NOT NULL COMMENT 'Dia da chuva (fuso de São Paulo)',
# MAGIC   sg_subprefeitura       STRING  NOT NULL COMMENT 'Sigla da subprefeitura',
# MAGIC   nr_chuva_mm            DOUBLE  COMMENT 'Acumulado diário médio das estações, em mm (RN_EXM_002)',
# MAGIC   nr_estacoes            BIGINT  COMMENT 'Estações com leitura válida no dia',
# MAGIC   dc_classificacao_chuva STRING  COMMENT 'Fraca, Moderada, Forte ou Muito forte (RN_EXM_003)',
# MAGIC   dt_carga               TIMESTAMP COMMENT 'Data e hora da gravação'
# MAGIC )
# MAGIC COMMENT 'Exemplo fictício: chuva diária por subprefeitura';
# MAGIC
# MAGIC MERGE INTO fiap_analytics.sandbox.exemplo_chuva_diaria t
# MAGIC USING stg_chuva_diaria s
# MAGIC   ON t.dt_referencia = s.dt_referencia AND t.sg_subprefeitura = s.sg_subprefeitura
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   nr_chuva_mm = s.nr_chuva_mm, nr_estacoes = s.nr_estacoes,
# MAGIC   dc_classificacao_chuva = s.dc_classificacao_chuva, dt_carga = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (dt_referencia, sg_subprefeitura, nr_chuva_mm, nr_estacoes, dc_classificacao_chuva, dt_carga)
# MAGIC VALUES (s.dt_referencia, s.sg_subprefeitura, s.nr_chuva_mm, s.nr_estacoes, s.dc_classificacao_chuva, current_timestamp());

# COMMAND ----------

# [PY] E06 Conferência
esperado = spark.table("stg_chuva_diaria").count()
gravado = spark.table(DESTINO).filter(f"dt_referencia = DATE'{DATA_REFERENCIA}'").count()
if gravado != esperado:
    raise RuntimeError(f"Conferência falhou para {DATA_REFERENCIA}: esperado {esperado}, gravado {gravado}")
print(f"OK: {gravado} subprefeituras gravadas para {DATA_REFERENCIA}")
