# Databricks notebook source
# MAGIC %md
# MAGIC # CEM_BRZ_002 02_cemaden_rainfall_readings_bronze
# MAGIC
# MAGIC ## 01. Identificação
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |---|---|
# MAGIC | Identificador | CEM_BRZ_002 |
# MAGIC | Nome | 02_cemaden_rainfall_readings_bronze |
# MAGIC | Projeto | ChuvaSegura |
# MAGIC | Domínio de negócio | Pluviometria (CEMADEN) |
# MAGIC | Camada | Bronze |
# MAGIC | Linguagem | Python |
# MAGIC | Responsável técnico | juniorOnData |
# MAGIC | Responsável funcional | A definir |
# MAGIC | Status | Homologação |
# MAGIC | Versão | 1.3 |
# MAGIC | Criticidade | Média |
# MAGIC | Caminho no repositório | `src/cemaden/transformations/bronze/02_cemaden_rainfall_readings_bronze.py` |
# MAGIC
# MAGIC ## 02. Objetivo
# MAGIC
# MAGIC Carregar na Bronze os meses de leituras de chuva baixados pela landing, substituindo a fatia de cada mês e conferindo a contagem com o log.
# MAGIC
# MAGIC ## 03. Contexto funcional
# MAGIC
# MAGIC Disponibiliza as leituras brutas em tabela para a futura Silver de chuva.
# MAGIC
# MAGIC ## 04. Arquitetura e fluxo
# MAGIC
# MAGIC | Posição | Item |
# MAGIC |---|---|
# MAGIC | Etapa anterior | `01_cemaden_rainfall_readings_landing` |
# MAGIC | Etapa atual | Grava `fiap_analytics.bronze.cemaden_rainfall_readings` |
# MAGIC | Etapa posterior | Silver do CEMADEN (a desenvolver) |
# MAGIC
# MAGIC ## 05. Dependências
# MAGIC
# MAGIC | Tipo | Item | Uso |
# MAGIC |---|---|---|
# MAGIC | Tabela | `fiap_analytics.bronze.cemaden_ingestion_log` | Meses pendentes, arquivo e contagem de cada mês |
# MAGIC | Volume | `/Volumes/fiap_analytics/bronze/cemaden_rainfall_readings` | Arquivos mensais |
# MAGIC | Módulo | `src/common/ingestion_log.py` | Registro no log |
# MAGIC | Permissão | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME no volume | Ler arquivos e gravar a tabela |
# MAGIC
# MAGIC ## 06. Parâmetros de entrada
# MAGIC
# MAGIC Não aplicável. Os meses pendentes são definidos pelo log; não há widgets.
# MAGIC
# MAGIC ## 07. Fontes de dados
# MAGIC
# MAGIC | Fonte | Formato | Origem | Tipo de leitura | Frequência | Chave | Filtros |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | Arquivos mensais registrados no `fiap_analytics.bronze.cemaden_ingestion_log` | JSON por linha (gzip) | Landing do CEMADEN | Incremental por mês | Diária | `codestacao` + `datahora` + `id_sensor` | Meses com landing `SUCESSO` ou `PARCIAL` e registros, posteriores à última carga |
# MAGIC
# MAGIC ## 08. Processamento técnico
# MAGIC
# MAGIC | Etapa | Linguagem | Célula | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | E01 | PY | Imports | Bibliotecas do notebook. |
# MAGIC | E02 | PY | Carga | Log de ingestão; seleção dos meses pendentes no log; leitura com schema em texto e `_rescued_data`; conferência da contagem; gravação com `replaceWhere` por `_window`; registro no log e resumo por mês. |
# MAGIC
# MAGIC ## 09. Regras de negócio
# MAGIC
# MAGIC Não aplicável. O notebook aplica apenas regras técnicas de carga (meses pendentes e substituição da fatia); as regras de negócio das leituras ficam na landing e na futura Silver.
# MAGIC
# MAGIC ## 10. Saídas de dados
# MAGIC
# MAGIC | Destino | Formato | Granularidade | Chave | Modo de escrita |
# MAGIC |---|---|---|---|---|
# MAGIC | `fiap_analytics.bronze.cemaden_rainfall_readings` | Delta, particionada por `_window_year` | Uma linha por leitura | `codestacao` + `datahora` + `id_sensor` | `overwrite` com `replaceWhere` por `_window` (AAAAMM) |
# MAGIC
# MAGIC As colunas de cada tabela estão no dicionário de dados (`docs/02_catalogos/dicionario_de_dados.csv`).
# MAGIC
# MAGIC ## 11. Qualidade dos dados
# MAGIC
# MAGIC Bloqueia a gravação de um mês se o arquivo tiver contagem diferente da registrada na landing. Campos novos vão para `_rescued_data` e aparecem no resumo.
# MAGIC
# MAGIC ## 12. Tratamento de erros
# MAGIC
# MAGIC | Situação | Comportamento | Registro | Recuperação |
# MAGIC |---|---|---|---|
# MAGIC | Mês com falha | Registra `FALHA`, segue para os outros meses e termina com erro | `FALHA` no `fiap_analytics.bronze.cemaden_ingestion_log` | Rodar de novo: o mês continua pendente |
# MAGIC
# MAGIC ## 13. Segurança e governança
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Classificação dos dados | Pública (dados da rede observacional do CEMADEN/MCTI; confirmar os termos de uso da PED) |
# MAGIC | Dados sensíveis | Nenhum nos dados. As credenciais da conta PED são sensíveis |
# MAGIC | Credenciais | Secret scope `cemaden`, chaves `ped_email` e `ped_password` |
# MAGIC | Permissões necessárias | USE CATALOG em `fiap_analytics`; USE SCHEMA, CREATE TABLE e MODIFY em `fiap_analytics.bronze`; READ VOLUME no volume |
# MAGIC | Políticas aplicáveis | Termos de uso da API PED do CEMADEN (confirmar); credenciais nunca impressas nem gravadas |
# MAGIC
# MAGIC ## 14. Monitoramento e operação
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Job e tarefa | `ChuvaSegura - CEMADEN` (`jobs/chuvasegura_cemaden.job.yml`), tarefa `cemaden_rainfall_readings_bronze` |
# MAGIC | Frequência | Diária, 07:00 (America/Sao_Paulo); agenda pausada até o backfill |
# MAGIC | SLA | A definir |
# MAGIC | Métricas | Linhas e estações por mês |
# MAGIC | Logs | `fiap_analytics.bronze.cemaden_ingestion_log` (etapa `bronze`) |
# MAGIC | Alertas | Não configurado (recomendação: notificação de falha no job) |
# MAGIC | Reprocessamento | Rodar de novo; um mês baixado de novo na landing volta a ficar pendente |
# MAGIC
# MAGIC ## 15. Histórico de alterações
# MAGIC
# MAGIC | Versão | Data | Responsável | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | 1.0 | 2026-11-10 | juniorOnData | Criação da Bronze de leituras de chuva (PR #6) |
# MAGIC | 1.1 | 2026-11-16 | juniorOnData | Log de ingestão próprio da origem e carga de meses parciais (PR #8) |
# MAGIC | 1.2 | 2026-11-16 | juniorOnData | Formato de código fonte, módulo de log compartilhado e novo nome do arquivo (PR #9) |
# MAGIC | 1.3 | 2026-11-16 | juniorOnData | Documentação no padrão oficial (PR #15) |

# COMMAND ----------

# [PY] E01 Imports
import os
import sys
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# [PY] E02 Carga
CATALOGO, SCHEMA = "fiap_analytics", "bronze"
BRONZE_TABLE = f"{CATALOGO}.{SCHEMA}.cemaden_rainfall_readings"
ENDPOINT = "/pcds/pcds-dados-pg"
BATCH_ID = str(uuid.uuid4())

SCHEMA_ORIGEM = T.StructType([
    T.StructField(c, T.StringType())
    for c in ["cidade", "codestacao", "datahora", "id_sensor", "latitude", "longitude",
              "nome", "offset", "qualificacao", "uf", "valor"]
])

# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.cemaden_ingestion_log"
FONTE = "cemaden_rainfall_readings"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

pendentes = spark.sql(f"""
WITH landing AS (
  SELECT janela,
         max_by(arquivo, finalizado_em)   AS arquivo,
         max_by(registros, finalizado_em) AS registros,
         max(finalizado_em)               AS landing_em
  FROM {LOG_TABLE}
  WHERE fonte = '{FONTE}' AND etapa = 'landing' AND status IN ('SUCESSO', 'PARCIAL') AND registros > 0
  GROUP BY janela
),
bronze AS (
  SELECT janela, max(finalizado_em) AS bronze_em
  FROM {LOG_TABLE}
  WHERE fonte = '{FONTE}' AND etapa = 'bronze' AND status = 'SUCESSO'
  GROUP BY janela
)
SELECT l.janela, l.arquivo, l.registros
FROM landing l LEFT JOIN bronze b USING (janela)
WHERE b.bronze_em IS NULL OR b.bronze_em < l.landing_em
ORDER BY l.janela
""").collect()

print(f"{len(pendentes)} meses pendentes:", [p.janela for p in pendentes])

falhas = []
for p in pendentes:
    iniciado_em = datetime.now(timezone.utc)
    try:
        df = (
            spark.read.schema(SCHEMA_ORIGEM)
            .option("rescuedDataColumn", "_rescued_data")  # guarda campos novos que a API passe a enviar
            .json(p.arquivo)
            .select(
                "*",
                F.col("_metadata.file_path").alias("_source_file"),
                F.lit(ENDPOINT).alias("_source_endpoint"),
                F.lit(p.janela).alias("_window"),
                F.lit(int(p.janela[:4])).alias("_window_year"),
                F.lit(int(p.janela[4:])).alias("_window_month"),
                F.lit(BATCH_ID).alias("_batch_id"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )
        lidos = df.count()
        if lidos != p.registros:
            raise RuntimeError(f"arquivo tem {lidos} linhas, a landing registrou {p.registros}")

        if not spark.catalog.tableExists(BRONZE_TABLE):
            df.write.format("delta").partitionBy("_window_year").saveAsTable(BRONZE_TABLE)
            spark.sql(f"COMMENT ON TABLE {BRONZE_TABLE} IS "
                      "'Leituras brutas da API PED do CEMADEN (pcds-dados-pg), município de São Paulo, horários em UTC. "
                      "Fonte: rede observacional do CEMADEN/MCTIC.'")
        else:
            (df.write.format("delta").mode("overwrite")
             .option("replaceWhere", f"_window = '{p.janela}'")
             .saveAsTable(BRONZE_TABLE))

        registrar(p.janela, "SUCESSO", iniciado_em, registros=lidos, arquivo=p.arquivo)
        print(f"{p.janela}: {lidos} linhas carregadas")
    except Exception as e:
        registrar(p.janela, "FALHA", iniciado_em, arquivo=p.arquivo, mensagem=str(e))
        falhas.append(p.janela)
        print(f"{p.janela}: FALHA {e}")

if falhas:
    raise RuntimeError(f"Meses com falha na Bronze: {falhas}")

display(spark.sql(f"""
SELECT _window, count(*) AS linhas, count(DISTINCT codestacao) AS estacoes,
       min(datahora) AS primeira_leitura, max(datahora) AS ultima_leitura,
       count_if(_rescued_data IS NOT NULL) AS linhas_com_campos_novos
FROM {BRONZE_TABLE}
GROUP BY _window
ORDER BY _window DESC
"""))
