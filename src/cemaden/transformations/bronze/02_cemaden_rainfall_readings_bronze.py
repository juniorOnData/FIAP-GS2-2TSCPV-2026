# Databricks notebook source
# MAGIC %md
# MAGIC # 02_cemaden_rainfall_readings_bronze
# MAGIC
# MAGIC Carrega as leituras de chuva do volume `fiap_analytics.bronze.cemaden_rainfall_readings` para a tabela de mesmo nome.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Meses pendentes: os que tiveram landing (`SUCESSO` ou `PARCIAL`) depois da última carga na Bronze, segundo o `cemaden_ingestion_log`.
# MAGIC 2. Tudo em string. Campos que a API passar a mandar vão para `_rescued_data`.
# MAGIC 3. Idempotência por mês: a fatia `_window = 'AAAAMM'` é substituída com `replaceWhere`. Tabela particionada por `_window_year`.
# MAGIC 4. Conferência: linhas lidas precisam bater com as registradas na landing.

# COMMAND ----------

import os
import sys
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

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
