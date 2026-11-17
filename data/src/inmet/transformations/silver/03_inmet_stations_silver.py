# Databricks notebook source
# MAGIC %md
# MAGIC # 03_inmet_stations_silver
# MAGIC
# MAGIC Recria a Silver das estações do INMET (`fiap_analytics.silver.inmet_stations`) a partir do cabeçalho dos CSVs carregados na Bronze: uma linha por estação, com os dados do arquivo mais recente em que ela aparece.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Latitude, longitude e altitude vêm com vírgula decimal e viram `DOUBLE`. A data de fundação vem como `dd/MM/aa` (ou `AAAA-MM-DD`) e vira `DATE`.
# MAGIC 2. Uma estação que mudar de posição entre anos fica com a posição do ano mais recente; `nr_anos_com_dados` conta os anos em que ela aparece.

# COMMAND ----------

import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.conversoes import texto_para_data, texto_para_double  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")

BRONZE = "fiap_analytics.bronze.inmet_weather_readings"
SILVER = "fiap_analytics.silver.inmet_stations"

# COMMAND ----------

mais_recente = Window.partitionBy("cod_estacao").orderBy(F.col("_window").desc(), F.col("_ingested_at").desc())

estacoes = (
    spark.table(BRONZE)
    .groupBy("estacao_codigo", "estacao_nome", "estacao_uf", "estacao_regiao", "estacao_latitude",
             "estacao_longitude", "estacao_altitude", "estacao_data_fundacao", "_window")
    .agg(F.max("_ingested_at").alias("_ingested_at"))
    .select(
        F.upper(F.trim("estacao_codigo")).alias("cod_estacao"),
        F.trim("estacao_nome").alias("nm_estacao"),
        F.col("estacao_uf").alias("sg_uf"),
        F.col("estacao_regiao").alias("sg_regiao"),
        texto_para_double("estacao_latitude").alias("nr_latitude"),
        texto_para_double("estacao_longitude").alias("nr_longitude"),
        texto_para_double("estacao_altitude").alias("nr_altitude_m"),
        texto_para_data("estacao_data_fundacao", ("dd/MM/yy", "yyyy-MM-dd", "dd/MM/yyyy")).alias("dt_fundacao"),
        "_window", "_ingested_at",
    )
)
anos = estacoes.groupBy("cod_estacao").agg(F.countDistinct("_window").alias("nr_anos_com_dados"),
                                           F.max("_window").alias("nr_ultimo_ano"))

(estacoes
 .withColumn("_ordem", F.row_number().over(mais_recente))
 .filter("_ordem = 1")
 .drop("_ordem", "_window")
 .join(anos, "cod_estacao")
 .withColumn("nr_ultimo_ano", F.col("nr_ultimo_ano").cast("int"))
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER))
spark.sql(f"""
    COMMENT ON TABLE {SILVER} IS
    'Silver das estações automáticas do INMET no município de São Paulo, uma linha por estação, a partir do cabeçalho dos CSVs do BDMEP.'
""")

display(spark.table(SILVER))
