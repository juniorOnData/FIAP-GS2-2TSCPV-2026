# Databricks notebook source
# MAGIC %md
# MAGIC # 05_sibh_stations_silver
# MAGIC
# MAGIC Recria a Silver dos sensores do SIBH (`fiap_analytics.silver.sibh_stations`) a partir de todos os snapshots da Bronze.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Uma linha por sensor (`cod_sensor`, o `id` do cadastro). No SIBH, chuva e nível de um mesmo local têm `id` diferentes e o mesmo `station_id` (`cod_estacao`).
# MAGIC 2. Versão do snapshot mais recente em que o sensor aparece. Sensores que saíram do cadastro continuam na Silver; `fl_no_cadastro_atual` indica se estão no último snapshot.
# MAGIC 3. Só a capital (`cod_ibge = 3550308`) e os tipos coletados pela landing de medições: `station_type_id` 2 (chuva) e 1 (nível).
# MAGIC 4. `dc_grandeza`: `Chuva` para o tipo 2 e `Nível` para o tipo 1.
# MAGIC 5. `date_last_measurement` vem como `Wed Nov 11 2026 16:00:00 GMT+0000 (...)` e vira `dt_hr_ultima_medicao` (UTC).

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
from common.conversoes import texto_para_double  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")  # datas e horas da fonte estão em UTC

BRONZE = "fiap_analytics.bronze.sibh_stations"
SILVER = "fiap_analytics.silver.sibh_stations"
COD_IBGE = "3550308"
GRANDEZAS = {"2": "Chuva", "1": "Nível"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versão mais recente de cada sensor

# COMMAND ----------

bronze = spark.table(BRONZE).filter((F.col("cod_ibge") == COD_IBGE) & F.col("station_type_id").isin(*GRANDEZAS))
ultimo_snapshot = bronze.agg(F.max("_snapshot_date")).first()[0]

mais_recente = Window.partitionBy("id").orderBy(F.col("_snapshot_date").desc(), F.col("_ingested_at").desc())
sensores = (
    bronze.filter(F.col("id").isNotNull())
    .withColumn("_ordem", F.row_number().over(mais_recente))
    .filter("_ordem = 1")
)

grandeza = F.when(F.col("station_type_id") == "2", F.lit(GRANDEZAS["2"])).when(F.col("station_type_id") == "1", F.lit(GRANDEZAS["1"]))
# 'Wed Nov 11 2026 16:00:00 GMT+0000 (...)': o Spark não lê o dia da semana, então a conversão usa o trecho 'Nov 11 2026 16:00:00'.
ultima_medicao = F.try_to_timestamp(F.substring(F.trim(F.col("date_last_measurement")), 5, 20), F.lit("MMM dd yyyy HH:mm:ss"))

silver = sensores.select(
    F.col("id").alias("cod_sensor"),
    F.col("station_id").alias("cod_estacao"),
    F.col("prefix").alias("cod_prefixo"),
    F.col("alt_prefix").alias("cod_prefixo_alternativo"),
    F.col("name").alias("nm_estacao"),
    F.col("station_type_id").alias("cod_tipo_sensor"),
    F.col("station_type").alias("dc_tipo_sensor"),
    grandeza.alias("dc_grandeza"),
    F.col("station_owner_id").alias("cod_instituicao"),
    F.col("station_owner").alias("nm_instituicao"),
    F.col("operation_status").alias("cd_situacao_operacao"),
    (F.col("operation_status") == "1").alias("fl_em_operacao"),
    F.col("transmission_status").alias("cd_situacao_transmissao"),
    ultima_medicao.alias("dt_hr_ultima_medicao"),
    texto_para_double("latitude").alias("nr_latitude"),
    texto_para_double("longitude").alias("nr_longitude"),
    F.col("city_id").alias("cod_municipio_sibh"),
    F.col("city_name").alias("nm_municipio"),
    F.col("cod_ibge"),
    F.col("ugrhi_cod").alias("sg_ugrhi"),
    F.col("ugrhi_name").alias("nm_ugrhi"),
    F.col("subugrhi").alias("nm_sub_ugrhi"),
    (F.col("_snapshot_date") == F.lit(ultimo_snapshot)).alias("fl_no_cadastro_atual"),
    F.col("_snapshot_date"),
    F.col("_ingested_at"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação

# COMMAND ----------

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER)
spark.sql(f"""
    COMMENT ON TABLE {SILVER} IS
    'Silver dos sensores de chuva e nível do SIBH (SP Águas) na capital: uma linha por sensor, versão do snapshot mais recente em que ele aparece. Datas e horas em UTC.'
""")

display(spark.sql(f"""
    SELECT nm_instituicao, dc_grandeza, fl_em_operacao, fl_no_cadastro_atual, COUNT(*) AS sensores,
           COUNT_IF(nr_latitude IS NULL OR nr_longitude IS NULL) AS sem_coordenada
    FROM {SILVER}
    GROUP BY ALL
    ORDER BY nm_instituicao, dc_grandeza
"""))
