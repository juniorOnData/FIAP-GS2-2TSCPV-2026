# Databricks notebook source
# MAGIC %md
# MAGIC # 03_cemaden_stations_silver
# MAGIC
# MAGIC Recria a Silver do cadastro de estações do CEMADEN (`fiap_analytics.silver.cemaden_stations`) a partir de todos os snapshots da Bronze.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Uma linha por estação (`cod_estacao`), com a versão do snapshot mais recente em que ela aparece. Estações que saíram do cadastro continuam na Silver, para que as leituras antigas tenham estação; `fl_no_cadastro_atual` indica se ela está no último snapshot.
# MAGIC 2. Só estações do município de São Paulo (`codibge = 3550308`).
# MAGIC 3. Colunas tipadas: coordenadas e cotas em `DOUBLE`, datas em `DATE` e `TIMESTAMP` (UTC). Valores que não convertem ficam nulos e são apontados pela etapa de qualidade.
# MAGIC 4. `fl_ativa`: verdadeiro quando `dh_inicio_inativo` está vazio.

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
from common.conversoes import texto_para_data, texto_para_double, texto_para_timestamp  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")  # datas e horas da fonte estão em UTC

BRONZE = "fiap_analytics.bronze.cemaden_stations"
SILVER = "fiap_analytics.silver.cemaden_stations"
COD_IBGE = "3550308"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versão mais recente de cada estação

# COMMAND ----------

bronze = spark.table(BRONZE).filter(F.col("codibge") == COD_IBGE)
ultimo_snapshot = bronze.agg(F.max("_snapshot_date")).first()[0]

mais_recente = Window.partitionBy("codestacao").orderBy(F.col("_snapshot_date").desc(), F.col("_ingested_at").desc(),
                                                        F.col("id_estacao").desc())
estacoes = (
    bronze.filter(F.col("codestacao").isNotNull())
    .withColumn("_ordem", F.row_number().over(mais_recente))
    .filter("_ordem = 1")
)

silver = estacoes.select(
    F.col("codestacao").alias("cod_estacao"),
    F.col("id_estacao").alias("cod_id_estacao"),
    F.col("nome").alias("nm_estacao"),
    F.col("cidade").alias("nm_cidade"),
    F.col("codibge").alias("cod_ibge"),
    F.col("uf").alias("sg_uf"),
    texto_para_double("latitude").alias("nr_latitude"),
    texto_para_double("longitude").alias("nr_longitude"),
    texto_para_double("altitude").alias("nr_altitude"),
    F.col("id_tipoestacao").alias("cod_tipo_estacao"),
    F.col("tipoestacao_descricao").alias("dc_tipo_estacao"),
    F.col("id_rede").alias("cod_rede"),
    F.col("rede_sigla").alias("sg_rede"),
    texto_para_data("data_instalacao").alias("dt_instalacao"),
    texto_para_timestamp("dh_cadastro").alias("dt_hr_cadastro"),
    texto_para_timestamp("dh_inicio_inativo").alias("dt_hr_inicio_inativo"),
    texto_para_timestamp("dh_ultima_remessa").alias("dt_hr_ultima_remessa"),
    (F.nullif(F.trim("dh_inicio_inativo"), F.lit("")).isNull()).alias("fl_ativa"),
    (F.col("_snapshot_date") == F.lit(ultimo_snapshot)).alias("fl_no_cadastro_atual"),
    texto_para_double("cota_atencao").alias("nr_cota_atencao"),
    texto_para_double("cota_alerta").alias("nr_cota_alerta"),
    texto_para_double("cota_transbordamento").alias("nr_cota_transbordamento"),
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
    'Silver do cadastro de estações do CEMADEN, município de São Paulo: uma linha por estação, versão do snapshot mais recente em que ela aparece. Datas e horas em UTC.'
""")

display(spark.sql(f"""
    SELECT dc_tipo_estacao, sg_rede, fl_ativa, fl_no_cadastro_atual, COUNT(*) AS estacoes,
           COUNT_IF(nr_latitude IS NULL OR nr_longitude IS NULL) AS sem_coordenada
    FROM {SILVER}
    GROUP BY ALL
    ORDER BY estacoes DESC
"""))
