# Databricks notebook source
# MAGIC %md
# MAGIC # 03_cemaden_rainfall_readings_silver
# MAGIC
# MAGIC Carrega a Silver das leituras de chuva do CEMADEN (`fiap_analytics.silver.cemaden_rainfall_readings`) a partir da Bronze, mês a mês.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. `datahora` da fonte está em UTC (comentário da Bronze). Vira `dt_hr_leitura_utc` e, no fuso de São Paulo, `dt_leitura`, `nr_hora` e `cod_dim_data` (chave da `dim_tempo`).
# MAGIC 2. `valor` é a chuva, em mm, no intervalo da leitura, e vira `nr_chuva_mm`.
# MAGIC 3. Uma linha por estação, sensor e data e hora da leitura: duplicidades da fonte são removidas, mantendo a carga mais recente da Bronze.
# MAGIC 4. Carga incremental pela janela da Bronze (`_window`, AAAAMM): um mês é reprocessado quando a Bronze foi carregada depois da Silver. A fatia do mês é substituída com `replaceWhere`. Meses que saíram da Bronze saem da Silver.
# MAGIC 5. Linhas cuja data e hora ou valor não convertem ficam com nulo e são apontadas pela etapa de qualidade.
# MAGIC
# MAGIC Widget `window`: `pending` (padrão: meses novos ou recarregados na Bronze), `all` ou um mês `AAAAMM`.

# COMMAND ----------

dbutils.widgets.text("window", "pending", "Mês: pending, all ou AAAAMM")

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
from common.conversoes import cod_dim_tempo, data_local, hora_local, texto_para_double, texto_para_timestamp  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")  # datahora da fonte está em UTC

BRONZE = "fiap_analytics.bronze.cemaden_rainfall_readings"
SILVER = "fiap_analytics.silver.cemaden_rainfall_readings"
WINDOW_ARG = dbutils.widgets.get("window").strip().lower() or "pending"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meses a processar

# COMMAND ----------


def ultima_carga_por_mes(tabela):
    if not spark.catalog.tableExists(tabela):
        return {}
    return {r["_window"]: r["carga"] for r in
            spark.table(tabela).groupBy("_window").agg(F.max("_ingested_at").alias("carga")).collect()}


bronze_meses = ultima_carga_por_mes(BRONZE)
silver_meses = ultima_carga_por_mes(SILVER)

if WINDOW_ARG == "pending":
    meses = sorted(m for m, carga in bronze_meses.items() if silver_meses.get(m) != carga)
elif WINDOW_ARG == "all":
    meses = sorted(bronze_meses)
else:
    if WINDOW_ARG not in bronze_meses:
        raise RuntimeError(f"Mês {WINDOW_ARG} não está na Bronze")
    meses = [WINDOW_ARG]
removidos = sorted(set(silver_meses) - set(bronze_meses)) if bronze_meses else []

print(f"Meses a processar: {len(meses)} {meses[:3]}{' ...' if len(meses) > 3 else ''}")
print(f"Meses a remover da Silver (não existem mais na Bronze): {removidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação

# COMMAND ----------

# A chave usa o texto original da data e hora (a mesma chave de duplicidade da landing): se a conversão falhar,
# as linhas continuam separadas e aparecem na etapa de qualidade, em vez de se juntarem numa só.
mais_recente = Window.partitionBy("cod_estacao", "cod_sensor", "ds_datahora_origem").orderBy(
    F.col("_ingested_at").desc(), F.col("nr_chuva_mm").desc_nulls_last())


def transformar(mes):
    leituras = (
        spark.table(BRONZE)
        .filter(F.col("_window") == mes)
        .select(
            F.col("codestacao").alias("cod_estacao"),
            F.col("id_sensor").alias("cod_sensor"),
            texto_para_timestamp("datahora").alias("dt_hr_leitura_utc"),
            texto_para_double("valor").alias("nr_chuva_mm"),
            F.col("qualificacao").alias("cd_qualificacao"),
            F.col("datahora").alias("ds_datahora_origem"),
            F.col("_window"),
            F.col("_window_year"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )
    return (
        leituras
        .withColumn("_ordem", F.row_number().over(mais_recente))
        .filter("_ordem = 1")
        .drop("_ordem")
        .withColumn("dt_leitura", data_local(F.col("dt_hr_leitura_utc")))
        .withColumn("nr_hora", hora_local(F.col("dt_hr_leitura_utc")))
        .withColumn("cod_dim_data", cod_dim_tempo(F.col("dt_leitura")))
        .select("cod_estacao", "cod_sensor", "dt_hr_leitura_utc", "dt_leitura", "nr_hora", "cod_dim_data",
                "nr_chuva_mm", "cd_qualificacao", "ds_datahora_origem", "_window", "_window_year",
                "_source_file", "_ingested_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação mês a mês

# COMMAND ----------

for mes in meses:
    df = transformar(mes)
    writer = df.write.format("delta").mode("overwrite")
    if not spark.catalog.tableExists(SILVER):
        writer.partitionBy("_window_year").saveAsTable(SILVER)
        spark.sql(f"""
            COMMENT ON TABLE {SILVER} IS
            'Silver das leituras de chuva do CEMADEN, município de São Paulo: uma linha por estação, sensor e data e hora da leitura. dt_hr_leitura_utc em UTC; dt_leitura e nr_hora no fuso de São Paulo. Uma fatia por mês da Bronze (_window).'
        """)
    else:
        writer.option("replaceWhere", f"_window = '{mes}'").saveAsTable(SILVER)
    print(f"{mes}: {spark.table(SILVER).filter(F.col('_window') == mes).count()} leituras")

if removidos:
    lista = ", ".join(f"'{m}'" for m in removidos)
    spark.sql(f"DELETE FROM {SILVER} WHERE _window IN ({lista})")

display(spark.sql(f"""
    SELECT _window, COUNT(*) AS leituras, COUNT(DISTINCT cod_estacao) AS estacoes,
           ROUND(SUM(nr_chuva_mm), 1) AS chuva_total_mm,
           COUNT_IF(dt_hr_leitura_utc IS NULL) AS sem_data_hora, COUNT_IF(nr_chuva_mm IS NULL) AS sem_valor
    FROM {SILVER}
    GROUP BY _window
    ORDER BY _window DESC
    LIMIT 24
"""))
