# Databricks notebook source
# MAGIC %md
# MAGIC # 05_sibh_measurements_silver
# MAGIC
# MAGIC Carrega a Silver das medições de chuva e nível do SIBH (`fiap_analytics.silver.sibh_measurements`) a partir da Bronze, mês a mês.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. `date` da fonte está em UTC (texto `AAAA/MM/DD HH:mm`, conforme a Bronze). Vira `dt_hr_medicao_utc` e, no fuso de São Paulo, `dt_medicao`, `nr_hora` e `cod_dim_data` (chave da `dim_tempo`).
# MAGIC 2. `value` vira `nr_valor`: chuva em mm no intervalo da medição para sensores de chuva; nível na unidade da fonte para sensores de nível. `read_value` é mantido como `nr_valor_lido`.
# MAGIC 3. `dc_grandeza` (`Chuva` ou `Nível`) vem do tipo do sensor na própria medição ou, se faltar, do cadastro (`silver.sibh_stations`).
# MAGIC 4. Uma linha por sensor e data e hora da medição: duplicidades são removidas, mantendo a carga mais recente da Bronze.
# MAGIC 5. Carga incremental pela janela da Bronze (`_window`, AAAA-MM): um mês é reprocessado quando a Bronze foi carregada depois da Silver, com `replaceWhere`. Meses que saíram da Bronze saem da Silver.
# MAGIC
# MAGIC Widget `window`: `pending` (padrão: meses novos ou recarregados na Bronze), `all` ou um mês `AAAA-MM`.

# COMMAND ----------

dbutils.widgets.text("window", "pending", "Mês: pending, all ou AAAA-MM")

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

spark.conf.set("spark.sql.session.timeZone", "UTC")  # date da fonte está em UTC

BRONZE = "fiap_analytics.bronze.sibh_measurements"
SENSORES = "fiap_analytics.silver.sibh_stations"
SILVER = "fiap_analytics.silver.sibh_measurements"
GRANDEZAS = {"2": "Chuva", "1": "Nível"}
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

# A chave usa o texto original da data e hora: se a conversão falhar, as linhas continuam separadas
# e aparecem na etapa de qualidade, em vez de se juntarem numa só.
mais_recente = Window.partitionBy("cod_sensor", "ds_date_origem").orderBy(
    F.col("_ingested_at").desc(), F.col("cod_medicao").desc_nulls_last())

tipo_cadastro = spark.table(SENSORES).select(F.col("cod_sensor").alias("_sensor"), F.col("cod_tipo_sensor").alias("_tipo_cadastro"))


def grandeza(tipo):
    return F.when(tipo == "2", F.lit(GRANDEZAS["2"])).when(tipo == "1", F.lit(GRANDEZAS["1"]))


def transformar(mes):
    medicoes = (
        spark.table(BRONZE)
        .filter(F.col("_window") == mes)
        .select(
            F.col("station_prefix_id").alias("cod_sensor"),
            F.col("measurement_id").alias("cod_medicao"),
            F.col("station_type_id").alias("_tipo_medicao"),
            texto_para_timestamp("date").alias("dt_hr_medicao_utc"),
            texto_para_double("value").alias("nr_valor"),
            texto_para_double("read_value").alias("nr_valor_lido"),
            F.col("measurement_classification_type_id").alias("cd_classificacao"),
            F.col("date").alias("ds_date_origem"),
            F.col("_window"),
            F.col("_window_year"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )
    return (
        medicoes
        .withColumn("_ordem", F.row_number().over(mais_recente))
        .filter("_ordem = 1")
        .drop("_ordem")
        .join(F.broadcast(tipo_cadastro), F.col("cod_sensor") == F.col("_sensor"), "left")
        .withColumn("cod_tipo_sensor", F.coalesce(F.col("_tipo_medicao"), F.col("_tipo_cadastro")))
        .withColumn("dc_grandeza", grandeza(F.col("cod_tipo_sensor")))
        .withColumn("dt_medicao", data_local(F.col("dt_hr_medicao_utc")))
        .withColumn("nr_hora", hora_local(F.col("dt_hr_medicao_utc")))
        .withColumn("cod_dim_data", cod_dim_tempo(F.col("dt_medicao")))
        .select("cod_sensor", "cod_medicao", "cod_tipo_sensor", "dc_grandeza", "dt_hr_medicao_utc", "dt_medicao",
                "nr_hora", "cod_dim_data", "nr_valor", "nr_valor_lido", "cd_classificacao", "ds_date_origem",
                "_window", "_window_year", "_source_file", "_ingested_at")
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
            'Silver das medições de chuva e nível do SIBH (SP Águas) na capital: uma linha por sensor e data e hora da medição. dt_hr_medicao_utc em UTC; dt_medicao e nr_hora no fuso de São Paulo. Uma fatia por mês da Bronze (_window).'
        """)
    else:
        writer.option("replaceWhere", f"_window = '{mes}'").saveAsTable(SILVER)
    print(f"{mes}: {spark.table(SILVER).filter(F.col('_window') == mes).count()} medições")

if removidos:
    lista = ", ".join(f"'{m}'" for m in removidos)
    spark.sql(f"DELETE FROM {SILVER} WHERE _window IN ({lista})")

display(spark.sql(f"""
    SELECT _window, dc_grandeza, COUNT(*) AS medicoes, COUNT(DISTINCT cod_sensor) AS sensores,
           COUNT_IF(dt_hr_medicao_utc IS NULL) AS sem_data_hora, COUNT_IF(nr_valor IS NULL) AS sem_valor
    FROM {SILVER}
    GROUP BY ALL
    ORDER BY _window DESC, dc_grandeza
    LIMIT 48
"""))
