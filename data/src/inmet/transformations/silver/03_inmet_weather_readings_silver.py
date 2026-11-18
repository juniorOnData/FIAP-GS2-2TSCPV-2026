# Databricks notebook source
# MAGIC %md
# MAGIC # 03_inmet_weather_readings_silver
# MAGIC
# MAGIC ## Funções de transformação
# MAGIC
# MAGIC `ultima_carga_por_ano` identifica os anos Bronze novos ou alterados. `medida` converte valores numéricos e trata o código de ausência usado pelo INMET. `transformar` padroniza as variáveis, converte o horário de fim de observação no intervalo horário correspondente e prepara a data e hora local.
# MAGIC
# MAGIC Carrega a Silver das medições horárias do INMET (`fiap_analytics.silver.inmet_weather_readings`) a partir da Bronze, ano a ano.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. `Data` (`AAAA/MM/DD` ou `AAAA-MM-DD`) e `Hora UTC` (`0000 UTC` ou `00:00`) viram `dt_hr_medicao_utc`, o horário da medição em UTC.
# MAGIC 2. As medidas horárias do INMET valem para a hora que termina no horário da medição (a precipitação das 13:00 UTC é a acumulada entre 12:00 e 13:00). Por isso `dt_hr_inicio_utc` é uma hora antes, e é dele que saem `dt_medicao`, `nr_hora` e `cod_dim_data` no fuso de São Paulo. Assim a hora 9 reúne o que aconteceu entre 09:00 e 09:59, como nas fatos do CEMADEN e do SIBH.
# MAGIC 3. Valores com vírgula decimal viram `DOUBLE`. `-9999` (falta de dado do INMET) vira nulo.
# MAGIC 4. Uma linha por estação e horário: duplicidades são removidas, mantendo a carga mais recente da Bronze.
# MAGIC 5. Carga incremental pela janela da Bronze (`_window`, ano): um ano é reprocessado quando a Bronze foi carregada depois da Silver, e a fatia é substituída com `replaceWhere`. Anos que saíram da Bronze saem da Silver.
# MAGIC
# MAGIC Widget `window`: `pending` (padrão), `all` ou um ano `AAAA`.

# COMMAND ----------

dbutils.widgets.text("window", "pending", "Ano: pending, all ou AAAA")

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
from common.conversoes import cod_dim_tempo, data_local, hora_local, texto_para_double  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")  # a hora da fonte está em UTC

BRONZE = "fiap_analytics.bronze.inmet_weather_readings"
SILVER = "fiap_analytics.silver.inmet_weather_readings"
WINDOW_ARG = dbutils.widgets.get("window").strip().lower() or "pending"

# Coluna da Bronze -> coluna da Silver.
MEDIDAS = {
    "precipitacao_total_mm": "nr_chuva_mm",
    "pressao_estacao_mb": "nr_pressao_mb",
    "pressao_max_mb": "nr_pressao_max_mb",
    "pressao_min_mb": "nr_pressao_min_mb",
    "radiacao_global_kj_m2": "nr_radiacao_kj_m2",
    "temperatura_ar_c": "nr_temperatura_c",
    "temperatura_orvalho_c": "nr_temperatura_orvalho_c",
    "temperatura_max_c": "nr_temperatura_max_c",
    "temperatura_min_c": "nr_temperatura_min_c",
    "orvalho_max_c": "nr_orvalho_max_c",
    "orvalho_min_c": "nr_orvalho_min_c",
    "umidade_max_pct": "nr_umidade_max_pct",
    "umidade_min_pct": "nr_umidade_min_pct",
    "umidade_relativa_pct": "nr_umidade_pct",
    "vento_direcao_graus": "nr_vento_direcao_graus",
    "vento_rajada_ms": "nr_vento_rajada_ms",
    "vento_velocidade_ms": "nr_vento_velocidade_ms",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anos a processar

# COMMAND ----------


def ultima_carga_por_ano(tabela):
    if not spark.catalog.tableExists(tabela):
        return {}
    return {r["_window"]: r["carga"] for r in
            spark.table(tabela).groupBy("_window").agg(F.max("_ingested_at").alias("carga")).collect()}


bronze_anos = ultima_carga_por_ano(BRONZE)
silver_anos = ultima_carga_por_ano(SILVER)

if WINDOW_ARG == "pending":
    anos = sorted(a for a, carga in bronze_anos.items() if silver_anos.get(a) != carga)
elif WINDOW_ARG == "all":
    anos = sorted(bronze_anos)
else:
    if WINDOW_ARG not in bronze_anos:
        raise RuntimeError(f"Ano {WINDOW_ARG} não está na Bronze")
    anos = [WINDOW_ARG]
removidos = sorted(set(silver_anos) - set(bronze_anos)) if bronze_anos else []
print(f"Anos a processar: {anos} | a remover da Silver: {removidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação

# COMMAND ----------


def medida(coluna):
    """Texto com vírgula decimal para DOUBLE; -9999 (falta de dado do INMET) vira nulo."""
    valor = texto_para_double(coluna)
    return F.when(valor <= -9999, None).otherwise(valor)


# Data com / ou -, hora com ou sem ':' e com ou sem ' UTC'.
data_texto = F.regexp_replace(F.trim("data"), "/", "-")
hora_texto = F.lpad(F.regexp_replace(F.col("hora_utc"), "[^0-9]", ""), 4, "0")
medicao_utc = F.try_to_timestamp(F.concat_ws(" ", data_texto, hora_texto), F.lit("yyyy-MM-dd HHmm"))

mais_recente = Window.partitionBy("cod_estacao", "ds_data_hora_origem").orderBy(F.col("_ingested_at").desc())


def transformar(ano):
    return (
        spark.table(BRONZE)
        .filter(F.col("_window") == ano)
        .select(
            F.upper(F.trim("estacao_codigo")).alias("cod_estacao"),
            medicao_utc.alias("dt_hr_medicao_utc"),
            *[medida(origem).alias(destino) for origem, destino in MEDIDAS.items()],
            F.concat_ws(" ", "data", "hora_utc").alias("ds_data_hora_origem"),
            "_window", "_source_member", "_ingested_at",
        )
        .withColumn("_ordem", F.row_number().over(mais_recente))
        .filter("_ordem = 1")
        .drop("_ordem")
        .withColumn("dt_hr_inicio_utc", F.col("dt_hr_medicao_utc") - F.expr("INTERVAL 1 HOUR"))
        .withColumn("dt_medicao", data_local(F.col("dt_hr_inicio_utc")))
        .withColumn("nr_hora", hora_local(F.col("dt_hr_inicio_utc")))
        .withColumn("cod_dim_data", cod_dim_tempo(F.col("dt_medicao")))
        .select("cod_estacao", "dt_hr_medicao_utc", "dt_hr_inicio_utc", "dt_medicao", "nr_hora", "cod_dim_data",
                *MEDIDAS.values(), "ds_data_hora_origem", "_window", "_source_member", "_ingested_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação ano a ano

# COMMAND ----------

for ano in anos:
    writer = transformar(ano).write.format("delta").mode("overwrite")
    if not spark.catalog.tableExists(SILVER):
        writer.saveAsTable(SILVER)
        spark.sql(f"""
            COMMENT ON TABLE {SILVER} IS
            'Silver das medições horárias do INMET no município de São Paulo: uma linha por estação e horário. dt_hr_medicao_utc em UTC (fim da hora medida); dt_medicao e nr_hora no fuso de São Paulo, a partir do início da hora. Uma fatia por ano da Bronze (_window).'
        """)
    else:
        writer.option("replaceWhere", f"_window = '{ano}'").saveAsTable(SILVER)
    print(f"{ano}: {spark.table(SILVER).filter(F.col('_window') == ano).count()} medições")

if removidos:
    lista = ", ".join(f"'{a}'" for a in removidos)
    spark.sql(f"DELETE FROM {SILVER} WHERE _window IN ({lista})")

display(spark.sql(f"""
    SELECT _window, cod_estacao, COUNT(*) AS medicoes, ROUND(SUM(nr_chuva_mm), 1) AS chuva_total_mm,
           ROUND(AVG(nr_temperatura_c), 1) AS temperatura_media_c,
           COUNT_IF(dt_hr_medicao_utc IS NULL) AS sem_data_hora, COUNT_IF(nr_chuva_mm IS NULL) AS sem_chuva
    FROM {SILVER}
    GROUP BY ALL
    ORDER BY _window DESC, cod_estacao
"""))
