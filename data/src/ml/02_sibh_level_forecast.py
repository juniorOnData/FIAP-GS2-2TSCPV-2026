# Databricks notebook source
# MAGIC %md
# MAGIC # 02_sibh_level_forecast
# MAGIC
# MAGIC Prevê a variação do nível de cada sensor do SIBH em 1 e 3 horas. O alvo é a variação da cota do próprio sensor (centímetros na Gold); níveis absolutos de sensores diferentes não são comparados. A referência é a persistência (nível não muda).
# MAGIC
# MAGIC A lógica está em `src/ml/nivel.py` (testada com dados sintéticos em `data/tests/test_ml_spark.py`):
# MAGIC
# MAGIC - grade horária completa por sensor: a defasagem k é sempre k horas, mesmo depois de uma falha do sensor;
# MAGIC - chuva dos pluviômetros a até 10 km do sensor e média da cidade, sem o CEMADEN (chuva horária em revisão);
# MAGIC - perda absoluta, para picos de uma hora não dominarem o ajuste;
# MAGIC - teste nos últimos 90 dias, com um intervalo do tamanho do horizonte antes dele; métricas gerais e por sensor, com o ganho sobre a persistência;
# MAGIC - previsão só para sensores com leitura até 3 h antes da última hora da série.
# MAGIC
# MAGIC As previsões são experimentais: a interpretação hidrológica exige o datum de cada sensor e limiares oficiais por estação.

# COMMAND ----------

dbutils.widgets.text("dias_teste", "90", "Dias finais reservados para teste")

# COMMAND ----------

from datetime import datetime, timezone
from functools import reduce
import os
import sys
import uuid

from pyspark.sql import functions as F

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "ml")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "ml")):
    raise RuntimeError(f"Pasta src/ml não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.insert(0, _src)
from ml import nivel, registro  # noqa: E402
from ml.features import hourly_level, hourly_rainfall, level_sensors  # noqa: E402

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "sibh_level_forecast"
MODEL_VERSION = "0.2.0"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
DIAS_TESTE = int(dbutils.widgets.get("dias_teste"))

# COMMAND ----------

try:
    base = nivel.montar_base(hourly_level(spark), hourly_rainfall(spark), level_sensors(spark))
    base.withColumn("run_id", F.lit(RUN_ID)).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.features_sibh_level_forecast")
    base = spark.table(f"{ML}.features_sibh_level_forecast")  # lê de volta: o treino não recalcula as janelas
    r = nivel.treinar(base, DIAS_TESTE)

    metricas, previsoes = [], []
    for x in r["resultados"]:
        h = x["horizonte"]
        metricas += [registro.linha_metrica(RUN_ID, MODEL_ID, h, "test", k, v, STARTED_AT) for k, v in x["metricas"].items()]
        for s in x["segmentos"]:
            seg = f"sensor:{s['sensor_id']}"
            metricas += [registro.linha_metrica(RUN_ID, MODEL_ID, h, "test", k, s[c], STARTED_AT, seg)
                         for k, c in (("mae_delta", "mae"), ("persistence_mae_delta", "mae_referencia"), ("skill_vs_persistence", "ganho"), ("n", "n"))]
        artefato = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}/horizon={h}"
        x["modelo"].write().save(artefato)
        previsoes.append(x["previsao"])
        print(f"{h} h: MAE {x['metricas']['mae_delta']:.2f} cm | persistência {x['metricas']['persistence_mae_delta']:.2f} cm | "
              f"ganho {x['metricas']['skill_vs_persistence']:.1%}")
    registro.gravar_metricas(spark, metricas)
    (reduce(lambda a, b: a.unionByName(b), previsoes)
     .select(F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"), "horizon_hours", "sensor_id", "issued_at", "forecast_for",
             "last_observed_level", "predicted_level", "predicted_delta", F.lit("experimental_sensor_level_forecast").alias("interpretation"),
             F.current_timestamp().alias("created_at"))
     .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{ML}.predictions_sibh_level"))
    registro.gravar_execucao(
        spark, RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT, treino=(r["inicio"], r["inicio_teste"]), teste=(r["inicio_teste"], r["fim"]),
        artefato=f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}",
        detalhes=f"Horizontes {[x['horizonte'] for x in r['resultados']]} h; alvo = variação da cota no mesmo sensor (cm); teste nos últimos {DIAS_TESTE} dias; sem CEMADEN.")
except Exception as exc:
    registro.gravar_execucao(spark, RUN_ID, MODEL_ID, MODEL_VERSION, "FALHA", STARTED_AT, detalhes=f"{type(exc).__name__}: {exc}")
    raise
