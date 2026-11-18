# Databricks notebook source
# MAGIC %md
# MAGIC # 03_rainfall_nowcast
# MAGIC
# MAGIC Estima a chuva da próxima hora em cada estação, a partir da chuva observada na estação e nas vizinhas. Não usa radar, satélite nem previsão meteorológica; não é previsão de alagamento nem alerta.
# MAGIC
# MAGIC A lógica está em `src/ml/chuva.py` (testada com dados sintéticos em `data/tests/test_ml_spark.py`):
# MAGIC
# MAGIC - redes SIBH e INMET; o CEMADEN fica fora até a revisão da chuva horária na Gold (valores incompatíveis com as vizinhas);
# MAGIC - grade horária completa por estação (defasagens em horas) e chuva das estações a até 15 km, na hora e na anterior;
# MAGIC - treino limitado aos últimos `dias_treino` dias, para caber no serverless; teste nos últimos `dias_teste`;
# MAGIC - como a maioria das horas é seca, compara com persistência e com "zero", mede o erro só nas horas com chuva e o acerto de evento (POD, FAR e CSI) em 1, 5 e 10 mm;
# MAGIC - previsão só para estações com leitura até 3 h antes da última hora da série.

# COMMAND ----------

dbutils.widgets.text("dias_treino", "730", "Dias de histórico usados (treino + teste)")
dbutils.widgets.text("dias_teste", "90", "Dias finais reservados para teste")

# COMMAND ----------

from datetime import datetime, timedelta, timezone
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
from ml import chuva, registro  # noqa: E402
from ml.features import hourly_rainfall  # noqa: E402

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "rainfall_next_hour"
MODEL_VERSION = "0.2.0"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
DIAS_TREINO = int(dbutils.widgets.get("dias_treino"))
DIAS_TESTE = int(dbutils.widgets.get("dias_teste"))

# COMMAND ----------

try:
    leituras = hourly_rainfall(spark)
    ultima = leituras.agg(F.max("ts")).first()[0]
    if ultima is None:
        raise RuntimeError("Não há chuva horária com coordenadas nas redes usadas.")
    leituras = leituras.where(F.col("ts") >= F.lit(ultima - timedelta(days=DIAS_TREINO)))
    chuva.montar_base(leituras).withColumn("run_id", F.lit(RUN_ID)).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.features_rainfall_nowcast")
    r = chuva.treinar(spark.table(f"{ML}.features_rainfall_nowcast"), DIAS_TESTE)

    registro.gravar_metricas(spark, [registro.linha_metrica(RUN_ID, MODEL_ID, 1, "test", k, v, STARTED_AT) for k, v in r["metricas"].items()])
    (r["previsao"]
     .select(F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"), "station_id", "network", "latitude", "longitude",
             "issued_at", "forecast_for", "last_observed_rain_mm", "predicted_rain_mm",
             F.lit("experimental_next_hour_rainfall").alias("interpretation"), F.current_timestamp().alias("created_at"))
     .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{ML}.predictions_rainfall_nowcast"))
    artefato = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}"
    r["modelo"].write().save(artefato)
    registro.gravar_execucao(spark, RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT, treino=(r["inicio"], r["inicio_teste"]),
                             teste=(r["inicio_teste"], r["fim"]), artefato=artefato,
                             detalhes=f"Horizonte 1 h; SIBH e INMET (CEMADEN em revisão); {DIAS_TREINO} dias de histórico; teste nos últimos {DIAS_TESTE}.")
    m = r["metricas"]
    print(f"MAE {m['mae_mm']:.3f} mm | persistência {m['persistence_mae_mm']:.3f} | zero {m['zero_mae_mm']:.3f} | "
          f"horas chuvosas {m['wet_hour_rate']:.1%} | CSI 1 mm: modelo {m['model_csi_1mm']} × persistência {m['persistence_csi_1mm']}")
except Exception as exc:
    registro.gravar_execucao(spark, RUN_ID, MODEL_ID, MODEL_VERSION, "FALHA", STARTED_AT, detalhes=f"{type(exc).__name__}: {exc}")
    raise
