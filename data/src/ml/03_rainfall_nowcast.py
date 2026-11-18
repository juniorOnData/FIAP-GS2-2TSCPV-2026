# Databricks notebook source
# MAGIC %md
# MAGIC # 03_rainfall_nowcast
# MAGIC
# MAGIC Estima a precipitação horária em cada estação para a próxima hora, a partir de chuva recente, localização e rede de origem. Usa fatos do CEMADEN, SIBH e INMET; estações sem leitura futura na hora-alvo não entram no treino.
# MAGIC
# MAGIC O teste reserva os 90 dias mais recentes e compara o modelo com persistência. A saída é um agora-cast experimental de chuva por estação; não é previsão de alagamento nem alerta oficial. Não há assimilação de radar, satélite ou previsão meteorológica futura nesta versão.

# COMMAND ----------

from datetime import datetime, timedelta, timezone
import os
import sys
import uuid

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.insert(0, _src)
from ml.features import hourly_rainfall

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "rainfall_next_hour"
MODEL_VERSION = "0.1.0"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
HORIZON = 1

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ML} COMMENT 'Modelos experimentais do ChuvaSegura'")

rain = hourly_rainfall(spark)
if rain.limit(1).count() == 0:
    raise RuntimeError("Não há chuva horária com coordenadas nas três redes Gold.")

by_station = Window.partitionBy("station_id").orderBy("ts")
base = rain
for lag in (1, 3, 6, 12, 24):
    base = base.withColumn(f"rain_lag_{lag}", F.lag("rain_mm", lag).over(by_station))
    base = base.withColumn(f"ts_lag_{lag}", F.lag("ts", lag).over(by_station))
    base = base.withColumn(
        f"age_lag_{lag}_hours",
        (F.unix_timestamp("ts") - F.unix_timestamp(F.col(f"ts_lag_{lag}"))) / 3600.0,
    )
base = (
    base.withColumn("hours_since_last", F.col("age_lag_1_hours"))
    .withColumn("hour_of_day", F.hour("ts").cast("double"))
    .withColumn("day_of_year", F.dayofyear("ts").cast("double"))
    .withColumn("month", F.month("ts").cast("double"))
)

FEATURES_TABLE = f"{ML}.features_rainfall_nowcast"
base.withColumn("run_id", F.lit(RUN_ID)).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

latest_time = rain.agg(F.max("ts")).first()[0]
test_start = latest_time - timedelta(days=90)
future = rain.select(
    F.col("station_id").alias("future_station"), F.col("ts").alias("future_ts"), F.col("rain_mm").alias("target_rain_mm")
)
labeled = (
    base.alias("x")
    .join(
        future.alias("y"),
        (F.col("x.station_id") == F.col("y.future_station"))
        & (F.col("y.future_ts") == F.col("x.ts") + F.expr(f"INTERVAL {HORIZON} HOURS")),
        "inner",
    )
    .select("x.*", F.col("y.target_rain_mm"), F.col("y.future_ts").alias("target_ts"))
)
train = labeled.where(F.col("ts") < F.lit(test_start))
test = labeled.where(F.col("ts") >= F.lit(test_start))
if train.limit(1).count() == 0 or test.limit(1).count() == 0:
    raise RuntimeError("Não há dados suficientes para a divisão temporal do agora-cast.")

numeric = [
    "NUM_LATITUDE", "NUM_LONGITUDE", "rain_mm", "rain_lag_1", "rain_lag_3", "rain_lag_6", "rain_lag_12", "rain_lag_24",
    "age_lag_1_hours", "age_lag_3_hours", "age_lag_6_hours", "age_lag_12_hours", "age_lag_24_hours",
    "hour_of_day", "day_of_year", "month",
]
imputed = [f"imp_{c}" for c in numeric]
pipeline = Pipeline(stages=[
    StringIndexer(inputCol="station_id", outputCol="station_index", handleInvalid="keep"),
    StringIndexer(inputCol="network", outputCol="network_index", handleInvalid="keep"),
    OneHotEncoder(inputCols=["station_index", "network_index"], outputCols=["station_vec", "network_vec"], handleInvalid="keep"),
    Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
    VectorAssembler(inputCols=imputed + ["station_vec", "network_vec"], outputCol="features", handleInvalid="keep"),
    GBTRegressor(featuresCol="features", labelCol="target_rain_mm", predictionCol="prediction", maxIter=80, maxDepth=5, stepSize=0.05, seed=42),
])

test_pipeline = pipeline.fit(train)
test_result = test_pipeline.transform(test)
evaluator = RegressionEvaluator(labelCol="target_rain_mm", predictionCol="prediction")
mae = evaluator.setMetricName("mae").evaluate(test_result)
rmse = evaluator.setMetricName("rmse").evaluate(test_result)
baseline_mae = test.select(F.avg(F.abs(F.col("target_rain_mm") - F.col("rain_mm")))).first()[0] or 0.0

# A probabilidade de chuva não é inferida por um corte operacional; também é registrada a frequência molhada do teste.
wet_rate = test.select(F.avg((F.col("target_rain_mm") > 0).cast("double"))).first()[0] or 0.0
metric_rows = [
    (RUN_ID, MODEL_ID, HORIZON, "test", "mae_mm", float(mae), STARTED_AT),
    (RUN_ID, MODEL_ID, HORIZON, "test", "rmse_mm", float(rmse), STARTED_AT),
    (RUN_ID, MODEL_ID, HORIZON, "test", "persistence_mae_mm", float(baseline_mae), STARTED_AT),
    (RUN_ID, MODEL_ID, HORIZON, "test", "wet_hour_rate", float(wet_rate), STARTED_AT),
]
metric_schema = "run_id string, model_id string, horizon_hours int, split string, metric string, value double, computed_at timestamp"
spark.createDataFrame(metric_rows, metric_schema).write.mode("append").saveAsTable(f"{ML}.model_metrics")

pipeline_final = pipeline.fit(labeled)
latest = (
    base.where(F.col("hours_since_last") <= 3)
    .withColumn("_rn", F.row_number().over(Window.partitionBy("station_id").orderBy(F.desc("ts"))))
    .where(F.col("_rn") == 1).drop("_rn")
)
forecast = (
    pipeline_final.transform(latest)
    .withColumn("forecast_for", F.col("ts") + F.expr("INTERVAL 1 HOUR"))
    .withColumn("predicted_rain_mm", F.greatest(F.col("prediction"), F.lit(0.0)))
    .select(
        F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"),
        F.col("station_id"), F.col("network"), F.col("NUM_LATITUDE").alias("latitude"),
        F.col("NUM_LONGITUDE").alias("longitude"), F.col("ts").alias("issued_at"), "forecast_for",
        F.col("rain_mm").alias("last_observed_rain_mm"), "predicted_rain_mm",
        F.lit("experimental_next_hour_rainfall").alias("interpretation"), F.current_timestamp().alias("created_at"),
    )
)
forecast.write.format("delta").mode("append").saveAsTable(f"{ML}.predictions_rainfall_nowcast")

artifact_path = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}"
pipeline_final.write().save(artifact_path)
run_schema = "run_id string, model_id string, model_version string, status string, started_at timestamp, training_start timestamp, training_end timestamp, test_start timestamp, test_end timestamp, artifact_path string, details string"
run = [(RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT, None, latest_time, test_start, latest_time, artifact_path,
        "Horizonte de 1h; teste nos últimos 90 dias; alvo = chuva horária na mesma estação; baseline = persistência.")]
spark.createDataFrame(run, run_schema).write.mode("append").saveAsTable(f"{ML}.model_runs")

print(f"Agora-cast concluído. MAE={mae:.3f} mm; baseline persistência MAE={baseline_mae:.3f} mm; teste desde {test_start}.")
