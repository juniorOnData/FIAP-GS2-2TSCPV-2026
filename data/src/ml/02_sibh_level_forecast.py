# Databricks notebook source
# MAGIC %md
# MAGIC # 02_sibh_level_forecast
# MAGIC
# MAGIC Prevê a variação do nível observado por sensor para 1 e 3 horas à frente, usando o histórico do próprio sensor e chuva agregada das redes da cidade. As previsões são individuais por sensor e mantêm a unidade publicada para aquele sensor; níveis absolutos de sensores diferentes não são comparados.
# MAGIC
# MAGIC O notebook testa o período mais recente como janela temporal separada e compara o modelo com persistência (prever que o nível não muda). As previsões são experimentais. A interpretação hidrológica exige conferir unidade, datum e cobertura de cada sensor e obter limiares oficiais por estação.

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
from ml.features import city_rainfall, hourly_level

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "sibh_level_forecast"
MODEL_VERSION = "0.1.0"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
HORIZONS = (1, 3)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ML} COMMENT 'Modelos experimentais do ChuvaSegura'")

levels = hourly_level(spark)
rain = city_rainfall(spark)
bounds = rain.agg(F.min("ts").alias("start"), F.max("ts").alias("end")).first()
if not bounds["start"] or not bounds["end"]:
    raise RuntimeError("Não há série horária de chuva nas três fatos Gold.")
hours = spark.range(1).select(
    F.explode(F.sequence(F.lit(bounds["start"]), F.lit(bounds["end"]), F.expr("INTERVAL 1 HOUR"))).alias("ts")
)
rain_hours = (
    hours.join(rain, "ts", "left")
    .withColumn("rain_sum_3h", F.sum("rain_mean_mm").over(Window.orderBy("ts").rowsBetween(-2, 0)))
    .withColumn("rain_sum_6h", F.sum("rain_mean_mm").over(Window.orderBy("ts").rowsBetween(-5, 0)))
    .select("ts", "rain_mean_mm", "rain_max_mm", "rain_stations_reporting", "rain_sum_3h", "rain_sum_6h")
)

by_sensor = Window.partitionBy("COD_SENSOR").orderBy("ts")
base = (
    levels
    .withColumn("level_lag_1", F.lag("level", 1).over(by_sensor))
    .withColumn("level_lag_3", F.lag("level", 3).over(by_sensor))
    .withColumn("level_lag_6", F.lag("level", 6).over(by_sensor))
    .withColumn("level_lag_12", F.lag("level", 12).over(by_sensor))
    .withColumn("level_lag_24", F.lag("level", 24).over(by_sensor))
    .withColumn("ts_lag_1", F.lag("ts", 1).over(by_sensor))
    .withColumn("hours_since_level", (F.unix_timestamp("ts") - F.unix_timestamp("ts_lag_1")) / 3600.0)
    .withColumn("level_delta_1", F.col("level") - F.col("level_lag_1"))
    .withColumn("level_delta_3", F.col("level") - F.col("level_lag_3"))
    .withColumn("level_delta_6", F.col("level") - F.col("level_lag_6"))
    .withColumn("level_delta_12", F.col("level") - F.col("level_lag_12"))
    .withColumn("level_delta_24", F.col("level") - F.col("level_lag_24"))
    .join(rain_hours, "ts", "left")
    .withColumn("hour_of_day", F.hour("ts").cast("double"))
    .withColumn("day_of_year", F.dayofyear("ts").cast("double"))
)

FEATURES_TABLE = f"{ML}.features_sibh_level_forecast"
base.withColumn("run_id", F.lit(RUN_ID)).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

max_ts = levels.agg(F.max("ts")).first()[0]
test_start = max_ts - timedelta(days=90)
future = levels.select(
    F.col("COD_SENSOR").alias("future_sensor"), F.col("ts").alias("future_ts"), F.col("level").alias("future_level")
)
numeric = [
    "level", "level_delta_1", "level_delta_3", "level_delta_6", "level_delta_12", "level_delta_24",
    "hours_since_level", "rain_mean_mm", "rain_max_mm", "rain_stations_reporting", "rain_sum_3h", "rain_sum_6h",
    "hour_of_day", "day_of_year",
]
imputed = [f"imp_{c}" for c in numeric]
assembler = VectorAssembler(inputCols=imputed + ["sensor_vec"], outputCol="features", handleInvalid="keep")
pipeline = Pipeline(stages=[
    StringIndexer(inputCol="COD_SENSOR", outputCol="sensor_index", handleInvalid="keep"),
    OneHotEncoder(inputCols=["sensor_index"], outputCols=["sensor_vec"], handleInvalid="keep"),
    Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
    assembler,
    GBTRegressor(featuresCol="features", labelCol="target_delta", predictionCol="prediction", maxIter=60, maxDepth=4, seed=42),
])
metric_schema = "run_id string, model_id string, horizon_hours int, split string, metric string, value double, computed_at timestamp"
evaluator = RegressionEvaluator(labelCol="target_delta", predictionCol="prediction")
run_rows = []
metric_rows = []
prediction_frames = []

for horizon in HORIZONS:
    labeled = (
        base.alias("x")
        .join(
            future.alias("y"),
            (F.col("x.COD_SENSOR") == F.col("y.future_sensor"))
            & (F.col("y.future_ts") == F.col("x.ts") + F.expr(f"INTERVAL {horizon} HOURS")),
            "inner",
        )
        .select("x.*", (F.col("y.future_level") - F.col("x.level")).alias("target_delta"), F.col("y.future_ts").alias("target_ts"))
    )
    train = labeled.where(F.col("ts") < F.lit(test_start)).where(F.col("target_delta").isNotNull())
    test = labeled.where(F.col("ts") >= F.lit(test_start)).where(F.col("target_delta").isNotNull())
    if train.limit(1).count() == 0 or test.limit(1).count() == 0:
        print(f"Sem dados suficientes no horizonte de {horizon}h; etapa ignorada.")
        continue

    test_pipeline = pipeline.fit(train)
    test_result = test_pipeline.transform(test)
    model_mae = evaluator.setMetricName("mae").evaluate(test_result)
    model_rmse = evaluator.setMetricName("rmse").evaluate(test_result)
    persistence_mae = test.select(F.avg(F.abs("target_delta")).alias("mae")).first()["mae"] or 0.0
    metric_rows.extend([
        (RUN_ID, MODEL_ID, horizon, "test", "mae_delta", float(model_mae), STARTED_AT),
        (RUN_ID, MODEL_ID, horizon, "test", "rmse_delta", float(model_rmse), STARTED_AT),
        (RUN_ID, MODEL_ID, horizon, "test", "persistence_mae_delta", float(persistence_mae), STARTED_AT),
    ])

    pipeline_final = pipeline.fit(labeled)
    latest = (
        base.withColumn("_rn", F.row_number().over(Window.partitionBy("COD_SENSOR").orderBy(F.desc("ts"))))
        .where(F.col("_rn") == 1).drop("_rn")
    )
    scored = (
        pipeline_final.transform(latest)
        .withColumn("predicted_level", F.col("level") + F.col("prediction"))
        .withColumn("forecast_for", F.col("ts") + F.expr(f"INTERVAL {horizon} HOURS"))
        .select(
            F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"),
            F.lit(horizon).alias("horizon_hours"), F.col("COD_SENSOR").alias("sensor_id"),
            F.col("ts").alias("issued_at"), "forecast_for", F.col("level").alias("last_observed_level"),
            F.col("predicted_level").alias("predicted_level"), F.col("prediction").alias("predicted_delta"),
            F.lit("experimental_sensor_level_forecast").alias("interpretation"), F.current_timestamp().alias("created_at"),
        )
    )
    prediction_frames.append(scored)
    artifact_path = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}/horizon={horizon}"
    pipeline_final.write().save(artifact_path)
    run_rows.append((RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT, None, None, test_start, max_ts, artifact_path,
                     f"Horizonte {horizon}h; alvo = variação do nível no mesmo sensor; teste nos últimos 90 dias; unidade mantida por sensor."))

if not prediction_frames:
    raise RuntimeError("Nenhum horizonte pôde ser treinado com dados Gold disponíveis.")

spark.createDataFrame(metric_rows, metric_schema).write.mode("append").saveAsTable(f"{ML}.model_metrics")
from functools import reduce
reduce(lambda a, b: a.unionByName(b), prediction_frames).write.format("delta").mode("append").saveAsTable(f"{ML}.predictions_sibh_level")
run_schema = "run_id string, model_id string, model_version string, status string, started_at timestamp, training_start timestamp, training_end timestamp, test_start timestamp, test_end timestamp, artifact_path string, details string"
spark.createDataFrame(run_rows, run_schema).write.mode("append").saveAsTable(f"{ML}.model_runs")

print(f"Previsões de nível concluídas para {len(prediction_frames)} horizontes; teste temporal iniciado em {test_start}.")
