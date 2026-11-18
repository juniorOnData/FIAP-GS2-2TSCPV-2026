# Databricks notebook source
# MAGIC %md
# MAGIC # 01_spatial_recurrence
# MAGIC
# MAGIC Modelo experimental de recorrência reportada de alagamentos e inundações por célula geográfica de aproximadamente 1 km. A unidade de observação é a contagem anual de registros do GeoSampa, não a ocorrência real de todos os eventos nem a probabilidade de alagamento.
# MAGIC
# MAGIC A fato do GeoSampa tem data, sem horário do evento. Este modelo é anual e não deve ser usado para previsão de curto prazo ou emissão de alerta. A validação usa anos inteiros anteriores e um ano temporalmente separado.
# MAGIC
# MAGIC O primeiro baseline usa somente recorrência passada e localização. O cruzamento exato com polígonos do SGB, setores do IBGE e vias do OSM será incluído após validar a alocação espacial; atribuir atributos de polígonos pelo centroide da feição pode enviesar os resultados.

# COMMAND ----------

from datetime import datetime, timezone
import uuid

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "spatial_flood_recurrence"
MODEL_VERSION = "0.1.0"
CELL_SCALE = 100  # 0,01 grau: cerca de 1 km nesta latitude; célula aproximada, não área administrativa.
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ML} COMMENT 'Modelos experimentais do ChuvaSegura'")

EVENTS = f"{CATALOG}.gold.fact_geosampa_flooding_events"
DIM_TIME = f"{CATALOG}.gold.dim_tempo"
DIM_TYPE = f"{CATALOG}.gold.dim_geosampa_type_flooding_events"

eventos = (
    spark.table(EVENTS).alias("f")
    .join(spark.table(DIM_TIME).alias("t"), F.col("f.COD_DIM_TEMPO") == F.col("t.COD_DIM_TEMPO"))
    .join(spark.table(DIM_TYPE).alias("d"), F.col("f.COD_DIM_OCORRENCIA") == F.col("d.COD_DIM_OCORRENCIA"))
    .where(F.lower(F.col("d.DSC_TIPO_OCORRENCIA")).rlike("alagamento|inunda"))
    .where(F.col("f.NUM_LATITUDE").isNotNull() & F.col("f.NUM_LONGITUDE").isNotNull())
    .select(F.col("t.NUM_ANO").alias("year"), F.col("f.NUM_LATITUDE").alias("lat"), F.col("f.NUM_LONGITUDE").alias("lon"))
    .withColumn("cell_y", F.floor(F.col("lat") * CELL_SCALE).cast("int"))
    .withColumn("cell_x", F.floor(F.col("lon") * CELL_SCALE).cast("int"))
    .withColumn("cell_id", F.concat_ws(":", "cell_y", "cell_x"))
)

max_observed_year = eventos.agg(F.max("year")).first()[0]
max_complete_year = min(max_observed_year, datetime.now().year - 1)
min_year = eventos.agg(F.min("year")).first()[0]
if max_complete_year - min_year < 5:
    raise RuntimeError("São necessários pelo menos seis anos civis de ocorrências para treinar este baseline.")

contagens = (
    eventos.where(F.col("year") <= max_complete_year)
    .groupBy("cell_id", "cell_x", "cell_y", "year")
    .agg(F.count(F.lit(1)).cast("double").alias("reported_events"))
)

celulas = contagens.select("cell_id", "cell_x", "cell_y").distinct()
anos = spark.range(min_year, max_complete_year + 1).select(F.col("id").cast("int").alias("year"))
painel = celulas.crossJoin(anos).join(contagens, ["cell_id", "cell_x", "cell_y", "year"], "left").fillna({"reported_events": 0.0})
janela = Window.partitionBy("cell_id").orderBy("year")
historico = (
    painel
    .withColumn("lag_1", F.lag("reported_events", 1).over(janela))
    .withColumn("lag_2", F.lag("reported_events", 2).over(janela))
    .withColumn("lag_3", F.lag("reported_events", 3).over(janela))
    .withColumn("mean_prev_3", F.avg("reported_events").over(janela.rowsBetween(-3, -1)))
    .withColumn("cell_lat", (F.col("cell_y") + F.lit(0.5)) / CELL_SCALE)
    .withColumn("cell_lon", (F.col("cell_x") + F.lit(0.5)) / CELL_SCALE)
    .fillna({"lag_1": 0.0, "lag_2": 0.0, "lag_3": 0.0, "mean_prev_3": 0.0})
    .withColumn("run_id", F.lit(RUN_ID))
)

FEATURES_TABLE = f"{ML}.features_spatial_recurrence"
historico.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# O último ano completo é reservado como teste. Nunca há divisão aleatória de anos ou células.
test_year = max_complete_year
train = historico.where((F.col("year") < test_year) & (F.col("year") >= min_year + 3))
test = historico.where((F.col("year") == test_year) & (F.col("year") >= min_year + 3))
if train.limit(1).count() == 0 or test.limit(1).count() == 0:
    raise RuntimeError("O painel não tem linhas suficientes para treino e teste temporal.")

feature_cols = ["cell_lat", "cell_lon", "year", "lag_1", "lag_2", "lag_3", "mean_prev_3"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
regressor = GBTRegressor(
    featuresCol="features", labelCol="reported_events", predictionCol="prediction",
    maxIter=60, maxDepth=4, stepSize=0.05, seed=42,
)
pipeline = Pipeline(stages=[assembler, regressor])
modelo_teste = pipeline.fit(train)
teste_score = modelo_teste.transform(test)

avaliador = RegressionEvaluator(labelCol="reported_events", predictionCol="prediction")
mae = avaliador.setMetricName("mae").evaluate(teste_score)
rmse = avaliador.setMetricName("rmse").evaluate(teste_score)
baseline_mae = test.select(F.avg(F.abs(F.col("reported_events") - F.col("lag_1")))).first()[0]

metrics = [(RUN_ID, MODEL_ID, None, "test", "mae", float(mae), STARTED_AT),
           (RUN_ID, MODEL_ID, None, "test", "rmse", float(rmse), STARTED_AT),
           (RUN_ID, MODEL_ID, None, "test", "baseline_lag1_mae", float(baseline_mae or 0.0), STARTED_AT)]
metric_schema = "run_id string, model_id string, horizon_hours int, split string, metric string, value double, computed_at timestamp"
spark.createDataFrame(metrics, metric_schema).write.mode("append").saveAsTable(f"{ML}.model_metrics")

# Reajusta com todos os anos completos e estima a contagem reportada no próximo ano civil.
modelo_final = pipeline.fit(historico.where(F.col("year") >= min_year + 3))
score_year = datetime.now().year + 1
features_score = (
    historico.where(F.col("year") == max_complete_year)
    .select(
        "cell_id", "cell_x", "cell_y", "cell_lat", "cell_lon",
        F.lit(score_year).cast("int").alias("year"),
        F.col("reported_events").alias("lag_1"), F.col("lag_1").alias("lag_2"),
        F.col("lag_2").alias("lag_3"),
        ((F.col("reported_events") + F.col("lag_1") + F.col("lag_2")) / 3.0).alias("mean_prev_3"),
    )
)
forecast = (
    modelo_final.transform(features_score)
    .withColumn("expected_reported_events", F.greatest(F.col("prediction"), F.lit(0.0)))
    .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("expected_reported_events"))))
    .select(
        F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(score_year).alias("prediction_year"), "cell_id", "cell_lat", "cell_lon", "rank",
        "expected_reported_events", F.lit(max_complete_year).alias("data_through_year"),
        F.lit("experimental_historical_recurrence").alias("interpretation"), F.current_timestamp().alias("created_at"),
    )
)
forecast.write.format("delta").mode("append").saveAsTable(f"{ML}.predictions_spatial_recurrence")

artifact_path = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}"
modelo_final.write().save(artifact_path)
run = [(RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT, None, None, None, None, artifact_path,
        f"Treino até {max_complete_year}; teste temporal em {test_year}; célula 0,01 grau; alvo = registros GeoSampa de alagamento/inundação.")]
spark.createDataFrame(run, "run_id string, model_id string, model_version string, status string, started_at timestamp, training_start timestamp, training_end timestamp, test_start timestamp, test_end timestamp, artifact_path string, details string").write.mode("append").saveAsTable(f"{ML}.model_runs")

print(f"Modelo {MODEL_ID} concluído. MAE={mae:.3f}; baseline MAE={baseline_mae:.3f}; previsão anual experimental para {score_year}.")
