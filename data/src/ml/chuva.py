"""Modelo 3: chuva da próxima hora em cada estação, a partir da chuva observada na estação e nas vizinhas.

A maioria das horas é seca, então o MAE sozinho engana: prever zero sempre já erra pouco. Por isso a avaliação compara
o modelo com duas referências (persistência e zero), mede o erro só nas horas com chuva e o acerto de evento em limiares.
"""

from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from ml.avaliacao import deteccao, erros, ganho
from ml.features import chuva_vizinha, completar_horas, horas_desde_leitura, pares_proximos

DEFASAGENS = (1, 2, 3, 6, 12, 24)
RAIO_VIZINHOS_KM = 15
LIMIARES_MM = (1.0, 5.0, 10.0)
HORAS_RECENTES = 3
NUMERICAS = (["lat", "lon", "rain_mm", "hours_since_reading"] + [f"rain_lag_{k}" for k in DEFASAGENS]
             + ["rain_sum_3h", "rain_sum_6h", "rain_sum_24h", "near_rain_mean_mm", "near_rain_max_mm", "near_stations_reporting",
                "near_rain_mean_lag_1", "near_rain_max_lag_1", "hour_of_day", "day_of_year"])


def montar_base(chuva):
    """chuva: station_id, network, ts, lat, lon, rain_mm. Uma linha por estação e hora com leitura, com o alvo target_rain_mm
    (chuva da hora seguinte na mesma estação)."""
    atributos = chuva.groupBy("station_id").agg(F.first("network").alias("network"), F.first("lat").alias("lat"), F.first("lon").alias("lon"))
    g = completar_horas(chuva, "station_id", ["rain_mm"]).join(atributos, "station_id")
    g = horas_desde_leitura(g, "station_id", "rain_mm")
    w = Window.partitionBy("station_id").orderBy("ts")
    for k in DEFASAGENS:
        g = g.withColumn(f"rain_lag_{k}", F.lag("rain_mm", k).over(w))
    pares = pares_proximos(atributos.select(F.col("station_id").alias("id"), "lat", "lon"),
                           atributos.select(F.col("station_id").alias("id"), "lat", "lon"), RAIO_VIZINHOS_KM, id_origem="station_id")
    g = (g.join(chuva_vizinha(chuva, pares, "station_id", "near"), ["station_id", "ts"], "left")
         .withColumn("rain_sum_3h", F.sum("rain_mm").over(w.rowsBetween(-2, 0)))
         .withColumn("rain_sum_6h", F.sum("rain_mm").over(w.rowsBetween(-5, 0)))
         .withColumn("rain_sum_24h", F.sum("rain_mm").over(w.rowsBetween(-23, 0)))
         .withColumn("near_rain_mean_lag_1", F.lag("near_rain_mean_mm", 1).over(w))
         .withColumn("near_rain_max_lag_1", F.lag("near_rain_max_mm", 1).over(w))
         .withColumn("target_rain_mm", F.lead("rain_mm", 1).over(w))
         .withColumn("hour_of_day", F.hour("ts").cast("double"))
         .withColumn("day_of_year", F.dayofyear("ts").cast("double")))
    return g.where(F.col("rain_mm").isNotNull())


def pipeline():
    imputadas = [f"imp_{c}" for c in NUMERICAS]
    return Pipeline(stages=[
        StringIndexer(inputCol="station_id", outputCol="station_index", handleInvalid="keep"),
        StringIndexer(inputCol="network", outputCol="network_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["station_index", "network_index"], outputCols=["station_vec", "network_vec"], handleInvalid="keep"),
        Imputer(inputCols=NUMERICAS, outputCols=imputadas, strategy="median"),
        VectorAssembler(inputCols=imputadas + ["station_vec", "network_vec"], outputCol="features", handleInvalid="keep"),
        GBTRegressor(featuresCol="features", labelCol="target_rain_mm", predictionCol="prediction", maxIter=80, maxDepth=5, stepSize=0.05, seed=42),
    ])


def treinar(base, dias_teste=90):
    fim = base.agg(F.max("ts")).first()[0]
    inicio = base.agg(F.min("ts")).first()[0]
    inicio_teste = fim - timedelta(days=dias_teste)
    rotulado = base.where(F.col("target_rain_mm").isNotNull())
    treino = rotulado.where(F.col("ts") < F.lit(inicio_teste - timedelta(hours=1)))
    teste = rotulado.where(F.col("ts") >= F.lit(inicio_teste))
    if treino.limit(1).count() == 0 or teste.limit(1).count() == 0:
        raise RuntimeError("Não há dados suficientes para a divisão temporal.")

    previsto = (pipeline().fit(treino).transform(teste)
                .withColumn("prediction", F.greatest("prediction", F.lit(0.0)))
                .withColumn("persistencia", F.col("rain_mm")).withColumn("zero", F.lit(0.0))).cache()
    m, p, z = erros(previsto, "target_rain_mm", "prediction"), erros(previsto, "target_rain_mm", "persistencia"), erros(previsto, "target_rain_mm", "zero")
    chuvosas = previsto.where(F.col("target_rain_mm") > 0)
    mc, pc = erros(chuvosas, "target_rain_mm", "prediction"), erros(chuvosas, "target_rain_mm", "persistencia")
    metricas = {
        "mae_mm": m["mae"], "rmse_mm": m["rmse"], "persistence_mae_mm": p["mae"], "zero_mae_mm": z["mae"],
        "skill_vs_persistence": ganho(m["mae"], p["mae"]), "skill_vs_zero": ganho(m["mae"], z["mae"]),
        "wet_hour_rate": previsto.select(F.avg((F.col("target_rain_mm") > 0).cast("double"))).first()[0],
        "wet_mae_mm": mc["mae"], "wet_persistence_mae_mm": pc["mae"],
    }
    for limiar in LIMIARES_MM:
        for nome, coluna in (("model", "prediction"), ("persistence", "persistencia")):
            d = deteccao(previsto, "target_rain_mm", coluna, limiar)
            for k in ("pod", "far", "csi"):
                metricas[f"{nome}_{k}_{limiar:g}mm"] = d[k]
        metricas[f"events_{limiar:g}mm"] = d["eventos"]
    previsto.unpersist()

    modelo = pipeline().fit(rotulado)
    recentes = (base.where(F.col("ts") >= F.lit(fim - timedelta(hours=HORAS_RECENTES)))
                .withColumn("_n", F.row_number().over(Window.partitionBy("station_id").orderBy(F.desc("ts")))).where("_n = 1").drop("_n"))
    previsao = (modelo.transform(recentes)
                .select("station_id", "network", F.col("lat").alias("latitude"), F.col("lon").alias("longitude"), F.col("ts").alias("issued_at"),
                        (F.col("ts") + F.expr("INTERVAL 1 HOUR")).alias("forecast_for"), F.col("rain_mm").alias("last_observed_rain_mm"),
                        F.greatest("prediction", F.lit(0.0)).alias("predicted_rain_mm")))
    return {"metricas": metricas, "modelo": modelo, "previsao": previsao, "inicio": inicio, "fim": fim, "inicio_teste": inicio_teste}
