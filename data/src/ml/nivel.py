"""Modelo 2: variação do nível de cada sensor do SIBH em 1 e 3 horas.

O alvo é a variação da cota do próprio sensor (cm na Gold), e a referência é a persistência (variação zero).
A série é posta numa grade horária completa por sensor, então as defasagens são em horas. A chuva vem dos
pluviômetros a até RAIO_CHUVA_KM do sensor e da média da cidade (sem as redes em revisão).
"""

from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from ml.avaliacao import erros, ganho, por_segmento
from ml.features import chuva_da_cidade, chuva_vizinha, completar_horas, horas_desde_leitura, pares_proximos

HORIZONTES = (1, 3)
DEFASAGENS = (1, 3, 6, 12, 24)
RAIO_CHUVA_KM = 10
HORAS_RECENTES = 3  # só recebe previsão o sensor com leitura até 3 h antes da última hora da série
NUMERICAS = (["level", "hours_since_level"] + [f"level_delta_{k}" for k in DEFASAGENS]
             + ["near_rain_mean_mm", "near_rain_max_mm", "near_stations_reporting", "near_rain_sum_3h", "near_rain_sum_6h",
                "city_rain_mean_mm", "city_rain_max_mm", "city_stations_reporting", "hour_of_day", "day_of_year"])


def montar_base(niveis, chuva, sensores):
    """niveis: sensor_id, ts, level; chuva: station_id, ts, lat, lon, rain_mm; sensores: sensor_id, lat, lon.
    Devolve uma linha por sensor e hora com leitura, com as variáveis e os alvos target_delta_<h>."""
    g = horas_desde_leitura(completar_horas(niveis, "sensor_id", ["level"]), "sensor_id", "level", "hours_since_level")
    w = Window.partitionBy("sensor_id").orderBy("ts")
    for k in DEFASAGENS:
        g = g.withColumn(f"level_delta_{k}", F.col("level") - F.lag("level", k).over(w))
    for h in HORIZONTES:
        g = g.withColumn(f"target_delta_{h}", F.lead("level", h).over(w) - F.col("level"))

    estacoes = chuva.groupBy(F.col("station_id").alias("id")).agg(F.first("lat").alias("lat"), F.first("lon").alias("lon"))
    pares = pares_proximos(sensores.select(F.col("sensor_id").alias("id"), "lat", "lon"), estacoes, RAIO_CHUVA_KM, id_origem="sensor_id")
    g = (g.join(chuva_vizinha(chuva, pares, "sensor_id", "near"), ["sensor_id", "ts"], "left")
         .join(chuva_da_cidade(chuva), "ts", "left")
         .withColumn("near_rain_sum_3h", F.sum("near_rain_mean_mm").over(w.rowsBetween(-2, 0)))
         .withColumn("near_rain_sum_6h", F.sum("near_rain_mean_mm").over(w.rowsBetween(-5, 0)))
         .withColumn("hour_of_day", F.hour("ts").cast("double"))
         .withColumn("day_of_year", F.dayofyear("ts").cast("double")))
    return g.where(F.col("level").isNotNull())


def pipeline(alvo):
    imputadas = [f"imp_{c}" for c in NUMERICAS]
    return Pipeline(stages=[
        StringIndexer(inputCol="sensor_id", outputCol="sensor_index", handleInvalid="keep"),
        OneHotEncoder(inputCols=["sensor_index"], outputCols=["sensor_vec"], handleInvalid="keep"),
        Imputer(inputCols=NUMERICAS, outputCols=imputadas, strategy="median"),
        VectorAssembler(inputCols=imputadas + ["sensor_vec"], outputCol="features", handleInvalid="keep"),
        # Perda absoluta: picos de uma hora (falhas do sensor) não dominam o ajuste como no erro quadrático.
        GBTRegressor(featuresCol="features", labelCol=alvo, predictionCol="prediction", lossType="absolute", maxIter=60, maxDepth=4, seed=42),
    ])


def treinar(base, dias_teste=90):
    """Treina e avalia cada horizonte. O teste são os últimos `dias_teste` dias; entre treino e teste fica um intervalo
    do tamanho do horizonte, para nenhum alvo do treino cair dentro do período de teste."""
    fim = base.agg(F.max("ts")).first()[0]
    inicio = base.agg(F.min("ts")).first()[0]
    inicio_teste = fim - timedelta(days=dias_teste)
    recentes = (base.where(F.col("ts") >= F.lit(fim - timedelta(hours=HORAS_RECENTES)))
                .withColumn("_n", F.row_number().over(Window.partitionBy("sensor_id").orderBy(F.desc("ts")))).where("_n = 1").drop("_n"))
    resultados = []
    for h in HORIZONTES:
        alvo = f"target_delta_{h}"
        rotulado = base.where(F.col(alvo).isNotNull())
        treino = rotulado.where(F.col("ts") < F.lit(inicio_teste - timedelta(hours=h)))
        teste = rotulado.where(F.col("ts") >= F.lit(inicio_teste))
        if treino.limit(1).count() == 0 or teste.limit(1).count() == 0:
            continue
        previsto = pipeline(alvo).fit(treino).transform(teste).withColumn("persistencia", F.lit(0.0)).cache()
        m = erros(previsto, alvo, "prediction")
        p = erros(previsto, alvo, "persistencia")
        segmentos = por_segmento(previsto, "sensor_id", alvo, "prediction", "persistencia").collect()
        previsto.unpersist()
        modelo = pipeline(alvo).fit(rotulado)
        previsao = (modelo.transform(recentes)
                    .select("sensor_id", F.col("ts").alias("issued_at"), (F.col("ts") + F.expr(f"INTERVAL {h} HOURS")).alias("forecast_for"),
                            F.col("level").alias("last_observed_level"), F.col("prediction").alias("predicted_delta"),
                            (F.col("level") + F.col("prediction")).alias("predicted_level"))
                    .withColumn("horizon_hours", F.lit(h)))
        resultados.append({
            "horizonte": h, "modelo": modelo, "previsao": previsao, "segmentos": segmentos,
            "metricas": {"mae_delta": m["mae"], "rmse_delta": m["rmse"], "persistence_mae_delta": p["mae"],
                         "skill_vs_persistence": ganho(m["mae"], p["mae"])},
        })
    if not resultados:
        raise RuntimeError("Nenhum horizonte teve dados de treino e de teste.")
    return {"resultados": resultados, "inicio": inicio, "fim": fim, "inicio_teste": inicio_teste}
