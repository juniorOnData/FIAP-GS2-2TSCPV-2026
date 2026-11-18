"""Modelo 1: recorrência anual de registros de alagamento e inundação por célula de cerca de 1 km.

O alvo é a contagem de registros do GeoSampa na célula e no ano (não a ocorrência real de eventos). O modelo aprende
log(1 + contagem) com as contagens dos três anos anteriores e a posição da célula, e é avaliado num ano inteiro
separado contra duas referências: o ano anterior e a média dos três anos anteriores.
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from ml.avaliacao import captura_no_topo, erros, ganho

ESCALA_CELULA = 100  # 0,01 grau: cerca de 1 km nesta latitude (célula aproximada, não área administrativa)
FEATURES = ["cell_lat", "cell_lon", "lag_1", "lag_2", "lag_3", "mean_prev_3"]


def com_celula(eventos):
    """eventos: year, lat, lon. Acrescenta cell_x, cell_y e cell_id."""
    return (eventos.withColumn("cell_y", F.floor(F.col("lat") * ESCALA_CELULA).cast("int"))
            .withColumn("cell_x", F.floor(F.col("lon") * ESCALA_CELULA).cast("int"))
            .withColumn("cell_id", F.concat_ws(":", "cell_y", "cell_x")))


def celulas(eventos):
    return com_celula(eventos).select("cell_id", "cell_x", "cell_y").distinct()


def painel(enchentes, universo, ano_inicial, ano_final):
    """Contagem anual de `enchentes` por célula do `universo`, de ano_inicial a ano_final (zero quando não há registro),
    com as defasagens. As três primeiras linhas de cada célula têm defasagens incompletas."""
    spark = universo.sparkSession
    anos = spark.range(ano_inicial, ano_final + 1).select(F.col("id").cast("int").alias("year"))
    contagens = (com_celula(enchentes).where(F.col("year").between(ano_inicial, ano_final))
                 .groupBy("cell_id", "year").agg(F.count(F.lit(1)).cast("double").alias("reported_events")))
    janela = Window.partitionBy("cell_id").orderBy("year")
    return (universo.crossJoin(anos).join(contagens, ["cell_id", "year"], "left").fillna({"reported_events": 0.0})
            .withColumn("lag_1", F.lag("reported_events", 1).over(janela))
            .withColumn("lag_2", F.lag("reported_events", 2).over(janela))
            .withColumn("lag_3", F.lag("reported_events", 3).over(janela))
            .withColumn("mean_prev_3", F.avg("reported_events").over(janela.rowsBetween(-3, -1)))
            .withColumn("cell_lat", (F.col("cell_y") + F.lit(0.5)) / ESCALA_CELULA)
            .withColumn("cell_lon", (F.col("cell_x") + F.lit(0.5)) / ESCALA_CELULA)
            .withColumn("log_events", F.log1p("reported_events")))


def pipeline():
    return Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="features"),
        GBTRegressor(featuresCol="features", labelCol="log_events", predictionCol="prediction", maxIter=60, maxDepth=4, stepSize=0.05, seed=42),
    ])


def prever(modelo, df):
    return modelo.transform(df).withColumn("expected_reported_events", F.greatest(F.expm1("prediction"), F.lit(0.0)))


def treinar(enchentes, todos, ano_teste):
    """enchentes: year, lat, lon dos registros de alagamento e inundação; todos: o mesmo para qualquer tipo de ocorrência.

    O universo de células do teste vem só dos anos anteriores ao de teste e inclui as células com qualquer registro
    (onde há cobertura de registros), mesmo sem alagamento: assim o teste também cobra os acertos de "nenhum registro".
    Devolve as métricas do teste, o painel e a previsão do ano seguinte ao último ano completo, e o modelo final."""
    ano_inicial = todos.agg(F.min("year")).first()[0]
    if ano_teste - ano_inicial < 5:
        raise RuntimeError("São necessários pelo menos seis anos civis de ocorrências para treinar e testar.")
    primeiro_completo = ano_inicial + 3

    hist = painel(enchentes, celulas(todos.where(F.col("year") < ano_teste)), ano_inicial, ano_teste)
    treino = hist.where(F.col("year").between(primeiro_completo, ano_teste - 1))
    teste = hist.where(F.col("year") == ano_teste)
    teste_prev = prever(pipeline().fit(treino), teste).cache()
    m = erros(teste_prev, "reported_events", "expected_reported_events")
    m_lag1 = erros(teste_prev, "reported_events", "lag_1")
    m_media3 = erros(teste_prev, "reported_events", "mean_prev_3")
    metricas = {
        "mae": m["mae"], "rmse": m["rmse"], "baseline_lag1_mae": m_lag1["mae"], "baseline_mean3_mae": m_media3["mae"],
        "skill_vs_mean3": ganho(m["mae"], m_media3["mae"]),
        "top10_capture": captura_no_topo(teste_prev, "expected_reported_events", "reported_events", 0.1),
        "baseline_mean3_top10_capture": captura_no_topo(teste_prev, "mean_prev_3", "reported_events", 0.1),
        "cells": teste_prev.count(),
    }
    teste_prev.unpersist()

    final = painel(enchentes, celulas(todos.where(F.col("year") <= ano_teste)), ano_inicial, ano_teste)
    modelo = pipeline().fit(final.where(F.col("year") >= primeiro_completo))
    proximo = (final.where(F.col("year") == ano_teste)
               .select("cell_id", "cell_x", "cell_y", "cell_lat", "cell_lon", (F.col("year") + 1).alias("year"),
                       F.col("reported_events").alias("lag_1"), F.col("lag_1").alias("lag_2"), F.col("lag_2").alias("lag_3"),
                       ((F.col("reported_events") + F.col("lag_1") + F.col("lag_2")) / 3.0).alias("mean_prev_3")))
    previsao = (prever(modelo, proximo)
                .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("expected_reported_events"), "cell_id")))
                .select("cell_id", "cell_lat", "cell_lon", "year", "rank", "expected_reported_events"))
    return {"metricas": metricas, "painel": final, "previsao": previsao, "modelo": modelo,
            "ano_inicial": ano_inicial, "ano_teste": ano_teste, "ano_previsto": ano_teste + 1}
