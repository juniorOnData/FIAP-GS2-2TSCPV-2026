# Databricks notebook source
# MAGIC %md
# MAGIC # 01_spatial_recurrence
# MAGIC
# MAGIC Modelo experimental de recorrência reportada de alagamentos e inundações por célula de cerca de 1 km (0,01 grau). A unidade de observação é a contagem anual de registros do GeoSampa, não a ocorrência real de todos os eventos nem a probabilidade de alagamento. A fato não tem horário do evento: o modelo é anual e não serve para previsão de curto prazo nem para alerta.
# MAGIC
# MAGIC A lógica está em `src/ml/recorrencia.py` (testada com dados sintéticos em `data/tests/test_ml_spark.py`):
# MAGIC
# MAGIC - o último ano completo é o teste; o universo de células sai só dos anos anteriores e inclui as células com qualquer registro, mesmo sem alagamento;
# MAGIC - o alvo é log(1 + contagem), com as contagens dos três anos anteriores e a posição da célula;
# MAGIC - referências: o ano anterior e a média dos três anos anteriores; como o uso é um ranking, também mede quanto dos registros do ano de teste cai nas 10% de células mais bem pontuadas;
# MAGIC - a previsão final é para o ano seguinte ao último ano completo.

# COMMAND ----------

from datetime import datetime, timezone
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
from ml import recorrencia, registro  # noqa: E402

CATALOG = "fiap_analytics"
ML = f"{CATALOG}.ml"
MODEL_ID = "spatial_flood_recurrence"
MODEL_VERSION = "0.2.0"
RUN_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------

try:
    ocorrencias = (
        spark.table(f"{CATALOG}.gold.fact_geosampa_flooding_events").alias("f")
        .join(spark.table(f"{CATALOG}.gold.dim_tempo").alias("t"), F.col("f.COD_DIM_TEMPO") == F.col("t.COD_DIM_TEMPO"))
        .join(spark.table(f"{CATALOG}.gold.dim_geosampa_type_flooding_events").alias("d"), F.col("f.COD_DIM_OCORRENCIA") == F.col("d.COD_DIM_OCORRENCIA"))
        .where(F.col("f.NUM_LATITUDE").isNotNull() & F.col("f.NUM_LONGITUDE").isNotNull())
        .select(F.col("t.NUM_ANO").cast("int").alias("year"), F.col("f.NUM_LATITUDE").alias("lat"), F.col("f.NUM_LONGITUDE").alias("lon"),
                F.lower(F.col("d.DSC_TIPO_OCORRENCIA")).alias("tipo"))
    ).cache()
    enchentes = ocorrencias.where(F.col("tipo").rlike("alagamento|inunda")).select("year", "lat", "lon")
    todos = ocorrencias.select("year", "lat", "lon")
    ultimo_completo = min(ocorrencias.agg(F.max("year")).first()[0], datetime.now().year - 1)

    r = recorrencia.treinar(enchentes, todos, ultimo_completo)

    r["painel"].withColumn("run_id", F.lit(RUN_ID)).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.features_spatial_recurrence")
    (r["previsao"]
     .select(F.lit(RUN_ID).alias("run_id"), F.lit(MODEL_VERSION).alias("model_version"), F.col("year").alias("prediction_year"),
             "cell_id", "cell_lat", "cell_lon", "rank", "expected_reported_events", F.lit(ultimo_completo).alias("data_through_year"),
             F.lit("experimental_historical_recurrence").alias("interpretation"), F.current_timestamp().alias("created_at"))
     .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{ML}.predictions_spatial_recurrence"))
    registro.gravar_metricas(spark, [registro.linha_metrica(RUN_ID, MODEL_ID, None, "test", k, v, STARTED_AT) for k, v in r["metricas"].items()])
    artefato = f"/Volumes/{CATALOG}/ml/model_artifacts/{MODEL_ID}/{RUN_ID}"
    r["modelo"].write().save(artefato)
    registro.gravar_execucao(
        spark, RUN_ID, MODEL_ID, MODEL_VERSION, "SUCESSO", STARTED_AT,
        treino=(datetime(r["ano_inicial"], 1, 1), datetime(ultimo_completo - 1, 12, 31)),
        teste=(datetime(ultimo_completo, 1, 1), datetime(ultimo_completo, 12, 31)), artefato=artefato,
        detalhes=f"Teste em {ultimo_completo}; previsão para {r['ano_previsto']}; célula 0,01 grau; alvo = log1p(registros de alagamento/inundação).")
    m = r["metricas"]
    print(f"MAE {m['mae']:.3f} | média de 3 anos {m['baseline_mean3_mae']:.3f} | ano anterior {m['baseline_lag1_mae']:.3f} | "
          f"captura no topo 10% {m['top10_capture']:.1%} (média de 3 anos: {m['baseline_mean3_top10_capture']:.1%}) | previsão para {r['ano_previsto']}")
except Exception as exc:
    registro.gravar_execucao(spark, RUN_ID, MODEL_ID, MODEL_VERSION, "FALHA", STARTED_AT, detalhes=f"{type(exc).__name__}: {exc}")
    raise
