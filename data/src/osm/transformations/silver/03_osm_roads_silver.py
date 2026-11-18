# Databricks notebook source
# MAGIC %md
# MAGIC # 03_osm_roads_silver
# MAGIC
# MAGIC ## Funções de transformação
# MAGIC
# MAGIC `inteiro` converte atributos numéricos do shapefile; `flag` padroniza atributos booleanos e categóricos de presença. `transformar` percorre as vias, normaliza os campos e calcula os atributos espaciais usados na análise, como extensão em metros.
# MAGIC
# MAGIC Recria a Silver das vias do OpenStreetMap (`fiap_analytics.silver.osm_roads`) a partir da Bronze: uma linha por via, com atributos tipados e geometria padronizada.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. `osm_id` vira `nr_osm_id` (identificador da *way* no OSM). `fclass` é a classe da via no esquema da Geofabrik (`motorway`, `primary`, `residential`...), e `cd_classe_geofabrik` é o código numérico correspondente.
# MAGIC 2. `oneway`: `F` (mão única no sentido do desenho), `T` (mão única no sentido contrário) ou `B` (mão dupla). Vira `dc_sentido` e `fl_mao_unica`. `maxspeed` 0 (desconhecida) vira nulo. `bridge` e `tunnel` (`T`/`F`) viram `fl_ponte` e `fl_tunel`.
# MAGIC 3. Geometria: a Bronze está em WGS84 (EPSG 4326), tratado como igual ao SIRGAS 2000 geográfico (diferença abaixo de 1 m), e `geom_wkb` é gravado como EPSG 4674, o padrão das fontes geoespaciais do projeto. Comprimento (`nr_comprimento_km`, em UTM 23S), centroide e envelope vêm de `src/common/geo.py`.
# MAGIC 4. A conversão roda no driver, em lotes (centenas de milhares de vias).
# MAGIC
# MAGIC Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------

import os
import sys

from pyspark.sql import types as T

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402

BRONZE = "fiap_analytics.bronze.osm_roads"
SILVER = "fiap_analytics.silver.osm_roads"
LOTE = 100_000
SENTIDO = {"B": "Mão dupla", "F": "Mão única (sentido do desenho)", "T": "Mão única (sentido contrário ao desenho)"}

SCHEMA_SILVER = T.StructType([
    T.StructField("nr_osm_id", T.LongType()),
    T.StructField("cd_classe", T.StringType()),
    T.StructField("cd_classe_geofabrik", T.IntegerType()),
    T.StructField("nm_via", T.StringType()),
    T.StructField("cd_referencia", T.StringType()),
    T.StructField("cd_sentido", T.StringType()),
    T.StructField("dc_sentido", T.StringType()),
    T.StructField("fl_mao_unica", T.BooleanType()),
    T.StructField("nr_velocidade_max_kmh", T.IntegerType()),
    T.StructField("nr_nivel", T.IntegerType()),
    T.StructField("fl_ponte", T.BooleanType()),
    T.StructField("fl_tunel", T.BooleanType()),
    T.StructField("geom_wkb", T.BinaryType()),
    T.StructField("dc_tipo_geometria", T.StringType()),
    T.StructField("nr_vertices", T.IntegerType()),
    T.StructField("nr_comprimento_km", T.DoubleType()),
    T.StructField("nr_centroide_latitude", T.DoubleType()),
    T.StructField("nr_centroide_longitude", T.DoubleType()),
    T.StructField("nr_lat_min", T.DoubleType()),
    T.StructField("nr_lat_max", T.DoubleType()),
    T.StructField("nr_lon_min", T.DoubleType()),
    T.StructField("nr_lon_max", T.DoubleType()),
    T.StructField("_source_feature_id", T.IntegerType()),
    T.StructField("_source_srs_id", T.IntegerType()),
    T.StructField("_source_sha256", T.StringType()),
    T.StructField("_source_version", T.StringType()),
    T.StructField("_ingested_at", T.TimestampType()),
])

# COMMAND ----------


def inteiro(valor):
    try:
        return int(float(valor)) if valor not in (None, "") else None
    except ValueError:
        return None


def flag(valor):
    return {"T": True, "F": False}.get((valor or "").strip().upper())


def transformar(r):
    g = geo.descrever(r["geom_wkb"], r["_source_srs_id"]) or {}
    sentido = (r["oneway"] or "").strip().upper() or None
    velocidade = inteiro(r["maxspeed"])
    return (
        inteiro(r["osm_id"]), r["fclass"], inteiro(r["code"]), r["name"], r["ref"],
        sentido, SENTIDO.get(sentido), sentido in ("F", "T") if sentido else None,
        velocidade if velocidade else None, inteiro(r["layer"]), flag(r["bridge"]), flag(r["tunnel"]),
        g.get("geom_wkb"), g.get("tipo"), g.get("pontos"), g.get("comprimento_km"),
        g.get("centroide_lat"), g.get("centroide_lon"),
        g.get("lat_min"), g.get("lat_max"), g.get("lon_min"), g.get("lon_max"),
        r["_source_feature_id"], r["_source_srs_id"], r["_source_sha256"], r["_source_version"], r["_ingested_at"],
    )


colunas = ["osm_id", "fclass", "code", "name", "ref", "oneway", "maxspeed", "layer", "bridge", "tunnel", "geom_wkb",
           "_source_feature_id", "_source_srs_id", "_source_sha256", "_source_version", "_ingested_at"]
lote, modo, total = [], "overwrite", 0
for r in spark.table(BRONZE).select(*colunas).toLocalIterator():
    lote.append(transformar(r))
    if len(lote) >= LOTE:
        (spark.createDataFrame(lote, SCHEMA_SILVER).write.format("delta").mode(modo)
         .option("overwriteSchema", "true").saveAsTable(SILVER))
        total, lote, modo = total + len(lote), [], "append"
if lote or modo == "overwrite":
    (spark.createDataFrame(lote, SCHEMA_SILVER).write.format("delta").mode(modo)
     .option("overwriteSchema", "true").saveAsTable(SILVER))
    total += len(lote)

spark.sql(f"""
    COMMENT ON TABLE {SILVER} IS
    'Silver das vias do OpenStreetMap no recorte do município de São Paulo: uma linha por via, com classe, nome, sentido, velocidade máxima, ponte e túnel, geometria em EPSG 4674, comprimento em km, centroide e envelope. © OpenStreetMap contributors, ODbL 1.0.'
""")
print(f"{total} vias gravadas em {SILVER}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT cd_classe, COUNT(*) AS vias, ROUND(SUM(nr_comprimento_km), 1) AS extensao_km,
           COUNT_IF(nm_via IS NOT NULL) AS com_nome, COUNT_IF(fl_mao_unica) AS mao_unica
    FROM {SILVER}
    GROUP BY cd_classe
    ORDER BY extensao_km DESC
"""))
