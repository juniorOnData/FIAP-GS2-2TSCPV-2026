# Databricks notebook source
# MAGIC %md
# MAGIC # 04_dim_osm_water
# MAGIC
# MAGIC Dimensões da hidrografia do OpenStreetMap no município de São Paulo, a partir da Bronze:
# MAGIC
# MAGIC - `gold.dim_osm_waterways`: cursos d'água (rios, córregos, canais e drenos), uma linha por *way*, com nome, classe, largura informada, comprimento e geometria;
# MAGIC - `gold.dim_osm_water_areas`: corpos d'água (represas, lagos, leitos de rio e várzeas), com área e geometria.
# MAGIC
# MAGIC Comprimento e área em SIRGAS 2000 / UTM 23S (`src/common/geo.py`); geometria em EPSG 4326. As tabelas são recriadas a cada extrato (a Geofabrik republica a camada inteira). Em São Paulo muitos córregos correm canalizados ou sob avenidas, e o OpenStreetMap os mapeia de forma desigual: a ausência de um curso d'água no mapa não quer dizer que ele não exista.
# MAGIC
# MAGIC Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------

import os
import sys

from pyspark.sql import types as T

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402

BRONZE_CURSOS = "fiap_analytics.bronze.osm_waterways"
BRONZE_CORPOS = "fiap_analytics.bronze.osm_water_areas"
DIM_CURSOS = "fiap_analytics.gold.dim_osm_waterways"
DIM_CORPOS = "fiap_analytics.gold.dim_osm_water_areas"

CLASSES_CURSO = {"river": "Rio", "stream": "Córrego", "canal": "Canal", "drain": "Dreno ou vala"}
CLASSES_CORPO = {"reservoir": "Represa ou reservatório", "water": "Lago ou espelho d'água", "river": "Leito de rio",
                 "wetland": "Várzea ou área alagadiça", "dock": "Doca"}

# COMMAND ----------


def numero(texto):
    try:
        v = float(str(texto).replace(",", "."))
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def montar(bronze, classes, medida):
    linhas, ignoradas = [], 0
    for r in spark.table(bronze).toLocalIterator():
        g = geo.descrever(r["geom_wkb"], r["_source_srs_id"])
        if not g:
            ignoradas += 1
            continue
        classe = r["fclass"]
        linhas.append((int(r["osm_id"]), r["name"], classe, classes.get(classe, "Outro"),
                       numero(r["width"]) if "width" in r.asDict() else None, g[medida], g["lat_min"], g["lat_max"], g["lon_min"],
                       g["lon_max"], g["geom_wkb"], r["_source_version"]))
    print(f"{bronze}: {len(linhas)} feições ({ignoradas} sem geometria)")
    return linhas


def gravar(linhas, tabela, nome_medida, comentario):
    schema = T.StructType([
        T.StructField("NUM_OSM_ID", T.LongType()), T.StructField("NOM_HIDRO", T.StringType()), T.StructField("COD_CLASSE", T.StringType()),
        T.StructField("DSC_CLASSE", T.StringType()), T.StructField("NUM_LARGURA_M", T.DoubleType()), T.StructField(nome_medida, T.DoubleType()),
        T.StructField("NUM_LAT_MIN", T.DoubleType()), T.StructField("NUM_LAT_MAX", T.DoubleType()), T.StructField("NUM_LON_MIN", T.DoubleType()),
        T.StructField("NUM_LON_MAX", T.DoubleType()), T.StructField("GEO_WKB", T.BinaryType()), T.StructField("DSC_VERSAO_FONTE", T.StringType()),
    ])
    (spark.createDataFrame(linhas, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela))
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario} © OpenStreetMap contributors, ODbL 1.0.'")


gravar(montar(BRONZE_CURSOS, CLASSES_CURSO, "comprimento_km"), DIM_CURSOS, "NUM_COMPRIMENTO_KM",
       "Cursos d agua do OpenStreetMap no municipio de Sao Paulo (rios, corregos, canais e drenos), geometria em EPSG 4326.")
gravar(montar(BRONZE_CORPOS, CLASSES_CORPO, "area_km2"), DIM_CORPOS, "NUM_AREA_KM2",
       "Corpos d agua do OpenStreetMap no municipio de Sao Paulo (represas, lagos, leitos de rio e varzeas), geometria em EPSG 4326.")
display(spark.sql(f"SELECT DSC_CLASSE, COUNT(*) AS n, ROUND(SUM(NUM_COMPRIMENTO_KM), 1) AS km FROM {DIM_CURSOS} GROUP BY 1 ORDER BY 2 DESC"))
