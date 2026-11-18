# Databricks notebook source
# MAGIC %md
# MAGIC # 05_osm_water_gold_quality
# MAGIC
# MAGIC Regras de qualidade das dimensões da hidrografia (`dim_osm_waterways` e `dim_osm_water_areas`). Resultados em `fiap_analytics.bronze.osm_agua_data_quality_log` (fonte `osm_agua`), que o `export_web` confere antes de publicar a camada.

# COMMAND ----------

import os
import sys
import uuid

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if _src not in sys.path:
    sys.path.append(_src)
from common.data_quality import DataQuality, condicao, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.osm_agua_data_quality_log"
CURSOS = "fiap_analytics.gold.dim_osm_waterways"
CORPOS = "fiap_analytics.gold.dim_osm_water_areas"
ENVELOPE = "NUM_LAT_MIN >= -24.1 AND NUM_LAT_MAX <= -23.3 AND NUM_LON_MIN >= -47.0 AND NUM_LON_MAX <= -46.3"

regras = [
    nao_vazia(CURSOS),
    unico(CURSOS, ["NUM_OSM_ID"]),
    nao_nulo(CURSOS, "GEO_WKB"),
    condicao("comprimento_positivo", CURSOS, "Curso d'água com comprimento maior que zero", "NOT (NUM_COMPRIMENTO_KM > 0)"),
    condicao("dentro_do_recorte", CURSOS, "Envelope dentro da área de São Paulo e entorno", f"NOT ({ENVELOPE})", severidade="AVISO"),
    condicao("classe_conhecida", CURSOS, "Classe do esquema da Geofabrik (river, stream, canal, drain)", "DSC_CLASSE = 'Outro'", severidade="AVISO"),
    nao_vazia(CORPOS),
    unico(CORPOS, ["NUM_OSM_ID"], severidade="AVISO"),  # way e relation do OSM podem ter o mesmo número
    nao_nulo(CORPOS, "GEO_WKB"),
    condicao("area_positiva", CORPOS, "Corpo d'água com área maior que zero", "NOT (NUM_AREA_KM2 > 0)"),
]

dq = DataQuality(spark, DQ_TABLE, "osm_agua", "gold", str(uuid.uuid4()))
resultados = dq.executar(regras)
display(dq.resultados_df(resultados))

# COMMAND ----------

dq.validar(resultados)
