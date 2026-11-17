# Databricks notebook source
# MAGIC %md
# MAGIC # 03_osm_silver_quality
# MAGIC
# MAGIC Regras de qualidade da Silver do OpenStreetMap (`fiap_analytics.silver.osm_roads`). Roda depois de `03_osm_roads_silver` e antes da Gold.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job: a Gold não é carregada com dados ruins.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.osm_data_quality_log`, uma linha por regra e execução.
# MAGIC
# MAGIC O OSM é editado por voluntários, então os atributos opcionais (nome, velocidade, sentido) só geram aviso.

# COMMAND ----------

import os
import sys
import uuid

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.data_quality import DataQuality, condicao, consulta, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.osm_data_quality_log"
FONTE = "osm"
ETAPA = "silver"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.osm_roads"
BRONZE = "fiap_analytics.bronze.osm_roads"

# Classes da camada roads da Geofabrik (as mesmas da dim_osm_road_class).
CLASSES = ["motorway", "trunk", "primary", "secondary", "tertiary", "motorway_link", "trunk_link", "primary_link",
           "secondary_link", "tertiary_link", "unclassified", "residential", "living_street", "busway", "service",
           "pedestrian", "footway", "steps", "cycleway", "path", "bridleway", "track", "track_grade1", "track_grade2",
           "track_grade3", "track_grade4", "track_grade5", "unknown"]
# Extensão mínima esperada das vias de veículos no recorte (km). O recorte (o município e as bordas dos vizinhos)
# tinha cerca de 31,7 mil km no extrato de novembro de 2026: abaixo da metade, o extrato ou o recorte veio truncado.
EXTENSAO_MINIMA_KM = 15000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------

lista_classes = ", ".join(f"'{c}'" for c in CLASSES)
veiculos = ("'motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'motorway_link', 'trunk_link', "
            "'primary_link', 'secondary_link', 'tertiary_link', 'unclassified', 'residential', 'living_street'")

regras = [
    # Volume e completude
    nao_vazia(SILVER),
    consulta(
        "linhas_iguais_bronze", SILVER, "A Silver tem as mesmas vias da Bronze",
        f"""SELECT ABS((SELECT COUNT(*) FROM {SILVER}) - (SELECT COUNT(*) FROM {BRONZE})) AS falhas,
                   (SELECT COUNT(*) FROM {BRONZE}) AS total""",
    ),
    consulta(
        "extensao_viaria_minima", SILVER,
        f"Vias de veículos (exceto serviço) somam pelo menos {EXTENSAO_MINIMA_KM} km no recorte",
        f"""SELECT CASE WHEN COALESCE(SUM(nr_comprimento_km), 0) < {EXTENSAO_MINIMA_KM} THEN 1 ELSE 0 END AS falhas, 1 AS total
            FROM {SILVER} WHERE cd_classe IN ({veiculos})""",
    ),

    # Identificação e classe
    nao_nulo(SILVER, "nr_osm_id"),
    unico(SILVER, ["nr_osm_id"]),
    nao_nulo(SILVER, "cd_classe"),
    condicao("classe_conhecida", SILVER, "Classes fora do esquema da Geofabrik (entram como 'Outras' na dimensão)",
             f"cd_classe NOT IN ({lista_classes})", severidade="AVISO"),

    # Geometria
    condicao("srid_4326", SILVER, "Geometria de origem em WGS84 (EPSG 4326)", "_source_srs_id <> 4326"),
    nao_nulo(SILVER, "geom_wkb"),
    condicao("geometria_linear", SILVER, "Geometria do tipo LineString ou MultiLineString",
             "dc_tipo_geometria NOT IN ('LineString', 'MultiLineString')"),
    condicao("comprimento_positivo", SILVER, "Comprimento maior que zero", "nr_comprimento_km IS NULL OR nr_comprimento_km <= 0"),
    condicao("via_na_area_de_sao_paulo", SILVER, "Via dentro da área de São Paulo e entorno",
             "nr_lat_min < -24.1 OR nr_lat_max > -23.3 OR nr_lon_min < -47.0 OR nr_lon_max > -46.3"),
    condicao("via_acima_de_50km", SILVER, "Vias com mais de 50 km (provável erro de edição)",
             "nr_comprimento_km > 50", severidade="AVISO"),

    # Atributos opcionais
    condicao("velocidade_plausivel", SILVER, "Velocidade máxima entre 5 e 130 km/h",
             "nr_velocidade_max_kmh NOT BETWEEN 5 AND 130", severidade="AVISO", limite=0.001),
    condicao("sentido_valido", SILVER, "Sentido B, F ou T", "cd_sentido NOT IN ('B', 'F', 'T')"),
    # Cerca de 14% das vias de veículos não tinham nome no extrato de novembro de 2026.
    nao_nulo(SILVER, "nm_via", severidade="AVISO", limite=0.20, filtro=f"cd_classe IN ({veiculos})"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

dq = DataQuality(spark, DQ_TABLE, FONTE, ETAPA, BATCH_ID)
resultados = dq.executar(regras)
display(dq.resultados_df(resultados))

# COMMAND ----------

dq.validar(resultados)
