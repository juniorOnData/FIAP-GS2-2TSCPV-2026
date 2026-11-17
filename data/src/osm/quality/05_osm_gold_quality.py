# Databricks notebook source
# MAGIC %md
# MAGIC # 05_osm_gold_quality
# MAGIC
# MAGIC Regras de qualidade da Gold do OpenStreetMap: `dim_osm_road_class` e `dim_osm_roads`. Roda depois de `05_dim_osm_roads`.
# MAGIC
# MAGIC 1. Regras `ERRO` que falham param a tarefa e o job.
# MAGIC 2. Regras `AVISO` só registram.
# MAGIC 3. Resultados em `fiap_analytics.bronze.osm_data_quality_log`, uma linha por regra e execução.

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
from common.data_quality import DataQuality, condicao, consulta, existe_em, nao_nulo, nao_vazia, unico  # noqa: E402

DQ_TABLE = "fiap_analytics.bronze.osm_data_quality_log"
FONTE = "osm"
ETAPA = "gold"
BATCH_ID = str(uuid.uuid4())

SILVER = "fiap_analytics.silver.osm_roads"
DIM_CLASSE = "fiap_analytics.gold.dim_osm_road_class"
DIM_VIA = "fiap_analytics.gold.dim_osm_roads"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras

# COMMAND ----------


def membro_desconhecido(dim, chave):
    return consulta(
        "membro_desconhecido", dim, "A dimensão tem exatamente uma linha -1 (não informado)",
        f"SELECT CASE WHEN COUNT_IF({chave} = -1) = 1 THEN 0 ELSE 1 END AS falhas, 1 AS total FROM {dim}",
    )


regras = [
    # Dimensão de classes
    nao_vazia(DIM_CLASSE),
    unico(DIM_CLASSE, ["COD_DIM_CLASSE_VIA"]),
    unico(DIM_CLASSE, ["COD_CLASSE"]),
    membro_desconhecido(DIM_CLASSE, "COD_DIM_CLASSE_VIA"),

    # Dimensão de vias: volume e conciliação com a Silver
    nao_vazia(DIM_VIA),
    consulta(
        "vias_e_extensao_iguais_silver_por_classe", DIM_VIA,
        "Por classe, a dimensão tem as mesmas vias e a mesma extensão da Silver",
        f"""SELECT COUNT_IF(NOT (s.n <=> g.n) OR ABS(COALESCE(s.km, 0) - COALESCE(g.km, 0)) > 1e-6) AS falhas, COUNT(*) AS total
            FROM (SELECT cd_classe AS classe, COUNT(*) AS n, SUM(nr_comprimento_km) AS km FROM {SILVER} GROUP BY 1) s
            FULL OUTER JOIN (SELECT c.COD_CLASSE AS classe, COUNT(*) AS n, SUM(v.NUM_COMPRIMENTO_KM) AS km
                             FROM {DIM_VIA} v JOIN {DIM_CLASSE} c ON v.COD_DIM_CLASSE_VIA = c.COD_DIM_CLASSE_VIA
                             WHERE v.COD_DIM_VIA <> -1 GROUP BY 1) g
              ON s.classe <=> g.classe""",
    ),
    unico(DIM_VIA, ["COD_DIM_VIA"]),
    unico(DIM_VIA, ["NUM_OSM_ID"]),
    membro_desconhecido(DIM_VIA, "COD_DIM_VIA"),

    # Dimensão de vias: integridade e geometria
    existe_em(DIM_VIA, "COD_DIM_CLASSE_VIA", DIM_CLASSE),
    condicao("classe_nao_informada", DIM_VIA, "Vias sem classe identificada (-1)",
             "COD_DIM_CLASSE_VIA = -1", severidade="AVISO", filtro="COD_DIM_VIA <> -1"),
    nao_nulo(DIM_VIA, "GEO_WKB", filtro="COD_DIM_VIA <> -1"),
    condicao("centroide_proximo_ao_envelope", DIM_VIA,
             "Centroide dentro do envelope da via (margem de 0,0001°; numa linha ele pode ficar fora do traçado, mas não do envelope)",
             "NUM_CENTROIDE_LATITUDE NOT BETWEEN NUM_LAT_MIN - 0.0001 AND NUM_LAT_MAX + 0.0001 "
             "OR NUM_CENTROIDE_LONGITUDE NOT BETWEEN NUM_LON_MIN - 0.0001 AND NUM_LON_MAX + 0.0001",
             filtro="COD_DIM_VIA <> -1"),
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
