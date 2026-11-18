# Databricks notebook source
# MAGIC %md
# MAGIC # 03_sgb_susceptibility_areas_silver
# MAGIC
# MAGIC ## Funções auxiliares
# MAGIC
# MAGIC `coluna` localiza um atributo mesmo quando o nome varia entre camadas do shapefile. `numero` converte valores numéricos recebidos como texto e retorna nulo para conteúdo ausente ou inválido.
# MAGIC
# MAGIC Recria a Silver das áreas da carta de suscetibilidade do SGB (`fiap_analytics.silver.sgb_susceptibility_areas`) a partir da Bronze: uma linha por feição, com camada, classe padronizada e geometria padronizada.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Camadas (`cd_camada`): `inundacao` (inundação, alagamento e assoreamento), `bacia_inundacao` (bacias de drenagem que contribuem para as inundações, que vão além do município), `enxurrada`, `movimento_massa` (deslizamento, queda de rocha e rastejo), `corrida_massa` e `relevo` (padrões de relevo). Uma camada que não esteja na lista entra com o nome do arquivo e é apontada pela qualidade.
# MAGIC 2. Classe (`dc_classe`): `Alta`, `Média` ou `Baixa`, com `nr_grau_suscetibilidade` 3, 2 ou 1. As camadas de enxurrada e corrida de massa não têm graduação (`Sem graduação`). No relevo, a classe é o padrão de relevo (Colinas, Morrotes...).
# MAGIC 3. Geometria: a Bronze está em SIRGAS 2000 / UTM 23S (EPSG 31983). A Silver grava `geom_wkb` em SIRGAS 2000 geográfico (EPSG 4674, longitude e latitude), o padrão das fontes geoespaciais do projeto, e calcula em UTM a área (`nr_area_km2`), o centroide e o envelope (`nr_lat_min`... `nr_lon_max`, útil para pré-filtrar cruzamentos espaciais em SQL). A conversão usa `src/common/geo.py`, no driver (são poucos milhares de feições).
# MAGIC 4. `nr_area_fonte_km2` traz a área informada pela própria fonte (`Shape_Area`), quando existe, para a qualidade conferir a conversão.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F
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

BRONZE = "fiap_analytics.bronze.sgb_susceptibility_areas"
SILVER = "fiap_analytics.silver.sgb_susceptibility_areas"

# Shapefile da carta -> (código da camada, descrição). A ordem é a da legenda da carta.
CAMADAS = {
    "SuscetibilidadeInundacao": ("inundacao", "Suscetibilidade a inundação, alagamento e assoreamento"),
    "SuscetibilidadeBacias": ("bacia_inundacao", "Bacias de drenagem contribuintes à inundação"),
    "SuscetibilidadeEnxurradas": ("enxurrada", "Áreas suscetíveis a enxurrada"),
    "SuscetibilidadeMovimentoDeMassa": ("movimento_massa", "Suscetibilidade a movimentos gravitacionais de massa"),
    "SuscetibilidadeCorridas": ("corrida_massa", "Áreas suscetíveis a corrida de massa"),
    "Relevo": ("relevo", "Padrões de relevo"),
}
CLASSES = {"alta": ("Alta", 3), "media": ("Média", 2), "média": ("Média", 2), "baixa": ("Baixa", 1)}
CAMADAS_SEM_GRADUACAO = {"enxurrada", "corrida_massa"}

# COMMAND ----------

colunas_bronze = set(spark.table(BRONZE).columns)


def coluna(nome):
    return F.col(nome) if nome in colunas_bronze else F.lit(None).cast("string")


linhas = (
    spark.table(BRONZE)
    .select("_source_layer", "_source_feature_id", coluna("classe").alias("classe"),
            coluna("processo").alias("processo"), coluna("shape_area").alias("shape_area"),
            "geom_wkb", "_source_srs_id", "_source_sha256", "_source_version", "_ingested_at")
    .collect()
)
print(f"{len(linhas)} feições na Bronze")

# COMMAND ----------


def numero(texto):
    try:
        return float(texto.replace(",", ".")) if texto else None
    except ValueError:
        return None


saida = []
for r in linhas:
    cd_camada, dc_camada = CAMADAS.get(r["_source_layer"], (r["_source_layer"].lower(), r["_source_layer"]))
    if cd_camada in CAMADAS_SEM_GRADUACAO:
        dc_classe, grau = "Sem graduação", None
    elif cd_camada == "relevo":
        dc_classe, grau = r["classe"], None
    else:
        dc_classe, grau = CLASSES.get((r["classe"] or "").strip().lower(), (r["classe"], None))
    g = geo.descrever(r["geom_wkb"], r["_source_srs_id"]) or {}
    area_fonte = numero(r["shape_area"])
    saida.append((
        cd_camada, dc_camada, r["_source_feature_id"], dc_classe, grau, r["processo"],
        g.get("geom_wkb"), g.get("tipo"), g.get("pontos"), g.get("area_km2"),
        area_fonte / 1e6 if area_fonte is not None else None,
        g.get("centroide_lat"), g.get("centroide_lon"),
        g.get("lat_min"), g.get("lat_max"), g.get("lon_min"), g.get("lon_max"),
        r["_source_layer"], r["_source_srs_id"], r["_source_sha256"], r["_source_version"], r["_ingested_at"],
    ))

SCHEMA_SILVER = T.StructType([
    T.StructField("cd_camada", T.StringType()),
    T.StructField("dc_camada", T.StringType()),
    T.StructField("nr_id_feicao", T.IntegerType()),
    T.StructField("dc_classe", T.StringType()),
    T.StructField("nr_grau_suscetibilidade", T.IntegerType()),
    T.StructField("dc_processo", T.StringType()),
    T.StructField("geom_wkb", T.BinaryType()),
    T.StructField("dc_tipo_geometria", T.StringType()),
    T.StructField("nr_vertices", T.IntegerType()),
    T.StructField("nr_area_km2", T.DoubleType()),
    T.StructField("nr_area_fonte_km2", T.DoubleType()),
    T.StructField("nr_centroide_latitude", T.DoubleType()),
    T.StructField("nr_centroide_longitude", T.DoubleType()),
    T.StructField("nr_lat_min", T.DoubleType()),
    T.StructField("nr_lat_max", T.DoubleType()),
    T.StructField("nr_lon_min", T.DoubleType()),
    T.StructField("nr_lon_max", T.DoubleType()),
    T.StructField("_source_layer", T.StringType()),
    T.StructField("_source_srs_id", T.IntegerType()),
    T.StructField("_source_sha256", T.StringType()),
    T.StructField("_source_version", T.StringType()),
    T.StructField("_ingested_at", T.TimestampType()),
])

(spark.createDataFrame(saida, SCHEMA_SILVER)
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER))
spark.sql(f"""
    COMMENT ON TABLE {SILVER} IS
    'Silver das áreas da carta de suscetibilidade do SGB/CPRM e IPT (2015), município de São Paulo: uma linha por feição de cada camada, com classe padronizada, geometria em EPSG 4674 (longitude, latitude), área em km2 calculada em UTM 23S, centroide e envelope.'
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT cd_camada, dc_classe, nr_grau_suscetibilidade, COUNT(*) AS feicoes,
           ROUND(SUM(nr_area_km2), 2) AS area_km2, COUNT_IF(geom_wkb IS NULL) AS sem_geometria
    FROM {SILVER}
    GROUP BY ALL
    ORDER BY cd_camada, nr_grau_suscetibilidade DESC NULLS LAST, dc_classe
"""))
