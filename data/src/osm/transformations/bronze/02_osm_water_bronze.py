# Databricks notebook source
# MAGIC %md
# MAGIC # 02_osm_water_bronze
# MAGIC
# MAGIC Lê o zip mais recente do volume `fiap_analytics.bronze.osm_water` e grava duas tabelas Bronze, recortadas no envelope do município: `osm_waterways` (cursos d'água, linhas) e `osm_water_areas` (corpos d'água, polígonos). Atributos em texto e geometria em WKB (EPSG 4326), sem alteração. Uma versão já carregada não é relida, salvo com `recarregar = sim`.
# MAGIC
# MAGIC Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------

dbutils.widgets.text("envelope", "-46.84,-24.02,-46.35,-23.34", "Recorte: lon_min,lat_min,lon_max,lat_max")
dbutils.widgets.dropdown("recarregar", "nao", ["nao", "sim"], "Recarregar a versão já carregada")

# COMMAND ----------

import os
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

VOLUME_PATH = "/Volumes/fiap_analytics/bronze/osm_water"
TABELAS = {"gis_osm_waterways_free_1": "fiap_analytics.bronze.osm_waterways", "gis_osm_water_a_free_1": "fiap_analytics.bronze.osm_water_areas"}
ENVELOPE = tuple(float(v) for v in dbutils.widgets.get("envelope").split(","))
RECARREGAR = dbutils.widgets.get("recarregar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402
from common.arquivos import ultimo_manifesto  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

registrar = IngestionLog(spark, "fiap_analytics.bronze.osm_ingestion_log", "osm_water", "bronze", BATCH_ID).registrar

# COMMAND ----------

manifesto = ultimo_manifesto(VOLUME_PATH)
if manifesto is None:
    raise RuntimeError(f"Nenhuma coleta em {VOLUME_PATH}. Rode antes o 01_osm_water_landing.")
carregada = all(spark.catalog.tableExists(t) and not spark.table(t).filter(F.col("_source_sha256") == manifesto["sha256"]).isEmpty()
                for t in TABELAS.values())
print(f"Extrato mais recente: {manifesto['snapshot_date']} ({manifesto['sha256'][:12]}) | já carregado: {carregada}")

# COMMAND ----------

if carregada and not RECARREGAR:
    dbutils.notebook.exit("Nada novo para carregar")

try:
    contagens = {}
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(manifesto["data_path"]) as z:
            for nome in z.namelist():
                if os.path.basename(nome) != nome:
                    raise ValueError(f"Caminho inesperado no zip: {nome}")
            z.extractall(tmp)
        for camada, tabela in TABELAS.items():
            caminho = os.path.join(tmp, f"{camada}.shp")
            if not os.path.exists(caminho):
                raise RuntimeError(f"Camada {camada} ausente do zip {manifesto['data_path']}")
            srid = geo.srid_shapefile(caminho)
            if srid != geo.SRID_WGS84:
                raise RuntimeError(f"Esperava WGS84 (EPSG 4326) no .prj de {camada}, veio {srid}")
            registros = list(geo.ler_shapefile(caminho, filtro_envelope=ENVELOPE))
            if not registros:
                raise RuntimeError(f"Nenhuma feição de {camada} no recorte {ENVELOPE}")
            campos = list(registros[0][1])
            schema = T.StructType([T.StructField("_source_feature_id", T.IntegerType())]
                                  + [T.StructField(c.lower(), T.StringType()) for c in campos]
                                  + [T.StructField("geom_wkb", T.BinaryType())])
            linhas = [(i,) + tuple(a.get(c) for c in campos) + (w,) for i, a, w in registros]
            (spark.createDataFrame(linhas, schema)
             .withColumn("_source_file", F.lit(manifesto["data_path"])).withColumn("_source_layer", F.lit(camada))
             .withColumn("_source_srs_id", F.lit(srid).cast("int")).withColumn("_source_sha256", F.lit(manifesto["sha256"]))
             .withColumn("_source_version", F.lit(manifesto["snapshot_date"])).withColumn("_batch_id", F.lit(BATCH_ID))
             .withColumn("_ingested_at", F.lit(STARTED_AT).cast("timestamp"))
             .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela))
            gravados = spark.table(tabela).count()
            assert gravados == len(linhas), f"{tabela}: lidas {len(linhas)}, gravadas {gravados}"
            contagens[tabela] = gravados
except Exception as exc:
    registrar(manifesto["snapshot_date"], "FALHA", STARTED_AT, arquivo=manifesto["data_path"], sha256=manifesto["sha256"], mensagem=str(exc))
    raise

registrar(manifesto["snapshot_date"], "SUCESSO", STARTED_AT, registros=sum(contagens.values()), arquivo=manifesto["data_path"],
          sha256=manifesto["sha256"], mensagem=f"recorte {ENVELOPE}: {contagens}")
print(f"OK: {contagens}")
