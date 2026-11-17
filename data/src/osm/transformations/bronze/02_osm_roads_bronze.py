# Databricks notebook source
# MAGIC %md
# MAGIC # 02_osm_roads_bronze
# MAGIC
# MAGIC Carrega na Bronze (`fiap_analytics.bronze.osm_roads`) as vias do OpenStreetMap no recorte do município de São Paulo, a partir do extrato da Geofabrik gravado no volume `fiap_analytics.bronze.osm_roads`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Lê a coleta mais recente do volume (manifesto). Se ela já está na tabela (mesmo SHA256), não faz nada. Com `recarregar = sim`, carrega de novo.
# MAGIC 2. Recorte pelo envelope do município com margem de 0,01° (widget `envelope`: lon_min, lat_min, lon_max, lat_max). O extrato é da região Sudeste inteira (quase 3 milhões de vias): entram as vias cujo envelope cruza o recorte. O arquivo completo continua no volume.
# MAGIC 3. Uma linha por via (*way* do OSM): atributos do `.dbf` em texto (`osm_id`, `code`, `fclass`, `name`, `ref`, `oneway`, `maxspeed`, `layer`, `bridge`, `tunnel`) e geometria em WKB (`geom_wkb`, WGS84, EPSG 4326).
# MAGIC 4. Uma versão por vez: a tabela é substituída pela versão nova.
# MAGIC 5. Conferência: vias gravadas precisam bater com as lidas no recorte. Uma linha no `osm_ingestion_log`.
# MAGIC
# MAGIC Dados © colaboradores do OpenStreetMap, ODbL 1.0.

# COMMAND ----------

dbutils.widgets.text("envelope", "-46.84,-24.02,-46.35,-23.34", "Recorte: lon_min,lat_min,lon_max,lat_max")
dbutils.widgets.dropdown("recarregar", "nao", ["nao", "sim"], "Recarregar a versão já carregada")

# COMMAND ----------

import itertools
import os
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "osm_roads"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TABLE = f"{CATALOG}.{SCHEMA}.osm_roads"
ENVELOPE = tuple(float(v) for v in dbutils.widgets.get("envelope").split(","))
RECARREGAR = dbutils.widgets.get("recarregar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)
LOTE = 100_000  # linhas por DataFrame enviado ao Spark

if len(ENVELOPE) != 4 or not (ENVELOPE[0] < ENVELOPE[2] and ENVELOPE[1] < ENVELOPE[3]):
    raise ValueError(f"Envelope inválido: {ENVELOPE}")

# Módulos de src/common. Sobe pelas pastas até achar src/ para importar.
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

LOG_TABLE = "fiap_analytics.bronze.osm_ingestion_log"
FONTE = "osm_roads"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

manifesto = ultimo_manifesto(VOLUME_PATH)
if manifesto is None:
    raise RuntimeError(f"Nenhuma coleta em {VOLUME_PATH}. Rode antes o 01_osm_roads_landing.")

carregada = (spark.catalog.tableExists(TABLE)
             and not spark.table(TABLE).filter(F.col("_source_sha256") == manifesto["sha256"]).isEmpty())
print(f"Extrato mais recente: {manifesto['snapshot_date']} ({manifesto['sha256'][:12]}) | já carregado: {carregada}")

# COMMAND ----------

if carregada and not RECARREGAR:
    print("Nada novo para carregar.")
else:
    try:
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(manifesto["data_path"]) as z:
                shp = [n for n in z.namelist() if n.lower().endswith(".shp")]
                if len(shp) != 1:
                    raise RuntimeError(f"Esperava um shapefile no zip, há {shp}")
                for nome in z.namelist():
                    if os.path.basename(nome) != nome:
                        raise ValueError(f"Caminho inesperado no zip: {nome}")
                z.extractall(tmp)
            caminho = os.path.join(tmp, shp[0])
            srid = geo.srid_shapefile(caminho)
            if srid != geo.SRID_WGS84:
                raise RuntimeError(f"Esperava WGS84 (EPSG 4326) no .prj, veio {srid}")

            registros = geo.ler_shapefile(caminho, filtro_envelope=ENVELOPE)
            primeiro = next(registros, None)
            if primeiro is None:
                raise RuntimeError(f"Nenhuma via no recorte {ENVELOPE}")
            campos = list(primeiro[1])
            colunas = [c.lower() for c in campos]
            schema = T.StructType(
                [T.StructField("_source_feature_id", T.IntegerType())]
                + [T.StructField(c, T.StringType()) for c in colunas]
                + [T.StructField("geom_wkb", T.BinaryType())]
            )

            # Grava em lotes para não montar um DataFrame enorme de uma vez no driver.
            lidos, lote, modo = 0, [], "overwrite"

            def gravar(linhas, modo):
                (spark.createDataFrame(linhas, schema)
                 .withColumn("_source_file", F.lit(manifesto["data_path"]))
                 .withColumn("_source_layer", F.lit(os.path.splitext(shp[0])[0]))
                 .withColumn("_source_srs_id", F.lit(srid).cast("int"))
                 .withColumn("_source_sha256", F.lit(manifesto["sha256"]))
                 .withColumn("_source_version", F.lit(manifesto["snapshot_date"]))
                 .withColumn("_batch_id", F.lit(BATCH_ID))
                 .withColumn("_ingested_at", F.lit(STARTED_AT).cast("timestamp"))
                 .write.format("delta").mode(modo).option("overwriteSchema", "true").saveAsTable(TABLE))

            for indice, attrs, wkb in itertools.chain([primeiro], registros):
                lote.append((indice,) + tuple(attrs.get(c) for c in campos) + (wkb,))
                if len(lote) >= LOTE:
                    gravar(lote, modo)
                    lidos, lote, modo = lidos + len(lote), [], "append"
            if lote:
                gravar(lote, modo)
                lidos += len(lote)

        spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Vias do OpenStreetMap no recorte do município de São Paulo "
                  "(extrato da Geofabrik, camada gis_osm_roads_free_1): atributos em texto e geometria em WKB (EPSG 4326). "
                  "© OpenStreetMap contributors, ODbL 1.0.'")
        gravados = spark.table(TABLE).filter(F.col("_batch_id") == BATCH_ID).count()
        total = spark.table(TABLE).count()
        assert gravados == lidos == total, f"Lidas {lidos}, gravadas nesta carga {gravados}, na tabela {total}"
    except Exception as exc:
        registrar(manifesto["snapshot_date"], "FALHA", STARTED_AT, arquivo=manifesto["data_path"],
                  sha256=manifesto["sha256"], mensagem=str(exc))
        raise

    registrar(manifesto["snapshot_date"], "SUCESSO", STARTED_AT, registros=gravados, arquivo=manifesto["data_path"],
              sha256=manifesto["sha256"], mensagem=f"recorte {ENVELOPE}")
    print(f"OK: {gravados} vias gravadas e conferidas em {TABLE}")

display(spark.table(TABLE).groupBy("fclass").agg(F.count("*").alias("vias")).orderBy(F.desc("vias")))
