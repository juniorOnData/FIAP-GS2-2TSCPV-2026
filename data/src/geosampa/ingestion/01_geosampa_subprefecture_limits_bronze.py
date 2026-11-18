# Databricks notebook source
# MAGIC %md
# MAGIC # 01_geosampa_subprefecture_limits_bronze
# MAGIC
# MAGIC Limites das subprefeituras (polígonos) a partir do shapefile do GeoSampa, camada *Limites Administrativos › Subprefeitura*.
# MAGIC
# MAGIC **Carga manual.** O GeoSampa protege os downloads com um desafio (CAPTCHA), então o arquivo não pode ser baixado pelo job. Baixe o shapefile no portal (Download de dados › Limites Administrativos › Subprefeitura, em SIRGAS 2000) e envie o `.zip`, ou os arquivos `.shp`, `.shx`, `.dbf` e `.prj`, para o Volume `fiap_analytics.bronze.geosampa_subprefecture_limits`. Os limites mudam raramente: basta repetir quando a Prefeitura publicar uma versão nova.
# MAGIC
# MAGIC 1. Usa o arquivo mais recente do Volume. Sem arquivo, registra `SEM_DADOS` e termina sem erro: o pipeline segue sem os limites.
# MAGIC 2. Se o SHA-256 do arquivo já está na Bronze, não recarrega.
# MAGIC 3. Lê o shapefile em Python puro (`src/common/geo.py`), converte para SIRGAS 2000 geográfico (EPSG 4674) e calcula área, centroide e envelope em UTM 23S.
# MAGIC 4. Grava a tabela inteira (substitui a versão anterior). Os atributos do shapefile ficam em JSON: a Gold descobre qual campo traz o nome ou a sigla comparando com o cadastro das subprefeituras.

# COMMAND ----------

import glob
import hashlib
import json
import os
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone

from pyspark.sql import types as T

VOLUME_PATH = "/Volumes/fiap_analytics/bronze/geosampa_subprefecture_limits"
TABLE = "fiap_analytics.bronze.geosampa_subprefecture_limits"
BATCH_ID = str(uuid.uuid4())

_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common import geo  # noqa: E402
from common.ingestion_log import IngestionLog  # noqa: E402

registrar = IngestionLog(spark, "fiap_analytics.bronze.geosampa_ingestion_log", "geosampa_subprefecture_limits", "bronze", BATCH_ID).registrar

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS fiap_analytics.bronze.geosampa_subprefecture_limits;

# COMMAND ----------


def sha256_of(caminhos):
    digest = hashlib.sha256()
    for caminho in sorted(caminhos):
        with open(caminho, "rb") as f:
            for bloco in iter(lambda: f.read(1 << 20), b""):
                digest.update(bloco)
    return digest.hexdigest()


inicio = datetime.now(timezone.utc)
zips = glob.glob(f"{VOLUME_PATH}/**/*.zip", recursive=True)
shps = glob.glob(f"{VOLUME_PATH}/**/*.shp", recursive=True)
candidatos = [(os.path.getmtime(p), p) for p in zips + shps]
if not candidatos:
    registrar("limites", "SEM_DADOS", inicio, mensagem=f"Nenhum .zip ou .shp em {VOLUME_PATH}: envie o shapefile das subprefeituras do GeoSampa.")
    dbutils.notebook.exit("SEM_DADOS: nenhum arquivo de limites no Volume")
_, origem = max(candidatos)
arquivos_origem = [origem] if origem.endswith(".zip") else glob.glob(os.path.splitext(origem)[0] + ".*")
sha = sha256_of(arquivos_origem)
print(f"Arquivo: {origem} | SHA-256 {sha[:12]}")

if spark.catalog.tableExists(TABLE) and not spark.table(TABLE).where(f"_source_sha256 = '{sha}'").isEmpty():
    registrar("limites", "SEM_DADOS", inicio, arquivo=origem, sha256=sha, mensagem="Arquivo já carregado.")
    dbutils.notebook.exit("Arquivo já carregado")

# COMMAND ----------

try:
    with tempfile.TemporaryDirectory() as tmp:
        if origem.endswith(".zip"):
            with zipfile.ZipFile(origem) as z:
                for nome in z.namelist():
                    destino = os.path.realpath(os.path.join(tmp, nome))
                    if not destino.startswith(os.path.realpath(tmp) + os.sep):
                        raise ValueError(f"Caminho inseguro dentro do zip: {nome}")
                z.extractall(tmp)
            shp = glob.glob(f"{tmp}/**/*.shp", recursive=True)
            if len(shp) != 1:
                raise ValueError(f"O zip deve ter exatamente um .shp; tem {len(shp)}: {shp}")
            shp = shp[0]
        else:
            shp = origem
        srid = geo.srid_shapefile(shp)
        if srid is None:
            raise ValueError("Sistema de referência não reconhecido no .prj (use SIRGAS 2000, UTM 23S ou geográfico).")
        linhas = []
        for indice, atributos, wkb in geo.ler_shapefile(shp):
            g = geo.descrever(wkb, srid)
            if not g:
                continue
            linhas.append((indice, json.dumps(atributos, ensure_ascii=False), g["geom_wkb"], g["area_km2"], g["centroide_lat"],
                           g["centroide_lon"], g["lat_min"], g["lat_max"], g["lon_min"], g["lon_max"]))
    if not linhas:
        raise ValueError("O shapefile não tem polígonos.")
    schema = T.StructType([
        T.StructField("nr_id_feicao", T.IntegerType()), T.StructField("ds_atributos_json", T.StringType()),
        T.StructField("geom_wkb", T.BinaryType()), T.StructField("nr_area_km2", T.DoubleType()),
        T.StructField("nr_centroide_latitude", T.DoubleType()), T.StructField("nr_centroide_longitude", T.DoubleType()),
        T.StructField("nr_lat_min", T.DoubleType()), T.StructField("nr_lat_max", T.DoubleType()),
        T.StructField("nr_lon_min", T.DoubleType()), T.StructField("nr_lon_max", T.DoubleType()),
    ])
    from pyspark.sql import functions as F
    df = (spark.createDataFrame(linhas, schema)
          .withColumn("_source_file", F.lit(os.path.basename(origem))).withColumn("_source_sha256", F.lit(sha))
          .withColumn("_source_srs_id", F.lit(srid)).withColumn("_ingested_at", F.current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
    area = sum(r[3] or 0 for r in linhas)
    registrar("limites", "SUCESSO", inicio, registros=len(linhas), arquivo=origem, sha256=sha)
    print(f"{len(linhas)} polígonos, {area:.1f} km² no total (SRID de origem {srid})")
except Exception as exc:
    registrar("limites", "FALHA", inicio, arquivo=origem, sha256=sha, mensagem=str(exc))
    raise
