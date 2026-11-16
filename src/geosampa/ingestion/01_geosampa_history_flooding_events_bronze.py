# Databricks notebook source
# MAGIC %md
# MAGIC # 01_geosampa_history_flooding_events_bronze
# MAGIC
# MAGIC Carrega na Bronze os arquivos de ocorrências (alagamento, deslizamento, inundação, queda de árvore) do GeoSampa enviados para o volume `fiap_analytics.bronze.geosampa_history_flooding_events`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Aceita `.gpkg` ou `.zip` com `.gpkg` dentro. O ano é lido do nome do arquivo (`20AA`).
# MAGIC 2. Cada camada do GeoPackage vira linhas com os atributos em string e a geometria em WKB (`geom_wkb`).
# MAGIC 3. Arquivo já carregado (mesmo SHA256) é pulado.
# MAGIC 4. Conferência por camada: linhas lidas precisam bater com as gravadas. Uma linha por arquivo no `geosampa_ingestion_log`.

# COMMAND ----------

import glob
import hashlib
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

# COMMAND ----------

VOLUME_PATH = "/Volumes/fiap_analytics/bronze/geosampa_history_flooding_events"
TABLE = "fiap_analytics.bronze.geosampa_history_flooding_events"

ENVELOPE_SIZE = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}

BATCH_ID = str(uuid.uuid4())

# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.geosampa_ingestion_log"
FONTE = "geosampa_history_flooding_events"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS fiap_analytics.bronze.geosampa_history_flooding_events;

# COMMAND ----------

def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gpkg_to_wkb(blob):
    """Remove o cabecalho do GeoPackage e devolve o WKB padrao."""
    if blob is None:
        return None
    data = bytes(blob)
    if data[:2] != b"GP":
        return None
    flags = data[3]
    if (flags >> 4) & 1:  # geometria vazia
        return None
    return data[8 + ENVELOPE_SIZE[(flags >> 1) & 7]:]


def to_text(value):
    return None if value is None else str(value)


def is_loaded(sha):
    if not spark.catalog.tableExists(TABLE):
        return False
    return not spark.table(TABLE).filter(F.col("_source_sha256") == sha).isEmpty()


def read_gpkg(local_gpkg, source_file, sha, year):
    """Le cada camada do GPKG e devolve [(camada, n_linhas, DataFrame)]."""
    conn = sqlite3.connect(local_gpkg)
    result = []
    try:
        layers = [r[0] for r in conn.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'")]
        for layer in layers:
            geom = conn.execute(
                "SELECT column_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
                (layer,)).fetchone()
            geom_col, srs_id = geom if geom else (None, -1)

            attrs = [r[1] for r in conn.execute(f'PRAGMA table_info("{layer}")')
                     if r[1] != geom_col]
            cols = [f'"{c}"' for c in attrs] + [f'"{geom_col}"' if geom_col else "NULL"]
            raw = conn.execute(f'SELECT {", ".join(cols)} FROM "{layer}"').fetchall()

            schema = StructType(
                [StructField(c, StringType(), True) for c in attrs]
                + [StructField("geom_wkb", BinaryType(), True)]
            )
            rows = [tuple(to_text(v) for v in r[:-1]) + (gpkg_to_wkb(r[-1]),) for r in raw]

            df = (
                spark.createDataFrame(rows, schema)
                .withColumn("_source_file", F.lit(source_file))
                .withColumn("_source_layer", F.lit(layer))
                .withColumn("_source_year", F.lit(year).cast("int"))
                .withColumn("_source_sha256", F.lit(sha))
                .withColumn("_source_srs_id", F.lit(int(srs_id)).cast("int"))
                .withColumn("_ingested_at", F.current_timestamp())
            )
            result.append((layer, len(rows), df))
    finally:
        conn.close()
    return result

# COMMAND ----------

sources = sorted(
    p for p in glob.glob(f"{VOLUME_PATH}/**/*", recursive=True)
    if os.path.isfile(p) and p.lower().endswith((".gpkg", ".zip"))
)

frames = []
expected = Counter()
pending_log = []  # (arquivo, sha256, janela, iniciado_em) dos arquivos lidos, registrados depois da gravação

for src in sources:
    sha = sha256_of(src)
    if is_loaded(sha):
        print(f"PULADO (ja carregado): {os.path.basename(src)}")
        continue

    match = re.search(r"(20\d{2})", os.path.basename(src))
    year = int(match.group(1)) if match else None
    janela = str(year) if year else os.path.basename(src)
    started = datetime.now(timezone.utc)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            if src.lower().endswith(".zip"):
                with zipfile.ZipFile(src) as z:
                    for name in z.namelist():
                        target = os.path.realpath(os.path.join(tmp, name))
                        if not target.startswith(os.path.realpath(tmp) + os.sep):
                            raise ValueError(f"Caminho suspeito no zip: {name}")
                    z.extractall(tmp)
                gpkgs = [p for p in glob.glob(f"{tmp}/**/*", recursive=True)
                         if p.lower().endswith(".gpkg")]
            else:
                local = os.path.join(tmp, "source.gpkg")
                shutil.copy(src, local)
                gpkgs = [local]

            for g in gpkgs:
                for layer, n, df in read_gpkg(g, src, sha, year):
                    frames.append(df)
                    expected[(sha, layer)] += n
                    print(f"LIDO: {os.path.basename(src)} / {layer}: {n} linhas (ano {year})")
    except Exception as exc:
        registrar(janela, "FALHA", started, arquivo=src, sha256=sha, mensagem=str(exc))
        raise

    pending_log.append((src, sha, janela, started))

# COMMAND ----------

if not frames:
    print("Nada novo para carregar.")
else:
    try:
        bronze = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

        total_expected = sum(expected.values())
        total_df = bronze.count()
        assert total_df == total_expected, f"Lido {total_expected}, no DataFrame {total_df}"

        (bronze.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TABLE))

        target = spark.table(TABLE)
        for (sha, layer), n in expected.items():
            got = target.filter(
                (F.col("_source_sha256") == sha) & (F.col("_source_layer") == layer)
            ).count()
            assert got == n, f"Camada {layer} ({sha[:8]}): lido {n}, gravado {got}"
    except Exception as exc:
        for src, sha, janela, started in pending_log:
            registrar(janela, "FALHA", started, arquivo=src, sha256=sha, mensagem=str(exc))
        raise

    for src, sha, janela, started in pending_log:
        rows = sum(n for (s, _), n in expected.items() if s == sha)
        registrar(janela, "SUCESSO" if rows else "SEM_DADOS", started, registros=rows, arquivo=src, sha256=sha)
    print(f"OK: {total_expected} linhas gravadas e conferidas em {TABLE}")

display(
    spark.table(TABLE).groupBy("_source_year").agg(F.count("*").alias("rows")).orderBy("_source_year")
)
