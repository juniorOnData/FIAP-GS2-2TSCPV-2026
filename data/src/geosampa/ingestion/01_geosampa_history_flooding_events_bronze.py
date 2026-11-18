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
# MAGIC 4. Uma versão por ano: o volume deve ter um arquivo por ano (a carga para se houver dois). Quando o arquivo de um ano é trocado por uma versão corrigida (SHA256 novo), a nova é carregada e as linhas da anterior são removidas.
# MAGIC 5. Conferência por camada: linhas lidas precisam bater com as gravadas. Uma linha por arquivo no `geosampa_ingestion_log`.

# MAGIC
# MAGIC ## Etapas do notebook
# MAGIC
# MAGIC 1. Lista recursivamente arquivos `.gpkg` e `.zip` no Volume. A carga é manual: os arquivos devem ser enviados ao Volume antes da execução.
# MAGIC 2. Agrupa os arquivos pelo ano identificado no nome. Se houver mais de um arquivo para o mesmo ano, registra `FALHA` e interrompe o processamento.
# MAGIC 3. Calcula o SHA-256. Arquivos com hash já presente na Bronze são ignorados; arquivos alterados seguem para leitura.
# MAGIC 4. Em arquivos ZIP, valida os caminhos internos antes de extrair os GeoPackages em uma pasta temporária.
# MAGIC 5. Lê as camadas vetoriais, anexa os metadados de origem e grava o lote com `mergeSchema` para acomodar variações entre camadas e anos.
# MAGIC 6. Confere as quantidades por hash e camada. Quando a carga de um ano é substituída, remove a versão anterior depois da gravação bem-sucedida.
# MAGIC 7. Registra sucesso, ausência de linhas ou falha no log comum de ingestão.
# MAGIC
# MAGIC ## Funções locais
# MAGIC
# MAGIC - `sha256_of(path)` calcula o hash do arquivo em blocos de 1 MiB, sem carregar o arquivo inteiro na memória.
# MAGIC - `gpkg_to_wkb(blob)` remove o cabeçalho GeoPackage e os bytes opcionais de envelope, devolvendo a geometria WKB. Geometrias nulas ou vazias retornam `None`.
# MAGIC - `to_text(value)` preserva nulos e converte atributos da camada em texto, formato usado na Bronze.
# MAGIC - `is_loaded(sha)` verifica se aquele conteúdo já existe na tabela Bronze.
# MAGIC - `source_year(src)` extrai o primeiro ano no formato `20AA` do nome do arquivo; sem ano, retorna `None`.
# MAGIC - `version_key(src)` define a chave usada para impedir duas versões ativas: o ano do arquivo ou, sem ano, o caminho.
# MAGIC - `remove_superseded()` mantém a versão de maior `_ingested_at` para cada chave e apaga hashes anteriores. Também confirma que não restaram versões duplicadas.
# MAGIC - `read_gpkg(local_gpkg, source_file, sha, year)` consulta o catálogo SQLite do GeoPackage, lê cada camada vetorial e devolve o nome da camada, a contagem e o DataFrame Spark com atributos e metadados.
# MAGIC
# MAGIC Erros de leitura, schema ou conferência são registrados no log antes de serem propagados, para que o job sinalize falha.

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


def source_year(src):
    match = re.search(r"(20\d{2})", os.path.basename(src))
    return int(match.group(1)) if match else None


def version_key(src):
    """Chave da versão: o ano do arquivo, ou o caminho quando o nome não tem ano."""
    year = source_year(src)
    return str(year) if year else src


# Mesma chave, calculada sobre a tabela.
VERSION_KEY = F.coalesce(F.col("_source_year").cast("string"), F.col("_source_file"))


def remove_superseded():
    """Mantém, para cada ano, só a versão carregada por último. Devolve {chave: linhas removidas}."""
    if not spark.catalog.tableExists(TABLE):
        return Counter()
    versions = (
        spark.table(TABLE)
        .groupBy(VERSION_KEY.alias("key"), "_source_sha256")
        .agg(F.max("_ingested_at").alias("loaded_at"), F.count("*").alias("rows"))
        .collect()
    )
    latest = {}
    for v in versions:
        if v.key not in latest or v.loaded_at > latest[v.key].loaded_at:
            latest[v.key] = v
    old = [v for v in versions if v._source_sha256 != latest[v.key]._source_sha256]
    if old:
        shas = ", ".join(f"'{v._source_sha256}'" for v in old)
        spark.sql(f"DELETE FROM {TABLE} WHERE _source_sha256 IN ({shas})")

    left = (spark.table(TABLE).groupBy(VERSION_KEY.alias("key"))
            .agg(F.countDistinct("_source_sha256").alias("n")).filter("n > 1").collect())
    assert not left, f"Ainda há mais de uma versão para: {[r.key for r in left]}"

    removed = Counter()
    for v in old:
        removed[v.key] += v.rows
        print(f"SUBSTITUIDO: versão anterior de {v.key} ({v._source_sha256[:8]}, {v.rows} linhas) removida")
    return removed


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

# Um arquivo por ano no volume: com dois, a versão antiga e a nova se substituiriam a cada execução.
by_key = {}
for src in sources:
    by_key.setdefault(version_key(src), []).append(os.path.basename(src))
conflicts = {k: v for k, v in by_key.items() if len(v) > 1}
if conflicts:
    for key, names in conflicts.items():
        registrar(key, "FALHA", datetime.now(timezone.utc),
                  mensagem=f"mais de um arquivo para o mesmo ano no volume: {names}")
    raise RuntimeError(f"Mais de um arquivo para o mesmo ano no volume: {conflicts}. "
                       "Deixe só a versão mais recente de cada ano e rode de novo.")

frames = []
expected = Counter()
pending_log = []  # (arquivo, sha256, janela, iniciado_em) dos arquivos lidos, registrados depois da gravação

for src in sources:
    sha = sha256_of(src)
    if is_loaded(sha):
        print(f"PULADO (ja carregado): {os.path.basename(src)}")
        continue

    year = source_year(src)
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
    # Termina a limpeza de uma execução interrompida entre a gravação e a remoção da versão anterior.
    remove_superseded()
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

        # Arquivo corrigido de um ano já carregado: a versão nova fica e a anterior sai.
        removed = remove_superseded()
    except Exception as exc:
        for src, sha, janela, started in pending_log:
            registrar(janela, "FALHA", started, arquivo=src, sha256=sha, mensagem=str(exc))
        raise

    for src, sha, janela, started in pending_log:
        rows = sum(n for (s, _), n in expected.items() if s == sha)
        replaced = removed.get(version_key(src), 0)
        registrar(janela, "SUCESSO" if rows else "SEM_DADOS", started, registros=rows, arquivo=src, sha256=sha,
                  mensagem=f"substituiu a versão anterior do arquivo ({replaced} linhas removidas)" if replaced else None)
    print(f"OK: {total_expected} linhas gravadas e conferidas em {TABLE}")

display(
    spark.table(TABLE).groupBy("_source_year").agg(F.count("*").alias("rows")).orderBy("_source_year")
)
