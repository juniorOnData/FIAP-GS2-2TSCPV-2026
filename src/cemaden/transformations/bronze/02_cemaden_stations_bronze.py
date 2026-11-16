# Databricks notebook source
# MAGIC %md
# MAGIC # 02_cemaden_stations_bronze
# MAGIC
# MAGIC Carrega o cadastro de estações do volume `fiap_analytics.bronze.cemaden_stations` para a tabela `fiap_analytics.bronze.cemaden_stations`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Tudo em string, sem filtros além do recorte da consulta (município). Tipagem, deduplicação e filtros ficam na Silver.
# MAGIC 2. Cada coleta é um snapshot, identificado por `_snapshot_date`. O histórico de snapshots fica na tabela (permite ver estações ativadas e desativadas ao longo do tempo).
# MAGIC 3. Idempotência por data: a fatia do snapshot é substituída com `replaceWhere`. Por isso não se usa Auto Loader.
# MAGIC 4. Só entram arquivos que têm manifesto (o manifesto é gravado depois do arquivo de dados).
# MAGIC 5. Conferência: linhas lidas de cada arquivo precisam bater com o `record_count` do manifesto, e as linhas na tabela precisam bater com as lidas.
# MAGIC 6. Campos que não estiverem no schema esperado vão para `_rescued_data` (nada se perde).
# MAGIC
# MAGIC Widget `snapshot_date`:
# MAGIC
# MAGIC 1. `pending` (padrão): coletas que ainda não estão na Bronze, ou que foram recoletadas depois da última carga.
# MAGIC 2. `latest`: só a coleta mais recente.
# MAGIC 3. `all`: recarrega todas as coletas do volume.
# MAGIC 4. `AAAA-MM-DD`: uma data específica.

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "pending", "Snapshot: pending, latest, all ou AAAA-MM-DD")

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "fiap_analytics"
SCHEMA = "bronze"
VOLUME = "cemaden_stations"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.cemaden_stations"
DATASET = "cemaden_stations"
ENDPOINT = "/pcds-cadastro/dados-cadastrais"

SNAPSHOT_ARG = dbutils.widgets.get("snapshot_date").strip().lower() or "pending"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

# Campos do cadastro de estações da PED. Todos em string na Bronze.
# Qualquer campo extra que a API devolver cai em _rescued_data e aparece no aviso da validação.
STATION_FIELDS = [
    "id_estacao",
    "codestacao",
    "nome",
    "cidade",
    "codibge",
    "uf",
    "latitude",
    "longitude",
    "altitude",
    "id_tipoestacao",
    "tipoestacao_descricao",
    "id_rede",
    "rede_sigla",
    "data_instalacao",
    "dh_cadastro",
    "dh_inicio_inativo",
    "dh_ultima_remessa",
    "cota_atencao",
    "cota_alerta",
    "cota_transbordamento",
    "offset",
]
SOURCE_SCHEMA = T.StructType([T.StructField(c, T.StringType(), True) for c in STATION_FIELDS])

print(f"batch_id={BATCH_ID} | snapshot={SNAPSHOT_ARG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log de execução

# COMMAND ----------

# Log padrão da origem (src/common/ingestion_log.py). Sobe pelas pastas até achar src/ para importar o módulo.
_src = os.getcwd()
while not os.path.isdir(os.path.join(_src, "common")) and os.path.dirname(_src) != _src:
    _src = os.path.dirname(_src)
if not os.path.isdir(os.path.join(_src, "common")):
    raise RuntimeError(f"Pasta src/common não encontrada a partir de {os.getcwd()}")
if _src not in sys.path:
    sys.path.append(_src)
from common.ingestion_log import IngestionLog  # noqa: E402

LOG_TABLE = "fiap_analytics.bronze.cemaden_ingestion_log"
FONTE = "cemaden_stations"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos a carregar

# COMMAND ----------

def list_landing_files():
    """Lista (snapshot_date, caminho_do_arquivo, manifesto) de todas as coletas completas no volume."""
    found = []
    if not os.path.isdir(VOLUME_PATH):
        return found
    for folder in sorted(os.listdir(VOLUME_PATH)):
        if not folder.startswith("data_coleta="):
            continue
        folder_path = f"{VOLUME_PATH}/{folder}"
        for name in sorted(os.listdir(folder_path)):
            if not name.endswith(".manifest.json"):
                continue
            with open(f"{folder_path}/{name}", encoding="utf-8") as fh:
                manifest = json.load(fh)
            data_path = f"{folder_path}/{manifest['file_name']}"
            if os.path.exists(data_path):
                found.append((manifest["snapshot_date"], data_path, manifest))
            else:
                print(f"Aviso: manifesto sem arquivo de dados, ignorado: {name}")
    return found


all_files = list_landing_files()
if not all_files:
    raise RuntimeError(f"Nenhuma coleta encontrada em {VOLUME_PATH}. Rode antes o 01_cemaden_stations_landing.")

available_dates = sorted({d for d, _, _ in all_files})


def pending_dates():
    """Datas ainda não carregadas ou recoletadas depois da última carga (landing_batch_id diferente)."""
    if not spark.catalog.tableExists(BRONZE_TABLE):
        return available_dates
    loaded = {
        (r["d"], r["b"])
        for r in spark.table(BRONZE_TABLE)
        .select(F.date_format("_snapshot_date", "yyyy-MM-dd").alias("d"), F.col("_landing_batch_id").alias("b"))
        .distinct().collect()
    }
    return sorted({d for d, _, m in all_files if (d, m.get("landing_batch_id")) not in loaded})


if SNAPSHOT_ARG == "pending":
    dates = pending_dates()
    if not dates:
        print("Nada pendente: todas as coletas do volume já estão na Bronze.")
        dbutils.notebook.exit("nothing_pending")
elif SNAPSHOT_ARG == "latest":
    dates = [available_dates[-1]]
elif SNAPSHOT_ARG == "all":
    dates = available_dates
else:
    if SNAPSHOT_ARG not in available_dates:
        raise RuntimeError(f"Snapshot {SNAPSHOT_ARG} não existe no volume. Disponíveis: {available_dates}")
    dates = [SNAPSHOT_ARG]

# Todos os arquivos de uma mesma data entram juntos, porque a fatia da data é substituída inteira.
selected = [(d, p, m) for d, p, m in all_files if d in dates]
paths = [p for _, p, _ in selected]
print(f"Snapshots a carregar: {dates}")
for _, p, m in selected:
    print(f"  {p} | {m['record_count']} registros | coletado em {m['collected_at_utc']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura e metadados

# COMMAND ----------

manifest_df = spark.createDataFrame(
    [(m["file_name"], m["collected_at_utc"], m.get("landing_batch_id")) for _, _, m in selected],
    "_file_name string, collected_at_utc string, landing_batch_id string",
)

raw = (
    spark.read
    .schema(SOURCE_SCHEMA)
    .option("mode", "PERMISSIVE")
    .option("rescuedDataColumn", "_rescued_data")
    .json(paths)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_name", F.element_at(F.split(F.col("_source_file"), "/"), -1))
)

bronze_df = (
    raw.join(F.broadcast(manifest_df), on="_file_name", how="left")
    .withColumn("_source_endpoint", F.lit(ENDPOINT))
    .withColumn("_snapshot_date",
                F.to_date(F.regexp_extract(F.col("_source_file"), r"data_coleta=(\d{4}-\d{2}-\d{2})", 1)))
    .withColumn("_collected_at", F.to_timestamp("collected_at_utc"))
    .withColumn("_landing_batch_id", F.col("landing_batch_id"))
    .withColumn("_batch_id", F.lit(BATCH_ID))
    .withColumn("_ingested_at", F.current_timestamp())
    .select(
        *[F.col(f"`{c}`") for c in STATION_FIELDS],
        "_source_file",
        "_source_endpoint",
        "_snapshot_date",
        "_collected_at",
        "_landing_batch_id",
        "_batch_id",
        "_ingested_at",
        "_rescued_data",
    )
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações antes de gravar

# COMMAND ----------

# 1. Contagem por arquivo contra o manifesto.
read_counts = {
    r["file_name"]: r["n"]
    for r in bronze_df.groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("file_name"))
                      .agg(F.count("*").alias("n")).collect()
}
mismatches = []
for _, _, m in selected:
    got = read_counts.get(m["file_name"], 0)
    if got != int(m["record_count"]):
        mismatches.append(f"{m['file_name']}: manifesto={m['record_count']} lido={got}")
if mismatches:
    raise RuntimeError("Contagem diferente do manifesto: " + "; ".join(mismatches))

# 2. Snapshot e horário da coleta preenchidos (garante que o join com o manifesto funcionou).
bad_meta = bronze_df.filter(F.col("_snapshot_date").isNull() | F.col("_collected_at").isNull()).count()
if bad_meta:
    raise RuntimeError(f"{bad_meta} linhas sem _snapshot_date ou _collected_at. Confira o caminho dos arquivos.")

# 3. Chave da estação presente. Se vier nula, o nome do campo na API é outro: ver _rescued_data.
null_code = bronze_df.filter(F.col("codestacao").isNull()).count()
if null_code:
    sample = bronze_df.filter(F.col("codestacao").isNull()).select("_rescued_data").limit(3).collect()
    raise RuntimeError(f"{null_code} linhas sem codestacao. Exemplo de _rescued_data: {[r[0] for r in sample]}")

# 4. Campos fora do schema esperado (não bloqueia, só avisa).
rescued = bronze_df.filter(F.col("_rescued_data").isNotNull())
rescued_rows = rescued.count()
if rescued_rows:
    keys = (rescued
            .select(F.explode(F.expr("json_object_keys(_rescued_data)")).alias("k"))
            .filter(F.col("k") != "_file_path")
            .distinct().collect())
    print(f"Aviso: {rescued_rows} linhas com campos fora do schema em _rescued_data: {sorted(r['k'] for r in keys)}")
    print("Se forem campos úteis, inclua em STATION_FIELDS e recarregue com snapshot_date=all.")

total_read = bronze_df.count()
print(f"Validações OK: {total_read} linhas em {len(dates)} snapshot(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação na Bronze

# COMMAND ----------

try:
    if not spark.catalog.tableExists(BRONZE_TABLE):
        (bronze_df.write.format("delta").mode("overwrite")
         .saveAsTable(BRONZE_TABLE))
        spark.sql(f"""
            COMMENT ON TABLE {BRONZE_TABLE} IS
            'Bronze do cadastro de estações do CEMADEN (PED {ENDPOINT}), município de São Paulo. Colunas originais em string. Um snapshot por coleta (_snapshot_date). Fonte: dados da rede observacional do CEMADEN/MCTI.'
        """)
        print(f"Tabela {BRONZE_TABLE} criada")
    else:
        condition = "_snapshot_date IN (" + ", ".join(f"DATE'{d}'" for d in dates) + ")"
        (bronze_df.write.format("delta").mode("overwrite")
         .option("replaceWhere", condition)
         .saveAsTable(BRONZE_TABLE))
        print(f"Fatia substituída: {condition}")

    # Conferência pós carga: linhas na tabela por snapshot precisam bater com as lidas.
    expected = {r["d"]: r["n"] for r in bronze_df.groupBy(F.date_format("_snapshot_date", "yyyy-MM-dd").alias("d"))
                                                .agg(F.count("*").alias("n")).collect()}
    in_table = {r["d"]: r["n"] for r in spark.table(BRONZE_TABLE)
                .filter(F.date_format("_snapshot_date", "yyyy-MM-dd").isin(dates))
                .groupBy(F.date_format("_snapshot_date", "yyyy-MM-dd").alias("d"))
                .agg(F.count("*").alias("n")).collect()}
    if expected != in_table:
        raise RuntimeError(f"Conferência falhou. Lidas={expected} | Na tabela={in_table}")

    for d in dates:
        files = ", ".join(p for sd, p, _ in selected if sd == d)
        registrar(d, "SUCESSO", STARTED_AT, registros=expected.get(d, 0), arquivo=files,
                  mensagem=f"campos_em_rescued={rescued_rows}")
    print(f"OK: {sum(expected.values())} linhas gravadas em {BRONZE_TABLE}")

except Exception as exc:
    for d in dates:
        registrar(d, "FALHA", STARTED_AT, mensagem=str(exc))
    raise
finally:
    bronze_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primeira olhada no cadastro (insumo para a Silver)

# COMMAND ----------

latest = max(dates)
display(spark.sql(f"""
    SELECT
        tipoestacao_descricao,
        rede_sigla,
        CASE WHEN dh_inicio_inativo IS NULL OR dh_inicio_inativo = '' THEN 'ativa' ELSE 'inativa' END AS situacao,
        COUNT(*) AS estacoes,
        COUNT(DISTINCT codestacao) AS codigos_distintos,
        SUM(CASE WHEN codestacao NOT LIKE '3550308%' THEN 1 ELSE 0 END) AS fora_do_municipio
    FROM {BRONZE_TABLE}
    WHERE _snapshot_date = DATE'{latest}'
    GROUP BY ALL
    ORDER BY estacoes DESC
"""))

# COMMAND ----------

# Estações que aparecem nas leituras de chuva e não aparecem no cadastro (deveria voltar vazio).
if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.cemaden_rainfall_readings"):
    display(spark.sql(f"""
        SELECT r.codestacao, MIN(r.nome) AS nome, COUNT(*) AS leituras
        FROM {CATALOG}.{SCHEMA}.cemaden_rainfall_readings r
        LEFT ANTI JOIN (SELECT DISTINCT codestacao FROM {BRONZE_TABLE} WHERE _snapshot_date = DATE'{latest}') s
          ON r.codestacao = s.codestacao
        GROUP BY r.codestacao
        ORDER BY leituras DESC
    """))
