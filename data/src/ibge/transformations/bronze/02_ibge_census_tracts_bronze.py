# Databricks notebook source
# MAGIC %md
# MAGIC # 02_ibge_census_tracts_bronze
# MAGIC
# MAGIC Carrega na Bronze (`fiap_analytics.bronze.ibge_census_tracts`) os setores censitários do município de São Paulo a partir da malha com atributos do Censo 2022 gravada no volume `fiap_analytics.bronze.ibge_census_tracts`.
# MAGIC
# MAGIC Regras:
# MAGIC
# MAGIC 1. Lê a coleta mais recente do volume (manifesto). Se ela já está na tabela (mesmo SHA256), não faz nada. Com `recarregar = sim`, carrega de novo.
# MAGIC 2. Só os setores do município `cod_ibge` (3550308). O arquivo é do estado inteiro (mais de 100 mil setores e cerca de 180 MB de geometria), e o projeto é da capital. O arquivo completo continua no volume.
# MAGIC 3. Atributos em texto, com o nome em minúsculas; geometria em WKB (`geom_wkb`) no sistema do arquivo (`_source_srs_id`; SIRGAS 2000 geográfico, EPSG 4674).
# MAGIC 4. Uma versão por vez: a tabela é substituída pela versão nova.
# MAGIC 5. Conferência: setores gravados precisam bater com os do município no manifesto. Uma linha no `ibge_ingestion_log`.

# COMMAND ----------

dbutils.widgets.text("cod_ibge", "3550308", "Município (código IBGE)")
dbutils.widgets.dropdown("recarregar", "nao", ["nao", "sim"], "Recarregar a versão já carregada")

# COMMAND ----------

import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG, SCHEMA, VOLUME = "fiap_analytics", "bronze", "ibge_census_tracts"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TABLE = f"{CATALOG}.{SCHEMA}.ibge_census_tracts"
COD_IBGE = dbutils.widgets.get("cod_ibge").strip()
RECARREGAR = dbutils.widgets.get("recarregar") == "sim"
BATCH_ID = str(uuid.uuid4())
STARTED_AT = datetime.now(timezone.utc)

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

LOG_TABLE = "fiap_analytics.bronze.ibge_ingestion_log"
FONTE = "ibge_census_tracts"
ETAPA = "bronze"
registrar = IngestionLog(spark, LOG_TABLE, FONTE, ETAPA, BATCH_ID).registrar

# COMMAND ----------

manifesto = ultimo_manifesto(VOLUME_PATH)
if manifesto is None:
    raise RuntimeError(f"Nenhuma coleta em {VOLUME_PATH}. Rode antes o 01_ibge_census_tracts_landing.")
if manifesto["municipality_code"] != COD_IBGE:
    raise RuntimeError(f"O manifesto conferiu o município {manifesto['municipality_code']}, não {COD_IBGE}. "
                       "Rode a landing com o mesmo cod_ibge.")

carregada = (spark.catalog.tableExists(TABLE)
             and not spark.table(TABLE).filter(F.col("_source_sha256") == manifesto["sha256"]).isEmpty())
print(f"Versão mais recente: {manifesto['snapshot_date']} ({manifesto['sha256'][:12]}) | já carregada: {carregada}")

# COMMAND ----------

if carregada and not RECARREGAR:
    print("Nada novo para carregar.")
else:
    try:
        camada, _, srid, atributos = next(c for c in geo.camadas_gpkg(manifesto["data_path"]) if c[0] == manifesto["layer"])
        colunas = [a.lower() for a in atributos]
        if len(set(colunas)) != len(colunas):
            raise RuntimeError(f"Campos que só diferem em maiúsculas: {atributos}")
        linhas = [tuple(attrs[a] for a in atributos) + (wkb,)
                  for attrs, wkb in geo.ler_gpkg(manifesto["data_path"], camada, where="CD_MUN = ?", parametros=(COD_IBGE,))]
        esperado = manifesto["municipality_count"]
        if len(linhas) != esperado:
            raise RuntimeError(f"Lidos {len(linhas)} setores, o manifesto registrou {esperado}")

        schema = T.StructType([T.StructField(c, T.StringType()) for c in colunas]
                              + [T.StructField("geom_wkb", T.BinaryType())])
        bronze = (
            spark.createDataFrame(linhas, schema)
            .withColumn("_source_file", F.lit(manifesto["data_path"]))
            .withColumn("_source_layer", F.lit(camada))
            .withColumn("_source_srs_id", F.lit(srid).cast("int"))
            .withColumn("_source_sha256", F.lit(manifesto["sha256"]))
            .withColumn("_source_version", F.lit(manifesto["snapshot_date"]))
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
        spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Setores censitários do Censo 2022 do município de São Paulo, da malha "
                  "com atributos do IBGE: atributos em texto (V0001 a V0007: pessoas e domicílios) e geometria em WKB (EPSG 4674).'")

        gravados = spark.table(TABLE).count()
        assert gravados == esperado, f"Lidos {esperado}, gravados {gravados}"
    except Exception as exc:
        registrar(manifesto["snapshot_date"], "FALHA", STARTED_AT, arquivo=manifesto["data_path"],
                  sha256=manifesto["sha256"], mensagem=str(exc))
        raise

    registrar(manifesto["snapshot_date"], "SUCESSO", STARTED_AT, registros=gravados, arquivo=manifesto["data_path"],
              sha256=manifesto["sha256"], mensagem=f"município {COD_IBGE}")
    print(f"OK: {gravados} setores gravados e conferidos em {TABLE}")

display(spark.table(TABLE).groupBy("_source_version", "_source_srs_id", "cd_mun")
        .agg(F.count("*").alias("setores"), F.sum(F.col("v0001").cast("bigint")).alias("pessoas")))
